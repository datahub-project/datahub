import hashlib
import logging
import re
from dataclasses import dataclass, replace
from typing import (
    AbstractSet,
    Any,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
)

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigurationError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    add_entity_to_container,
    add_owner_to_entity_wu,
    add_tags_to_entity_wu,
    gen_containers,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceReport,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIContainerSubTypes,
    DatasetSubTypes,
    SourceCapabilityModifier,
)
from datahub.ingestion.source.sigma.config import (
    PlatformDetail,
    SigmaSourceConfig,
    SigmaSourceReport,
    WorkspaceCounts,
)
from datahub.ingestion.source.sigma.connection_registry import (
    SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
    SigmaConnectionRegistry,
)
from datahub.ingestion.source.sigma.data_classes import (
    CustomSqlEntry,
    DataModelElementUpstream,
    DataModelKey,
    DatasetUpstream,
    Element,
    Page,
    SheetUpstream,
    SigmaDataModel,
    SigmaDataModelColumn,
    SigmaDataModelElement,
    SigmaDataset,
    WarehouseTableUpstream,
    Workbook,
    WorkbookKey,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    candidate_source_column_splits,
    extract_bracket_refs,
)
from datahub.ingestion.source.sigma.sigma_api import (
    BASE_ELEMENT_TYPES,
    INGESTED_ELEMENT_TYPES,
    SigmaAPI,
)
from datahub.ingestion.source.sigma.spec_parser import (
    DataModelSpecIndex,
    SpecColumnRef,
    parse_data_model_spec,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    Status,
    SubTypes,
    TimeStamp,
)
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    DatasetLineageType,
    DatasetProperties,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    Upstream,
    UpstreamLineage,
)
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BrowsePathEntryClass,
    BrowsePathsV2Class,
    ChangeAuditStampsClass,
    ChartInfoClass,
    DashboardInfoClass,
    DataPlatformInstanceClass,
    EdgeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    NullTypeClass,
    OtherSchemaClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TagAssociationClass,
)
from datahub.metadata.urns import SchemaFieldUrn
from datahub.sql_parsing.sql_parsing_aggregator import SqlParsingAggregator
from datahub.sql_parsing.sqlglot_lineage import create_lineage_sql_parsed_result
from datahub.utilities.urns.dataset_urn import DatasetUrn
from datahub.utilities.urns.error import InvalidUrnError

# Logger instance
logger = logging.getLogger(__name__)


# The Sigma ``/dataModels/{id}/columns`` endpoint does not currently
# return a per-column native type on the fields we consume (name,
# label, formula, elementId). We emit ``NullType`` + this sentinel so
# that downstream systems can recognize the absence of type info
# rather than trusting a lie. When the API starts returning a typed
# column field (or we add SQL-based inference), swap this out here.
SIGMA_DM_UNKNOWN_COLUMN_NATIVE_TYPE = "unknown"

# FGL confidence scores for customSQL element lineage.
_FGL_CONFIDENCE_SQL_PARSED: float = (
    0.2  # aggregator-derived; matches SqlParsingAggregator
)
_FGL_CONFIDENCE_FORMULA_DERIVED: float = 0.1  # SELECT * synthesis from formula refs
# A formula ref naming a warehouse table the element declares. The table match
# is exact -- the element's own source_ids carry that inode -- but the warehouse
# COLUMN name is inferred from Sigma's display name (Title Case of snake_case),
# because a column with an opaque columnId carries no native name anywhere in
# the API. Scored below an exact match so consumers can tell the two apart.
_FGL_CONFIDENCE_WAREHOUSE_NAME_DERIVED: float = 0.5
# Same declared-table match, but the column name was read from the columnId
# rather than inferred from the display name, so nothing about the edge is a
# guess -- it ranks with the columnId-driven pass-through path.
_FGL_CONFIDENCE_WAREHOUSE_NAME_EXACT_COLUMN: float = 1.0
# Same as above, except the TABLE was not declared by the element either --
# it was found by name in the tenant-wide /v2/files listing. Both ends of the
# match are inferred, so it scores below the declared-table case.
_FGL_CONFIDENCE_WAREHOUSE_GLOBAL_NAME_DERIVED: float = 0.3
# The other side of a JOIN predicate. The two key columns are stated to hold
# the same value, which makes the unnamed side a genuine upstream -- but it is
# an equality, not a copy, so it scores below a formula-derived edge to let
# consumers that want only value-propagation lineage filter these out.
_FGL_CONFIDENCE_JOIN_KEY: float = 0.7
# Same, under an OUTER join. The equality holds only for rows the join matched;
# on the rest the unmatched side is NULL. Still a real upstream, but a weaker
# claim than an inner join's, so it gets its own tier rather than being dropped.
_FGL_CONFIDENCE_JOIN_KEY_OUTER: float = 0.6
# A union stacks rows, so an output column IS each branch's column rather than
# a value derived from one. /spec states the pairing per branch explicitly, so
# this is as exact as a formula-derived edge.
_FGL_CONFIDENCE_UNION_BRANCH: float = 1.0

# Why one chart formula ref did not resolve to an upstream. Each names the step
# that gave up, so the aggregate bucket can be split by cause rather than
# re-derived by reading thousands of debug lines.
_CHART_REF_MISS_SELF_OR_AMBIGUOUS_CANDIDATES = "self_ref_or_ambiguous_candidates"
_CHART_REF_MISS_AMBIGUOUS_SIBLING = "ambiguous_sibling_element_name"
_CHART_REF_MISS_UPSTREAM_FILTERED = "named_element_filtered_from_emission"
_CHART_REF_MISS_NAMED_BUT_NOT_AN_UPSTREAM = "element_named_but_not_a_lineage_upstream"
_CHART_REF_MISS_AMBIGUOUS_WAREHOUSE = "ambiguous_warehouse_table_name"
_CHART_REF_MISS_UNKNOWN_SOURCE = "source_name_unknown_to_this_workbook"


def _warehouse_column_from_display_name(display_name: str) -> str:
    """Invert Sigma's display-name convention for a warehouse column.

    Sigma renders a warehouse column ``COL_ID`` as ``Col Id``. When the
    column's ``columnId`` is inode-shaped the native name is carried verbatim
    and this is not needed; it is only for columns whose columnId is opaque,
    where the display name is the sole remaining signal.
    """
    return display_name.strip().replace(" ", "_").upper()


def _dm_column_ranks_above(
    candidate: SigmaDataModelColumn, incumbent: SigmaDataModelColumn
) -> bool:
    """Tie-breaking order for duplicate-fieldPath columns within a DM element.

    Prefers the row with a non-empty formula (the user-authored calculated
    field Sigma surfaces in the UI). Tie-break: lexicographically smaller
    columnId so the choice is stable across runs even if /columns ordering
    shifts.
    """
    if bool(candidate.formula) != bool(incumbent.formula):
        return bool(candidate.formula)
    return candidate.columnId < incumbent.columnId


def _normalize_element_name(name: str) -> str:
    """Key for tolerant workbook-element-name lookup.

    Sigma element names routinely carry trailing non-breaking spaces, leading
    spaces, and case differences from what a formula ref spells. Those refs
    resolve to nothing today: the lookup is exact-match, so the element sits in
    the index unreachable. Observed on one tenant (2026-09) as 6,493 near-misses plus 30
    case-only mismatches -- e.g. 'Some Joined Element\xa0'
    and ' Another Element'.
    """
    return name.replace("\xa0", " ").strip().casefold()


def _is_warehouse_column_id(column_id: Optional[str]) -> bool:
    """True when a DM column's ``columnId`` names a warehouse column.

    Sigma encodes warehouse pass-throughs as ``inode-<url_id>/<NATIVE_NAME>``.
    Shared by _try_emit_warehouse_passthrough_fgl (which resolves it) and the
    no-resolvable-ref counters (which bucket on it), so the two cannot drift
    and start mis-bucketing a real /files miss as expected volume.
    """
    return (column_id or "").startswith("inode-")


def _native_column_from_column_id(
    column_id: Optional[str], *, allowed_prefixes: AbstractSet[str]
) -> Optional[str]:
    """The warehouse column name Sigma already put in a ``columnId``.

    Sigma spells a pass-through column ``<prefix>/<NATIVE_NAME>`` -- the prefix
    is ``inode-<urlId>`` when /columns reports the table, and the element's own
    id when it does not. The segment after the slash IS the warehouse column
    name, so it beats re-deriving one from the display name: "Order Ref Id"
    only round-trips to ORDER_REF_ID by convention, and a column whose
    display name was edited breaks that convention silently.

    The prefix must be one the caller RECOGNISES, not merely present. "Has a
    slash" is not evidence of this shape, and a columnId in some other two-part
    form would otherwise mint a fabricated field path at full confidence --
    wrong and trusted at once. Same rule as ``_side_ref`` in the spec parser:
    the shape identifies itself or it is refused.

    Returns None for an opaque or unrecognised columnId, where the display name
    really is the only signal and the edge is scored as inferred.
    """
    prefix, sep, native = (column_id or "").rpartition("/")
    if not sep or not native or prefix not in allowed_prefixes:
        return None
    return native


def _dedup_dm_element_columns(
    columns: List[SigmaDataModelColumn],
) -> Tuple[
    Dict[str, SigmaDataModelColumn],
    List[Tuple[SigmaDataModelColumn, SigmaDataModelColumn]],
]:
    """Return (winners_by_name, displaced_pairs) for an element's column list.

    ``winners_by_name`` maps each unique column name to the winning column
    when duplicates exist (per _dm_column_ranks_above).  ``displaced_pairs``
    is a list of (winner, loser) tuples — each caller uses this to update its
    own counters and logs.

    Called by both _gen_data_model_element_schema_metadata and
    _build_dm_element_fine_grained_lineages so the two sites share a single
    source of truth for which fieldPaths are "live".  Any future change to the
    tie-break logic only needs to be made here.
    """
    by_name: Dict[str, SigmaDataModelColumn] = {}
    displaced: List[Tuple[SigmaDataModelColumn, SigmaDataModelColumn]] = []
    for column in columns:
        if not column.name:
            continue
        existing = by_name.get(column.name)
        if existing is None:
            by_name[column.name] = column
            continue
        winner, loser = (
            (column, existing)
            if _dm_column_ranks_above(column, existing)
            else (existing, column)
        )
        by_name[column.name] = winner
        displaced.append((winner, loser))
    return by_name, displaced


CrossDmOutcome = Literal[
    "strict",
    "ambiguous",
    "single_element_fallback",
    "malformed",
    "self_reference",
    "consumer_name_missing",
    "dm_unknown",
    "name_unmatched_but_dm_known",
]

# Platforms that require a case bridge: Sigma's /files path reports identifiers
# in the catalog's native casing, but the DataHub connector for these platforms
# lower-cases identifiers before URN construction.
#
# Snowflake is the only platform in this set: Sigma reports uppercase
# (e.g. "MYDB/PUBLIC/MYTABLE"), but the Snowflake connector lowercases via
# snowflake_config.convert_urns_to_lowercase (default=True, see
# snowflake_config.py). If that flag is set to False, the Snowflake connector
# emits upper-cased URNs and the edges produced here will dangle; use
# connection_to_platform_map.convert_urns_to_lowercase=False to match.
#
# Other platforms (Postgres, Redshift, etc.) preserve catalog casing in their
# connectors; Sigma's /files path uses the same casing, so no bridge is needed
# and they work correctly by default.
_WAREHOUSE_LOWERCASE_PLATFORMS: frozenset[str] = frozenset({"snowflake"})

# Expected root segment of the /files path for warehouse tables.
_FILES_PATH_ROOT = "Connection Root"

# A join-chain segment can carry Sigma's "and N more joins" label, e.g.
# "DIM_A + 3", which is a display string rather than an element name.
_JOIN_COUNT_SUFFIX = re.compile(r"^(.*?)\s*\+\s*\d+$")


def _normalize_warehouse_identifier(name: str, platform: str, lowercase: bool) -> str:
    """Apply platform-appropriate casing to a warehouse identifier (table or column).

    Mirrors _WarehouseTableRef.fq_name's casing logic so table and column
    identifiers in schemaField URNs use the same convention.
    """
    if platform.lower() in _WAREHOUSE_LOWERCASE_PLATFORMS and lowercase:
        return name.lower()
    return name


@dataclass(frozen=True)
class _WarehouseTableRef:
    """Resolved warehouse table coordinates derived from a /files response."""

    connection_id: str
    db: Optional[str]
    schema: str
    table: str

    def fq_name(self, platform: str, *, lowercase: bool = True) -> str:
        # db is None for platforms with a 2-segment /files path (e.g. Redshift:
        # "Connection Root/<SCHEMA>"). Emit schema.table (never "None.schema.table")
        # so the URN matches what the warehouse connector produces for that platform.
        # A warning is emitted at build-time (see _missing_default_db_warned) so
        # the operator can configure default_database to get a 3-segment URN instead.
        name = (
            f"{self.schema}.{self.table}"
            if self.db is None
            else f"{self.db}.{self.schema}.{self.table}"
        )
        if platform.lower() in _WAREHOUSE_LOWERCASE_PLATFORMS and lowercase:
            return name.lower()
        return name


@dataclass
class _WorkbookWarehouseIndex:
    """Dual lookup index built from /v2/workbooks/{id}/lineage type=table entries.

    by_url_id: urlId -> warehouse Dataset URN (confident 1:1 match).
    by_name:   UPPER(table_name) -> [warehouse Dataset URN, ...] (collision-aware).
    """

    by_url_id: Dict[str, str]
    by_name: Dict[str, List[str]]


@dataclass
class _ResolvedRef:
    """A single formula ref resolved to an upstream dataset field."""

    upstream_urn: str
    upstream_field: str
    ref: BracketRef


@dataclass
class _CustomSqlRegistration:
    """Carries the per-kind variant parameters for _register_customsql_with_aggregator."""

    urn: str
    registered_set: Set[str]
    counter_prefix: str  # "dm_customsql" or "workbook_customsql"
    label: str  # "DM element" or "workbook chart"


@platform_name("Sigma")
@config_class(SigmaSourceConfig)
@support_status(SupportStatus.GA)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
    subtype_modifier=[
        SourceCapabilityModifier.SIGMA_WORKSPACE,
        SourceCapabilityModifier.SIGMA_DATA_MODEL,
    ],
)
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default.")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default.")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.SCHEMA_METADATA, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured using `ingest_owner`",
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class SigmaSource(StatefulIngestionSourceBase, TestableSource):
    """
    This plugin extracts the following:
    - Sigma Workspaces and Workbooks as Container.
    - Sigma Datasets
    - Pages as Dashboard and its Elements as Charts
    - Sigma Data Models as Container, with one Dataset per element inside the Data Model.
    """

    config: SigmaSourceConfig
    reporter: SigmaSourceReport
    connection_registry: SigmaConnectionRegistry
    platform: str = "sigma"

    def __init__(self, config: SigmaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.reporter = SigmaSourceReport()
        self.dataset_upstream_urn_mapping: Dict[str, List[str]] = {}
        # Sigma Dataset url_id -> dataset URN. Used to resolve DM element
        # ``inode-<urlId>`` upstreams.
        self.sigma_dataset_urn_by_url_id: Dict[str, str] = {}
        # DM urlId -> {lowercased element name: [element Dataset URN]}.
        # Bridges workbook ``data-model`` lineage nodes to the specific
        # element Dataset URN. A name may map to multiple URNs when a DM
        # has duplicate-named elements.
        self.dm_element_urn_by_name: Dict[str, Dict[str, List[str]]] = {}
        # bridge_key -> columnId -> [(element Dataset URN, column name)].
        # columnId is a warehouse-column identity Sigma reuses verbatim across
        # every element that passes the column through, so matching a consumer
        # column to a producer column by columnId is exact -- unlike matching by
        # display name, which would guess. Used to recover cross-DM column
        # lineage for pass-through columns, which carry no formula ref naming
        # their producer.
        self.dm_element_columnid_index: Dict[str, Dict[str, List[Tuple[str, str]]]] = {}
        # DM urlId -> DM Container URN. Last-resort fallback.
        self.dm_container_urn_by_url_id: Dict[str, str] = {}
        # DM urlId -> total element count (includes blank-named elements
        # that are absent from ``dm_element_urn_by_name``). Used by the
        # cross-DM single-element fallback.
        self.dm_total_element_count_by_url_id: Dict[str, int] = {}
        # dataModelIds whose bridge key collided with an earlier DM. The
        # emit loop skips these to avoid unlinked orphan Containers.
        self.dm_collided_data_model_ids: Set[str] = set()
        # Per-platform SqlParsingAggregator instances for customSQL DM elements,
        # keyed by (platform, env, platform_instance).
        self._sql_aggregators: Dict[
            Tuple[str, str, Optional[str]], SqlParsingAggregator
        ] = {}
        # element_dataset_urn -> {sql_col_lower -> sigma_col_name}.
        # Built from ALL formula refs for FGL rewriting of named-column SELECTs.
        self._customsql_col_mappings: Dict[str, Dict[str, str]] = {}
        # Same key, but only single-ref (passthrough) formulas.
        # Used for SELECT * synthesis to avoid fabricating upstream edges for
        # computed expressions that reference multiple SQL columns.
        self._customsql_passthrough_mappings: Dict[str, Dict[str, str]] = {}
        # Element URNs registered with an aggregator via add_view_definition.
        # Used to guard against multiple customSQL source_ids on one element
        # and to scope dm_customsql_upstream_emitted to known registrations.
        self._customsql_registered_urns: Set[str] = set()
        # element_urn -> non-customSQL Upstream / FGL objects stashed from the
        # per-element emit path.  Merged into the aggregator's UpstreamLineage
        # MCP at drain so the final aspect is consolidated.
        self._customsql_extra_upstreams: Dict[str, List[Upstream]] = {}
        self._customsql_extra_fgls: Dict[str, List[FineGrainedLineageClass]] = {}
        # Chart URNs registered with the aggregator via the workbook customSQL path.
        # Separate from _customsql_registered_urns (DM Dataset URNs) to prevent
        # counter bleed between the two namespaces.
        self._workbook_customsql_registered_urns: Set[str] = set()
        # chart_urn → formula-derived InputField list stashed at emit time.
        # Merged at drain time so warehouse-resolved fields supplement
        # (not replace) formula-derived column entries.
        self._workbook_customsql_formula_fields: Dict[str, List[InputFieldClass]] = {}
        # DM urlId → DM dataModelId (UUID). Reverse of get_url_id(); used to
        # correlate ``data-model`` lineage entries (keyed by dataModelId) with
        # source_id prefixes (keyed by urlId) in cross-DM upstream resolution.
        self.data_model_id_by_url_id: Dict[str, str] = {}
        # Global: element Dataset URN → {lowercased column name: canonical column name}.
        # Same dedup logic as the per-element urn_to_cols in the FGL builder so
        # cross-DM column validation uses the winner set rather than raw columns.
        self.dm_element_urn_to_cols: Dict[
            str, Dict[str, str]
        ] = {}  # {lowercase_col: canonical_col}
        # Global: element Dataset URN → the bridge key of its Data Model.
        # Lets a chart-side join-chain ref walk from a resolved DM element back
        # to its siblings via ``dm_element_urn_by_name``. The URN itself
        # encodes the DM, but only as an opaque name string, so parsing it
        # would couple this lookup to the URN format.
        self.dm_key_by_element_urn: Dict[str, str] = {}
        # (Data Model key, elementId) -> element Dataset URN, registered under
        # BOTH the bridge key (urlId slug) and the dataModelId, because a
        # /spec join side names the model by dataModelId while ``source_ids``
        # name it by slug. Element ids are NOT unique across models -- one
        # tenant has the same id in two -- so an elementId alone cannot resolve.
        self.dm_element_urn_by_key_and_eid: Dict[Tuple[str, str], str] = {}
        # elementId -> every Data Model key that defines it. Lets an
        # unqualified side be resolved when exactly one model owns the id, and
        # refused when several do.
        self.dm_keys_by_element_id: Dict[str, Set[str]] = {}
        # Surface as a structured report warning so operators running
        # under ``--strict`` or CI dashboards that gate on report
        # warnings (rather than stdout logs) notice the misconfiguration.
        #
        # We compare the ``allow`` / ``deny`` lists directly rather than
        # against ``AllowDenyPattern.allow_all()`` because
        # ``AllowDenyPattern.__eq__`` is ``__dict__``-based and has two
        # ``@cached_property`` compiled-regex attributes; once either
        # side's ``.allowed()`` has been invoked its ``__dict__`` gains
        # a cache entry the other side doesn't have, flipping equality
        # to False. Today nothing calls ``.allowed()`` before this
        # check, but any future reorder in ``__init__`` would silently
        # emit a spurious warning -- the direct list check is
        # cache-state-independent.
        # ``ignoreCase`` is intentionally excluded from this check: a user
        # who pins ``ignoreCase: False`` on an otherwise-default pattern is
        # semantically still the "match everything" pattern (``.*`` matches
        # regardless of case). Including it would fire a spurious "pattern
        # ignored" warning on a benign config tweak.
        dm_pattern = self.config.data_model_pattern
        _dm_pattern_is_default = dm_pattern.allow == [".*"] and not dm_pattern.deny
        if not self.config.ingest_data_models and not _dm_pattern_is_default:
            self.reporter.warning(
                title="data_model_pattern ignored",
                message="data_model_pattern is set but ingest_data_models is "
                "False -- the pattern has no effect. Enable ingest_data_models "
                "or remove data_model_pattern to silence this warning.",
            )
        # Instance-level cache for /files/{inodeId} responses.
        # Keyed by inodeId (UUID); value is the raw JSON dict or None on failure.
        # Shared across all DMs so inodes that appear in multiple DMs hit the
        # network only once.  The cache is unbounded — each entry is a small
        # JSON dict and the number of unique warehouse-table inodes per tenant
        # is expected to be in the hundreds, not millions.  If this assumption
        # proves wrong, an LRU cap can be added without changing the interface.
        self._files_cache: Dict[str, Optional[Dict[str, Any]]] = {}
        # Global url_id -> warehouse table, built lazily from /v2/files the
        # first time a Data Model's own /lineage turns out to omit a table its
        # elements reference. None until built; {} means built-and-empty.
        # Single-entry memos for maps derived from per-workbook indexes.
        #
        # Each holds the SOURCE objects alongside the derived map and the
        # reader compares them with `is`. Keying on id() alone was wrong:
        # CPython reuses an address once an object is freed, and these indexes
        # are built and dropped one per workbook, so the next workbook's index
        # could land on the previous one's address and be served its data --
        # which for _chart_cols_memo is a wrong column list used to VALIDATE a
        # join-chain split, i.e. a wrong split accepted rather than a miss.
        self._normalized_index_memo: Optional[
            Tuple[Dict[str, List[Element]], Dict[str, List[Element]]]
        ] = None
        # Built lazily on the first chart-ref miss, by which point every Data
        # Model has been walked. Used only to classify misses, never to resolve.
        self._known_dm_element_index: Optional[Dict[str, List[str]]] = None
        self._chart_cols_memo: Optional[
            Tuple[
                Dict[str, List[Element]],
                Dict[str, str],
                Dict[str, Dict[str, str]],
            ]
        ] = None
        # urlId -> /files entry, or None when Sigma 404s (a stale reference to a
        # deleted table). One call per distinct url_id.
        self._warehouse_file_by_url_id: Dict[str, Optional[Dict[str, Any]]] = {}
        self._stale_warehouse_refs_seen: Set[str] = set()
        # One /spec parse per Data Model, keyed by dataModelId.
        self._dm_spec_index_cache: Dict[str, DataModelSpecIndex] = {}
        # Join partners, built once per Data Model rather than per element.
        self._join_partner_cache: Dict[
            str, Dict[Tuple[str, str], Set[Tuple[str, str, str, bool]]]
        ] = {}
        # Intra-DM element ancestry, keyed by dataModelId.
        self._dm_ancestors_cache: Dict[str, Dict[str, Set[str]]] = {}
        # Built once per run, lazily, by _ensure_global_warehouse_index.
        self._global_warehouse_index_built: bool = False
        self._global_warehouse_file_entries: Dict[str, Dict[str, Any]] = {}
        # Same entries keyed by casefolded table NAME. A name is not unique --
        # the same table name recurs across schemas and databases -- so the
        # value is a list and every consumer must resolve the ambiguity or
        # refuse. See _lookup_global_warehouse_table_by_name.
        self._global_warehouse_files_by_name: Dict[str, List[Dict[str, Any]]] = {}
        # Inodes whose /files path already produced an unparseable warning;
        # prevents N identical warnings when the same inode spans N DMs (H3).
        self._files_path_unparseable_seen: Set[str] = set()
        # Rebuilt per-workbook by _build_workbook_warehouse_table_index.
        # Maps BFS table urlId -> connectionId for per-connection casing in the
        # column-name bridge (_gen_elements_workunit).
        self._wb_url_id_to_conn_id: Dict[str, str] = {}
        # Connections for which a "default_database not configured" warning has
        # been emitted; dedup so multi-DM tenants don't flood the report.
        self._missing_default_db_warned: Set[str] = set()
        # Table names for which an "ambiguous warehouse table name" warning has
        # been emitted; dedup so many charts with the same ambiguous name don't
        # flood the report.
        self._ambiguous_table_name_warned: Set[str] = set()
        # (upstream_urn, display_name) pairs for which a "column bridge unresolved"
        # warning has been emitted; dedup so repeated charts with the same unresolved
        # column don't flood the report.
        self._bridge_unresolved_warned: Set[Tuple[str, str]] = set()
        # Upstream DM elements whose empty schema has already been reported; one
        # failed /columns fetch empties every element in a DM, so without this
        # every ref into that DM would emit its own warning.
        self._upstream_schema_unavailable_warned: Set[str] = set()
        # Once-per-run gate flags so noisy global conditions don't flood logs.
        self._registry_empty_warned: bool = False
        # Per-platform set: platforms for which we've emitted a "first emission"
        # info message noting that casing is unverified.
        self._warned_unvalidated_platforms: Set[str] = set()
        # Per-connection set: connectionIds emitted without a
        # connection_to_platform_map entry; listed in the info message context.
        self._no_platform_map_conn_ids: Set[str] = set()
        try:
            self.sigma_api = SigmaAPI(self.config, self.reporter)
        except Exception as e:
            raise ConfigurationError("Unable to connect sigma API") from e

        self.connection_registry = self._build_connection_registry()
        # Warn on connection_to_platform_map keys that don't exist in the
        # registry — a typo in the recipe UUID would silently make the override
        # inactive and the operator would never know.
        unknown_override_keys = [
            k
            for k in self.config.connection_to_platform_map
            if k not in self.connection_registry.by_id
        ]
        if unknown_override_keys:
            self.reporter.warning(
                title="connection_to_platform_map references unknown connectionIds",
                message=(
                    "One or more keys in connection_to_platform_map do not match "
                    "any Sigma connection returned by /v2/connections. The overrides "
                    "for these keys will be silently ignored."
                ),
                context=f"unknown_keys={unknown_override_keys}",
            )

    def _build_connection_registry(self) -> SigmaConnectionRegistry:
        """Fetch /v2/connections and build the in-memory registry.

        Transport errors are handled inside _paginated_raw_entries (returns
        partial results, emits a report warning); the try/except here only
        covers bugs in build() itself.
        """
        try:
            return SigmaConnectionRegistry.build(
                self.sigma_api.get_connections(),
                reporter=self.reporter,
                type_to_platform_map=SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP,
            )
        except Exception as e:
            logger.exception(
                "Failed to build Sigma Connection registry; continuing with empty registry."
            )
            self.reporter.warning(
                title="Sigma Connection registry build failed",
                message="Connection registry is empty; warehouse-URN resolution "
                "for downstream lineage will be unavailable.",
                exc=e,
            )
            return SigmaConnectionRegistry()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        test_report = TestConnectionReport()
        try:
            SigmaAPI(
                SigmaSourceConfig.parse_obj_allow_extras(config_dict),
                SigmaSourceReport(),
            )
            test_report.basic_connectivity = CapabilityReport(capable=True)
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=str(e)
            )
        return test_report

    @classmethod
    def create(cls, config_dict, ctx):
        config = SigmaSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def _gen_workbook_key(self, workbook_id: str) -> WorkbookKey:
        return WorkbookKey(
            workbookId=workbook_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_workspace_key(self, workspace_id: str) -> WorkspaceKey:
        return WorkspaceKey(
            workspaceId=workspace_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _get_allowed_workspaces(self) -> List[Workspace]:
        all_workspaces = self.sigma_api.workspaces.values()
        logger.info(f"Number of workspaces = {len(all_workspaces)}")

        allowed_workspaces = []
        for workspace in all_workspaces:
            if self.config.workspace_pattern.allowed(workspace.name):
                allowed_workspaces.append(workspace)
            else:
                self.reporter.workspaces.dropped(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        logger.info(f"Number of allowed workspaces = {len(allowed_workspaces)}")

        return allowed_workspaces

    def _gen_workspace_workunit(
        self, workspace: Workspace
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workspace to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workspace.createdBy)
        yield from gen_containers(
            container_key=self._gen_workspace_key(workspace.workspaceId),
            name=workspace.name,
            sub_types=[BIContainerSubTypes.SIGMA_WORKSPACE],
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            created=int(workspace.createdAt.timestamp() * 1000),
            last_modified=int(workspace.updatedAt.timestamp() * 1000),
        )

    def _gen_sigma_dataset_urn(self, dataset_identifier: str) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=dataset_identifier,
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _gen_entity_status_aspect(self, entity_urn: str) -> MetadataWorkUnit:
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn, aspect=Status(removed=False)
        ).as_workunit()

    def _gen_dataset_properties(
        self, dataset_urn: str, dataset: SigmaDataset
    ) -> MetadataWorkUnit:
        dataset_properties = DatasetProperties(
            name=dataset.name,
            description=dataset.description,
            qualifiedName=dataset.name,
            externalUrl=dataset.url,
            created=TimeStamp(time=int(dataset.createdAt.timestamp() * 1000)),
            lastModified=TimeStamp(time=int(dataset.updatedAt.timestamp() * 1000)),
            customProperties={"datasetId": dataset.datasetId},
            tags=[dataset.badge] if dataset.badge else None,
        )
        if dataset.path:
            dataset_properties.customProperties["path"] = dataset.path
        return MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=dataset_properties
        ).as_workunit()

    def _gen_dataplatform_instance_aspect(
        self, entity_urn: str
    ) -> Optional[MetadataWorkUnit]:
        if self.config.platform_instance:
            aspect = DataPlatformInstanceClass(
                platform=builder.make_data_platform_urn(self.platform),
                instance=builder.make_dataplatform_instance_urn(
                    self.platform, self.config.platform_instance
                ),
            )
            return MetadataChangeProposalWrapper(
                entityUrn=entity_urn, aspect=aspect
            ).as_workunit()
        else:
            return None

    def _gen_entity_owner_aspect(
        self, entity_urn: str, user_name: str
    ) -> MetadataWorkUnit:
        aspect = OwnershipClass(
            owners=[
                OwnerClass(
                    owner=builder.make_user_urn(user_name),
                    type=OwnershipTypeClass.DATAOWNER,
                )
            ]
        )
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect,
        ).as_workunit()

    def _gen_entity_browsepath_aspect(
        self,
        entity_urn: str,
        parent_entity_urn: str,
        paths: List[str],
    ) -> MetadataWorkUnit:
        entries = [
            BrowsePathEntryClass(id=parent_entity_urn, urn=parent_entity_urn)
        ] + [BrowsePathEntryClass(id=path) for path in paths]
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=BrowsePathsV2Class(entries),
        ).as_workunit()

    def _gen_dataset_workunit(
        self, dataset: SigmaDataset
    ) -> Iterable[MetadataWorkUnit]:
        dataset_urn = self._gen_sigma_dataset_urn(dataset.get_urn_part())

        yield self._gen_entity_status_aspect(dataset_urn)

        yield self._gen_dataset_properties(dataset_urn, dataset)

        if dataset.workspaceId:
            self.reporter.workspaces.increment_datasets_count(dataset.workspaceId)
            yield from add_entity_to_container(
                container_key=self._gen_workspace_key(dataset.workspaceId),
                entity_type="dataset",
                entity_urn=dataset_urn,
            )

        dpi_aspect = self._gen_dataplatform_instance_aspect(dataset_urn)
        if dpi_aspect:
            yield dpi_aspect

        owner_username = self.sigma_api.get_user_name(dataset.createdBy)
        if self.config.ingest_owner and owner_username:
            yield self._gen_entity_owner_aspect(dataset_urn, owner_username)

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn,
            aspect=SubTypes(typeNames=[DatasetSubTypes.SIGMA_DATASET]),
        ).as_workunit()

        if dataset.path and dataset.workspaceId:
            paths = dataset.path.split("/")[1:]
            if len(paths) > 0:
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dataset_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(dataset.workspaceId)
                    ),
                    paths=paths,
                )

        if dataset.badge:
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=GlobalTagsClass(
                    tags=[TagAssociationClass(builder.make_tag_urn(dataset.badge))]
                ),
            ).as_workunit()

    def _gen_data_model_key(self, data_model_id: str) -> DataModelKey:
        return DataModelKey(
            dataModelId=data_model_id,
            platform=self.platform,
            instance=self.config.platform_instance,
        )

    def _gen_data_model_element_urn(
        self, data_model: SigmaDataModel, element: SigmaDataModelElement
    ) -> str:
        return builder.make_dataset_urn_with_platform_instance(
            name=data_model.get_element_urn_part(element),
            env=self.config.env,
            platform=self.platform,
            platform_instance=self.config.platform_instance,
        )

    def _resolve_dm_element_external_upstream(self, source_id: str) -> Optional[str]:
        """Resolve an ``inode-<suffix>`` source_id to a Sigma Dataset URN.

        Returns the URN if the suffix matches a Sigma Dataset ingested in
        this run; otherwise ``None``. We do not fabricate URNs for targets
        (un-ingested Sigma Datasets, warehouse tables) we didn't emit.
        """
        if not source_id.startswith("inode-"):
            return None
        suffix = source_id[len("inode-") :]
        return self.sigma_dataset_urn_by_url_id.get(suffix)

    def _get_file_metadata_cached(self, inode_id: str) -> Optional[Dict[str, Any]]:
        """Fetch /files/{inodeId} with instance-level caching.

        Stores None on failure so repeated calls for a broken inode don't
        retry the network.
        """
        if inode_id not in self._files_cache:
            self._files_cache[inode_id] = self.sigma_api.get_file_metadata(inode_id)
        return self._files_cache[inode_id]

    def _warehouse_ref_from_file_entry(
        self, entry: Dict[str, Any], connection_id: str
    ) -> Optional[_WarehouseTableRef]:
        """Build a table ref from a /files entry, or None if its path is unusable.

        Shares the path contract of :meth:`_build_dm_warehouse_url_id_map`:
        ``Connection Root/<SCHEMA>`` or ``Connection Root/<DB>/<SCHEMA>``.
        """
        path = str(entry.get("path") or "")
        table_name = str(entry.get("name") or "")
        parts = path.split("/")
        if not (table_name and 2 <= len(parts) <= 3 and all(parts)):
            logger.debug(
                "GLOBAL WAREHOUSE INDEX reject: url_id=%r path=%r name=%r -- "
                "expected 2-3 non-empty segments and a table name",
                entry.get("urlId"),
                path,
                table_name,
            )
            return None
        if parts[0] != _FILES_PATH_ROOT:
            logger.debug(
                "GLOBAL WAREHOUSE INDEX reject: url_id=%r path root %r is not "
                "%r; the /files path shape is unrecognised",
                entry.get("urlId"),
                parts[0],
                _FILES_PATH_ROOT,
            )
            return None
        db = parts[1] if len(parts) == 3 else None
        schema = parts[-1]
        return _WarehouseTableRef(
            connection_id=connection_id, db=db, schema=schema, table=table_name
        )

    def _infer_connection_id(
        self, warehouse_map: Dict[str, _WarehouseTableRef]
    ) -> Optional[str]:
        """Connection to attribute a table /files does not tell us the connection for.

        A /files entry carries no connectionId, so it is taken from the Data
        Model's own already-resolved tables when they agree, else from the
        tenant's sole mappable connection. Ambiguity is refused rather than
        guessed: attributing a table to the wrong connection would emit a URN
        pointing at the wrong platform or instance.
        """
        conns = {ref.connection_id for ref in warehouse_map.values()}
        if len(conns) == 1:
            return next(iter(conns))
        if conns:
            return None
        mappable = [
            cid
            for cid, rec in self.connection_registry.by_id.items()
            if rec.is_mappable
        ]
        return mappable[0] if len(mappable) == 1 else None

    def _ensure_global_warehouse_index(self, trigger: str) -> None:
        """List /v2/files once per run and index it by url_id and by name.

        Built lazily and only on first need, so tenants whose Data Model
        /lineage is complete never pay for the listing.
        """
        if self._global_warehouse_index_built:
            return
        self._global_warehouse_index_built = True
        logger.debug(
            "GLOBAL WAREHOUSE INDEX: first miss (%s) -- listing "
            "/v2/files?typeFilters=table. Built once per run and only on "
            "demand, so a tenant whose lineage is complete never pays for "
            "this.",
            trigger,
        )
        entries = self.sigma_api.list_warehouse_table_files()
        index: Dict[str, Dict[str, Any]] = {}
        by_name: Dict[str, List[Dict[str, Any]]] = {}
        for raw in entries:
            key = str(raw.get("urlId") or "")
            if key:
                index[key] = raw
            name = str(raw.get("name") or "").strip().casefold()
            if name:
                by_name.setdefault(name, []).append(raw)
        self._global_warehouse_file_entries = index
        self._global_warehouse_files_by_name = by_name

        self.reporter.warehouse_files_listed = len(entries)
        # 10,000 exactly is the number to watch: /v2/files reported
        # 'total: 10000' on a live tenant (2026-09), suspiciously round, so the
        # listing may be server-capped. A capped listing is silently
        # incomplete and would need name-filtered fetching instead.
        truncated = len(entries) in (10000, 100000)
        colliding = sum(1 for rows in by_name.values() if len(rows) > 1)
        logger.debug(
            "GLOBAL WAREHOUSE INDEX built from /v2/files: %d table entries, "
            "%d distinct url_ids, %d entries lacking a urlId, %d distinct "
            "table names of which %d are shared by 2+ tables (those can only "
            "be resolved by name when the Data Model's own tables disambiguate "
            "them)%s",
            len(entries),
            len(index),
            len(entries) - len(index),
            len(by_name),
            colliding,
            "  *** SUSPECT SERVER-SIDE CAP: listing may be truncated ***"
            if truncated
            else "",
        )
        if truncated:
            self.reporter.warning(
                title="Sigma /v2/files listing may be truncated",
                message=(
                    "The warehouse table listing returned exactly a round "
                    "number of entries, which suggests a server-side cap "
                    "rather than the true total. Data Model elements whose "
                    "table falls outside the listing will still resolve to "
                    "no warehouse lineage."
                ),
                context=f"entries={len(entries)}",
            )

    def _lookup_global_warehouse_table_by_name(
        self,
        *,
        table_name: str,
        warehouse_map: Dict[str, _WarehouseTableRef],
    ) -> Optional[_WarehouseTableRef]:
        """Resolve a warehouse table the element names but does not declare.

        A formula can reference a warehouse table by name that the element's
        own ``source_ids`` never mention -- the same Sigma under-reporting that
        motivates the url_id path, one level further out. The only remaining
        signal is the name, which is NOT unique: the same table name recurs
        across schemas and databases, and picking the wrong one emits an edge
        to a real but unrelated dataset.

        So the match must be unambiguous. A single global entry is accepted
        outright; several are narrowed to those sharing a (db, schema) with a
        table this Data Model already resolved, and accepted only if exactly
        one survives. Anything still ambiguous is refused, not guessed.
        """
        self._ensure_global_warehouse_index(f"table name {table_name!r}")
        rows = self._global_warehouse_files_by_name.get(
            table_name.strip().casefold(), []
        )
        if not rows:
            self.reporter.dm_element_warehouse_name_index_miss += 1
            logger.debug(
                "GLOBAL WAREHOUSE NAME miss: no /v2/files table is named %r "
                "(index holds %d distinct names)",
                table_name,
                len(self._global_warehouse_files_by_name),
            )
            return None
        connection_id = self._infer_connection_id(warehouse_map)
        if connection_id is None:
            self.reporter.dm_element_warehouse_connection_ambiguous += 1
            logger.debug(
                "GLOBAL WAREHOUSE NAME: %r matched %d entries but the "
                "connection to attribute them to could not be inferred; "
                "refusing rather than guessing the platform",
                table_name,
                len(rows),
            )
            return None
        candidates = [
            ref
            for ref in (
                self._warehouse_ref_from_file_entry(row, connection_id) for row in rows
            )
            if ref is not None
        ]
        if not candidates:
            self.reporter.dm_element_warehouse_path_unparseable += 1
            return None
        if len(candidates) > 1:
            # Narrow by the scopes this Data Model already demonstrably reads
            # from. A table in a schema the DM never touches is far more likely
            # a same-named table elsewhere in the warehouse than the referent.
            known_scopes = {(r.db, r.schema) for r in warehouse_map.values()}
            narrowed = [c for c in candidates if (c.db, c.schema) in known_scopes]
            logger.debug(
                "GLOBAL WAREHOUSE NAME ambiguous: %r matched %d tables %r; "
                "narrowing to the Data Model's own scopes %r left %d",
                table_name,
                len(candidates),
                [(c.db, c.schema) for c in candidates],
                sorted(known_scopes),
                len(narrowed),
            )
            candidates = narrowed
        if len(candidates) != 1:
            self.reporter.dm_element_warehouse_name_index_ambiguous += 1
            logger.debug(
                "GLOBAL WAREHOUSE NAME unresolved: %r left %d candidates after "
                "narrowing; no edge emitted",
                table_name,
                len(candidates),
            )
            return None
        ref = candidates[0]
        self.reporter.dm_element_warehouse_name_index_resolved += 1
        logger.debug(
            "GLOBAL WAREHOUSE NAME hit: %r -> db=%r schema=%r table=%r via "
            "connection %r (inferred); this table is named by a formula but "
            "declared by neither the element nor the Data Model's /lineage",
            table_name,
            ref.db,
            ref.schema,
            ref.table,
            connection_id,
        )
        return ref

    def _lookup_global_warehouse_table(
        self, url_id: str, connection_id: str
    ) -> Optional[_WarehouseTableRef]:
        """Resolve a url_id the owning Data Model's /lineage never described.

        Asks ``/v2/files/{urlId}`` directly, one call per distinct miss.

        An earlier version listed every table on the tenant and looked the
        url_id up in that index. That recovered NOTHING across a full customer
        run, while this direct call recovered 52 -- the tenant's
        ``/files?typeFilters=table`` listing does not include every table a
        ``GET /files/{urlId}`` can resolve, so the listing was both more
        expensive (~41 paged calls) and less complete.

        A 404 means only that this token cannot resolve the url_id -- the file
        may be deleted, or simply outside what the credential can see. We cannot
        tell which, so it is counted as an unresolved reference and nothing is
        inferred about why.
        """
        if url_id not in self._warehouse_file_by_url_id:
            self._warehouse_file_by_url_id[url_id] = (
                self.sigma_api.get_file_metadata_by_url_id(url_id)
            )
        entry = self._warehouse_file_by_url_id[url_id]
        if entry is None:
            self.reporter.dm_element_warehouse_url_id_unresolvable += 1
            if url_id not in self._stale_warehouse_refs_seen:
                self._stale_warehouse_refs_seen.add(url_id)
                logger.debug(
                    "WAREHOUSE UNRESOLVED REF: url_id %r is not resolvable by "
                    "this token (/v2/files/{urlId} returned 404), so columns "
                    "naming it get no warehouse column lineage. The file may be "
                    "deleted or merely outside the credential's visibility -- "
                    "the API does not distinguish them, so check the token's "
                    "access before concluding the reference is stale.",
                    url_id,
                )
            return None
        ref = self._warehouse_ref_from_file_entry(entry, connection_id)
        if ref is None:
            self.reporter.dm_element_warehouse_path_unparseable += 1
            return None
        self.reporter.dm_element_warehouse_recovered_by_url_id_lookup += 1
        logger.debug(
            "WAREHOUSE DIRECT LOOKUP hit: url_id %r -> db=%r schema=%r table=%r "
            "via connection %r (inferred, since a /files entry carries none); "
            "this table was absent from the owning Data Model's /lineage",
            url_id,
            ref.db,
            ref.schema,
            ref.table,
            connection_id,
        )
        return ref

    def _build_dm_warehouse_url_id_map(
        self, data_model: SigmaDataModel
    ) -> Dict[str, _WarehouseTableRef]:
        """For each type=table lineage inode on this DM, call /files/{inodeId}
        (cached) to get the ``urlId`` and ``path``.  Returns a map of
        urlId -> _WarehouseTableRef so _gen_data_model_element_upstream_lineage
        can look up by the suffix that element ``sourceIds`` use.

        Path shape assumption: ``Connection Root/<DB>/<SCHEMA>`` (3 segments).
        This is empirically confirmed for Snowflake.  For other platforms the
        shape is unverified — MySQL (DB == schema, possibly 2 segments),
        BigQuery (project/dataset — also 2 without a DB layer), and
        platforms with deeper catalog hierarchies may produce a different
        segment count and land in dm_element_warehouse_path_unparseable.
        TODO: validate /files path shapes for non-Snowflake platforms and
        adjust the segment parser accordingly.

        Counters bumped here:
          - dm_element_warehouse_table_lookup_failed
          - dm_element_warehouse_path_unparseable
        """
        result: Dict[str, _WarehouseTableRef] = {}
        # This map is the sole bridge between a column's inode-shaped columnId
        # and a warehouse Dataset URN. It is built from the DM's /lineage
        # type=table rows only, so a DM whose /lineage reports no table rows
        # yields an EMPTY map and every warehouse passthrough in it silently
        # fails with url_id_not_in_warehouse_map -- previously with no way to
        # see that the map was empty, or which url_ids it did contain.
        logger.debug(
            "WAREHOUSE MAP DM %s: building from %d type=table lineage inode(s): %r",
            data_model.dataModelId,
            len(data_model.warehouse_inodes_by_inode_id),
            sorted(data_model.warehouse_inodes_by_inode_id)[:20],
        )
        for inode_id, raw in data_model.warehouse_inodes_by_inode_id.items():
            conn_id = raw["connectionId"]
            first_attempt = inode_id not in self._files_cache
            files_data = self._get_file_metadata_cached(inode_id)
            if files_data is None:
                # Only count on first failure; cache hits of a prior None
                # (same inode referenced from N DMs) should not inflate.
                if first_attempt:
                    self.reporter.dm_element_warehouse_table_lookup_failed += 1
                logger.debug(
                    "DM %s: /files lookup failed for inode %r; skipping.",
                    data_model.dataModelId,
                    inode_id,
                )
                continue
            url_id = str(files_data.get("urlId") or "")
            path = str(files_data.get("path") or "")
            # Use /files["name"] as the canonical table name — it is the
            # authoritative source and avoids trusting the lineage entry's
            # name field, which may be a display label or absent.
            table_name = str(files_data.get("name") or "")
            parts = path.split("/")
            # Accept 2–3 total segments after splitting on "/" (1–2 non-root parts,
            # all non-empty) plus a non-empty urlId:
            #   "Connection Root/<SCHEMA>"      — Redshift, MySQL (no DB layer)
            #   "Connection Root/<DB>/<SCHEMA>" — Snowflake, Postgres
            # Fewer total segments (e.g. "Acryl Workspace" = 1), empty segments,
            # or 4+ total segments (not yet mapped) are all rejected.
            path_invalid = not (
                url_id and table_name and 2 <= len(parts) <= 3 and all(parts)
            )
            root_unexpected = (not path_invalid) and parts[0] != _FILES_PATH_ROOT
            if path_invalid or root_unexpected:
                # Dedup per inode: a single misconfigured inode shared across N
                # DMs must not flood the report with N identical warnings (H3).
                if inode_id not in self._files_path_unparseable_seen:
                    self._files_path_unparseable_seen.add(inode_id)
                    self.reporter.dm_element_warehouse_path_unparseable += 1
                    self.reporter.warning(
                        title=(
                            "Sigma warehouse path has unexpected root segment"
                            if root_unexpected
                            else "Sigma warehouse /files path unparseable"
                        ),
                        message=(
                            "Expected 'Connection Root/<SCHEMA>' or "
                            "'Connection Root/<DB>/<SCHEMA>' with no empty "
                            "segments and a non-empty urlId. "
                            "Warehouse upstream skipped for this inode."
                        ),
                        context=(
                            f"inode={inode_id}, path={path!r}, "
                            f"url_id={url_id!r}, table_name={table_name!r}"
                        ),
                    )
                continue
            logger.debug(
                "WAREHOUSE MAP DM %s: inode %s -> url_id=%r path=%r table=%r",
                data_model.dataModelId,
                inode_id,
                url_id,
                path,
                table_name,
            )
            if url_id in result:
                logger.warning(
                    "DM %s: two inodes share the same urlId %r; "
                    "the earlier entry will be overwritten. "
                    "This is unexpected — please report to DataHub.",
                    data_model.dataModelId,
                    url_id,
                )
            # 3-segment path → DB + SCHEMA (e.g. Snowflake "Connection Root/<DB>/<SCHEMA>")
            # 2-segment path → SCHEMA only; fall back to connection's default_database
            # (e.g. Redshift "Connection Root/<SCHEMA>" where the DB lives in the connection)
            db: Optional[str]
            if len(parts) == 3:
                db, schema = parts[1], parts[2]
            else:
                # No DB in path — resolve from connection_to_platform_map override
                # first, then fall back to the connection registry's default_database.
                conn_override = self.config.connection_to_platform_map.get(conn_id)
                conn_record = self.connection_registry.get(conn_id)
                db = (conn_override.default_database if conn_override else None) or (
                    conn_record.default_database if conn_record else None
                )
                schema = parts[1]
                if db is None and conn_id not in self._missing_default_db_warned:
                    self._missing_default_db_warned.add(conn_id)
                    self.reporter.warning(
                        title="Sigma warehouse default_database not configured",
                        message=(
                            "The /files path for this connection has no database layer "
                            "(e.g. 'Connection Root/<SCHEMA>'). The emitted warehouse "
                            "URN will use schema.table only, which will not match a "
                            "connector that uses db.schema.table. Set "
                            "connection_to_platform_map.<connectionId>.default_database "
                            "in the recipe to fix the URN."
                        ),
                        context=f"connectionId={conn_id}, path={path!r}",
                    )
            result[url_id] = _WarehouseTableRef(
                connection_id=conn_id,
                db=db,
                schema=schema,
                table=table_name,
            )
        logger.debug(
            "WAREHOUSE MAP DM %s: final map has %d url_id(s): %r",
            data_model.dataModelId,
            len(result),
            sorted(result),
        )
        return result

    def _resolve_dm_element_warehouse_upstream(
        self,
        *,
        url_id_suffix: str,
        warehouse_map: Dict[str, _WarehouseTableRef],
    ) -> Optional[str]:
        """Resolve a warehouse-table-backed inode sourceId to a fully-qualified
        warehouse Dataset URN via the connection registry.

        Returns None silently (no counter) when:
          - url_id_suffix is not in warehouse_map — this inode is a Sigma Dataset,
            not a warehouse table; not a failure, just not applicable here.

        Returns None and bumps the appropriate counter when:
          - connection_id is not in the registry or is_mappable=False
            (dm_element_warehouse_unknown_connection)

        Note: dm_element_warehouse_upstream_emitted is NOT bumped here; the
        caller bumps it post-dedup so diamond source_ids (multiple inode-
        entries resolving to the same URN) don't inflate the counter.

        env and platform_instance are resolved from
        ``config.connection_to_platform_map`` when a matching entry exists,
        falling back to the Sigma source's own env + platform_instance=None.
        For multi-env or multi-instance warehouse setups, add entries to
        ``connection_to_platform_map`` in the recipe so emitted edges point
        at the URNs the warehouse connector actually produced.

        Note: platform-specific identifier normalization (e.g. BigQuery
        date-sharded tables, wildcard refs) is not applied — the name
        emitted is exactly what Sigma's /files path and lineage entry carry.
        Tables with non-standard identifiers may produce dangling edges.
        """
        ref = warehouse_map.get(url_id_suffix)
        if ref is None:
            # The owning Data Model's /lineage never described this table. Fall
            # back to the global /v2/files index before giving up.
            inferred = self._infer_connection_id(warehouse_map)
            if inferred is not None:
                ref = self._lookup_global_warehouse_table(url_id_suffix, inferred)
            else:
                self.reporter.dm_element_warehouse_connection_ambiguous += 1
                logger.debug(
                    "WAREHOUSE RESOLVE: url_id %r absent from the DM map and the "
                    "connection could not be inferred unambiguously; skipping "
                    "the global index",
                    url_id_suffix,
                )
        if ref is None:
            # Silent until now, and the single most common way a warehouse edge
            # is lost: the element declares this inode but the DM's /lineage
            # never produced a type=table row for it, so it is absent from the
            # map. Compare url_id against the "WAREHOUSE MAP ... final map"
            # line for this Data Model.
            logger.debug(
                "WAREHOUSE RESOLVE miss: url_id %r absent from the warehouse "
                "map (map has %d entries: %r)",
                url_id_suffix,
                len(warehouse_map),
                sorted(warehouse_map)[:20],
            )
            return None

        return self._warehouse_urn_from_ref(ref, context=f"url_id {url_id_suffix!r}")

    def _warehouse_urn_from_ref(
        self, ref: _WarehouseTableRef, *, context: str
    ) -> Optional[str]:
        """Turn resolved warehouse coordinates into a Dataset URN.

        Split out of _resolve_dm_element_warehouse_upstream so a ref reached by
        table NAME rather than by url_id produces an identically-shaped URN --
        same casing, env and platform_instance overrides, same one-shot operator
        warnings. ``context`` only labels the debug lines with how the ref was
        reached.
        """
        record = self.connection_registry.get(ref.connection_id)
        if record is None or not record.is_mappable:
            # Counter is bumped by caller gated on unresolved_seen to avoid
            # inflating on diamond source_ids.
            logger.debug(
                "%s: connectionId %r not resolvable to a warehouse platform "
                "(missing from registry or is_mappable=False).",
                context,
                ref.connection_id,
            )
            return None

        conn_override = self.config.connection_to_platform_map.get(ref.connection_id)
        # Use per-connection convert_urns_to_lowercase to handle warehouses
        # where the connector was run with that flag set to False.  Default=True
        # matches both the Snowflake connector default and most other platforms.
        lowercase = conn_override.convert_urns_to_lowercase if conn_override else True
        fq = ref.fq_name(record.datahub_platform, lowercase=lowercase)
        # Use per-connection env / platform_instance overrides so the emitted
        # URN matches what the warehouse connector actually produced.  Falls
        # back to the Sigma recipe's own env + platform_instance=None, which
        # is correct for single-env single-instance deployments.
        target_env = conn_override.env if conn_override else self.config.env
        target_platform_instance = (
            conn_override.platform_instance if conn_override else None
        )
        logger.debug(
            "WAREHOUSE RESOLVE hit: %s -> platform=%r fq=%r env=%r "
            "platform_instance=%r (connection=%r)",
            context,
            record.datahub_platform,
            fq,
            target_env,
            target_platform_instance,
            ref.connection_id,
        )
        # Once-per-platform info when emitting for a platform not in
        # _WAREHOUSE_LOWERCASE_PLATFORMS, so operators know to verify that
        # a few emitted edges actually resolve in their DataHub instance.
        if record.datahub_platform not in _WAREHOUSE_LOWERCASE_PLATFORMS:
            if record.datahub_platform not in self._warned_unvalidated_platforms:
                self._warned_unvalidated_platforms.add(record.datahub_platform)
                self.reporter.info(
                    title="Sigma warehouse URNs emitted for unvalidated platform",
                    message=(
                        "Warehouse Dataset URNs are being emitted for a platform "
                        "that has not been empirically verified to produce matching "
                        "URN casing. Spot-check a few lineage edges in your DataHub "
                        "instance to confirm they resolve correctly."
                    ),
                    context=f"platform={record.datahub_platform}",
                )
        # Once per unique connectionId without an override, list the IDs in the
        # info message so operators know which connections to add to
        # connection_to_platform_map if env / platform_instance mismatches appear.
        # Fire once per unique connectionId that has no override — gated on
        # "not already seen" so repeated emissions from the same connection
        # don't re-fire the message.
        if (
            conn_override is None
            and ref.connection_id not in self._no_platform_map_conn_ids
        ):
            self._no_platform_map_conn_ids.add(ref.connection_id)
            self.reporter.info(
                title="Sigma warehouse URNs emitted without connection_to_platform_map",
                message=(
                    "Warehouse Dataset URNs are being emitted using the Sigma "
                    "recipe's env and platform_instance=None. If your warehouse "
                    "connector uses a different env or platform_instance, configure "
                    "connection_to_platform_map in the Sigma recipe to match."
                ),
                context=f"connection_ids_without_override={sorted(self._no_platform_map_conn_ids)}",
            )
        return builder.make_dataset_urn_with_platform_instance(
            platform=record.datahub_platform,
            name=fq,
            env=target_env,
            platform_instance=target_platform_instance,
        )

    def _resolve_dm_element_cross_dm_upstream(
        self,
        source_id: str,
        consuming_element: SigmaDataModelElement,
        consuming_data_model: SigmaDataModel,
    ) -> Tuple[Optional[str], CrossDmOutcome]:
        """Resolve a cross-DM ``sourceId`` (``<otherDmUrlId>/<suffix>``) to
        the referenced DM element Dataset URN.

        Returns ``(urn, outcome)``. Success outcomes: ``strict``,
        ``ambiguous``, ``single_element_fallback``. Failure outcomes:
        ``malformed``, ``self_reference``, ``consumer_name_missing``,
        ``dm_unknown``, ``name_unmatched_but_dm_known``. Callers bump the
        matching counter (for success outcomes, gated on dedup).
        """
        other_dm_url_id, _, suffix = source_id.partition("/")
        if not other_dm_url_id or not suffix:
            return None, "malformed"

        # Primary self-reference check: current urlId slug or UUID.
        # Extended check: if the urlId was rotated, a stale lineage record may
        # reference the old slug. Compare container URNs so that any historical
        # alias that maps to the same DM is caught rather than misclassified as
        # ``dm_unknown`` (wasted API call + misleading triage counter).
        consuming_container_urn = self.dm_container_urn_by_url_id.get(
            consuming_data_model.get_url_id()
        ) or self.dm_container_urn_by_url_id.get(consuming_data_model.dataModelId)
        if (
            other_dm_url_id == consuming_data_model.get_url_id()
            or other_dm_url_id == consuming_data_model.dataModelId
            or (
                consuming_container_urn is not None
                and self.dm_container_urn_by_url_id.get(other_dm_url_id)
                == consuming_container_urn
            )
        ):
            return None, "self_reference"

        if not consuming_element.name:
            if other_dm_url_id not in self.dm_container_urn_by_url_id:
                return None, "dm_unknown"
            return None, "consumer_name_missing"

        # Registration (dm_container_urn_by_url_id) is the single source of
        # truth for ``dm_unknown``. ``dm_element_urn_by_name`` can legitimately
        # be ``{}`` for a registered DM with only blank-named elements, which
        # we must not conflate with "DM never registered".
        if other_dm_url_id not in self.dm_container_urn_by_url_id:
            return None, "dm_unknown"
        name_map = self.dm_element_urn_by_name.get(other_dm_url_id, {})

        # Prefer source element names from /lineage ``data-model`` entries:
        # these directly name the element consumed from the source DM and
        # work even when the consuming element has a different display name.
        other_dm_id = self.data_model_id_by_url_id.get(other_dm_url_id)
        if other_dm_id:
            src_names = consuming_data_model.source_dm_element_names.get(
                other_dm_id, []
            )
            if len(src_names) == 1:
                # Exactly one element consumed from this source DM — unambiguous.
                candidates = name_map.get(src_names[0].lower())
                if candidates:
                    if len(candidates) > 1:
                        return sorted(candidates)[0], "ambiguous"
                    return candidates[0], "strict"
            elif len(src_names) > 1:
                # Multiple elements consumed from the same source DM; cannot
                # determine which one maps to this consuming element without
                # per-element scoping. Fall through to name-based / fallback.
                logger.debug(
                    "DM %s element %s: %d consumed names from source DM %s — "
                    "cannot disambiguate via lineage entries alone; falling "
                    "back to consuming-element-name lookup.",
                    consuming_data_model.dataModelId,
                    consuming_element.elementId,
                    len(src_names),
                    other_dm_id,
                )

        candidates = name_map.get(consuming_element.name.lower())
        if not candidates:
            # Single-element fallback: if the producer DM has exactly one
            # element total (and one named), the reference is unambiguous.
            # Both checks are required: blank-named elements are excluded
            # from the name-map, so the total-count guard prevents a
            # spurious match when a DM has 1 named + N blank-named.
            total_elements = self.dm_total_element_count_by_url_id.get(other_dm_url_id)
            all_named_urns: List[str] = [
                urn for urns in name_map.values() for urn in urns
            ]
            if total_elements == 1 and len(all_named_urns) == 1:
                return all_named_urns[0], "single_element_fallback"
            return None, "name_unmatched_but_dm_known"

        if len(candidates) > 1:
            return sorted(candidates)[0], "ambiguous"
        return candidates[0], "strict"

    def _bump_cross_dm_success(self, outcome: str) -> None:
        """Bump cross-DM success counters. Callers are responsible for
        dedup-gating so diamond source_ids don't inflate the signal.
        ``ambiguous`` and ``single_element_fallback`` are sub-shapes of
        ``_resolved`` and both counters increment.
        """
        self.reporter.data_model_element_cross_dm_upstreams_resolved += 1
        if outcome == "ambiguous":
            self.reporter.data_model_element_cross_dm_upstreams_ambiguous += 1
        elif outcome == "single_element_fallback":
            self.reporter.data_model_element_cross_dm_upstreams_single_element_fallback += 1

    def _bump_cross_dm_failure(self, outcome: Optional[str]) -> None:
        """Bump cross-DM failure counters (one per source_id)."""
        if outcome == "self_reference":
            self.reporter.data_model_element_cross_dm_upstreams_self_reference += 1
        elif outcome == "consumer_name_missing":
            self.reporter.data_model_element_cross_dm_upstreams_consumer_name_missing += 1
        elif outcome == "dm_unknown":
            self.reporter.data_model_element_cross_dm_upstreams_dm_unknown += 1
        elif outcome == "name_unmatched_but_dm_known":
            self.reporter.data_model_element_cross_dm_upstreams_name_unmatched_but_dm_known += 1
        # ``malformed`` has no dedicated counter; falls into the caller's
        # generic ``data_model_element_upstreams_unresolved`` bump.

    # ------------------------------------------------------------------
    # customSQL DM element SQL parsing
    # ------------------------------------------------------------------

    def _get_sql_aggregator(
        self,
        platform: str,
        env: str,
        platform_instance: Optional[str],
    ) -> SqlParsingAggregator:
        """Return (or lazily create) the per-platform aggregator instance.

        This path is independent of the ``generate_column_lineage=False``
        kill-switch in ``_get_element_input_details`` (the workbook element
        SQL path).  That flag guards ``sqlglot.lineage()`` /
        ``create_lineage_sql_parsed_result`` which caused OOM on large workbook
        SQL; the aggregator's ``add_view_definition`` uses a different, bounded
        parsing path and does not share that kill-switch.
        """
        cache_key = (platform, env, platform_instance)
        if cache_key not in self._sql_aggregators:
            self._sql_aggregators[cache_key] = SqlParsingAggregator(
                platform=platform,
                platform_instance=platform_instance,
                env=env,
                schema_resolver=None,
                graph=None,
                generate_lineage=True,
                generate_queries=False,
                generate_query_subject_fields=False,
                generate_usage_statistics=False,
                generate_query_usage_statistics=False,
                generate_operations=False,
            )
        return self._sql_aggregators[cache_key]

    def _inc_counter(self, prefix: str, suffix: str) -> None:
        attr = f"{prefix}_{suffix}"
        setattr(self.reporter, attr, getattr(self.reporter, attr) + 1)

    @staticmethod
    def _make_string_schema_field(field_path: str) -> SchemaFieldClass:
        """Placeholder SchemaFieldClass for a chart column in lineage InputField entries."""
        return SchemaFieldClass(
            fieldPath=field_path,
            type=SchemaFieldDataTypeClass(StringTypeClass()),
            nativeDataType="String",
        )

    def _extract_customsql_sql_col_name(
        self, column_id: Optional[str]
    ) -> Optional[str]:
        """Extract the SQL column name from a Sigma customSQL columnId.

        Two valid formats:
          - ``inode-{urlId}/{NATIVE_NAME}``: warehouse-backed passthrough; return NATIVE_NAME.
          - ``{bare_sql_id}`` with no slash: strict UPPER_SNAKE identifier only
            (e.g. ``CUSTOMER_ID``).  Only uppercase is accepted — lowercase and
            mixed-case identifiers are not yet confirmed safe for non-Snowflake
            tenants and are left to a future probe.

        Returns None and increments ``dm_customsql_col_mapping_columnid_rejected`` for
        any columnId that cannot be used as a SQL column name — opaque hashes (composition
        formulas), non-inode slash-shaped IDs, and inode entries with an empty native part.
        Empty/None columnIds are silently ignored (no counter).
        """
        if not column_id:
            return None
        if "/" in column_id:
            prefix, _, native = column_id.partition("/")
            if prefix.startswith("inode-") and native:
                return native
            # Non-inode slash-shaped ID or empty native part — unrecognised format.
            self.reporter.dm_customsql_col_mapping_columnid_rejected += 1
            logger.debug(
                "customSQL columnId %r rejected: slash-shaped but not inode- prefix "
                "or empty native part; skipping column bridge.",
                column_id,
            )
            return None
        # Bare identifier heuristic: UPPER_SNAKE only (e.g. CUSTOMER_ID, TOTAL_SPENT).
        # Snowflake dev-tenant probe shows all valid passthrough columnIds are uppercase;
        # opaque Sigma hashes always contain mixed case, hyphens, or leading digits and
        # never match this pattern. Lowercase and mixed-case bare identifiers are
        # counted but not bridged until confirmed safe on non-Snowflake tenants.
        if re.match(r"^[A-Z][A-Z0-9_]*$", column_id):
            return column_id
        self.reporter.dm_customsql_col_mapping_columnid_rejected += 1
        logger.debug(
            "customSQL columnId %r rejected: not UPPER_SNAKE bare identifier "
            "(opaque hash or mixed-case); skipping column bridge.",
            column_id,
        )
        return None

    def _build_customsql_col_mapping(
        self,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
    ) -> None:
        """Populate ``_customsql_col_mappings`` for one customSQL-backed element.

        Two complementary paths populate the mapping:

        1. **columnId path** (new): for each column, ``_extract_customsql_sql_col_name``
           extracts the SQL identifier from the column's ``columnId`` field.  Handles
           pure-passthrough columns and columns whose formula bracket-ref source name
           does not match the element name (e.g. element "Custom SQL2" with formula
           ``[Custom SQL/CUSTOMER_ID]``).

        2. **formula-ref path** (existing): scans each column's formula for bracket refs
           of the form ``[{element.name}/COL]`` and maps SQL name (lowercased) to the
           Sigma display column name.  Only refs whose namespace matches the element's
           own name are registered.

        Both paths write to the same ``mapping`` dict with identical collision semantics.
        When both paths produce an entry for the same SQL column, the formula-ref result
        overwrites the columnId result; if both agree, no collision is logged.
        """
        mapping: Dict[str, str] = {}
        passthrough: Dict[str, str] = {}
        via_columnid = False

        for col in element.columns:
            # columnId path
            sql_col = self._extract_customsql_sql_col_name(col.columnId)
            if sql_col is not None:
                sql_col_lower = sql_col.lower()
                existing = mapping.get(sql_col_lower)
                if existing is not None and existing != col.name:
                    logger.warning(
                        "DM element %r: SQL column %r (via columnId) referenced by "
                        "multiple Sigma display columns (%r and %r); using the latter.",
                        element_dataset_urn,
                        sql_col,
                        existing,
                        col.name,
                    )
                mapping[sql_col_lower] = col.name
                passthrough[sql_col_lower] = col.name
                via_columnid = True

            # formula-ref path
            refs = extract_bracket_refs(col.formula)
            for ref in refs:
                if (
                    ref.column is not None
                    and ref.source.lower() == element.name.lower()
                ):
                    sql_col_lower = ref.column.lower()
                    if sql_col_lower in mapping and mapping[sql_col_lower] != col.name:
                        logger.warning(
                            "DM element %r: SQL column %r referenced by multiple Sigma "
                            "display columns (%r and %r); using the latter for FGL.",
                            element_dataset_urn,
                            ref.column,
                            mapping[sql_col_lower],
                            col.name,
                        )
                    mapping[sql_col_lower] = col.name
                    # Single-ref formula = direct passthrough of an upstream column.
                    # Multi-ref = computed expression; exclude from SELECT * synthesis
                    # to avoid fabricating upstream edges for non-existent columns.
                    if len(refs) == 1:
                        passthrough[sql_col_lower] = col.name

        if mapping:
            self._customsql_col_mappings[element_dataset_urn] = mapping
            if via_columnid:
                self.reporter.dm_customsql_col_mapping_via_columnid += 1
        if passthrough:
            self._customsql_passthrough_mappings[element_dataset_urn] = passthrough

    def _build_workbook_customsql_registry(
        self,
        workbook: Workbook,
    ) -> Tuple[Dict[str, CustomSqlEntry], Dict[str, List[str]]]:
        """Parse /v2/workbooks/{id}/lineage entries into lookup maps.

        Returns:
            custom_sql_by_name: customSQL name → CustomSqlEntry
            element_id_by_customsql_name: customSQL name → list of elementIds of charts
              that source from it (one customSQL may feed multiple charts)
        """
        entries = self.sigma_api.get_workbook_lineage_entries(workbook.workbookId)
        custom_sql_by_name: Dict[str, CustomSqlEntry] = {}
        element_entries: List[Dict[str, Any]] = []
        for entry in entries:
            entry_type = entry.get("type", "")
            if entry_type == "customSQL":
                name = entry.get("name", "")
                if not name:
                    logger.debug("Skipping unnamed customSQL lineage entry: %r", entry)
                    continue
                try:
                    custom_sql_by_name[name] = CustomSqlEntry.model_validate(entry)
                except Exception as e:
                    self.reporter.workbook_customsql_skipped += 1
                    self.reporter.warning(
                        title="Sigma workbook customSQL lineage entry invalid",
                        message="Failed to parse customSQL lineage entry; it will be skipped.",
                        context=f"customsql_name={name!r}",
                        exc=e,
                    )
            elif entry_type == "element":
                element_entries.append(entry)
        # Resolve element → customSQL references after both have been collected,
        # since either type can appear first in the API response.
        # A single customSQL may be referenced by multiple chart elements.
        element_id_by_customsql_name: Dict[str, List[str]] = {}
        for entry in element_entries:
            element_id = entry.get("elementId", "")
            source_ids = entry.get("sourceIds") or []
            if element_id and isinstance(source_ids, list):
                for source_id in source_ids:
                    if isinstance(source_id, str) and source_id in custom_sql_by_name:
                        element_id_by_customsql_name.setdefault(source_id, []).append(
                            element_id
                        )
        return custom_sql_by_name, element_id_by_customsql_name

    def _build_workbook_customsql_col_mapping(
        self,
        element: Element,
        chart_urn: str,
    ) -> None:
        """Populate ``_customsql_passthrough_mappings`` for a customSQL workbook chart.

        Only single-ref formulas are registered so computed expressions
        (``[Custom SQL/A] + [Custom SQL/B]``) do not fabricate upstream edges
        for columns that have no direct pass-through from the SQL source.

        Workbook charts source columns directly from the SQL SELECT list and Sigma
        surfaces them in formula refs verbatim (e.g. ``[Custom SQL/customer_id]``),
        so the SQL column name is the display name without any alias translation.
        If Sigma ever allows renaming customSQL columns in the workbook UI (as DM
        elements do), this assumption breaks and a ``_customsql_col_mappings`` pass
        like the DM path would be needed.
        """
        passthrough: Dict[str, str] = {}
        for sigma_col, formula in element.column_formulas.items():
            if not formula:
                continue
            refs = extract_bracket_refs(formula)
            if len(refs) != 1:
                continue
            ref = refs[0]
            if ref.column is not None and ref.source.lower() == element.name.lower():
                passthrough[ref.column.lower()] = sigma_col
        if passthrough:
            self._customsql_passthrough_mappings[chart_urn] = passthrough

    def _parse_customsql_upstream_dataset_urns(
        self, customsql_entry: CustomSqlEntry
    ) -> List[str]:
        """Synchronously parse customSQL definition to extract upstream dataset URNs.

        Used to populate ChartInfo.inputs before the SQL aggregator drains.
        Returns an empty list on any failure.

        Note: this is a second, independent invocation of the SQL parser for the same
        SQL that ``_process_workbook_customsql_element`` later registers with the
        aggregator (which re-parses at drain time).  The duplication is intentional:
        the aggregator's resolved in_tables are not available until after all
        workunits have been emitted, but ``ChartInfo.inputs`` must be written before
        the page workunits are yielded so that entity-level upstream lineage appears
        in the UI immediately.  In practice both parses use identical inputs and
        produce identical URN lists.
        """
        definition = (customsql_entry.definition or "").strip()
        connection_id = customsql_entry.connectionId
        if not definition or not connection_id:
            return []
        record = self.connection_registry.get(connection_id)
        if record is None or not record.is_mappable:
            return []
        override = self.config.connection_to_platform_map.get(connection_id)
        default_db = (
            override.default_database if override else None
        ) or record.default_database
        default_schema = (
            override.default_schema if override else None
        ) or record.default_schema
        target_env = override.env if override else self.config.env
        target_platform_instance = (
            override.platform_instance if override else self.config.platform_instance
        )
        try:
            result = create_lineage_sql_parsed_result(
                query=definition,
                default_db=default_db,
                default_schema=default_schema,
                platform=record.datahub_platform,
                env=target_env,
                platform_instance=target_platform_instance,
                generate_column_lineage=False,
            )
            return result.in_tables or []
        except Exception as e:
            self.reporter.workbook_customsql_parse_failed += 1
            self.reporter.warning(
                title="Sigma workbook customSQL inputs parse failed",
                message="create_lineage_sql_parsed_result raised; ChartInfo.inputs will not include warehouse upstreams.",
                context=f"customsql_name={customsql_entry.name!r}, connection_id={customsql_entry.connectionId!r}",
                exc=e,
            )
            return []

    def _register_customsql_with_aggregator(
        self,
        reg: _CustomSqlRegistration,
        customsql_entry: CustomSqlEntry,
    ) -> None:
        """Register one customSQL view with the SQL aggregator.

        Shared by the DM-element and workbook-chart paths.  Guard clauses are
        identical; per-kind variation (counters, warning labels, registered set)
        is carried in ``reg``.
        """
        if reg.urn in reg.registered_set:
            self._inc_counter(reg.counter_prefix, "skipped")
            self.reporter.warning(
                title=f"Sigma {reg.label} has multiple customSQL source_ids",
                message="Only the first customSQL source is used for warehouse lineage; subsequent sources are skipped.",
                context=f"urn={reg.urn!r}, customsql_name={customsql_entry.name!r}",
            )
            return
        definition = (customsql_entry.definition or "").strip()
        if not definition:
            self._inc_counter(reg.counter_prefix, "skipped")
            return
        connection_id = customsql_entry.connectionId
        if not connection_id:
            self._inc_counter(reg.counter_prefix, "skipped")
            return
        record = self.connection_registry.get(connection_id)
        if record is None:
            self._inc_counter(reg.counter_prefix, "skipped")
            self.reporter.warning(
                title=f"Sigma {reg.label} customSQL connection not found",
                message="connectionId not found in connection registry; warehouse lineage will be absent. Check /v2/connections scope/permissions.",
                context=f"urn={reg.urn!r}, connection_id={connection_id!r}",
            )
            return
        if not record.is_mappable:
            self._inc_counter(reg.counter_prefix, "skipped")
            self.reporter.warning(
                title=f"Sigma {reg.label} customSQL platform not mapped",
                message="Sigma connection type is not mapped to a DataHub platform; warehouse lineage will be absent. Add it to SIGMA_TYPE_TO_DATAHUB_PLATFORM_MAP if supported.",
                context=f"urn={reg.urn!r}, sigma_type={record.sigma_type!r}",
            )
            return
        override = self.config.connection_to_platform_map.get(connection_id)
        default_db = (
            override.default_database if override else None
        ) or record.default_database
        default_schema = (
            override.default_schema if override else None
        ) or record.default_schema
        target_env = override.env if override else self.config.env
        target_platform_instance = (
            override.platform_instance if override else self.config.platform_instance
        )
        aggregator = self._get_sql_aggregator(
            platform=record.datahub_platform,
            env=target_env,
            platform_instance=target_platform_instance,
        )
        # The aggregator parses lazily at drain, and sqlglot's own warnings
        # ("Unknown subquery scope: SELECT ...") carry no Sigma identity -- one
        # run produced 72 of them with nothing to attribute them to. Log the
        # registration with a fingerprint of the SQL so a later warning quoting
        # that SQL can be tied back to the element that owns it.
        logger.debug(
            "CUSTOMSQL REGISTER %s urn=%s customsql_name=%r platform=%s "
            "default_db=%r default_schema=%r sql_len=%d sql_sha1=%s "
            "sql_first_line=%r",
            reg.label,
            reg.urn,
            customsql_entry.name,
            record.datahub_platform,
            default_db,
            default_schema,
            len(definition),
            hashlib.sha1(definition.encode("utf-8")).hexdigest()[:12],
            definition.strip().splitlines()[0][:120] if definition.strip() else "",
        )
        try:
            aggregator.add_view_definition(
                view_urn=reg.urn,
                view_definition=definition,
                default_db=default_db,
                default_schema=default_schema,
            )
            self._inc_counter(reg.counter_prefix, "aggregator_invocations")
            reg.registered_set.add(reg.urn)
        except Exception as e:
            self._inc_counter(reg.counter_prefix, "aggregator_invocation_errors")
            self.reporter.warning(
                title=f"Sigma {reg.label} customSQL registration failed",
                message="SqlParsingAggregator.add_view_definition raised; this entity will be emitted without warehouse lineage.",
                context=f"urn={reg.urn!r}, customsql_name={customsql_entry.name!r}, connection_id={connection_id!r}, platform={record.datahub_platform}",
                exc=e,
            )

    def _process_workbook_customsql_element(
        self,
        chart_urn: str,
        customsql_entry: CustomSqlEntry,
    ) -> None:
        """Register one workbook customSQL chart with the per-platform aggregator."""
        self._register_customsql_with_aggregator(
            _CustomSqlRegistration(
                urn=chart_urn,
                registered_set=self._workbook_customsql_registered_urns,
                counter_prefix="workbook_customsql",
                label="workbook chart",
            ),
            customsql_entry,
        )

    def _process_dm_customsql_element(
        self,
        element_dataset_urn: str,
        customsql_entry: CustomSqlEntry,
    ) -> None:
        """Register one customSQL DM element with the per-platform aggregator."""
        self._register_customsql_with_aggregator(
            _CustomSqlRegistration(
                urn=element_dataset_urn,
                registered_set=self._customsql_registered_urns,
                counter_prefix="dm_customsql",
                label="DM element",
            ),
            customsql_entry,
        )

    def _build_workbook_chart_input_fields_mcp(
        self,
        entity_urn: str,
        aspect: UpstreamLineage,
    ) -> MetadataChangeProposalWrapper:
        """Convert an UpstreamLineage aspect for a workbook chart into InputFields.

        Charts do not accept ``upstreamLineage``; this converts the aggregator
        output into the ``inputFields`` aspect that DataHub's chart entity accepts.
        """
        self.reporter.workbook_customsql_upstream_emitted += 1
        input_fields: List[InputFieldClass] = []
        if aspect.fineGrainedLineages:
            input_fields = self._fgl_to_input_fields(aspect.fineGrainedLineages)
        elif len(aspect.upstreams) == 1:
            col_mapping = self._customsql_passthrough_mappings.get(entity_urn)
            if col_mapping:
                upstream_urn = aspect.upstreams[0].dataset
                input_fields = self._passthrough_to_input_fields(
                    upstream_urn, col_mapping
                )
        if input_fields:
            self.reporter.workbook_customsql_column_lineage_emitted += 1
        fallback_fields = self._workbook_customsql_formula_fields.get(entity_urn, [])
        if fallback_fields:
            covered_paths = {
                f.schemaField.fieldPath
                for f in input_fields
                if f.schemaField is not None
            }
            for fb in fallback_fields:
                if (
                    fb.schemaField is not None
                    and fb.schemaField.fieldPath not in covered_paths
                ):
                    input_fields.append(fb)
        return MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=InputFieldsClass(fields=input_fields),
        )

    def _fgl_to_input_fields(
        self, fgls: List[FineGrainedLineageClass]
    ) -> List[InputFieldClass]:
        """Build InputField entries from named-column FGL entries."""
        input_fields: List[InputFieldClass] = []
        for fgl in fgls:
            downstreams = fgl.downstreams or []
            if not downstreams:
                self.reporter.workbook_customsql_fgl_downstream_unmapped += 1
                continue
            for ds_urn in downstreams:
                try:
                    chart_col = SchemaFieldUrn.from_string(ds_urn).field_path
                except InvalidUrnError:
                    self.reporter.workbook_customsql_fgl_downstream_unmapped += 1
                    logger.debug(
                        "Skipping FGL entry with invalid downstream URN %r", ds_urn
                    )
                    continue
                for upstream_sf_urn in fgl.upstreams or []:
                    input_fields.append(
                        InputFieldClass(
                            schemaFieldUrn=upstream_sf_urn,
                            schemaField=self._make_string_schema_field(chart_col),
                        )
                    )
        return input_fields

    def _passthrough_to_input_fields(
        self, upstream_urn: str, col_mapping: Dict[str, str]
    ) -> List[InputFieldClass]:
        """Build InputField entries from a SELECT * passthrough mapping."""
        return [
            InputFieldClass(
                schemaFieldUrn=builder.make_schema_field_urn(upstream_urn, sql_col),
                schemaField=self._make_string_schema_field(sigma_col),
            )
            for sql_col, sigma_col in col_mapping.items()
        ]

    def _rewrite_fgl_downstreams(
        self, mcp: MetadataChangeProposalWrapper
    ) -> MetadataChangeProposalWrapper:
        """Rewrite FGL downstream schemaField URNs to use Sigma column names.

        The aggregator derives downstream field names from the SQL SELECT list
        (e.g. ``customer_id``), but DataHub's SchemaMetadata for Sigma elements
        uses the display names from ``/columns`` (e.g. ``Customer Id``).
        ``_customsql_col_mappings`` bridges the two via the formula ref
        ``[Custom SQL/CUSTOMER_ID]`` that Sigma stores on each column.

        FGL entries whose downstreams cannot be rewritten are dropped; the
        entity-level ``upstreams`` list on the aspect is always preserved.
        """
        aspect = mcp.aspect
        if not isinstance(aspect, UpstreamLineage):
            return mcp
        entity_urn = str(mcp.entityUrn)

        # Workbook chart URNs: convert UpstreamLineage to InputFields, since
        # DataHub's chart entity does not accept the upstreamLineage aspect.
        if entity_urn in self._workbook_customsql_registered_urns:
            return self._build_workbook_chart_input_fields_mcp(entity_urn, aspect)

        # Only count MCPs for element URNs we registered — the aggregator
        # should only emit for those, but guard in case of future changes.
        if entity_urn in self._customsql_registered_urns:
            self.reporter.dm_customsql_upstream_emitted += 1

        # Merge non-customSQL upstreams stashed from the per-element emit so the
        # final aspect is consolidated rather than the second emission overwriting.
        extra_upstreams = self._customsql_extra_upstreams.get(entity_urn)
        if extra_upstreams:
            aspect.upstreams = list(aspect.upstreams or []) + extra_upstreams

        # Extra FGLs come from the non-customSQL path and already use Sigma
        # display names — keep them separate from the aggregator FGL so the
        # rewrite loop below doesn't try to remap them.
        extra_fgls = self._customsql_extra_fgls.get(entity_urn)

        if not aspect.fineGrainedLineages:
            # For SELECT * on a single upstream, synthesize FGL from passthrough
            # formula refs: only single-ref formulas are included so computed
            # expressions (If([X]>0,[Y],0)) don't fabricate non-existent upstream
            # column edges.  Confidence 0.1 (formula-derived, lower than SQL-parsed 0.2).
            if len(aspect.upstreams) == 1:
                col_mapping = self._customsql_passthrough_mappings.get(entity_urn)
                if col_mapping:
                    upstream_urn = aspect.upstreams[0].dataset
                    aspect.fineGrainedLineages = [
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[
                                builder.make_schema_field_urn(upstream_urn, sql_col)
                            ],
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[
                                builder.make_schema_field_urn(entity_urn, sigma_col)
                            ],
                            confidenceScore=_FGL_CONFIDENCE_FORMULA_DERIVED,
                        )
                        for sql_col, sigma_col in col_mapping.items()
                    ]
                    self.reporter.dm_customsql_column_lineage_emitted += 1
                    if extra_fgls:
                        aspect.fineGrainedLineages = (
                            list(aspect.fineGrainedLineages) + extra_fgls
                        )
                    return mcp
            # No synthesis possible; still merge stashed non-customSQL FGL.
            if extra_fgls:
                aspect.fineGrainedLineages = extra_fgls
            return mcp

        rewritten_fgls = []
        for fgl in aspect.fineGrainedLineages:
            rewritten_downstreams = []
            for ds_urn in fgl.downstreams or []:
                try:
                    sfu = SchemaFieldUrn.from_string(ds_urn)
                except InvalidUrnError:
                    self.reporter.dm_customsql_fgl_downstream_unmapped += 1
                    logger.warning(
                        "Aggregator emitted malformed schemaField URN %r; dropping FGL entry.",
                        ds_urn,
                    )
                    continue
                ds_parent_urn, field_path = sfu.parent, sfu.field_path
                col_mapping = self._customsql_col_mappings.get(ds_parent_urn)
                if col_mapping is None:
                    rewritten_downstreams.append(ds_urn)
                    continue
                sigma_col = col_mapping.get(field_path.lower())
                if sigma_col:
                    rewritten_downstreams.append(
                        builder.make_schema_field_urn(ds_parent_urn, sigma_col)
                    )
                else:
                    self.reporter.dm_customsql_fgl_downstream_unmapped += 1
                    logger.debug(
                        "customSQL FGL downstream unmapped: SQL column %r on "
                        "%s has no matching Sigma display column; known=%r",
                        field_path,
                        ds_parent_urn,
                        sorted(col_mapping.values())[:25],
                    )
            if rewritten_downstreams:
                rewritten_fgls.append(
                    FineGrainedLineageClass(
                        upstreamType=fgl.upstreamType,
                        upstreams=fgl.upstreams,
                        downstreamType=fgl.downstreamType,
                        downstreams=rewritten_downstreams,
                        confidenceScore=fgl.confidenceScore,
                    )
                )

        # Append non-customSQL FGL after rewriting; they use Sigma display names
        # already and must not be passed through the rewrite loop above.
        all_fgls = rewritten_fgls + (extra_fgls or [])
        aspect.fineGrainedLineages = all_fgls or None
        if rewritten_fgls:
            self.reporter.dm_customsql_column_lineage_emitted += 1
        return mcp

    def _drain_sql_aggregators(self) -> Iterable[MetadataWorkUnit]:
        """Drain all per-platform aggregators and emit lineage workunits.

        Intentionally deferred to the end of ``get_workunits_internal``: all DM
        elements must be registered via ``add_view_definition`` before parsing
        runs, so the aggregator sees the full view set in one pass.  The
        consolidated ``UpstreamLineage`` MCP emitted here is the source of truth
        for customSQL-backed element lineage; any earlier per-element
        ``UpstreamLineage`` MCP (for non-customSQL upstreams) is superseded by
        the merged aspect yielded below.  FGL downstream schemaField URNs are
        rewritten to Sigma display names before yielding.  Each aggregator is
        closed in a finally block so SQLite-backed tempfiles are released even
        if gen_metadata raises.
        """
        for cache_key, aggregator in sorted(
            self._sql_aggregators.items(),
            key=lambda kv: tuple(x or "" for x in kv[0]),
        ):
            try:
                for mcp in aggregator.gen_metadata():
                    yield self._rewrite_fgl_downstreams(mcp).as_workunit()
                agg_report = aggregator.report
                fail_urns: Dict[str, Any] = agg_report.views_parse_failures or {}
                wb_failures = sum(
                    u in self._workbook_customsql_registered_urns for u in fail_urns
                )
                dm_failures = sum(
                    u in self._customsql_registered_urns for u in fail_urns
                )
                # views_parse_failures is lossy past 10 entries; use num_views_failed
                # for the accurate total and attribute any residual (past-10 failures)
                # to the DM counter as best-effort.
                residual = max(
                    0, agg_report.num_views_failed - wb_failures - dm_failures
                )
                self.reporter.workbook_customsql_parse_failed += wb_failures
                self.reporter.dm_customsql_parse_failed += dm_failures + residual
            except Exception as e:
                self.reporter.warning(
                    title="Sigma DM customSQL aggregator drain failed",
                    message="SqlParsingAggregator.gen_metadata raised; warehouse lineage for this platform's customSQL elements may be partial.",
                    context=f"aggregator_key={cache_key!r}",
                    exc=e,
                )
            finally:
                aggregator.close()

    def _gen_data_model_element_upstream_lineage(
        self,
        element: SigmaDataModelElement,
        data_model: SigmaDataModel,
        element_dataset_urn: str,
        *,
        elementId_to_dataset_urn: Dict[str, str],
        element_name_to_eids: Dict[str, List[str]],
        warehouse_url_id_map: Dict[str, _WarehouseTableRef],
    ) -> Optional[UpstreamLineage]:
        # Success counters bump once per unique URN; diamond source_ids
        # resolving to the same URN should not inflate the signal.
        # Failure counters bump once per unique ``source_id`` too -- a
        # vendor payload that repeats the same failed ``inode-X`` inside
        # one element's ``sourceIds`` would otherwise double-count the
        # unresolved buckets while the success path above carefully
        # dedupes, leaving asymmetric triage numbers.
        upstream_urns: List[str] = []
        seen: Set[str] = set()
        unresolved_seen: Set[str] = set()
        # Separate dedup set for warehouse connection failures so the counter
        # fires once per unique source_id regardless of whether the SD path
        # resolves (which would leave the source_id out of unresolved_seen).
        warehouse_failure_seen: Set[str] = set()
        for source_id in element.source_ids:
            upstream_urn: Optional[str] = None
            shape: str = ""
            cross_dm_outcome: Optional[str] = None
            if source_id in elementId_to_dataset_urn:
                upstream_urn = elementId_to_dataset_urn[source_id]
                shape = "intra"
            elif source_id.startswith("inode-"):
                url_id_suffix = source_id[len("inode-") :]
                # Check warehouse map (type=table nodes) and the existing SD
                # resolver (type=dataset nodes) in parallel; both can fire for
                # the same inode when a file is catalogued as both a Sigma
                # Dataset and a warehouse table.  Emitting both as direct
                # upstreams is intentional: the warehouse edge gives operators
                # end-to-end lineage to the source table, while the SD edge
                # preserves the Sigma-Dataset hop.  Downstream lineage queries
                # may see the warehouse table as a direct upstream of the DM
                # element — this is correct for the entity-level graph.
                warehouse_urn = self._resolve_dm_element_warehouse_upstream(
                    url_id_suffix=url_id_suffix,
                    warehouse_map=warehouse_url_id_map,
                )
                upstream_urn = self._resolve_dm_element_external_upstream(source_id)
                shape = "external"
                # Both URN types use the same ``seen`` set so a warehouse URN
                # and an SD URN that happen to collide are still deduped.
                # warehouse_urn is appended here (inside the inode- branch) with
                # its own seen-check; upstream_urn follows the standard gate at
                # the bottom of the for-loop.
                if warehouse_urn and warehouse_urn not in seen:
                    upstream_urns.append(warehouse_urn)
                    seen.add(warehouse_urn)
                    self.reporter.dm_element_warehouse_upstream_emitted += 1
                # Connection-failure counter: uses its own dedup set so a
                # duplicate source_id fires at most once even when the SD path
                # resolves (which would leave source_id out of unresolved_seen).
                if (
                    warehouse_urn is None
                    and url_id_suffix in warehouse_url_id_map
                    and source_id not in warehouse_failure_seen
                ):
                    warehouse_failure_seen.add(source_id)
                    self.reporter.dm_element_warehouse_unknown_connection += 1
                if (
                    upstream_urn is None
                    and warehouse_urn is None
                    and source_id not in unresolved_seen
                ):
                    unresolved_seen.add(source_id)
                    self.reporter.data_model_element_upstreams_unresolved_external += 1
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: external upstream %r unresolved "
                        "(target Sigma Dataset filtered out or not ingested)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )
            elif "/" in source_id:
                upstream_urn, cross_dm_outcome = (
                    self._resolve_dm_element_cross_dm_upstream(
                        source_id, element, data_model
                    )
                )
                shape = "cross_dm"
                if upstream_urn is None and source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
                    self._bump_cross_dm_failure(cross_dm_outcome)
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: cross-DM upstream %r unresolved (%s)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                        cross_dm_outcome,
                    )
            elif source_id in data_model.custom_sql_by_name:
                # customSQL source: register with the aggregator for deferred
                # SQL parsing.  The aggregator emits UpstreamLineage + FGL at
                # drain time; no upstream_urn is added to the entity-level list
                # here because the aggregator owns that emission.
                self._process_dm_customsql_element(
                    element_dataset_urn,
                    data_model.custom_sql_by_name[source_id],
                )
                shape = "customSQL"
            else:
                # Any other shape we don't parse (future Sigma vendor
                # shape, or an existing shape we missed). Counted
                # separately from legitimate "external but un-ingested"
                # and from cross-DM so operators can triage
                # "upstream exists but wasn't emitted" (filter / perm)
                # vs "we don't yet handle this shape." Note: cross-DM
                # ``<prefix>/<suffix>`` refs are caught in the
                # ``"/" in source_id`` branch above, so this bucket is
                # genuinely "nothing we recognize."
                if not source_id:
                    # An EMPTY entry in source_ids, which some tenants send.
                    # There is no shape here to recognise, so filing it under
                    # "unknown shape" sends a reader hunting for a parser gap
                    # that does not exist -- on one tenant (2026-09) every one
                    # of the 19 "unknown shapes" was this.
                    self.reporter.data_model_element_upstreams_empty_source_id += 1
                elif source_id not in unresolved_seen:
                    unresolved_seen.add(source_id)
                    self.reporter.data_model_element_upstreams_unknown_shape += 1
                    self.reporter.data_model_element_upstreams_unresolved += 1
                    logger.debug(
                        "DM %s element %s: upstream source_id %r has an unknown shape "
                        "(not ``inode-`` external, not intra-DM, not ``<dm>/<suffix>`` cross-DM)",
                        data_model.dataModelId,
                        element.elementId,
                        source_id,
                    )

            if upstream_urn and upstream_urn not in seen:
                upstream_urns.append(upstream_urn)
                seen.add(upstream_urn)
                if shape == "intra":
                    self.reporter.data_model_element_intra_upstreams += 1
                elif shape == "external":
                    self.reporter.data_model_element_external_upstreams += 1
                elif shape == "cross_dm" and cross_dm_outcome is not None:
                    self._bump_cross_dm_success(cross_dm_outcome)
                    if cross_dm_outcome == "ambiguous":
                        logger.warning(
                            "Ambiguous cross-DM element name %r for "
                            "consumer %s.%s; picked %s deterministically.",
                            element.name,
                            data_model.dataModelId,
                            element.elementId,
                            upstream_urn,
                        )

        # Sort for deterministic emission order: Sigma's /lineage API does
        # not document ordering, and Upstream entries have no semantic order.
        upstream_urns.sort()
        if not upstream_urns:
            # The most important case to be able to see, and previously the only
            # one that was completely silent: this element gets NO
            # upstreamLineage aspect at all, and the early return happens before
            # the FGL builder, so neither the ELEMENT nor any COLUMN record is
            # produced either. An element with no lineage whatsoever left no
            # trace of why.
            self.reporter.data_model_element_no_upstreams += 1
            logger.debug(
                "ELEMENT NO-UPSTREAM DM %s element %s %r: source_ids=%r "
                "columns=%d -- no source_id resolved to a URN, so no "
                "upstreamLineage and no column lineage is emitted for this "
                "element at all. Each unresolved source_id is reported above "
                "with its shape (intra-DM / inode / cross-DM / customSQL / "
                "unknown).",
                data_model.dataModelId,
                element.elementId,
                element.name,
                element.source_ids,
                len(element.columns),
            )
            return None
        # Elements reached only through a join chain: Sigma's /lineage reports
        # the direct join element, not the transitive source the formula names.
        discovered_upstreams: Set[str] = set()
        fine_grained = self._build_dm_element_fine_grained_lineages(
            element=element,
            element_dataset_urn=element_dataset_urn,
            element_name_to_eids=element_name_to_eids,
            elementId_to_dataset_urn=elementId_to_dataset_urn,
            entity_level_upstream_urns=set(upstream_urns),
            data_model=data_model,
            warehouse_url_id_map=warehouse_url_id_map,
            discovered_upstreams=discovered_upstreams,
        )
        # Promote them to entity-level upstreams so every emitted schemaField
        # has a declared parent Dataset; without this the FGL points at a
        # Dataset missing from ``upstreams`` and the UI will not render it.
        for urn in sorted(discovered_upstreams):
            if urn not in upstream_urns:
                upstream_urns.append(urn)
                logger.debug(
                    "DM %s element %s: promoted join-chain source %s to an "
                    "entity-level upstream (absent from Sigma /lineage)",
                    data_model.dataModelId,
                    element.elementId,
                    urn,
                )
        upstream_urns.sort()
        # Element-level decision record. Pairs with the per-column COLUMN lines:
        # this says which upstreams the element ended up declaring and how many
        # column edges were produced, so "why is there no CLL from X to Y" can be
        # answered by grepping one element id instead of reconstructing it.
        logger.debug(
            "ELEMENT DM %s element %s %r: source_ids=%r -> upstreams=%r "
            "promoted=%r fgl_count=%d columns=%d",
            data_model.dataModelId,
            element.elementId,
            element.name,
            element.source_ids,
            upstream_urns,
            sorted(discovered_upstreams),
            len(fine_grained or []),
            len(element.columns),
        )
        return UpstreamLineage(
            upstreams=[
                Upstream(dataset=urn, type=DatasetLineageType.TRANSFORMED)
                for urn in upstream_urns
            ],
            fineGrainedLineages=fine_grained or None,
        )

    def _gen_data_model_element_schema_metadata(
        self, element_dataset_urn: str, element: SigmaDataModelElement
    ) -> MetadataWorkUnit:
        # Dedup by ``fieldPath`` within the element: Sigma can return two
        # columns with the same name (e.g. a calculated field shadowing a
        # native column), and GMS rejects / non-deterministically dedupes
        # duplicate ``fieldPath``s. Tie-break logic lives in _dedup_dm_element_columns
        # so it stays in sync with the FGL builder's winner set.
        by_name, displaced = _dedup_dm_element_columns(element.columns)
        dropped_column_ids_by_name: Dict[str, List[str]] = {}
        for winner, loser in displaced:
            self.reporter.data_model_element_columns_duplicate_fieldpath_dropped += 1
            dropped_column_ids_by_name.setdefault(winner.name, []).append(
                loser.columnId
            )
        if dropped_column_ids_by_name:
            logger.debug(
                "DM element %s: dropped duplicate-fieldPath columns %s "
                "(kept the row with formula set, or the lexicographically "
                "smallest columnId as a tiebreak)",
                element.elementId,
                dropped_column_ids_by_name,
            )
        # Sort fields by ``column.name`` (== ``fieldPath``) so the emitted
        # ``SchemaMetadata`` is stable across runs even if Sigma's
        # ``/columns`` response re-orders. Without this, a Sigma-side
        # reorder would churn the aspect on every ingest (re-upserting
        # the same field set under different ordering), showing up as
        # spurious graph updates in downstream consumers of the
        # aspect-version timeline. The API does not document an
        # ordering contract, so we enforce our own.
        fields: List[SchemaFieldClass] = []
        for column in sorted(by_name.values(), key=lambda c: c.name):
            fields.append(
                SchemaFieldClass(
                    fieldPath=column.name,
                    # Sigma's ``/columns`` endpoint does not expose a
                    # per-column native type today. Emit ``NullType`` +
                    # a sentinel ``nativeDataType`` so downstream type
                    # checks recognize "unknown" instead of trusting a
                    # fake "String" that was never returned by Sigma.
                    type=SchemaFieldDataTypeClass(NullTypeClass()),
                    nativeDataType=SIGMA_DM_UNKNOWN_COLUMN_NATIVE_TYPE,
                    description=column.label or None,
                )
            )
        schema_metadata = SchemaMetadataClass(
            schemaName=element.name,
            platform=builder.make_data_platform_urn(self.platform),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )
        return MetadataChangeProposalWrapper(
            entityUrn=element_dataset_urn, aspect=schema_metadata
        ).as_workunit()

    def _note_warehouse_miss(
        self,
        reason: str,
        column: SigmaDataModelColumn,
        element: SigmaDataModelElement,
        col_id: str,
    ) -> None:
        """Record why columnId-based warehouse resolution produced nothing.

        Aggregated into a per-reason histogram on the report so the largest
        deferred bucket becomes triageable without grepping the log, plus a
        debug line naming the column for the first occurrences.
        """
        self.reporter.warehouse_passthrough_miss_reasons[reason] = (
            self.reporter.warehouse_passthrough_miss_reasons.get(reason, 0) + 1
        )
        # Cap generously rather than tightly: at 25 the sample was exhausted by
        # the first couple of Data Models and the elements actually being
        # investigated never appeared. Each line is short and only failures
        # reach here.
        if self.reporter.warehouse_passthrough_miss_reasons[reason] <= 2000:
            logger.debug(
                "warehouse passthrough miss (%s): element=%s column=%r "
                "columnId=%r source_ids=%r",
                reason,
                element.elementId,
                column.name,
                col_id,
                element.source_ids[:8],
            )

    def _try_emit_warehouse_passthrough_fgl(
        self,
        *,
        column: SigmaDataModelColumn,
        element: SigmaDataModelElement,
        downstream_field: str,
        warehouse_url_id_map: Dict[str, _WarehouseTableRef],
    ) -> Optional[FineGrainedLineageClass]:
        """Attempt to build a warehouse-passthrough FineGrainedLineage entry.

        Resolves the column's warehouse identity via the columnId field, which
        Sigma encodes as ``inode-<url_id>/<WAREHOUSE_COLUMN_NAME>``.  This is
        more reliable than the formula bracket ref, which carries the DM display
        name (e.g. "Customer Id") rather than the warehouse identifier
        ("CUSTOMER_ID" / "customer_id").

        Returns a FineGrainedLineageClass on success; returns None on any
        resolution failure.  Counter bookkeeping and dedup (emitted_pairs) are
        the caller's responsibility so that None unambiguously means failure.
        """
        # Parse columnId → url_id + warehouse column name.
        # Every early return below records WHY, because
        # fgl_warehouse_passthrough_deferred is the largest bucket in the report
        # (10,806 on one tenant, 2026-09) and previously said nothing about cause.
        col_id = column.columnId or ""
        if not _is_warehouse_column_id(col_id):
            self._note_warehouse_miss(
                "columnId_not_inode_shaped", column, element, col_id
            )
            return None
        suffix = col_id[len("inode-") :]
        url_id, sep, warehouse_col = suffix.partition("/")
        if not sep or not warehouse_col:
            self._note_warehouse_miss(
                "columnId_missing_native_part", column, element, col_id
            )
            return None

        # Verify this url_id is one of the element's declared warehouse sources.
        # Mismatches can occur if a column belongs to a different element's inode
        # (shouldn't happen with well-formed API data, but guards against drift).
        if f"inode-{url_id}" not in element.source_ids:
            # The element does not declare this inode. That is expected when the
            # element is sourced transitively -- e.g. every source_id is a
            # cross-DM ``<dmUrlId>/<suffix>`` ref -- because the warehouse table
            # is declared by the producer element in the other Data Model, not
            # here. Same shape of mistake as gating join-chain resolution on
            # Sigma's direct /lineage list.
            #
            # Accept it only when the element declares no inode source at all
            # AND the url_id is in this Data Model's warehouse map, i.e. some
            # element here does reach that table. If the element declares its
            # own inodes and this column names a different one, that is genuine
            # payload drift and stays rejected.
            declares_any_inode = any(
                sid.startswith("inode-") for sid in element.source_ids
            )
            if declares_any_inode or url_id not in warehouse_url_id_map:
                self._note_warehouse_miss(
                    "url_id_not_in_element_source_ids", column, element, col_id
                )
                return None
            self.reporter.dm_element_warehouse_transitive_inode_accepted += 1
            logger.debug(
                "WAREHOUSE transitive accept: element %s column %r columnId=%r "
                "-- element declares no inode source (source_ids=%r) but url_id "
                "is in this DM's warehouse map",
                element.elementId,
                column.name,
                col_id,
                element.source_ids,
            )

        # The Data Model's /lineage does not describe every table its elements
        # reference, so a miss here is not the end: ask /v2/files/{urlId}
        # directly, exactly as the entity-level path does. Without this the
        # recovery only ever produced a table-level edge while the COLUMN that
        # motivated it stayed unresolved -- 1,305 columns on one tenant (2026-09).
        # The lookup is cached per url_id, so repeats across columns are free.
        wh_ref = warehouse_url_id_map.get(url_id)
        if wh_ref is None:
            inferred = self._infer_connection_id(warehouse_url_id_map)
            if inferred is not None:
                wh_ref = self._lookup_global_warehouse_table(url_id, inferred)
            if wh_ref is not None:
                self.reporter.dm_element_warehouse_column_recovered_by_lookup += 1
                logger.debug(
                    "WAREHOUSE COLUMN RECOVERED: element %s column %r url_id %r "
                    "was absent from this Data Model's warehouse map but "
                    "/v2/files/{urlId} resolved it to db=%r schema=%r table=%r",
                    element.elementId,
                    column.name,
                    url_id,
                    wh_ref.db,
                    wh_ref.schema,
                    wh_ref.table,
                )
        if wh_ref is None:
            # Neither the DM's /lineage nor a direct lookup describes this table.
            self._note_warehouse_miss(
                "url_id_not_in_warehouse_map", column, element, col_id
            )
            return None

        # Resolve the parent Dataset URN.  This is the only allowed path for
        # URN construction — env, platform_instance, and casing all live here,
        # so bypassing it risks orphan schemaFields on casing or instance drift.
        parent_urn = (
            self._resolve_dm_element_warehouse_upstream(
                url_id_suffix=url_id,
                warehouse_map=warehouse_url_id_map,
            )
            if url_id in warehouse_url_id_map
            else self._warehouse_urn_from_ref(
                wh_ref, context=f"recovered url_id {url_id!r}"
            )
        )
        if parent_urn is None:
            self._note_warehouse_miss("parent_urn_unresolved", column, element, col_id)
            return None

        # Normalize column casing to match the platform convention used by the
        # warehouse connector.  _resolve_dm_element_warehouse_upstream already
        # resolved the registry record and override, so these lookups are cheap
        # dict hits on the same objects.
        record = self.connection_registry.get(wh_ref.connection_id)
        if record is None:
            self._note_warehouse_miss(
                "connection_not_in_registry", column, element, col_id
            )
            return None
        conn_override = self.config.connection_to_platform_map.get(wh_ref.connection_id)
        lowercase = conn_override.convert_urns_to_lowercase if conn_override else True
        normalized_col = _normalize_warehouse_identifier(
            warehouse_col, record.datahub_platform, lowercase
        )
        upstream_field = builder.make_schema_field_urn(parent_urn, normalized_col)
        return FineGrainedLineageClass(
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[downstream_field],
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[upstream_field],
            confidenceScore=1.0,
        )

    def _try_emit_self_named_cross_dm_fgl(
        self,
        *,
        ref: "BracketRef",
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        entity_level_upstream_urns: Set[str],
        downstream_field: str,
        emitted_pairs: Set[Tuple[str, str]],
        cross_dm_fgls: List[FineGrainedLineageClass],
    ) -> bool:
        """Try cross-DM FGL for a ref whose source name matches the element itself.

        Called when all intra-DM candidates were self-references (element named
        after a cross-DM source element rather than its warehouse table). Returns
        True when a cross-DM FGL is resolved and appended; False otherwise.

        The source_ids guard short-circuits for elements that have no cross-DM
        source entries (format <dm-url-id>/<suffix>). This prevents a false
        data_model_element_fgl_cross_dm_deferred increment when source_ids
        contains only bare intra-DM element IDs or is empty entirely.

        Note: when this returns False because _resolve_cross_dm_fgl deferred,
        both data_model_element_fgl_cross_dm_deferred (from the callee) and
        data_model_element_fgl_warehouse_passthrough_deferred (from the caller's
        fall-through) are incremented for the same ref. This is intentional —
        the two counters measure independent dimensions (cross-DM attempt outcome
        vs. warehouse-passthrough gate), and operators should not sum them.
        """
        if not any(
            "/" in sid and not sid.startswith("inode-") for sid in element.source_ids
        ):
            logger.debug(
                "element %s: no cross-DM source_ids — skipping self-named cross-DM FGL",
                element.elementId,
            )
            return False
        try:
            cross_dm_fgl = self._resolve_cross_dm_fgl(
                ref=ref,
                element=element,
                element_dataset_urn=element_dataset_urn,
                entity_level_upstream_urns=entity_level_upstream_urns,
                downstream_field=downstream_field,
            )
        except Exception as e:
            self.reporter.warning(
                title="Cross-DM FGL resolution failed",
                message=(
                    f"Unexpected error resolving cross-DM FGL for element "
                    f"{element.elementId} ref {ref.raw!r}: {e}"
                ),
                context=f"ref={ref.raw!r}, element={element.elementId}",
            )
            return False
        if cross_dm_fgl is None:
            return False
        # _resolve_cross_dm_fgl always returns a single-upstream FGL or None.
        assert cross_dm_fgl.upstreams
        pair = (downstream_field, cross_dm_fgl.upstreams[0])
        if pair not in emitted_pairs:
            emitted_pairs.add(pair)
            cross_dm_fgls.append(cross_dm_fgl)
            self.reporter.data_model_element_fgl_cross_dm_resolved += 1
        return True

    def _resolve_cross_dm_fgl(
        self,
        *,
        ref: "BracketRef",
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        entity_level_upstream_urns: Set[str],
        downstream_field: str,
    ) -> Optional[FineGrainedLineageClass]:
        """Resolve a formula ref against cross-DM sources and return an FGL.

        Returns None (and bumps the appropriate deferred/dropped counter) when
        resolution fails; counter bookkeeping for the success case is the
        caller's responsibility.
        """
        assert (
            ref.column is not None
        )  # callers guard on ref.column is None before dispatching
        # Cross-DM source_ids use the shape <dm-url-id>/<suffix>; intra-DM
        # source_ids are bare elementIds (no "/"). Filter accordingly.
        source_dm_url_ids = {
            sid.partition("/")[0]
            for sid in element.source_ids
            if "/" in sid and not sid.startswith("inode-")
        }
        cross_dm_candidate_urns = sorted(
            {
                urn
                for dm_url_id in source_dm_url_ids
                for urn in self.dm_element_urn_by_name.get(dm_url_id, {}).get(
                    ref.source.lower(), []
                )
                if urn != element_dataset_urn
            }
        )
        if not cross_dm_candidate_urns:
            self.reporter.data_model_element_fgl_cross_dm_deferred += 1
            logger.debug(
                "element %s: cross-DM deferred, no candidate for ref %r "
                "(segments=%r, source_dms=%r)",
                element.elementId,
                ref.raw,
                ref.parts,
                sorted(source_dm_url_ids),
            )
            return None
        if len(cross_dm_candidate_urns) > 1:
            # Restrict to entity-level confirmed candidates whenever any exist.
            confirmed = [
                u for u in cross_dm_candidate_urns if u in entity_level_upstream_urns
            ]
            if confirmed:
                cross_dm_candidate_urns = confirmed
            if len(cross_dm_candidate_urns) > 1:
                self.reporter.data_model_element_fgl_cross_dm_collision_pick_first += 1
        chosen_upstream_urn = cross_dm_candidate_urns[0]
        # dm_element_urn_to_cols is populated for every URN in
        # dm_element_urn_by_name (same loop in _prepopulate_dm_bridge_maps),
        # so this get() will only be None if a URN reaches this point
        # without going through prepopulation — defensively handled.
        upstream_cols = self.dm_element_urn_to_cols.get(chosen_upstream_urn)
        if upstream_cols is None:
            self.reporter.data_model_element_fgl_cross_dm_deferred += 1
            logger.debug(
                "element %s: cross-DM deferred, producer %s absent from the "
                "bridge column map for ref %r (segments=%r)",
                element.elementId,
                chosen_upstream_urn,
                ref.raw,
                ref.parts,
            )
            return None
        if not upstream_cols:
            # Producer element is known but carries no columns -- the producer
            # DM's /columns fetch came back empty. Distinct from the `is None`
            # case above (producer absent from the bridge map entirely), which
            # stays on the deferred counter. Counted separately from the
            # intra-DM equivalent: an empty sibling in this DM and an empty
            # producer in another DM are different investigations.
            self.reporter.data_model_element_fgl_cross_dm_upstream_schema_unavailable += 1
            self._warn_upstream_schema_unavailable(chosen_upstream_urn, element)
            return None
        canonical_col = upstream_cols.get(ref.column.lower())
        if canonical_col is None:
            self.reporter.data_model_element_fgl_cross_dm_dropped_unknown_upstream_column += 1
            logger.debug(
                "element %s: cross-DM drop, ref %r column %r absent from "
                "producer %s (segments=%r, producer_has=%r)",
                element.elementId,
                ref.raw,
                ref.column,
                chosen_upstream_urn,
                ref.parts,
                sorted(upstream_cols.values())[:25],
            )
            return None
        return FineGrainedLineageClass(
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
            downstreams=[downstream_field],
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=[
                builder.make_schema_field_urn(chosen_upstream_urn, canonical_col)
            ],
            confidenceScore=1.0,
        )

    def _warn_upstream_schema_unavailable(
        self, upstream_urn: str, element: SigmaDataModelElement
    ) -> None:
        """Warn once per upstream URN that its schema came back empty."""
        if upstream_urn in self._upstream_schema_unavailable_warned:
            return
        self._upstream_schema_unavailable_warned.add(upstream_urn)
        self.reporter.warning(
            title="Sigma DM element upstream schema unavailable",
            message=(
                "A formula references a column on an upstream Data Model element "
                "whose column list came back empty, so the column-level lineage "
                "edge was dropped. Check for a `Sigma paginated endpoint aborted` "
                "warning naming that Data Model: if one is present its /columns "
                "fetch failed partway through, and if none is present the "
                "upstream element genuinely has no columns."
            ),
            context=f"upstream={upstream_urn}, element={element.elementId}",
        )

    def _resolve_intra_dm_fgl(
        self,
        *,
        ref: "BracketRef",
        candidate_eids_after_self_strip: List[str],
        elementId_to_dataset_urn: Dict[str, str],
        entity_level_upstream_urns: Set[str],
        urn_to_cols: Dict[str, Dict[str, str]],
        downstream_field: str,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        data_model: SigmaDataModel,
        fgls: List[FineGrainedLineageClass],
        cross_dm_fgls: List[FineGrainedLineageClass],
        emitted_pairs: Set[Tuple[str, str]],
        discovered_upstreams: Set[str],
    ) -> None:
        """Resolve a formula ref against intra-DM sibling elements.

        Appends to fgls / cross_dm_fgls and emitted_pairs on success; bumps
        the appropriate counter on any resolution failure.  Counter bookkeeping
        and dedup are handled here so the caller needs no conditional on the result.
        """
        assert (
            ref.column is not None
        )  # callers guard on ref.column is None before dispatching
        candidate_urns = sorted(
            elementId_to_dataset_urn[eid]
            for eid in candidate_eids_after_self_strip
            if eid in elementId_to_dataset_urn
        )
        surviving_urns = sorted(
            u for u in candidate_urns if u in entity_level_upstream_urns
        )

        if not surviving_urns:
            # Intra-DM candidate(s) exist but none appear in /lineage upstreams.
            # Two distinct causes:
            #   (a) name collision: a sibling shares the name but the actual
            #       upstream is cross-DM (e.g. two "Custom SQL" elements in one
            #       DM where the consumer pulls from a cross-DM "Custom SQL")
            #   (b) genuine orphan: Sigma /lineage reporting gap
            # Try cross-DM first; falls through to orphan-drop for case (b).
            if self._try_emit_self_named_cross_dm_fgl(
                ref=ref,
                element=element,
                element_dataset_urn=element_dataset_urn,
                entity_level_upstream_urns=entity_level_upstream_urns,
                downstream_field=downstream_field,
                emitted_pairs=emitted_pairs,
                cross_dm_fgls=cross_dm_fgls,
            ):
                return
            # The named sibling exists in this Data Model but Sigma's /lineage
            # did not list it as an upstream. Where that sibling actually owns
            # the referenced column, the ref is trustworthy and the omission is
            # a reporting gap -- the same situation as a join chain, whose
            # owning element /lineage never lists either. Accept it on the
            # strength of the schema and promote it to an entity-level upstream,
            # so the emitted schemaField has a declared parent Dataset.
            # A run measured 162 of 167 orphan drops as recoverable this way.
            owning = [
                u
                for u in candidate_urns
                if urn_to_cols.get(u, {}).get(ref.column.lower()) is not None
            ]
            if owning:
                chosen = owning[0]
                canonical = urn_to_cols[chosen][ref.column.lower()]
                upstream_field = builder.make_schema_field_urn(chosen, canonical)
                pair = (downstream_field, upstream_field)
                if pair not in emitted_pairs:
                    emitted_pairs.add(pair)
                    fgls.append(
                        FineGrainedLineageClass(
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            downstreams=[downstream_field],
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            upstreams=[upstream_field],
                            confidenceScore=1.0,
                        )
                    )
                discovered_upstreams.add(chosen)
                self.reporter.data_model_element_fgl_orphan_recovered += 1
                logger.debug(
                    "DM %s element %s: orphan ref %r recovered -- sibling %s "
                    "owns column %r though /lineage did not list it as an "
                    "upstream; promoted to an entity-level upstream",
                    data_model.dataModelId,
                    element.elementId,
                    ref.raw,
                    chosen,
                    canonical,
                )
                return
            self.reporter.data_model_element_fgl_dropped_orphan_upstream += 1
            logger.debug(
                "DM %s element %s: orphan drop for ref %r -- name matched an "
                "intra-DM sibling but /lineage did not list it, the sibling "
                "does not own the column, and cross-DM rescue failed "
                "(segments=%r, candidates=%r)",
                data_model.dataModelId,
                element.elementId,
                ref.raw,
                ref.parts,
                candidate_urns,
            )
            return

        # Collision handling: multiple siblings passed /lineage filter.
        # Pick sorted-first to match the collision policy used elsewhere
        # in this connector and Sigma's server-side coalescing.
        if len(surviving_urns) > 1:
            self.reporter.data_model_element_fgl_collision_pick_first += 1
            self.reporter.warning(
                title="Ambiguous DM element name in formula ref",
                message=(
                    f"Formula ref {ref.raw!r} in element {element.elementId} "
                    f"resolves to {len(surviving_urns)} elements with the same "
                    f"display name — picking the lexicographically-first URN "
                    f"({surviving_urns[0]!r}). Rename duplicate elements in "
                    f"Data Model {data_model.dataModelId} to remove ambiguity."
                ),
                context=f"ref={ref.raw!r}, candidates={surviving_urns}",
            )
        chosen_upstream_urn = surviving_urns[0]

        # Validate and normalise ref.column against the chosen upstream
        # element's schema winners to avoid a dangling schemaField URN.
        source_cols = urn_to_cols.get(chosen_upstream_urn, {})
        if not source_cols:
            # The sibling resolved but carries no columns, so the ref cannot be
            # validated against a real fieldPath. Counted apart from the
            # name-miss below so operators can tell a fetch problem from a
            # genuine missing column. Reachable when /columns aborted partway
            # through pagination (earlier pages are preserved, so some siblings
            # are populated and later ones are not) or when the sibling really
            # has no columns -- note a whole-DM /columns failure cannot reach
            # here, since it empties this element too and the caller's column
            # loop never runs.
            self.reporter.data_model_element_fgl_upstream_schema_unavailable += 1
            self._warn_upstream_schema_unavailable(chosen_upstream_urn, element)
            return
        canonical_col = source_cols.get(ref.column.lower())
        if canonical_col is None:
            self.reporter.data_model_element_fgl_dropped_unknown_upstream_column += 1
            logger.debug(
                "DM %s element %s: ref %r column %r not found in upstream "
                "element %s schema winners; dropping FGL entry. segments=%r, "
                "upstream_has=%r",
                data_model.dataModelId,
                element.elementId,
                ref.raw,
                ref.column,
                chosen_upstream_urn,
                ref.parts,
                # Sample of what the chosen upstream actually exposes, so a
                # verification run shows whether the intended column is there
                # under a different name (or not at all) without a live API call.
                sorted(source_cols.values())[:25],
            )
            return

        upstream_field = builder.make_schema_field_urn(
            chosen_upstream_urn, canonical_col
        )
        pair = (downstream_field, upstream_field)
        if pair in emitted_pairs:
            return
        emitted_pairs.add(pair)
        fgls.append(
            FineGrainedLineageClass(
                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                downstreams=[downstream_field],
                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                upstreams=[upstream_field],
                confidenceScore=1.0,
            )
        )

    @staticmethod
    def _multi_segment_refs(refs: List["BracketRef"]) -> List[str]:
        """Raw text of refs carrying a join-chain shape (3+ segments).

        Used only to keep the diagnostic probes cheap: the chart path processes
        hundreds of thousands of columns, so probes log the raw ref text only
        when the join-chain shape is actually present.
        """
        return [r.raw for r in refs if len(r.parts) >= 3]

    def _try_resolve_join_chain_ref(
        self,
        *,
        ref: "BracketRef",
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        element_name_to_eids: Dict[str, List[str]],
        elementId_to_dataset_urn: Dict[str, str],
        entity_level_upstream_urns: Set[str],
        urn_to_cols: Dict[str, Dict[str, str]],
        downstream_field: str,
        emitted_pairs: Set[Tuple[str, str]],
        fgls: List[FineGrainedLineageClass],
        cross_dm_fgls: List[FineGrainedLineageClass],
        discovered_upstreams: Set[str],
    ) -> bool:
        """Resolve a multi-segment ref by trying alternative (source, column) splits.

        Sigma writes a column reached through a join as
        ``[JoinElement/SourceElement/Column]``, so the owning element is the
        segment before the column. ``ref.source`` holds the *first* segment,
        which for these refs is the join element -- a real sibling, which is why
        the legacy path resolves it, then fails the column lookup and drops the
        edge.

        Returns True when an edge was emitted. Deliberately counts nothing on
        failure: the caller falls through to the legacy path, which owns the
        residual bucket, so one dropped ref is never counted twice. Each
        candidate is self-stripped and tried intra-DM *then* cross-DM before the
        next is considered, so neither the self-name warehouse short-circuit nor
        an intra-DM first segment can hide a resolvable later segment.

        Refs with fewer than three segments are left entirely to the legacy
        path -- their only candidate *is* the legacy split -- so single-slash
        behaviour and its counters are untouched.
        """
        if len(ref.parts) < 3:
            return False
        # Per-candidate verdicts, so a failure line says WHICH check rejected
        # WHICH candidate. Without this the previous run could only say "no
        # candidate resolved" and the blocking gate had to be inferred by
        # reading the code.
        trace: List[str] = []
        for source, col in candidate_source_column_splits(ref):
            # Intra-DM: sibling elements, self-references stripped per candidate.
            eids = [
                eid
                for eid in element_name_to_eids.get(source.lower(), [])
                if eid != element.elementId
            ]
            urns = sorted(
                elementId_to_dataset_urn[eid]
                for eid in eids
                if eid in elementId_to_dataset_urn
            )
            # NOTE: deliberately NOT filtered by entity_level_upstream_urns.
            # A join chain reaches its owning element *through* the join, so
            # Sigma's element-level /lineage lists only the direct join element
            # and never the transitive one. Requiring membership here made every
            # intra-DM candidate fail: in one production run all 6 successes came
            # from the cross-DM branch, which has no such gate, and 0 from here.
            # The chosen element is instead recorded in discovered_upstreams and
            # added to the entity-level upstreams by the caller, so the emitted
            # schemaField still has a declared parent Dataset.
            if not urns:
                trace.append(f"{source!r}: no intra-DM element with that name")
            if urns:
                # Prefer a direct upstream when one matches, purely for
                # determinism; correctness does not depend on it.
                ordered = [u for u in urns if u in entity_level_upstream_urns] + [
                    u for u in urns if u not in entity_level_upstream_urns
                ]
                cols_here = urn_to_cols.get(ordered[0], {})
                canonical = cols_here.get(col.lower())
                if canonical is None:
                    trace.append(
                        f"{source!r}: element found ({ordered[0].split('.')[-1]}) "
                        f"but column {col!r} absent from its {len(cols_here)} "
                        f"columns; in_direct_lineage="
                        f"{ordered[0] in entity_level_upstream_urns}"
                    )
                if canonical is not None:
                    surviving = ordered
                    if ordered[0] not in entity_level_upstream_urns:
                        discovered_upstreams.add(ordered[0])
                        self.reporter.data_model_element_fgl_join_chain_upstream_added += 1
                    upstream_field = builder.make_schema_field_urn(
                        ordered[0], canonical
                    )
                    pair = (downstream_field, upstream_field)
                    if pair not in emitted_pairs:
                        emitted_pairs.add(pair)
                        fgls.append(
                            FineGrainedLineageClass(
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                downstreams=[downstream_field],
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                upstreams=[upstream_field],
                                confidenceScore=1.0,
                            )
                        )
                    self.reporter.data_model_element_fgl_join_chain_resolved += 1
                    logger.debug(
                        "element %s: join-chain ref %r resolved intra-DM to "
                        "source=%r column=%r (upstream=%s)",
                        element.elementId,
                        ref.raw,
                        source,
                        canonical,
                        surviving[0],
                    )
                    return True

            # Cross-DM: the owning element may live in a source data model even
            # when an earlier segment matched a local sibling.
            source_dm_url_ids = {
                sid.partition("/")[0]
                for sid in element.source_ids
                if "/" in sid and not sid.startswith("inode-")
            }
            for dm_url_id in sorted(source_dm_url_ids):
                for urn in sorted(
                    self.dm_element_urn_by_name.get(dm_url_id, {}).get(
                        source.lower(), []
                    )
                ):
                    if urn == element_dataset_urn:
                        continue
                    canonical = (self.dm_element_urn_to_cols.get(urn) or {}).get(
                        col.lower()
                    )
                    if canonical is None:
                        continue
                    upstream_field = builder.make_schema_field_urn(urn, canonical)
                    pair = (downstream_field, upstream_field)
                    if pair not in emitted_pairs:
                        emitted_pairs.add(pair)
                        cross_dm_fgls.append(
                            FineGrainedLineageClass(
                                downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                                downstreams=[downstream_field],
                                upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                                upstreams=[upstream_field],
                                confidenceScore=1.0,
                            )
                        )
                    self.reporter.data_model_element_fgl_join_chain_resolved += 1
                    logger.debug(
                        "element %s: join-chain ref %r resolved cross-DM to "
                        "source=%r column=%r (upstream=%s)",
                        element.elementId,
                        ref.raw,
                        source,
                        canonical,
                        urn,
                    )
                    return True
        # Sub-count of whichever residual bucket the legacy path settles on --
        # never an independent drop, so report totals stay additive.
        self.reporter.data_model_element_fgl_join_chain_unresolved += 1
        logger.debug(
            "element %s: no candidate split of ref %r resolved; candidates "
            "tried=%r verdicts=%r; falling back to the first-slash split",
            element.elementId,
            ref.raw,
            candidate_source_column_splits(ref),
            trace,
        )
        return False

    def _resolve_non_sibling_ref(
        self,
        *,
        ref: "BracketRef",
        element: SigmaDataModelElement,
        column: Optional[SigmaDataModelColumn],
        element_dataset_urn: str,
        entity_level_upstream_urns: Set[str],
        downstream_field: str,
        warehouse_url_id_map: Dict[str, _WarehouseTableRef],
        emitted_pairs: Set[Tuple[str, str]],
        fgls: List[FineGrainedLineageClass],
    ) -> Optional[FineGrainedLineageClass]:
        """Resolve a ref whose source is not a sibling element in this DM.

        Three attempts, in descending order of how much of the match is
        evidenced rather than inferred:

        1. a warehouse table the element itself DECLARES (exact table match);
        2. an element in another Data Model (exact name and column match);
        3. a warehouse table found by NAME in the tenant-wide /v2/files listing,
           which nothing declares -- the last resort, and the only one that can
           trigger the extra listing call.

        Steps 1 and 3 append to ``fgls`` themselves and return None on success;
        step 2's result is returned for the caller to append to
        ``cross_dm_fgls``.
        """
        if self._try_resolve_warehouse_table_name_ref(
            ref=ref,
            element=element,
            column=column,
            downstream_field=downstream_field,
            warehouse_url_id_map=warehouse_url_id_map,
            emitted_pairs=emitted_pairs,
            fgls=fgls,
            allow_global_name_index=False,
        ):
            return None
        cross_dm = self._resolve_cross_dm_fgl(
            ref=ref,
            element=element,
            element_dataset_urn=element_dataset_urn,
            entity_level_upstream_urns=entity_level_upstream_urns,
            downstream_field=downstream_field,
        )
        if cross_dm is not None:
            return cross_dm
        # Nothing in the tenant's own Sigma graph names this source. Only now is
        # the global /v2/files listing worth the call: it is the sole remaining
        # place the referenced table could be described.
        self._try_resolve_warehouse_table_name_ref(
            ref=ref,
            element=element,
            column=column,
            downstream_field=downstream_field,
            warehouse_url_id_map=warehouse_url_id_map,
            emitted_pairs=emitted_pairs,
            fgls=fgls,
            allow_global_name_index=True,
        )
        return None

    def _try_resolve_warehouse_table_name_ref(
        self,
        *,
        ref: "BracketRef",
        element: SigmaDataModelElement,
        column: Optional[SigmaDataModelColumn],
        downstream_field: str,
        warehouse_url_id_map: Dict[str, _WarehouseTableRef],
        emitted_pairs: Set[Tuple[str, str]],
        fgls: List[FineGrainedLineageClass],
        allow_global_name_index: bool,
    ) -> bool:
        """Resolve a ref naming a warehouse TABLE the element declares.

        The columnId path handles pass-through columns, whose columnId is
        ``inode-<urlId>/<NATIVE>``. A calculated or renamed column has an opaque
        columnId instead, yet its formula still names the warehouse table --
        e.g. ``[WAREHOUSE_TABLE_A/Col Id]`` on an element
        declaring that table's inode. Those refs previously resolved to nothing
        at all: no intra-DM element bears the table's name, and there are no
        cross-DM sources to search.

        The table match is exact (the element declares that inode and the ref
        names that table). Only the warehouse column name is inferred, so the
        edge is emitted at a reduced confidence.

        With ``allow_global_name_index`` the search widens to the tenant-wide
        /v2/files table listing when the element declares no table of that name
        -- Sigma also under-reports an element's tables, so a formula can name a
        real warehouse table that appears in neither the element's source_ids
        nor its Data Model's /lineage. That is strictly a last resort: it is the
        only path that can trigger the listing call, and the table identity is
        then inferred rather than declared, so callers should try every exact
        match first.
        """
        if ref.column is None:
            return False
        wanted = ref.source.strip().lower()
        matches = [
            (sid[len("inode-") :], warehouse_url_id_map[sid[len("inode-") :]])
            for sid in element.source_ids
            if sid.startswith("inode-")
            and sid[len("inode-") :] in warehouse_url_id_map
            and warehouse_url_id_map[sid[len("inode-") :]].table.strip().lower()
            == wanted
        ]
        if len(matches) > 1:
            logger.debug(
                "WAREHOUSE NAME REF ambiguous: element %s ref %r matched %d "
                "declared warehouse tables; skipping",
                element.elementId,
                ref.raw,
                len(matches),
            )
            return False
        derived_globally = not matches
        if derived_globally:
            if not allow_global_name_index:
                # Deliberately quiet: the caller will retry with the global
                # index enabled once the exact paths have all failed.
                return False
            if not any(sid.startswith("inode-") for sid in element.source_ids):
                # This element reads from no inode at all, so a bare name in its
                # formula is not plausibly a warehouse table -- and searching the
                # tenant-wide listing for it would only risk a same-named
                # coincidence. Also keeps elements sourced purely from other Data
                # Models from triggering the listing call.
                logger.debug(
                    "WAREHOUSE NAME REF: element %s declares no inode source; "
                    "not searching the global /v2/files index for ref %r",
                    element.elementId,
                    ref.raw,
                )
                return False
            # The element declares no table by this name. Sigma under-reports
            # here exactly as it does for url_ids, so consult the tenant-wide
            # /v2/files table listing before giving up.
            logger.debug(
                "WAREHOUSE NAME REF: element %s ref %r names no warehouse table "
                "the element declares (declared: %r); trying the global index",
                element.elementId,
                ref.raw,
                sorted(
                    warehouse_url_id_map[sid[len("inode-") :]].table
                    for sid in element.source_ids
                    if sid.startswith("inode-")
                    and sid[len("inode-") :] in warehouse_url_id_map
                ),
            )
            global_ref = self._lookup_global_warehouse_table_by_name(
                table_name=ref.source, warehouse_map=warehouse_url_id_map
            )
            if global_ref is None:
                return False
            url_id, wh_ref = "", global_ref
            parent_urn = self._warehouse_urn_from_ref(
                global_ref, context=f"global table name {ref.source!r}"
            )
        else:
            url_id, wh_ref = matches[0]
            parent_urn = self._resolve_dm_element_warehouse_upstream(
                url_id_suffix=url_id, warehouse_map=warehouse_url_id_map
            )
        record = self.connection_registry.get(wh_ref.connection_id)
        if parent_urn is None or record is None:
            return False
        conn_override = self.config.connection_to_platform_map.get(wh_ref.connection_id)
        lowercase = conn_override.convert_urns_to_lowercase if conn_override else True
        # Prefer the name Sigma already recorded. Deriving it from the display
        # name is a convention ("Order Ref Id" -> ORDER_REF_ID) that a
        # renamed column breaks without saying so.
        # The prefixes this element can legitimately produce: its own id, and
        # any warehouse inode it declares.
        exact = _native_column_from_column_id(
            column.columnId if column else None,
            allowed_prefixes={element.elementId, *element.source_ids},
        )
        native = _normalize_warehouse_identifier(
            exact
            if exact is not None
            else _warehouse_column_from_display_name(ref.column),
            record.datahub_platform,
            lowercase,
        )
        upstream_field = builder.make_schema_field_urn(parent_urn, native)
        pair = (downstream_field, upstream_field)
        if pair not in emitted_pairs:
            emitted_pairs.add(pair)
            fgls.append(
                FineGrainedLineageClass(
                    downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                    downstreams=[downstream_field],
                    upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                    upstreams=[upstream_field],
                    confidenceScore=(
                        _FGL_CONFIDENCE_WAREHOUSE_GLOBAL_NAME_DERIVED
                        if derived_globally
                        else _FGL_CONFIDENCE_WAREHOUSE_NAME_EXACT_COLUMN
                        if exact is not None
                        else _FGL_CONFIDENCE_WAREHOUSE_NAME_DERIVED
                    ),
                )
            )
        self.reporter.data_model_element_fgl_warehouse_table_name_resolved += 1
        if derived_globally:
            self.reporter.data_model_element_fgl_warehouse_global_name_resolved += 1
        logger.debug(
            "WAREHOUSE NAME REF hit (%s): element %s ref %r -> table %r "
            "(url_id=%r), column display %r -> native %r, upstream=%s",
            "global /v2/files index" if derived_globally else "element-declared",
            element.elementId,
            ref.raw,
            wh_ref.table,
            url_id or "n/a",
            ref.column,
            native,
            parent_urn,
        )
        return True

    def _admitted_element_types(self) -> FrozenSet[str]:
        """The workbook element types this run actually ingests.

        Mirrors SigmaAPI's own narrowing so a log line cannot name a type that
        was never admitted.
        """
        return (
            INGESTED_ELEMENT_TYPES
            if self.config.ingest_pivot_and_input_tables
            else BASE_ELEMENT_TYPES
        )

    def _resolve_no_ref_column_fgl(
        self,
        *,
        column: SigmaDataModelColumn,
        warehouse_fgl: Optional[FineGrainedLineageClass],
        downstream_field: str,
        fgls: List[FineGrainedLineageClass],
        emitted_pairs: Set[Tuple[str, str]],
    ) -> None:
        """Handle a column no bracket ref could be resolved from.

        Covers an empty/absent formula (Sigma's shape for a pass-through
        column), a constant expression, and a formula whose only refs are
        parameters or bare sibling-column refs. In each case no ref reached a
        resolver, so the columnId-driven warehouse FGL -- which needs no formula
        at all -- would otherwise be computed and discarded.

        Callers must gate this on "no ref reached a resolver" rather than on
        ``warehouse_consumed``: intra-DM resolution never sets that flag, so a
        "no warehouse append happened" gate would give a column carrying both a
        sibling ref and an inode columnId a second, wrong upstream.
        """
        if warehouse_fgl is None:
            # Split the same way the ref-bearing path does: an inode-shaped
            # columnId that failed to resolve is a real warehouse failure (a
            # /files miss or an unmappable connection) and is worth chasing,
            # while any other columnId is an intra-DM or Sigma Dataset
            # passthrough with nothing to resolve against -- expected volume.
            if _is_warehouse_column_id(column.columnId):
                self.reporter.data_model_element_fgl_no_ref_warehouse_unresolved += 1
                logger.debug(
                    "column %r has a warehouse-shaped columnId %r but no ref "
                    "resolved and warehouse resolution failed; no CLL emitted",
                    column.name,
                    column.columnId,
                )
            else:
                self.reporter.data_model_element_fgl_no_ref_unresolved += 1
                logger.debug(
                    "column %r produced no resolvable ref and its columnId %r "
                    "is not warehouse-shaped; no CLL emitted (formula=%r)",
                    column.name,
                    column.columnId,
                    column.formula,
                )
            return
        assert warehouse_fgl.upstreams
        # No emitted_pairs check: this runs at most once per column, only when
        # nothing in the ref loop appended for that column, and downstream_field
        # is unique per column (columns are deduped by name upstream) -- so the
        # pair cannot already be present.
        emitted_pairs.add((downstream_field, warehouse_fgl.upstreams[0]))
        fgls.append(warehouse_fgl)
        self.reporter.data_model_element_fgl_warehouse_resolved += 1

    def _get_dm_spec_index(self, data_model: SigmaDataModel) -> DataModelSpecIndex:
        """Per-run cached join-key index for one Data Model.

        Costs one ``/spec`` call per Data Model. That is negligible beside the
        per-element fan-out that dominates a run, and unlike the warehouse
        listing it cannot be deferred to a failure path: join-key edges are
        ADDITIONAL to the edges a formula produces, so a column that resolved
        perfectly well may still be missing its other side.
        """
        dm_id = data_model.dataModelId
        if not self.config.extract_join_key_lineage:
            return DataModelSpecIndex()
        cached = self._dm_spec_index_cache.get(dm_id)
        if cached is None:
            cached = parse_data_model_spec(
                self.sigma_api.get_data_model_spec(dm_id), data_model_id=dm_id
            )
            self._dm_spec_index_cache[dm_id] = cached
            self.reporter.data_model_join_key_pairs_read += len(cached.pairs)
            self.reporter.data_model_join_warehouse_side_predicates += (
                cached.warehouse_side_predicates
            )
            self.reporter.data_model_join_elements_unreadable += len(
                cached.unreadable_join_element_ids
            )
            self.reporter.data_model_union_output_columns_read += len(cached.unions)
            self.reporter.data_model_union_branch_index_out_of_range += (
                cached.union_branch_index_out_of_range
            )
        return cached

    def _build_join_partner_map(
        self,
        *,
        spec: DataModelSpecIndex,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Dict[Tuple[str, str], Set[Tuple[str, str, str, bool]]]:
        """(urn, column name) -> (join element id, partner urn, partner column, is_outer).

        A predicate side names its column by DISPLAY NAME, but columnId is also
        accepted: the spec does not label which it uses, and matching both costs
        nothing while making the lookup robust if Sigma switches.

        The join element id travels with each partner because a predicate is
        only evidence for elements that read THROUGH that join. Without it the
        map is Data-Model-wide and any element referencing a key column would
        inherit the other side, inventing lineage for a path the join is not on.
        """
        col_name_by_key: Dict[str, Dict[str, str]] = {}
        for dm_el in data_model.elements:
            winners, _ = _dedup_dm_element_columns(dm_el.columns)
            keys: Dict[str, str] = {}
            for col in winners.values():
                if col.columnId:
                    keys[col.columnId] = col.name
                keys[col.name.strip().lower()] = col.name
            col_name_by_key[dm_el.elementId] = keys

        # Data Model keys this model's elements read from, by slug. A /spec
        # join side can name an element in ANOTHER model, and these are the
        # only models it can plausibly be in.
        source_dm_keys = {
            sid.partition("/")[0]
            for el in data_model.elements
            for sid in el.source_ids
            if "/" in sid and not sid.startswith("inode-")
        }

        def resolve_foreign(side: SpecColumnRef) -> Optional[Tuple[str, str]]:
            """A side naming an element in a different Data Model.

            Element ids repeat across models, so the model has to be pinned:
            by the side's own ``dataModelId`` when Sigma sends one, else by the
            models this one actually sources from. More than one candidate is
            refused rather than guessed -- picking wrong would attach a real
            column to the wrong dataset, which is worse than no edge.
            """
            assert side.element_id is not None
            eid = side.element_id
            owning = self.dm_keys_by_element_id.get(eid, set())
            if side.data_model_id:
                candidates = owning & {side.data_model_id}
                basis = "side.dataModelId"
            else:
                candidates = owning & source_dm_keys
                basis = "source_ids of this model"
                if not candidates and len(owning) == 1:
                    candidates = set(owning)
                    basis = "sole owning model"
            if not candidates:
                self.reporter.data_model_join_key_foreign_dm_unknown += 1
                logger.debug(
                    "JOIN KEY FOREIGN %s: element %r column %r -- no Data Model "
                    "pinned (side.dataModelId=%r, models defining this id=%r, "
                    "models this one sources from=%r)",
                    data_model.dataModelId,
                    eid,
                    side.column,
                    side.data_model_id,
                    sorted(owning)[:10],
                    sorted(source_dm_keys)[:10],
                )
                return None
            if len(candidates) > 1:
                self.reporter.data_model_join_key_foreign_ambiguous += 1
                logger.debug(
                    "JOIN KEY FOREIGN %s: element %r column %r is defined in "
                    "%d candidate Data Models %r (basis=%s) -- refusing to guess",
                    data_model.dataModelId,
                    eid,
                    side.column,
                    len(candidates),
                    sorted(candidates)[:10],
                    basis,
                )
                return None
            key = next(iter(candidates))
            urn = self.dm_element_urn_by_key_and_eid.get((key, eid))
            cols = self.dm_element_urn_to_cols.get(urn or "") or {}
            name = cols.get(side.column.strip().lower())
            if urn is None or name is None:
                self.reporter.data_model_join_key_foreign_column_absent += 1
                logger.debug(
                    "JOIN KEY FOREIGN %s: element %r resolved to model %r "
                    "(basis=%s, urn=%s) but column %r is not among its %d "
                    "columns",
                    data_model.dataModelId,
                    eid,
                    key,
                    basis,
                    urn,
                    side.column,
                    len(cols),
                )
                return None
            self.reporter.data_model_join_key_foreign_resolved += 1
            logger.debug(
                "JOIN KEY FOREIGN %s: element %r column %r -> %s/%s "
                "(model %r, basis=%s)",
                data_model.dataModelId,
                eid,
                side.column,
                urn,
                name,
                key,
                basis,
            )
            return (urn, name)

        def resolve(side: SpecColumnRef) -> Optional[Tuple[str, str]]:
            if side.element_id is None:
                return None
            keys = col_name_by_key.get(side.element_id)
            urn = elementId_to_dataset_urn.get(side.element_id)
            if keys is None or urn is None:
                # Not an element of THIS model. On one tenant a single shared
                # mapping element accounted for 9 of 10 unresolved predicates,
                # joined into nine different models.
                return resolve_foreign(side)
            name = keys.get(side.column) or keys.get(side.column.strip().lower())
            if name is None:
                logger.debug(
                    "JOIN KEY DM %s: local element %r has no column matching "
                    "%r among its %d columns",
                    data_model.dataModelId,
                    side.element_id,
                    side.column,
                    len(keys),
                )
                return None
            return (urn, name)

        partners: Dict[Tuple[str, str], Set[Tuple[str, str, str, bool]]] = {}
        for predicate in spec.pairs:
            a, b = resolve(predicate.left), resolve(predicate.right)
            if a is None or b is None:
                self.reporter.data_model_join_key_partner_unresolved += 1
                # Name WHICH side failed and what it referenced. A side is a
                # Sigma formula, so this line is the only place that shows both
                # the raw expression and the column extracted from it.
                logger.debug(
                    "JOIN KEY DM %s: predicate from join element %s did not "
                    "resolve -- left(element=%s column=%r expr=%r)=%s "
                    "right(element=%s column=%r expr=%r)=%s",
                    data_model.dataModelId,
                    predicate.join_element_id,
                    predicate.left.element_id,
                    predicate.left.column,
                    predicate.left.expression,
                    "ok" if a else "UNRESOLVED",
                    predicate.right.element_id,
                    predicate.right.column,
                    predicate.right.expression,
                    "ok" if b else "UNRESOLVED",
                )
                continue
            join_id = predicate.join_element_id
            outer = predicate.is_outer
            partners.setdefault(a, set()).add((join_id, b[0], b[1], outer))
            partners.setdefault(b, set()).add((join_id, a[0], a[1], outer))
        logger.debug(
            "JOIN KEY DM %s: %d predicate(s) -> %d column(s) with partners",
            data_model.dataModelId,
            len(spec.pairs),
            len(partners),
        )
        return partners

    def _dm_element_ancestors(self, data_model: SigmaDataModel) -> Dict[str, Set[str]]:
        """elementId -> every intra-DM element it reads from, plus itself.

        Memoized per Data Model: the walk is O(elements x edges) and every
        element of the model asks for it.

        ``source_ids`` entries are ``inode-<urlId>`` for a warehouse table and
        ``<dm-url-id>/<suffix>`` for another Data Model; a bare id is a sibling
        element in this model. Only the last kind can carry a join, so only it
        is walked.
        """
        memo = self._dm_ancestors_cache.get(data_model.dataModelId)
        if memo is not None:
            return memo
        direct: Dict[str, Set[str]] = {}
        for el in data_model.elements:
            direct[el.elementId] = {
                sid
                for sid in el.source_ids
                # ``sid`` truthiness matters: one tenant's source_ids carried an
                # empty entry, which would otherwise sit in the closure as a
                # blank element id.
                if sid and "/" not in sid and not sid.startswith("inode-")
            }
        closure: Dict[str, Set[str]] = {}
        for start in direct:
            seen = {start}
            stack = list(direct[start])
            while stack:
                node = stack.pop()
                if node in seen:
                    continue
                seen.add(node)
                stack.extend(direct.get(node, ()))
            closure[start] = seen
        self._dm_ancestors_cache[data_model.dataModelId] = closure
        return closure

    @staticmethod
    def _dm_element_column_lookup(
        element: SigmaDataModelElement,
    ) -> Dict[str, str]:
        """Column id AND lowercased display name -> canonical display name.

        /spec names a column without saying which of the two it used, the same
        ambiguity join predicate sides have, so both are accepted.
        """
        winners, _ = _dedup_dm_element_columns(element.columns)
        keys: Dict[str, str] = {}
        for col in winners.values():
            if col.columnId:
                keys[col.columnId] = col.name
            keys[col.name.strip().lower()] = col.name
        return keys

    def _add_union_fgls(
        self,
        *,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
        fgls: List[FineGrainedLineageClass],
        emitted_pairs: Set[Tuple[str, str]],
        discovered_upstreams: Set[str],
    ) -> None:
        """Add one edge per branch for every output column of a union element.

        A union's output column has an upstream in EVERY branch, but its
        /columns formula names at most one of them, so all the other branches
        are unreachable from formulas alone -- the same multi-source blind spot
        joins have, and the reason a union element's downstreams looked
        single-sourced.

        The edges score 1.0: a union stacks rows, so the output column IS the
        branch column, not a value derived from it.
        """
        spec = self._get_dm_spec_index(data_model)
        if not spec.unions:
            return
        outputs = [u for u in spec.unions if u.union_element_id == element.elementId]
        if not outputs:
            return
        own_columns = self._dm_element_column_lookup(element)
        branch_columns: Dict[str, Dict[str, str]] = {}
        for dm_el in data_model.elements:
            branch_columns[dm_el.elementId] = self._dm_element_column_lookup(dm_el)

        added = 0
        for output in outputs:
            downstream_name = own_columns.get(output.output_column) or own_columns.get(
                output.output_column.strip().lower()
            )
            if downstream_name is None:
                self.reporter.data_model_union_output_column_absent += 1
                logger.debug(
                    "UNION DM %s element %s: output column %r is not among the "
                    "element's %d /columns entries -- no edge for its %d "
                    "branch(es)",
                    data_model.dataModelId,
                    element.elementId,
                    output.output_column,
                    len(element.columns),
                    len(output.branches),
                )
                continue
            downstream_field = builder.make_schema_field_urn(
                element_dataset_urn, downstream_name
            )
            for branch_element_id, branch_column in output.branches:
                branch_urn = elementId_to_dataset_urn.get(branch_element_id)
                if branch_urn is None:
                    # The branch is filtered out of this run, or lives in
                    # another Data Model. /spec gives no dataModelId on a union
                    # source, so there is nothing to pin it with.
                    self.reporter.data_model_union_branch_element_unknown += 1
                    logger.debug(
                        "UNION DM %s element %s: branch element %r for output "
                        "column %r is not an element of this Data Model",
                        data_model.dataModelId,
                        element.elementId,
                        branch_element_id,
                        output.output_column,
                    )
                    continue
                keys = branch_columns.get(branch_element_id) or {}
                upstream_name = keys.get(branch_column) or keys.get(
                    branch_column.strip().lower()
                )
                if upstream_name is None:
                    self.reporter.data_model_union_branch_column_absent += 1
                    logger.debug(
                        "UNION DM %s element %s: output column %r names column "
                        "%r in branch %s, but that branch has %d columns and "
                        "none of them match by id or by name",
                        data_model.dataModelId,
                        element.elementId,
                        output.output_column,
                        branch_column,
                        branch_element_id,
                        len(keys),
                    )
                    continue
                upstream_field = builder.make_schema_field_urn(
                    branch_urn, upstream_name
                )
                pair = (downstream_field, upstream_field)
                if pair in emitted_pairs or downstream_field == upstream_field:
                    continue
                emitted_pairs.add(pair)
                fgls.append(
                    FineGrainedLineageClass(
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field],
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[upstream_field],
                        confidenceScore=_FGL_CONFIDENCE_UNION_BRANCH,
                    )
                )
                discovered_upstreams.add(branch_urn)
                added += 1
                self.reporter.data_model_element_fgl_union_resolved += 1
        logger.debug(
            "UNION DM %s element %s: %d output column(s) in /spec produced %d edge(s)",
            data_model.dataModelId,
            element.elementId,
            len(outputs),
            added,
        )

    def _add_join_key_fgls(
        self,
        *,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
        urn_to_cols: Dict[str, Dict[str, str]],
        fgls: List[FineGrainedLineageClass],
        cross_dm_fgls: List[FineGrainedLineageClass],
        emitted_pairs: Set[Tuple[str, str]],
        discovered_upstreams: Set[str],
    ) -> None:
        """Add the other side of every join predicate an edge already touches.

        A join's output column carries a formula naming ONE side, so /columns
        alone can only ever produce one edge -- the reported symptom. The ON
        clause says the two key columns hold the same value, which makes the
        unnamed side just as much an upstream of that output column. Only
        columns an edge already reaches are expanded, so this never invents
        lineage for a column the formulas said nothing about.

        A predicate applies only to elements that read THROUGH its join. Two
        elements can reference the same key column while only one of them sits
        downstream of the join that constrains it; expanding the other would
        assert an equality its data path never applies. The join element must
        therefore be in the element's own upstream closure, and predicates
        skipped by that test are counted in
        ``data_model_join_key_out_of_join_path``.

        The predicate is an equality, not a copy, so these edges score below a
        formula-derived one: consumers wanting only value-propagation lineage
        can filter them out by confidence.
        """
        spec = self._get_dm_spec_index(data_model)
        if not spec.pairs:
            return
        # Built once per Data Model, not once per element. Rebuilding it per
        # element re-walked every element's columns and re-counted every
        # unresolved predicate -- on one tenant (2026-09) 61 predicates were reported as
        # 1,234 failures, which made the counter unreadable.
        # Keyed by dataModelId alone. The element->URN map was part of the key
        # via id(), which added nothing (the URNs are a pure function of the
        # model) and risked a reused address matching a different map.
        dm_id = data_model.dataModelId
        partners = self._join_partner_cache.get(dm_id)
        if partners is None:
            partners = self._build_join_partner_map(
                spec=spec,
                data_model=data_model,
                elementId_to_dataset_urn=elementId_to_dataset_urn,
            )
            self._join_partner_cache = {dm_id: partners}
        if not partners:
            return
        element_id_by_urn = {urn: eid for eid, urn in elementId_to_dataset_urn.items()}
        # The joins this element actually reads through: itself (a join
        # element's own output IS the join) plus every intra-DM ancestor.
        in_scope_joins = self._dm_element_ancestors(data_model).get(
            element.elementId, {element.elementId}
        )

        added = 0
        # (upstream dataset urn, field) pairs this element's edges actually
        # reached. Keyed by URN, not element id, so the diagnostic below can be
        # compared key-for-key against the partner map.
        looked_up: Set[Tuple[str, str]] = set()
        for fgl in list(fgls) + list(cross_dm_fgls):
            if not fgl.upstreams or not fgl.downstreams:
                continue
            downstream_field = fgl.downstreams[0]
            try:
                upstream = SchemaFieldUrn.from_string(fgl.upstreams[0])
            except InvalidUrnError:
                continue
            parent = str(upstream.parent)
            parent_key = (parent, upstream.field_path)
            if parent not in element_id_by_urn and parent_key not in partners:
                # A warehouse table, or an element of another Data Model that
                # no predicate in this model's spec names. The membership test
                # cannot be "is it one of OUR elements" -- a join whose two
                # sides both live in other models is exactly the case that
                # needs this map, and it was silently dropped here while the
                # partner map had already resolved both sides.
                self.reporter.data_model_join_key_parent_outside_model += 1
                continue
            looked_up.add(parent_key)
            for join_element_id, partner_urn, partner_col, is_outer in sorted(
                partners.get((parent, upstream.field_path), set())
            ):
                if join_element_id not in in_scope_joins:
                    # This element references a key column but does not read
                    # through the join that constrains it.
                    self.reporter.data_model_join_key_out_of_join_path += 1
                    logger.debug(
                        "JOIN KEY DM %s element %s: predicate from join element "
                        "%s names %s/%s, but that join is not in this element's "
                        "upstream closure %r -- skipping",
                        data_model.dataModelId,
                        element.elementId,
                        join_element_id,
                        element_id_by_urn.get(parent, parent),
                        upstream.field_path,
                        sorted(in_scope_joins)[:10],
                    )
                    continue
                if partner_urn == element_dataset_urn:
                    # The partner is this element itself; a self-loop upstream
                    # is not lineage.
                    continue
                canonical = (urn_to_cols.get(partner_urn) or {}).get(
                    partner_col.lower(), partner_col
                )
                partner_field = builder.make_schema_field_urn(partner_urn, canonical)
                if partner_field == downstream_field:
                    # A self-join can pair a column back to itself.
                    continue
                pair = (downstream_field, partner_field)
                if pair in emitted_pairs:
                    continue
                emitted_pairs.add(pair)
                fgls.append(
                    FineGrainedLineageClass(
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        downstreams=[downstream_field],
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        upstreams=[partner_field],
                        confidenceScore=(
                            _FGL_CONFIDENCE_JOIN_KEY_OUTER
                            if is_outer
                            else _FGL_CONFIDENCE_JOIN_KEY
                        ),
                    )
                )
                discovered_upstreams.add(partner_urn)
                added += 1
                self.reporter.data_model_element_fgl_join_key_resolved += 1
                logger.debug(
                    "JOIN KEY DM %s element %s: %s already links to %s/%s; a "
                    "join predicate equates that with %s, so adding it",
                    data_model.dataModelId,
                    element.elementId,
                    downstream_field,
                    element_id_by_urn.get(parent, parent),
                    upstream.field_path,
                    partner_field,
                )
        if added:
            logger.debug(
                "JOIN KEY DM %s element %s: added %d edge(s) from join predicates",
                data_model.dataModelId,
                element.elementId,
                added,
            )
        elif looked_up:
            # The remaining way this can produce nothing, and the only one with
            # no counter of its own: predicates resolved into partners, but no
            # edge this element already has lands on a column any predicate
            # names. Without both sets side by side the report would say
            # "0 join-key edges" for a reason no counter distinguishes.
            self.reporter.data_model_join_key_no_matching_edge += 1
            logger.debug(
                "JOIN KEY DM %s element %s: %d partner column(s) available but "
                "none matched this element's %d upstream field(s). "
                "looked_up=%r available=%r",
                data_model.dataModelId,
                element.elementId,
                len(partners),
                len(looked_up),
                sorted(looked_up)[:10],
                sorted(partners)[:10],
            )

    @staticmethod
    def _log_dm_column_outcome(
        *,
        data_model_id: str,
        element_id: str,
        column: SigmaDataModelColumn,
        resolution_attempted: bool,
        new_fgls: List[FineGrainedLineageClass],
    ) -> None:
        """Per-column verdict line for the Data Model FGL builder.

        Guarded, and extracted for it: logger.debug evaluates its arguments
        eagerly, so re-parsing every formula for a line the default log level
        discards is pure waste on a path that runs for every column of every
        element.
        """
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            "COLUMN DM %s element %s %r: columnId=%r formula=%r refs=%r "
            "ref_resolution_attempted=%s -> emitted=%r",
            data_model_id,
            element_id,
            column.name,
            column.columnId,
            column.formula,
            [r.raw for r in extract_bracket_refs(column.formula)],
            resolution_attempted,
            [(f.upstreams or [""])[0] for f in new_fgls],
        )

    def _build_dm_element_fine_grained_lineages(
        self,
        *,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
        element_name_to_eids: Dict[str, List[str]],
        elementId_to_dataset_urn: Dict[str, str],
        entity_level_upstream_urns: Set[str],
        data_model: SigmaDataModel,
        warehouse_url_id_map: Dict[str, _WarehouseTableRef],
        discovered_upstreams: Set[str],
    ) -> List[FineGrainedLineageClass]:
        """Build FineGrainedLineage entries for intra-DM [ElementName/col] refs.

        ``discovered_upstreams`` is an OUT parameter: elements reached only
        through a join chain are not in Sigma's direct /lineage list, so the
        caller must add them to the entity-level upstreams or the emitted
        schemaField would reference a Dataset absent from ``upstreams``.

        element_name_to_eids maps lowercased element name → list of elementIds.
        Self-references are stripped before resolution: when a DM element is named
        after its warehouse source (e.g., element "data.csv" with formula
        "[data.csv/col]"), the formula ref matches the element's own name and must
        be filtered out to avoid self-referential FGL.  Warehouse-passthrough
        refs are resolved via _try_emit_warehouse_passthrough_fgl; unresolved
        remainder is counted under fgl_warehouse_passthrough_deferred.

        Columns from which no ref reached a resolver -- an empty/absent formula
        (Sigma's shape for a pass-through column), a constant, or a formula whose
        only refs are parameters or bare sibling-column refs -- are handled by
        _resolve_no_ref_column_fgl, which still resolves warehouse lineage from
        columnId and counts the remainder under fgl_no_ref_warehouse_unresolved
        (columnId named a warehouse column but did not resolve) or
        fgl_no_ref_unresolved (nothing to resolve against). Those columns are
        never name-matched against siblings: with no ref to resolve, matching on
        column name alone would fabricate an edge to both sides of every join.

        When multiple sibling elements share a name and both pass the /lineage filter,
        the lexicographically-first URN is chosen (matching the collision policy
        used elsewhere in this connector and Sigma's server-side coalescing behaviour).
        """
        by_name, _ = _dedup_dm_element_columns(element.columns)

        # Build URN → winner-column map directly from elementId_to_dataset_urn,
        # which gives an unambiguous elementId→URN mapping for every DM element.
        urn_to_cols: Dict[str, Dict[str, str]] = {}
        for dm_el in data_model.elements:
            el_urn = elementId_to_dataset_urn.get(dm_el.elementId)
            if el_urn:
                el_by_name, _ = _dedup_dm_element_columns(dm_el.columns)
                urn_to_cols[el_urn] = {c.lower(): c for c in el_by_name}

        fgls: List[FineGrainedLineageClass] = []
        cross_dm_fgls: List[FineGrainedLineageClass] = []
        # Track emitted (downstream, upstream) schemaField pairs to deduplicate
        # multiple occurrences of the same bracket ref in one formula
        # (e.g. If([A/x] = 0, [A/x], [A/x] / 2) → one FGL, not three).
        emitted_pairs: Set[Tuple[str, str]] = set()
        for column in sorted(by_name.values(), key=lambda c: c.name):
            # Decision record: every column emits exactly one summary line
            # naming what it resolved to, or nothing. Failure paths each log
            # their own reason, but successes were silent, so an element whose
            # columns all resolved produced no output at all and there was no
            # way to tell WHICH upstream a given column bound to.
            fgl_mark = len(fgls)
            cross_mark = len(cross_dm_fgls)
            downstream_field = builder.make_schema_field_urn(
                element_dataset_urn, column.name
            )
            # Compute warehouse FGL once per column. columnId encodes a single
            # warehouse column identity regardless of how many bracket refs the
            # formula contains, so calling _try_emit per-ref would emit duplicates
            # for multi-ref formulas like concat([T/a], [T/b]).
            warehouse_fgl = self._try_emit_warehouse_passthrough_fgl(
                column=column,
                element=element,
                downstream_field=downstream_field,
                warehouse_url_id_map=warehouse_url_id_map,
            )
            warehouse_consumed = False
            # True once any ref has reached a resolver. Distinct from
            # `warehouse_consumed`, which only the warehouse branches set:
            # gating the no-ref fallback on that would append a second, wrong
            # upstream to a column that already resolved intra-DM.
            resolution_attempted = False
            for ref in extract_bracket_refs(column.formula):
                if ref.is_parameter or ref.column is None:
                    # [P_*] parameter refs and bare [col] intra-element refs
                    # are not cross-Dataset lineage; skip.
                    continue
                resolution_attempted = True

                # Join-chain refs ([JoinElement/SourceElement/Column], nesting
                # further for chained joins) name the owning element in the
                # second-to-last segment, not the first. Try the alternative
                # readings before the legacy first-slash split, which would
                # resolve to the wrong sibling and then fail the column check.
                # Gated on >=3 segments so single-slash refs keep exactly their
                # current path, counters included.
                if self._try_resolve_join_chain_ref(
                    discovered_upstreams=discovered_upstreams,
                    ref=ref,
                    element=element,
                    element_dataset_urn=element_dataset_urn,
                    element_name_to_eids=element_name_to_eids,
                    elementId_to_dataset_urn=elementId_to_dataset_urn,
                    entity_level_upstream_urns=entity_level_upstream_urns,
                    urn_to_cols=urn_to_cols,
                    downstream_field=downstream_field,
                    emitted_pairs=emitted_pairs,
                    fgls=fgls,
                    cross_dm_fgls=cross_dm_fgls,
                ):
                    continue

                candidate_eids = element_name_to_eids.get(ref.source.lower(), [])

                # Strip self-references: element-name == warehouse-table name is a
                # common Sigma authoring pattern.  The formula ref resolves to the
                # element itself, which is not a valid FGL upstream (the real upstream
                # is the warehouse inode reported by /lineage).
                candidate_eids_after_self_strip = [
                    eid for eid in candidate_eids if eid != element.elementId
                ]

                if not candidate_eids_after_self_strip:
                    if candidate_eids:
                        # All intra-DM candidates were self-references. Two scenarios:
                        # (a) element named after its warehouse source → warehouse FGL
                        # (b) element named after a cross-DM source element → cross-DM FGL
                        # Try cross-DM first; returns True if a cross-DM FGL was emitted.
                        if self._try_emit_self_named_cross_dm_fgl(
                            ref=ref,
                            element=element,
                            element_dataset_urn=element_dataset_urn,
                            entity_level_upstream_urns=entity_level_upstream_urns,
                            downstream_field=downstream_field,
                            emitted_pairs=emitted_pairs,
                            cross_dm_fgls=cross_dm_fgls,
                        ):
                            continue
                        # No cross-DM match; element is named after its warehouse source.
                        if not warehouse_consumed:
                            warehouse_consumed = True
                            if warehouse_fgl is not None:
                                assert warehouse_fgl.upstreams
                                pair = (downstream_field, warehouse_fgl.upstreams[0])
                                if pair not in emitted_pairs:
                                    emitted_pairs.add(pair)
                                    fgls.append(warehouse_fgl)
                                    self.reporter.data_model_element_fgl_warehouse_resolved += 1
                                continue
                            self.reporter.data_model_element_fgl_warehouse_passthrough_deferred += 1
                            # Sub-count naming WHICH of the two deferral sites
                            # fired. Both bump the counter above, so without
                            # this the self-named case is indistinguishable
                            # from an inode columnId that failed to resolve --
                            # and they need opposite fixes.
                            self.reporter.data_model_element_fgl_self_named_no_passthrough += 1
                        # There is no pre-built warehouse FGL, because that is
                        # derived from an ``inode-<urlId>/<NATIVE>`` columnId and
                        # this column's is ``<element>/<NATIVE>``. The ref still
                        # names the warehouse table, and the element declares
                        # exactly that table, so resolve it by name.
                        #
                        # Without this the branch dead-ended: a single-element
                        # Data Model named after its own warehouse table emitted
                        # table-level lineage and NO column lineage at all, while
                        # every one of its columns carried a formula naming the
                        # table. Kept to tables the element DECLARES
                        # (allow_global_name_index=False) so the tenant-wide
                        # /v2/files listing is not triggered from a path that
                        # deferred ~10,700 times in one run (2026-09).
                        self._try_resolve_warehouse_table_name_ref(
                            ref=ref,
                            element=element,
                            column=column,
                            downstream_field=downstream_field,
                            warehouse_url_id_map=warehouse_url_id_map,
                            emitted_pairs=emitted_pairs,
                            fgls=fgls,
                            allow_global_name_index=False,
                        )
                        continue
                    # No intra-DM candidate. Before cross-DM search, try the
                    # warehouse path via columnId. Sigma elements sometimes use
                    # the warehouse table name as the formula source (e.g.
                    # "[CUSTOMERS/col]" on an element that isn't named "CUSTOMERS")
                    # rather than the element name — columnId is authoritative.
                    # If columnId is warehouse-shaped but resolution failed,
                    # count as warehouse-deferred rather than cross-DM-deferred.
                    if not warehouse_consumed:
                        warehouse_consumed = True
                        if warehouse_fgl is not None:
                            assert warehouse_fgl.upstreams
                            pair = (downstream_field, warehouse_fgl.upstreams[0])
                            if pair not in emitted_pairs:
                                emitted_pairs.add(pair)
                                fgls.append(warehouse_fgl)
                                self.reporter.data_model_element_fgl_warehouse_resolved += 1
                            continue
                        if _is_warehouse_column_id(column.columnId):
                            # Record the warehouse failure, but do NOT stop
                            # here. The formula still names a source, and for a
                            # cross-DM-sourced element that source is a producer
                            # element in another Data Model -- the warehouse
                            # inode belongs to the producer, so failing to
                            # resolve it says nothing about whether the ref
                            # itself resolves. Falling through to cross-DM
                            # resolution below is the whole point of having it.
                            self.reporter.data_model_element_fgl_warehouse_passthrough_deferred += 1

                    # The ref may name a warehouse TABLE this element
                    # declares, rather than any element. Columns with an opaque
                    # columnId reach the warehouse only this way. Falls through
                    # to cross-DM resolution when it does not apply.
                    cross_dm_fgl = self._resolve_non_sibling_ref(
                        ref=ref,
                        element=element,
                        column=column,
                        element_dataset_urn=element_dataset_urn,
                        entity_level_upstream_urns=entity_level_upstream_urns,
                        downstream_field=downstream_field,
                        warehouse_url_id_map=warehouse_url_id_map,
                        emitted_pairs=emitted_pairs,
                        fgls=fgls,
                    )
                    if cross_dm_fgl is not None:
                        assert cross_dm_fgl.upstreams
                        pair = (downstream_field, cross_dm_fgl.upstreams[0])
                        if pair not in emitted_pairs:
                            emitted_pairs.add(pair)
                            cross_dm_fgls.append(cross_dm_fgl)
                            self.reporter.data_model_element_fgl_cross_dm_resolved += 1
                    continue

                self._resolve_intra_dm_fgl(
                    ref=ref,
                    candidate_eids_after_self_strip=candidate_eids_after_self_strip,
                    elementId_to_dataset_urn=elementId_to_dataset_urn,
                    entity_level_upstream_urns=entity_level_upstream_urns,
                    urn_to_cols=urn_to_cols,
                    downstream_field=downstream_field,
                    element=element,
                    element_dataset_urn=element_dataset_urn,
                    data_model=data_model,
                    fgls=fgls,
                    cross_dm_fgls=cross_dm_fgls,
                    emitted_pairs=emitted_pairs,
                    discovered_upstreams=discovered_upstreams,
                )

            if not resolution_attempted:
                self._resolve_no_ref_column_fgl(
                    column=column,
                    warehouse_fgl=warehouse_fgl,
                    downstream_field=downstream_field,
                    fgls=fgls,
                    emitted_pairs=emitted_pairs,
                )

            self._log_dm_column_outcome(
                data_model_id=data_model.dataModelId,
                element_id=element.elementId,
                column=column,
                resolution_attempted=resolution_attempted,
                new_fgls=fgls[fgl_mark:] + cross_dm_fgls[cross_mark:],
            )
        # fgl_emitted is the umbrella count for everything appended to `fgls`:
        # intra-DM, warehouse-passthrough, warehouse-table-name and join-key
        # edges. Cross-DM is tracked separately via fgl_cross_dm_resolved.
        # Sub-counts overlap it deliberately (fgl_warehouse_resolved,
        # fgl_join_key_resolved) so each mechanism can be triaged on its own.
        self._add_union_fgls(
            element=element,
            element_dataset_urn=element_dataset_urn,
            data_model=data_model,
            elementId_to_dataset_urn=elementId_to_dataset_urn,
            fgls=fgls,
            emitted_pairs=emitted_pairs,
            discovered_upstreams=discovered_upstreams,
        )
        self._add_join_key_fgls(
            element=element,
            element_dataset_urn=element_dataset_urn,
            data_model=data_model,
            elementId_to_dataset_urn=elementId_to_dataset_urn,
            urn_to_cols=urn_to_cols,
            fgls=fgls,
            cross_dm_fgls=cross_dm_fgls,
            emitted_pairs=emitted_pairs,
            discovered_upstreams=discovered_upstreams,
        )
        self.reporter.data_model_element_fgl_emitted += len(fgls)
        all_fgls = fgls + cross_dm_fgls
        all_fgls.sort(
            key=lambda fgl: (
                (fgl.downstreams or [""])[0],
                (fgl.upstreams or [""])[0],
            )
        )
        return all_fgls

    def _gen_data_model_element_workunits(
        self,
        data_model: SigmaDataModel,
        data_model_key: DataModelKey,
        data_model_container_urn: str,
        elementId_to_dataset_urn: Dict[str, str],
        element_name_to_eids: Dict[str, List[str]],
        owner_username: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        dm_url_id = data_model.get_url_id()
        # Resolve all type=table inodes for this DM once before the element loop.
        # One /files call per unique inode (cached across DMs).
        warehouse_url_id_map = self._build_dm_warehouse_url_id_map(data_model)
        # Warn once per run if the registry is empty while warehouse inodes exist.
        if (
            not self._registry_empty_warned
            and data_model.warehouse_inodes_by_inode_id
            and not self.connection_registry.by_id
        ):
            self._registry_empty_warned = True
            self.reporter.warning(
                title="Sigma connection registry is empty — warehouse lineage unavailable",
                message=(
                    "The connection registry contains no records. All warehouse "
                    "upstream edges for DM elements will be skipped for this run. "
                    "Check whether the /v2/connections fetch succeeded and the "
                    "connections_skipped_missing_id counter."
                ),
            )
        # ``data_model.path`` starts with the workspace name (e.g.
        # "Acryl Data/Marketing"); drop index 0 because the workspace is
        # already the enclosing Container. ``split`` on a path with no
        # separator yields ``["Acryl Data"]`` so [1:] safely degrades
        # to ``[]``.
        paths = data_model.path.split("/")[1:] if data_model.path else []
        workspace_container_urn: Optional[str] = None
        if data_model.workspaceId:
            workspace_container_urn = builder.make_container_urn(
                self._gen_workspace_key(data_model.workspaceId)
            )

        for element in data_model.elements:
            element_dataset_urn = elementId_to_dataset_urn[element.elementId]

            yield self._gen_entity_status_aspect(element_dataset_urn)

            # Sigma's /elements API has no element-level description, so
            # we omit the field (description=None) rather than emitting an
            # explicit empty string; aspect-replace semantics would blank
            # any description a user edited in the UI. qualifiedName uses
            # "/" as the separator; names containing "/" are still
            # disambiguated by the element URN.
            # ``?:nodeId=<elementId>`` is Sigma's standard deep-link shape
            # (same pattern used for workbook elements in sigma_api.py);
            # gate on ``data_model.url`` because it is ``Optional[str]``.
            element_external_url: Optional[str] = (
                f"{data_model.url}?:nodeId={element.elementId}"
                if data_model.url
                else None
            )
            element_properties = DatasetProperties(
                name=element.name,
                description=None,
                qualifiedName=f"{data_model.name}/{element.name}",
                externalUrl=element_external_url,
                customProperties={
                    "dataModelId": data_model.dataModelId,
                    "dataModelUrlId": dm_url_id,
                    "elementId": element.elementId,
                    "type": element.type or "Unknown",
                },
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn, aspect=element_properties
            ).as_workunit()

            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn,
                aspect=SubTypesClass(
                    typeNames=[DatasetSubTypes.SIGMA_DATA_MODEL_ELEMENT]
                ),
            ).as_workunit()

            dpi_aspect = self._gen_dataplatform_instance_aspect(element_dataset_urn)
            if dpi_aspect:
                yield dpi_aspect

            yield self._gen_data_model_element_schema_metadata(
                element_dataset_urn, element
            )

            # Propagate DM-level ownership (``data_model.createdBy``) onto
            # every element Dataset. The DM API has no element-level owner
            # field, so we mirror the Container owner on each child
            # Dataset -- otherwise "Datasets owned by X" filters in the
            # DataHub UI would miss DM elements entirely even though the
            # author shows up on the enclosing Container. Gated on
            # ``ingest_owner`` so operators who opt out of user-URN
            # emission (typically for privacy / SSO sync reasons) are
            # respected.
            if self.config.ingest_owner and owner_username:
                yield self._gen_entity_owner_aspect(element_dataset_urn, owner_username)

            yield from add_entity_to_container(
                container_key=data_model_key,
                entity_type="dataset",
                entity_urn=element_dataset_urn,
            )

            # BrowsePaths: workspace urn, DM path segments, DM Container
            # urn (typed so UI breadcrumbs are clickable). Matches the
            # terminal-at-parent shape used by the rest of this connector
            # (_gen_entity_browsepath_aspect) -- the element's own name
            # is rendered from DatasetProperties, not the breadcrumb, so
            # appending it here produced a duplicate unclickable crumb.
            # Orphan / personal-space DMs lack a workspace container;
            # skip that entry rather than duplicating the DM Container
            # URN.
            browse_entries: List[BrowsePathEntryClass] = []
            if workspace_container_urn:
                browse_entries.append(
                    BrowsePathEntryClass(
                        id=workspace_container_urn, urn=workspace_container_urn
                    )
                )
            browse_entries.extend(BrowsePathEntryClass(id=path) for path in paths)
            browse_entries.append(
                BrowsePathEntryClass(
                    id=data_model_container_urn, urn=data_model_container_urn
                )
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=element_dataset_urn,
                aspect=BrowsePathsV2Class(browse_entries),
            ).as_workunit()

            has_customsql = any(
                sid in data_model.custom_sql_by_name for sid in element.source_ids
            )
            # Build formula-based col mapping before processing lineage so
            # _drain_sql_aggregators can rewrite FGL downstream field names.
            if has_customsql:
                self._build_customsql_col_mapping(element, element_dataset_urn)

            upstream_lineage = self._gen_data_model_element_upstream_lineage(
                element,
                data_model,
                element_dataset_urn,
                elementId_to_dataset_urn=elementId_to_dataset_urn,
                element_name_to_eids=element_name_to_eids,
                warehouse_url_id_map=warehouse_url_id_map,
            )
            if upstream_lineage is not None:
                if (
                    has_customsql
                    and element_dataset_urn in self._customsql_registered_urns
                ):
                    # customSQL pre-flight succeeded: stash non-customSQL
                    # upstreams/FGL and skip the immediate emit.  Drain will
                    # merge them into the single consolidated UpstreamLineage
                    # MCP, avoiding a redundant overwrite.
                    if upstream_lineage.upstreams:
                        self._customsql_extra_upstreams[element_dataset_urn] = list(
                            upstream_lineage.upstreams
                        )
                    if upstream_lineage.fineGrainedLineages:
                        self._customsql_extra_fgls[element_dataset_urn] = list(
                            upstream_lineage.fineGrainedLineages
                        )
                else:
                    # No customSQL registration (pre-flight failed or no
                    # customSQL on this element): emit immediately.
                    yield MetadataChangeProposalWrapper(
                        entityUrn=element_dataset_urn, aspect=upstream_lineage
                    ).as_workunit()

            self.reporter.data_model_elements_emitted += 1
            if data_model.workspaceId:
                self.reporter.workspaces.increment_data_model_elements_count(
                    data_model.workspaceId
                )

    def _register_dm_alias(self, alias: str, canonical_key: str) -> None:
        """Register *alias* as a secondary lookup key pointing at the same
        bridge-map entries as *canonical_key*.

        Called when the discovery loop fetches a DM by a stale/rotated slug
        that differs from the DM's current canonical urlId. The canonical key
        must already be present in ``dm_container_urn_by_url_id`` (either from
        the round-1 prepopulate or from ``_prepopulate_dm_bridge_maps``).
        No-ops if the alias already maps to any DM or if alias == canonical_key.
        """
        if alias == canonical_key or alias in self.dm_container_urn_by_url_id:
            return
        container_urn = self.dm_container_urn_by_url_id.get(canonical_key)
        if container_urn is None:
            return
        self.dm_container_urn_by_url_id[alias] = container_urn
        self.dm_element_urn_by_name[alias] = self.dm_element_urn_by_name.get(
            canonical_key, {}
        )
        self.dm_total_element_count_by_url_id[alias] = (
            self.dm_total_element_count_by_url_id.get(canonical_key, 0)
        )
        if canonical_key in self.data_model_id_by_url_id:
            self.data_model_id_by_url_id[alias] = self.data_model_id_by_url_id[
                canonical_key
            ]

    def _prepopulate_dm_bridge_maps(
        self,
        data_model: SigmaDataModel,
        requested_alias: Optional[str] = None,
    ) -> Dict[str, str]:
        """Populate the global DM bridge maps for one DM and return the
        local ``elementId -> Dataset URN`` map for intra-DM lineage.

        Called eagerly for every DM before any DM emits workunits so
        cross-DM and workbook-to-DM lineage resolve regardless of DM
        iteration order. Bridge keys use ``get_url_id()`` (the urlId slug
        when present, else the dataModelId UUID).

        ``requested_alias``: if the discovery loop fetched this DM by a
        stale/rotated prefix that differs from the canonical ``get_url_id()``,
        pass it here. The same container URN and name maps will be registered
        under both the canonical key and the alias so that source_ids carrying
        the old slug resolve correctly.
        """
        data_model_key = self._gen_data_model_key(data_model.dataModelId)
        data_model_container_urn = builder.make_container_urn(data_model_key)
        bridge_key = data_model.get_url_id()

        # ``urlId`` is a short slug; Sigma can reissue it after deletion.
        # On a true collision (different URN for the same key), keep the
        # first registration and warn. Re-registering the same DM is benign.
        existing_container = self.dm_container_urn_by_url_id.get(bridge_key)
        container_collision = (
            existing_container is not None
            and existing_container != data_model_container_urn
        )
        if container_collision:
            logger.warning(
                "Sigma DM bridge key %r already registered to %s; new DM "
                "%s (%s) resolves to %s. Keeping the first registration "
                "and skipping emission of the colliding DM.",
                bridge_key,
                existing_container,
                data_model.name,
                data_model.dataModelId,
                data_model_container_urn,
            )
            self.reporter.data_models_bridge_key_collision += 1
            self.dm_collided_data_model_ids.add(data_model.dataModelId)
            # Collided DMs are filtered out of emission by
            # ``dm_collided_data_model_ids`` in ``get_workunits_internal``
            # (see the emission loop). No downstream consumer reads the
            # per-element map for a collided DM, so skip the allocation
            # entirely and return an empty dict -- the first-registered
            # DM's maps win uniformly.
            return {}

        name_map: Dict[str, List[str]] = {}
        elementId_to_dataset_urn: Dict[str, str] = {}
        for element in data_model.elements:
            element_dataset_urn = self._gen_data_model_element_urn(data_model, element)
            elementId_to_dataset_urn[element.elementId] = element_dataset_urn
            # Build global column index for cross-DM FGL column validation.
            el_by_name, _ = _dedup_dm_element_columns(element.columns)
            self.dm_element_urn_to_cols[element_dataset_urn] = {
                c.lower(): c for c in el_by_name
            }
            self.dm_key_by_element_urn[element_dataset_urn] = bridge_key
            for key in {bridge_key, data_model.dataModelId}:
                self.dm_element_urn_by_key_and_eid[(key, element.elementId)] = (
                    element_dataset_urn
                )
                self.dm_keys_by_element_id.setdefault(element.elementId, set()).add(key)
            # Blank-named elements are excluded from ``name_map`` so they
            # don't collapse into a single spuriously-ambiguous candidate.
            if element.name:
                name_map.setdefault(element.name.lower(), []).append(
                    element_dataset_urn
                )

        self.dm_container_urn_by_url_id[bridge_key] = data_model_container_urn
        self.dm_element_urn_by_name[bridge_key] = name_map
        self.data_model_id_by_url_id[bridge_key] = data_model.dataModelId
        # Total count (including blank-named elements) used by the
        # cross-DM single-element fallback to verify "DM has exactly one
        # element" before attributing an unmatched-name reference.
        self.dm_total_element_count_by_url_id[bridge_key] = len(data_model.elements)

        if requested_alias:
            self._register_dm_alias(requested_alias, bridge_key)

        return elementId_to_dataset_urn

    def _collect_unresolved_cross_dm_prefixes(
        self, data_models: List[SigmaDataModel]
    ) -> Set[str]:
        """Return cross-DM ``<prefix>`` values (from ``<prefix>/<suffix>``
        source_ids) whose DM is not yet registered. These point at
        personal-space or unlisted DMs that the discovery loop can fetch
        by urlId.

        ``inode-``-prefixed source_ids are skipped: those are the
        external-upstream shape (``inode-<datasetUrlId>`` / ``inode-<tableId>``)
        and would pollute ``data_model_external_reference_unresolved`` if
        attempted as a DM fetch.
        """
        unresolved: Set[str] = set()
        for data_model in data_models:
            for element in data_model.elements:
                for source_id in element.source_ids:
                    if "/" not in source_id:
                        continue
                    if source_id.startswith("inode-"):
                        continue
                    prefix, _, suffix = source_id.partition("/")
                    if not prefix or not suffix:
                        continue
                    if prefix in self.dm_container_urn_by_url_id:
                        continue
                    unresolved.add(prefix)
        return unresolved

    def _gen_data_model_workunit(
        self,
        data_model: SigmaDataModel,
        elementId_to_dataset_urn: Dict[str, str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a DM as a Container plus one Dataset per element, with
        intra-DM, external, and cross-DM UpstreamLineage on each element.

        Caller must have invoked ``_prepopulate_dm_bridge_maps`` for every
        DM first so cross-DM / workbook-to-DM bridges resolve.
        """
        data_model_key = self._gen_data_model_key(data_model.dataModelId)
        data_model_container_urn = builder.make_container_urn(data_model_key)

        owner_username = (
            self.sigma_api.get_user_name(data_model.createdBy)
            if data_model.createdBy
            else None
        )
        parent_container_key: Optional[WorkspaceKey] = (
            self._gen_workspace_key(data_model.workspaceId)
            if data_model.workspaceId
            else None
        )
        extra_properties: Dict[str, str] = {
            "dataModelId": data_model.dataModelId,
            "dataModelUrlId": data_model.get_url_id(),
        }
        if data_model.latestVersion is not None:
            extra_properties["latestVersion"] = str(data_model.latestVersion)
        if data_model.path:
            extra_properties["path"] = data_model.path
        # Flag personal-space / unlisted DMs. Lowercase ``"true"`` matches
        # the JSON boolean convention used by other DataHub connectors.
        if data_model.workspaceId is None:
            extra_properties["isPersonalDataModel"] = "true"

        yield from gen_containers(
            container_key=data_model_key,
            name=data_model.name,
            sub_types=[BIContainerSubTypes.SIGMA_DATA_MODEL],
            parent_container_key=parent_container_key,
            # Pass ``None`` through (rather than ``""``) when Sigma has no
            # description so ``ContainerProperties`` aspect-replace cannot
            # blank out a description the user edited in the DataHub UI.
            # Mirrors the element-Dataset fix above.
            description=data_model.description,
            external_url=data_model.url,
            extra_properties=extra_properties,
            owner_urn=(
                builder.make_user_urn(owner_username)
                if self.config.ingest_owner and owner_username
                else None
            ),
            tags=[data_model.badge] if data_model.badge else None,
            created=int(data_model.createdAt.timestamp() * 1000),
            last_modified=int(data_model.updatedAt.timestamp() * 1000),
        )

        if data_model.workspaceId:
            self.reporter.workspaces.increment_data_models_count(data_model.workspaceId)

        # Keys are lowercased so formula refs — which users type and Sigma
        # autocompletes from canonical names — match even when case differs.
        # Values are elementIds (not URNs) so the FGL builder can strip self-references
        # by elementId before mapping to URNs via elementId_to_dataset_urn.
        element_name_to_eids: Dict[str, List[str]] = {}
        for element in data_model.elements:
            if element.name:
                element_name_to_eids.setdefault(element.name.lower(), []).append(
                    element.elementId
                )

        yield from self._gen_data_model_element_workunits(
            data_model,
            data_model_key,
            data_model_container_urn,
            elementId_to_dataset_urn,
            element_name_to_eids,
            owner_username=owner_username,
        )

    def _gen_dashboard_urn(self, dashboard_identifier: str) -> str:
        return builder.make_dashboard_urn(
            platform=self.platform,
            platform_instance=self.config.platform_instance,
            name=dashboard_identifier,
        )

    def _gen_dashboard_info_workunit(self, page: Page) -> MetadataWorkUnit:
        dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())
        dashboard_info_cls = DashboardInfoClass(
            title=page.name,
            description="",
            charts=[
                builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=element.get_urn_part(),
                )
                for element in page.elements
            ],
            lastModified=ChangeAuditStampsClass(),
            customProperties={"ElementsCount": str(len(page.elements))},
        )
        return MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

    def _get_element_data_source_platform_details(
        self, full_path: str
    ) -> Optional[PlatformDetail]:
        data_source_platform_details: Optional[PlatformDetail] = None
        while full_path != "":
            if full_path in self.config.chart_sources_platform_mapping:
                data_source_platform_details = (
                    self.config.chart_sources_platform_mapping[full_path]
                )
                break
            else:
                full_path = "/".join(full_path.split("/")[:-1])
        if (
            not data_source_platform_details
            and "*" in self.config.chart_sources_platform_mapping
        ):
            data_source_platform_details = self.config.chart_sources_platform_mapping[
                "*"
            ]

        return data_source_platform_details

    def _resolve_dm_element_upstream_urn(
        self, upstream: DataModelElementUpstream
    ) -> Tuple[Optional[str], List[str]]:
        """Resolve a workbook element's DM upstream to a DM element
        Dataset URN. Returns ``(urn, candidates)``:

        - ``urn`` is None when the name cannot be matched
          (``ChartInfo.inputs`` requires Dataset URNs, not Container URNs).
        - ``candidates`` is the name-matched URN list (0 = miss, 1 = unique,
          >=2 = ambiguous). The caller uses it to bump
          ``element_dm_edge.ambiguous`` once per unique chart-to-DM edge.

        Candidates are sorted deterministically by their Dataset URN
        (which embeds ``dataModelId.elementId``), so the pick is stable
        across runs; within a single DM this degenerates to the lowest
        elementId.
        """
        dm_known = upstream.data_model_url_id in self.dm_container_urn_by_url_id
        name_map = self.dm_element_urn_by_name.get(upstream.data_model_url_id)

        if not upstream.name:
            if dm_known:
                self.reporter.element_dm_edge.upstream_name_missing += 1
            else:
                self.reporter.element_dm_edge.unresolved += 1
            return None, []

        if name_map:
            raw_candidates = name_map.get(upstream.name.lower())
            if raw_candidates:
                candidates = sorted(raw_candidates)
                return candidates[0], candidates

        # Intentional asymmetry with ``_resolve_dm_element_cross_dm_upstream``:
        # that path falls back to the producer DM's sole element when the
        # producer has exactly one element, because cross-DM source_ids
        # (``<prefix>/<suffix>``) don't always carry a matching name on the
        # ``type: element`` row. Here, the workbook-to-DM edge is
        # synthesized (edge-only / formula-parsed) with ``upstream.name``
        # *set to the DM element's name at synthesis time* -- a mismatch
        # against every ``name_map`` key indicates the DM element was
        # renamed after the workbook last referenced it, not an
        # ambiguous-resolution gap. Falling back to the sole element
        # would silently bind the edge to the post-rename element, so we
        # surface via ``element_dm_edge.name_unmatched_but_dm_known`` and
        # let operators decide whether to re-save the workbook.
        if dm_known:
            self.reporter.element_dm_edge.name_unmatched_but_dm_known += 1
        else:
            self.reporter.element_dm_edge.unresolved += 1
        return None, []

    @staticmethod
    def _build_workbook_element_index(
        workbook: Workbook,
    ) -> Dict[str, List[Element]]:
        """Map element name -> list of elements with that name across all workbook pages.

        Multiple entries for the same name indicate a name collision; callers must
        disambiguate via lineage sourceIds.  Verified live: at least one workbook in
        the test tenant has 3 elements named 'random data model'.
        """
        index: Dict[str, List[Element]] = {}
        for page in workbook.pages:
            for element in page.elements:
                index.setdefault(element.name, []).append(element)
        return index

    def _normalized_element_index(
        self, wb_element_index: Dict[str, List[Element]]
    ) -> Dict[str, List[Element]]:
        """Memoized :meth:`_build_normalized_element_index`.

        The exact-match miss path is one of the hottest in the connector -- one
        tenant reached it ~51k times -- and rebuilding this map each time walked
        every element in the workbook.

        The memo holds the source index itself and compares with ``is``, rather
        than keying on ``id()``: an id is only unique among LIVE objects, and
        these indexes are built and dropped one per workbook, so a freed one's
        address can be reused by the next workbook's index and serve it the
        previous workbook's elements.
        """
        memo = self._normalized_index_memo
        if memo is not None and memo[0] is wb_element_index:
            return memo[1]
        built = self._build_normalized_element_index(wb_element_index)
        self._normalized_index_memo = (wb_element_index, built)
        return built

    @staticmethod
    def _build_normalized_element_index(
        wb_element_index: Dict[str, List[Element]],
    ) -> Dict[str, List[Element]]:
        """Whitespace/case-insensitive view of the workbook element index.

        Kept as a separate map rather than replacing the exact one: exact
        matches must keep winning, and a normalized key that collapses two
        genuinely distinct element names is ambiguous and must not be used.
        """
        normalized: Dict[str, List[Element]] = {}
        for name, elements in wb_element_index.items():
            normalized.setdefault(_normalize_element_name(name), []).extend(elements)
        return normalized

    @staticmethod
    def _build_element_warehouse_table_index(
        dataset_inputs: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        """Map uppercase short table name -> list of warehouse Dataset URNs.

        Sigma chart formulas reference warehouse tables by their short identifier
        (e.g. 'FIVETRAN_LOG__CONNECTOR_STATUS'), not by the full 3-part name.
        This index is built from the SQL-parsed dataset_inputs for the current element:
          - Sigma Dataset entries (value=[warehouse_urn, …]) contribute warehouse URNs.
          - Direct unmatched warehouse entries (value=[]) contribute the key URN itself.

        Multiple URNs under the same short name indicate a schema-level collision.
        Callers must resolve only when exactly one candidate is present.
        """
        index: Dict[str, List[str]] = {}
        for dataset_urn, warehouse_urns in dataset_inputs.items():
            candidates: List[str] = warehouse_urns
            if not candidates:
                try:
                    parsed_dataset_urn = DatasetUrn.from_string(dataset_urn)
                    if (
                        parsed_dataset_urn.get_data_platform_urn().platform_name
                        == "sigma"
                    ):
                        # Sigma-platform entries with no bridged warehouse URNs
                        # are Data Model element datasets, not warehouse tables.
                        continue
                    candidates = [dataset_urn]
                except InvalidUrnError as e:
                    logger.debug("Skipping invalid dataset URN %r: %s", dataset_urn, e)
                    continue
            for urn in candidates:
                try:
                    short = DatasetUrn.from_string(urn).name.split(".")[-1].upper()
                    index.setdefault(short, []).append(urn)
                except InvalidUrnError as e:
                    logger.debug("Skipping invalid dataset URN %r: %s", urn, e)
        return index

    def _build_workbook_warehouse_table_index(
        self, workbook: Workbook
    ) -> _WorkbookWarehouseIndex:
        """Build a dual lookup index from /v2/workbooks/{id}/lineage type=table entries.

        Returns a _WorkbookWarehouseIndex with:
          by_url_id: urlId -> warehouse Dataset URN (confident 1:1 match)
          by_name:   UPPER(table_name) -> [URN, ...] (collision-aware name fallback)

        by_name key shape matches _build_element_warehouse_table_index; merged with the
        per-element index in _gen_elements_workunit before passing to
        _resolve_chart_formula_upstream.

        Accepts both 2-segment (Connection Root/<SCHEMA>, e.g. Redshift) and
        3-segment (Connection Root/<DB>/<SCHEMA>, e.g. Snowflake) /files paths,
        consistent with _build_dm_warehouse_url_id_map.

        Mirrors _build_dm_warehouse_url_id_map's path-handling (2-vs-3 segment
        logic, default_database fallback, dedup sets) — URN identity with the
        DM element warehouse upstream path is guaranteed because both builders
        route through _resolve_dm_element_warehouse_upstream.
        """
        by_name: Dict[str, List[str]] = {}
        self._wb_url_id_to_conn_id = {}
        by_url_id: Dict[str, str] = {}
        transient_map: Dict[str, _WarehouseTableRef] = {}
        entries = self.sigma_api.get_workbook_lineage(workbook.workbookId)
        if entries is None:
            self.reporter.chart_input_fields_warehouse_index_lookup_failed += 1
            logger.debug(
                "workbook %s: /lineage returned nothing, so no warehouse-table "
                "index is available; every chart column in this workbook loses "
                "warehouse qualification",
                workbook.workbookId,
            )
            return _WorkbookWarehouseIndex(by_url_id={}, by_name={})

        for entry in entries:
            inode_id = entry.inodeId
            conn_id = entry.connectionId

            first_attempt = inode_id not in self._files_cache
            files_data = self._get_file_metadata_cached(inode_id)
            if files_data is None:
                if first_attempt:
                    self.reporter.chart_input_fields_warehouse_table_lookup_failed += 1
                logger.debug(
                    "Workbook %s: /files lookup failed for inode %r; skipping.",
                    workbook.workbookId,
                    inode_id,
                )
                continue

            url_id = str(files_data.get("urlId") or "")
            path = str(files_data.get("path") or "")
            table_name = str(files_data.get("name") or "")
            parts = path.split("/")
            # Accept 2–3 segments: "Connection Root/<SCHEMA>" (Redshift) or
            # "Connection Root/<DB>/<SCHEMA>" (Snowflake/Postgres).
            path_invalid = not (
                url_id and table_name and 2 <= len(parts) <= 3 and all(parts)
            )
            root_unexpected = (not path_invalid) and parts[0] != _FILES_PATH_ROOT
            if path_invalid or root_unexpected:
                if inode_id not in self._files_path_unparseable_seen:
                    self._files_path_unparseable_seen.add(inode_id)
                    self.reporter.chart_input_fields_warehouse_path_unparseable += 1
                    self.reporter.warning(
                        title=(
                            "Sigma workbook lineage path has unexpected root segment"
                            if root_unexpected
                            else "Sigma workbook lineage /files path unparseable"
                        ),
                        message=(
                            "Expected 'Connection Root/<SCHEMA>' or "
                            "'Connection Root/<DB>/<SCHEMA>' with no empty "
                            "segments and a non-empty urlId. "
                            "Warehouse table index entry skipped for this inode."
                        ),
                        context=(
                            f"workbook={workbook.workbookId}, inode={inode_id}, "
                            f"path={path!r}, url_id={url_id!r}, "
                            f"table_name={table_name!r}"
                        ),
                    )
                continue

            db: Optional[str]
            if len(parts) == 3:
                db, schema = parts[1], parts[2]
            else:
                conn_override = self.config.connection_to_platform_map.get(conn_id)
                conn_record = self.connection_registry.get(conn_id)
                db = (conn_override.default_database if conn_override else None) or (
                    conn_record.default_database if conn_record else None
                )
                schema = parts[1]
                if db is None and conn_id not in self._missing_default_db_warned:
                    self._missing_default_db_warned.add(conn_id)
                    self.reporter.warning(
                        title="Sigma warehouse default_database not configured",
                        message=(
                            "The /files path for this connection has no database layer "
                            "(e.g. 'Connection Root/<SCHEMA>'). The emitted warehouse "
                            "URN will use schema.table only, which will not match a "
                            "connector that uses db.schema.table. Set "
                            "connection_to_platform_map.<connectionId>.default_database "
                            "in the recipe to fix the URN."
                        ),
                        context=f"connectionId={conn_id}, path={path!r}",
                    )

            if url_id in transient_map:
                logger.warning(
                    "Workbook %s: two type=table lineage entries share the same "
                    "urlId %r; the earlier entry will be overwritten.",
                    workbook.workbookId,
                    url_id,
                )
            transient_map[url_id] = _WarehouseTableRef(
                connection_id=conn_id,
                db=db,
                schema=schema,
                table=table_name,
            )

        for url_id in transient_map:
            table_name = transient_map[url_id].table
            urn = self._resolve_dm_element_warehouse_upstream(
                url_id_suffix=url_id,
                warehouse_map=transient_map,
            )
            if urn is None:
                self.reporter.chart_input_fields_warehouse_unknown_connection += 1
                logger.debug(
                    "Workbook %s: could not resolve warehouse URN for table %r "
                    "(connection not in registry or is_mappable=False).",
                    workbook.workbookId,
                    table_name,
                )
                continue
            by_url_id[url_id] = urn
            by_name.setdefault(table_name.upper(), []).append(urn)

        # Expose urlId -> connectionId for the column-name bridge.
        self._wb_url_id_to_conn_id = {
            url_id: ref.connection_id for url_id, ref in transient_map.items()
        }
        return _WorkbookWarehouseIndex(by_url_id=by_url_id, by_name=by_name)

    @staticmethod
    def _merge_warehouse_table_indices(
        primary: Dict[str, List[str]],
        supplementary: Dict[str, List[str]],
    ) -> Dict[str, List[str]]:
        """Merge two short-name → URN-list indices.

        Per-element entries (primary) take precedence on key conflict — the SQL
        parser attributes warehouse tables specifically to the chart's own data
        path; workbook-level entries are broader and may include tables the
        chart doesn't actually use.

        Conflict resolution: if both indices have key K, primary's URN list wins
        entirely (do NOT concat — could fabricate cross-table joins).
        """
        result = dict(primary)
        for key, urns in supplementary.items():
            if key not in result:
                result[key] = urns
        return result

    @staticmethod
    def _chart_urn_column_index(
        wb_element_index: Dict[str, List[Element]],
        elementId_to_chart_urn: Dict[str, str],
    ) -> Dict[str, Dict[str, str]]:
        """chart URN -> {lowercased column name: column name} for this workbook.

        The schema half of join-chain validation. Without it a mis-split emits
        an InputField naming a column the upstream does not have, which renders
        as a dangling field rather than as no lineage.
        """
        out: Dict[str, Dict[str, str]] = {}
        for elements in wb_element_index.values():
            for element in elements:
                urn = elementId_to_chart_urn.get(element.elementId)
                if urn:
                    out.setdefault(urn, {}).update(
                        {c.lower(): c for c in element.columns}
                    )
        return out

    def _resolve_chart_join_chain_ref(
        self,
        ref: BracketRef,
        *,
        chart_element_id: str,
        chart_upstream_element_ids: Set[str],
        dm_upstream_urn_by_element_name: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        element_warehouse_table_index: Dict[str, List[str]],
        elementId_to_chart_urn: Dict[str, str],
    ) -> Optional[Tuple[str, str]]:
        """Resolve ``[JoinElement/SourceElement/Column]`` on the chart path.

        The first-slash split reads such a ref as source=``JoinElement``,
        column=``SourceElement/Column`` -- a column no upstream has. The Data
        Model path already tries every split and validates each against the
        candidate's real schema; this brings the chart path to the same
        standard, which it needs more, not less: the chart resolver returns
        ``ref.column`` verbatim, so a mis-split emits a dangling InputField
        instead of simply resolving to nothing.

        Only a candidate whose resolved upstream actually HAS the column is
        accepted. When none does, this returns None and the caller keeps the
        legacy first-slash reading, so nothing that resolves today stops
        resolving.
        """
        candidates = candidate_source_column_splits(ref)
        if len(candidates) < 2:
            return None
        # Identity comparison, not id(): a freed workbook index's address can be
        # reused by the next one, and this map is what VALIDATES a candidate
        # split -- serving another workbook's columns would accept a wrong
        # split, not merely miss a right one.
        memo = self._chart_cols_memo
        if (
            memo is not None
            and memo[0] is wb_element_index
            and memo[1] is elementId_to_chart_urn
        ):
            chart_cols = memo[2]
        else:
            chart_cols = self._chart_urn_column_index(
                wb_element_index, elementId_to_chart_urn
            )
            # Single entry: both maps are rebuilt per workbook, so holding more
            # would just retain dead workbooks' columns.
            self._chart_cols_memo = (
                wb_element_index,
                elementId_to_chart_urn,
                chart_cols,
            )
        trace: List[str] = []
        for source, column in candidates:
            probe = replace(
                ref, source=source, column=column, segments=(source, column)
            )
            # count=False: this is speculative. Up to 2N-3 splits are tried per
            # ref, and letting each bump the name-matching counters would make
            # them measure attempts instead of refs.
            result = self._resolve_chart_formula_upstream(
                probe,
                chart_element_id=chart_element_id,
                chart_upstream_element_ids=chart_upstream_element_ids,
                dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                wb_element_index=wb_element_index,
                element_warehouse_table_index=element_warehouse_table_index,
                elementId_to_chart_urn=elementId_to_chart_urn,
                count=False,
            )
            if result is None:
                trace.append(f"{source!r}: no upstream")
                continue
            upstream_urn, field = result
            known = chart_cols.get(upstream_urn) or self.dm_element_urn_to_cols.get(
                upstream_urn
            )
            if known is None:
                # A warehouse table: this connector never learns its columns, so
                # the candidate cannot be confirmed. Refusing it keeps the
                # guarantee that an accepted split was checked against a schema.
                trace.append(f"{source!r}: upstream schema unknown (warehouse)")
                self.reporter.chart_join_chain_upstream_schema_unavailable += 1
                continue
            canonical = known.get(column.lower())
            if canonical is None:
                trace.append(
                    f"{source!r}: upstream found but column {column!r} absent "
                    f"from its {len(known)} columns"
                )
                continue
            self.reporter.chart_join_chain_resolved += 1
            logger.debug(
                "chart element %s: join-chain ref %r resolved to source=%r "
                "column=%r (upstream=%s); rejected candidates=%r",
                chart_element_id,
                ref.raw,
                source,
                canonical,
                upstream_urn,
                trace,
            )
            return (upstream_urn, canonical)

        # No split named an upstream the chart declares. The commonest reason,
        # by far, is that the middle segment is a table joined in *inside* the
        # Data Model: the chart's own upstream is the join element, and the
        # joined table is that element's sibling, invisible from here.
        sibling = self._resolve_join_chain_via_dm_sibling(
            ref,
            chart_element_id=chart_element_id,
            chart_upstream_element_ids=chart_upstream_element_ids,
            dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
            wb_element_index=wb_element_index,
            element_warehouse_table_index=element_warehouse_table_index,
            elementId_to_chart_urn=elementId_to_chart_urn,
            trace=trace,
        )
        if sibling is not None:
            return sibling

        self.reporter.chart_join_chain_unresolved += 1
        logger.debug(
            "chart element %s: no candidate split of join-chain ref %r "
            "validated; candidates tried=%r verdicts=%r. The first-slash "
            "reading would name column %r, which no upstream has, so no "
            "InputField upstream is emitted and the column self-references.",
            chart_element_id,
            ref.raw,
            candidates,
            trace,
            ref.column,
        )
        return None

    @staticmethod
    def _strip_join_count_suffix(name: str) -> str:
        """``'ACCOUNTS + 3'`` -> ``'ACCOUNTS'``.

        Sigma labels a join node in a ref with the number of further tables
        joined onto it, so the segment is a display label rather than the
        element's own name. Only a trailing ``" + <digits>"`` is removed; a
        name that genuinely ends that way is indistinguishable, but the
        stripped name is only ever *tried*, never preferred over an exact
        match, so a false strip cannot displace a real element.
        """
        match = _JOIN_COUNT_SUFFIX.match(name)
        return match.group(1) if match else name

    def _resolve_join_chain_via_dm_sibling(
        self,
        ref: BracketRef,
        *,
        chart_element_id: str,
        chart_upstream_element_ids: Set[str],
        dm_upstream_urn_by_element_name: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        element_warehouse_table_index: Dict[str, List[str]],
        elementId_to_chart_urn: Dict[str, str],
        trace: List[str],
    ) -> Optional[Tuple[str, str]]:
        """Resolve ``[JoinElement/JoinedTable/Column]`` through the DM's siblings.

        The chart declares the *join element* as its upstream; the table joined
        into it is a sibling element of the same Data Model and appears nowhere
        in the chart's own indices. So the middle segment is looked up among
        the siblings of whatever Data Model the first segment resolved into.

        Measured on one tenant (2026-09): 806 of 832 multi-segment refs failed with the
        verdict pair "<middle>: no upstream" and "<first>: upstream found but
        column absent" -- the exact signature of this shape. The Data Model
        path already resolves the same refs this way (via
        ``element_name_to_eids``) and succeeds ~1,470 times per run (2026-09).

        Like that path, this deliberately does NOT require the sibling to be a
        declared upstream: Sigma's element-level lineage lists only the direct
        join element, never what the join reaches through. The guarantee is
        kept by the schema check instead -- the sibling must actually have the
        column -- and an ambiguous name is refused rather than guessed.
        """
        segments = ref.parts
        if len(segments) < 3:
            return None
        column = segments[-1]
        join_probe = replace(
            ref,
            source=segments[0],
            column=column,
            segments=(segments[0], column),
        )
        join_result = self._resolve_chart_formula_upstream(
            join_probe,
            chart_element_id=chart_element_id,
            chart_upstream_element_ids=chart_upstream_element_ids,
            dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
            wb_element_index=wb_element_index,
            element_warehouse_table_index=element_warehouse_table_index,
            elementId_to_chart_urn=elementId_to_chart_urn,
            count=False,
        )
        if join_result is None:
            trace.append(f"{segments[0]!r}: join element itself has no upstream")
            return None
        dm_key = self.dm_key_by_element_urn.get(join_result[0])
        if dm_key is None:
            # The first segment resolved to a chart or a warehouse table, so
            # there is no Data Model whose siblings could be searched.
            self.reporter.chart_join_chain_sibling_dm_unknown += 1
            trace.append(f"{segments[0]!r}: upstream is not a Data Model element")
            return None
        name_map = self.dm_element_urn_by_name.get(dm_key, {})

        # Right to left: with nested joins the owning element is the segment
        # nearest the column, matching the Data Model path's ordering.
        for middle in reversed(segments[1:-1]):
            for name in dict.fromkeys((middle, self._strip_join_count_suffix(middle))):
                urns = name_map.get(name.lower(), [])
                if not urns:
                    trace.append(f"{name!r}: no sibling element in the same DM")
                    continue
                if len(urns) > 1:
                    self.reporter.chart_join_chain_sibling_ambiguous += 1
                    trace.append(
                        f"{name!r}: {len(urns)} sibling elements share the name"
                    )
                    continue
                cols = self.dm_element_urn_to_cols.get(urns[0]) or {}
                canonical = cols.get(column.lower())
                if canonical is None:
                    self.reporter.chart_join_chain_sibling_column_absent += 1
                    trace.append(
                        f"{name!r}: sibling found but column {column!r} absent "
                        f"from its {len(cols)} columns"
                    )
                    continue
                self.reporter.chart_join_chain_sibling_resolved += 1
                self.reporter.chart_join_chain_resolved += 1
                logger.debug(
                    "chart element %s: join-chain ref %r resolved via DM "
                    "sibling %r column=%r (upstream=%s); earlier verdicts=%r",
                    chart_element_id,
                    ref.raw,
                    name,
                    canonical,
                    urns[0],
                    trace,
                )
                return (urns[0], canonical)
        return None

    def _chart_ref_candidates(
        self,
        ref: BracketRef,
        *,
        chart_element_id: str,
        wb_element_index: Dict[str, List[Element]],
        count: bool,
    ) -> Optional[List[Element]]:
        """Workbook elements a formula ref's source name could denote.

        Exact match first, then a whitespace/case-normalized retry, which is
        accepted only when it does not collapse two genuinely distinct names.
        Split out of _resolve_chart_formula_upstream to keep that method under
        the complexity limit; it is the whole 'which element is this' story.

        Returns an empty list when no element matched but resolution may still
        continue to the warehouse-table fallback, and ``None`` when the ref must
        be REFUSED outright -- a case-only mismatch against a real element name,
        where falling through to a warehouse table would resolve to the wrong
        thing entirely. Collapsing those two into one value is what the caller's
        control flow used to encode directly."""
        candidates = wb_element_index.get(ref.source, [])
        if not candidates:
            # Exact match failed. Retry on a normalized key -- Sigma element
            # names routinely differ from the ref only by a trailing
            # non-breaking space, a leading space, or case. Only accept it when
            # the normalized key is unambiguous: if it collapses two genuinely
            # distinct element names, resolving would be a guess.
            normalized_index = self._normalized_element_index(wb_element_index)
            normalized_hits = normalized_index.get(
                _normalize_element_name(ref.source), []
            )
            distinct_names = {e.name for e in normalized_hits}
            if normalized_hits and len(distinct_names) == 1:
                if count:
                    self.reporter.chart_ref_source_normalized_match += 1
                logger.debug(
                    "chart element %s: formula ref source %r matched element "
                    "%r after whitespace/case normalization",
                    chart_element_id,
                    ref.source,
                    next(iter(distinct_names)),
                )
                candidates = normalized_hits
            elif len(distinct_names) > 1:
                if count:
                    self.reporter.chart_ref_source_normalized_ambiguous += 1
                logger.debug(
                    "chart element %s: formula ref source %r normalizes to %d "
                    "distinct element names %r; refusing to guess",
                    chart_element_id,
                    ref.source,
                    len(distinct_names),
                    sorted(distinct_names),
                )
        if not candidates:
            case_mismatched_names = [
                name for name in wb_element_index if name.lower() == ref.source.lower()
            ]
            if case_mismatched_names:
                if count:
                    self.reporter.chart_input_fields_case_mismatch += 1
                logger.debug(
                    "No exact-case workbook element match for formula ref source %r; "
                    "case-insensitive workbook element candidates were %s. "
                    "Treating as unresolved rather than falling back to warehouse "
                    "resolution.",
                    ref.source,
                    case_mismatched_names,
                )
                return None
            else:
                # Say WHY the name missed, not just that it did. This is the
                # single largest unexplained bucket in the report
                # (chart_input_fields_self_ref_unresolved_refs, ~51k), and the
                # ref source alone cannot distinguish "the element exists but
                # the lookup is too strict" from "the element was never indexed
                # at all" (e.g. dropped for being a pivot-table or input-table).
                # Reuse the one normalization definition; a second inline copy
                # would drift from it silently.
                normalized = _normalize_element_name(ref.source)
                near = [
                    name
                    for name in wb_element_index
                    if _normalize_element_name(name) == normalized
                ]
                if near:
                    # Reachable only when the normalized lookup above already
                    # ran and found MORE than one distinct name -- i.e. the
                    # ambiguous case, which chart_ref_source_normalized_ambiguous
                    # counts. Kept as a sub-count of that: it says the ambiguity
                    # cost a resolvable name, rather than "the lookup is too
                    # strict", which the normalized retry has since fixed.
                    if count:
                        self.reporter.chart_ref_source_near_miss += 1
                # Guarded: this is one of the hottest paths in the connector
                # (~51k hits on one tenant, 2026-09) and sorting the whole workbook
                # index for a discarded log line is pure waste.
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(
                        "No exact-case workbook element match for formula ref "
                        "source %r (normalized=%r); near_matches=%r; "
                        "index_size=%d index_sample=%r; falling back to "
                        "warehouse-table resolution.",
                        ref.source,
                        normalized,
                        near,
                        len(wb_element_index),
                        sorted(wb_element_index)[:15],
                    )

        return candidates

    def _global_dm_element_index(self) -> Dict[str, List[str]]:
        """Lowercased Data Model element name -> every URN carrying that name.

        Flattened across all Data Models, so a name is resolvable only when the
        list has exactly one entry. A chart formula ref that names an element
        the chart's own upstream list does not offer is otherwise unresolvable,
        and on one tenant (2026-09) that was the single largest gap in the run.
        """
        if self._known_dm_element_index is None:
            index: Dict[str, List[str]] = {}
            for by_name in self.dm_element_urn_by_name.values():
                if not isinstance(by_name, dict):
                    continue
                for name, urns in by_name.items():
                    key = str(name).strip().lower()
                    for urn in urns if isinstance(urns, list) else [urns]:
                        if urn not in index.setdefault(key, []):
                            index[key].append(urn)
            self._known_dm_element_index = index
        return self._known_dm_element_index

    def _resolve_ref_by_workbook_element_name(
        self,
        ref: BracketRef,
        *,
        candidates: List[Element],
        chart_element_id: str,
        elementId_to_chart_urn: Dict[str, str],
        count: bool,
    ) -> Optional[Tuple[str, str]]:
        """The ref names an element of THIS workbook that /lineage did not list.

        Sigma's per-element ``/lineage`` does not declare every element a
        formula reaches, so a ref can name a sibling chart on the same page and
        still fail Step 3a. On one tenant (2026-09) 8,814 refs across 62 names
        ended here.

        The same two guards as the global-name step, for the same reason
        (InputFields carry no confidenceScore): the name must identify exactly
        one element in this workbook, and that element must actually have the
        column. A name collision or a missing column means no edge.
        """
        if ref.column is None:
            return None
        emitted = [
            elem
            for elem in candidates
            if elem.elementId != chart_element_id
            and elementId_to_chart_urn.get(elem.elementId)
        ]
        if len(emitted) != 1:
            if emitted and count:
                self.reporter.chart_ref_workbook_name_ambiguous += 1
            return None
        target = emitted[0]
        wanted = ref.column.strip().lower()
        canonical = next(
            (c for c in target.columns if c.strip().lower() == wanted), None
        )
        if canonical is None:
            if count:
                self.reporter.chart_ref_workbook_name_column_absent += 1
            return None
        if count:
            self.reporter.chart_ref_workbook_name_resolved += 1
            logger.debug(
                "CHART REF WORKBOOK NAME element %s ref=%r: %r is the only "
                "emitted element of this workbook with that name and it has "
                "column %r, though /lineage did not list it as an upstream",
                chart_element_id,
                ref.raw,
                ref.source,
                canonical,
            )
        return (elementId_to_chart_urn[target.elementId], canonical)

    def _resolve_ref_by_global_element_name(
        self, ref: BracketRef, *, chart_element_id: str, count: bool
    ) -> Optional[Tuple[str, str]]:
        """Last resort: the ref names exactly one element in the whole run.

        InputFields carry no confidenceScore, so a wrong edge here would be
        indistinguishable from a right one. Two conditions therefore both have
        to hold, and either failing means no edge rather than a guess:

        * the name identifies exactly ONE Data Model element across every model
          in the run -- Sigma element names repeat, and a collision resolved by
          picking one would attach a real column to the wrong dataset;
        * that element actually OWNS the referenced column. This is what makes
          the widened scope safe: a same-named element that does not have the
          column is a coincidence, not the upstream.
        """
        if ref.column is None:
            return None
        candidates = self._global_dm_element_index().get(ref.source.strip().lower())
        if not candidates:
            return None
        if len(candidates) > 1:
            if count:
                self.reporter.chart_ref_global_name_ambiguous += 1
            return None
        urn = candidates[0]
        cols = self.dm_element_urn_to_cols.get(urn) or {}
        canonical = cols.get(ref.column.strip().lower())
        if canonical is None:
            if count:
                self.reporter.chart_ref_global_name_column_absent += 1
            return None
        if count:
            self.reporter.chart_ref_global_name_resolved += 1
            logger.debug(
                "CHART REF GLOBAL NAME element %s ref=%r: %r names exactly one "
                "Data Model element in this run and it owns column %r -> %s",
                chart_element_id,
                ref.raw,
                ref.source,
                canonical,
                urn,
            )
        return (urn, canonical)

    def _note_chart_ref_miss(
        self, reason: str, *, ref: BracketRef, chart_element_id: str, count: bool
    ) -> None:
        """Record WHY one formula ref did not resolve.

        ``chart_input_fields_self_ref_unresolved_refs`` counted 17,944 misses on
        one tenant (2026-09) with no breakdown, and they turned out to
        concentrate in 87 distinct source names -- so the bucket is a handful of
        causes, not seventeen thousand. ``count`` is False for the speculative
        candidate splits a join-chain ref tries, which would otherwise report
        several misses per ref.
        """
        if not count:
            return
        self.reporter.chart_ref_miss_reasons[reason] = (
            self.reporter.chart_ref_miss_reasons.get(reason, 0) + 1
        )
        if reason == _CHART_REF_MISS_UNKNOWN_SOURCE:
            known = ref.source.strip().lower() in self._global_dm_element_index()
            key = (
                "unknown_source_but_name_exists_in_another_data_model"
                if known
                else "unknown_source_absent_from_entire_run"
            )
            self.reporter.chart_ref_miss_reasons[key] = (
                self.reporter.chart_ref_miss_reasons.get(key, 0) + 1
            )
        if not logger.isEnabledFor(logging.DEBUG):
            return
        logger.debug(
            "CHART REF MISS element %s ref=%r reason=%s: source %r column %r",
            chart_element_id,
            ref.raw,
            reason,
            ref.source,
            ref.column,
        )

    def _resolve_chart_formula_upstream(
        self,
        ref: BracketRef,
        *,
        chart_element_id: str,
        chart_upstream_element_ids: Set[str],
        dm_upstream_urn_by_element_name: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        element_warehouse_table_index: Dict[str, List[str]],
        elementId_to_chart_urn: Dict[str, str],
        count: bool = True,
    ) -> Optional[Tuple[str, str]]:
        """Resolve a single bracket ref to (entity_urn, field_path), or None.

        Column-level counting (resolved / self_ref_fallback / skipped_parameter
        / skipped_sibling) happens in the caller (_build_element_input_fields) so
        every chart column lands in exactly one counter bucket regardless of how
        many refs its formula contains.

        This method does bump a few *diagnostic* name-matching counters, which is
        why ``count`` exists. A join-chain ref is resolved by trying up to 2N-3
        candidate splits through here, and each speculative attempt would
        otherwise inflate those counters several times over for one ref. Pass
        ``count=False`` when probing; the winning candidate is not re-counted
        either, so these counters measure refs, not attempts.

        Returns None for parameter and bare-sibling refs (caller handles those
        at the column level).  Returns (upstream_urn, ref.column) on success.

        Resolution order:
          1. is_parameter -> None.
          2. column is None (bare [col]) -> None (sibling ref).
          3. wb_element_index match, filtered first by chart_upstream_element_ids
             (SheetUpstream element_ids) then by dm_upstream_urn_by_element_name
             (DataModelElementUpstream, keyed by DM element name):
             - SheetUpstream: -> sibling chart URN + ref.column.
             - DM element: -> DM element Dataset URN + ref.column.
             - Ambiguous (>1 sheet match, none passing the filters) -> None.
          3c. No workbook page element named ref.source, but dm_upstream_urn_by_element_name
              has a match — covers DM elements that are formula upstreams of this chart
              but are not exposed as page elements.
          4. element_warehouse_table_index match with exactly one candidate
             -> warehouse Dataset URN + ref.column.
             NOTE: this index is built from the current element's dataset_inputs
             only. A formula that references a warehouse table whose SQL query
             is parsed on a different sibling element will not resolve here.
          5. else -> None.
        """
        if ref.is_parameter:
            return None

        if ref.column is None:
            # Bare refs are same-element sibling references.
            return None

        maybe_candidates = self._chart_ref_candidates(
            ref,
            chart_element_id=chart_element_id,
            wb_element_index=wb_element_index,
            count=count,
        )
        if maybe_candidates is None:
            self._note_chart_ref_miss(
                _CHART_REF_MISS_SELF_OR_AMBIGUOUS_CANDIDATES,
                ref=ref,
                chart_element_id=chart_element_id,
                count=count,
            )
            return None
        candidates = maybe_candidates
        if candidates:
            # Step 3a: SheetUpstream match (intra-workbook chart→chart lineage).
            sheet_matches = [
                elem
                for elem in candidates
                if elem.elementId in chart_upstream_element_ids
                and elem.elementId != chart_element_id
            ]
            if len(sheet_matches) == 1:
                elem_urn = elementId_to_chart_urn.get(sheet_matches[0].elementId)
                if elem_urn:
                    return (elem_urn, ref.column)
                # Element exists in the workbook but was filtered from chart emission
                # (e.g. pivot-table or control). Fall through to DM check.
            elif len(sheet_matches) > 1:
                # Ambiguous name collision not resolved by lineage filter.
                self._note_chart_ref_miss(
                    _CHART_REF_MISS_AMBIGUOUS_SIBLING,
                    ref=ref,
                    chart_element_id=chart_element_id,
                    count=count,
                )
                return None

            # Step 3b: DataModelElementUpstream match — ref.source is the DM
            # element's workbook-page name; resolve to its Dataset URN.
            dm_urn = dm_upstream_urn_by_element_name.get(ref.source)
            if dm_urn:
                return (dm_urn, ref.column)

            # If the element IS a registered upstream (sheet_matches==1) but was
            # filtered from chart emission and has no DM match, stop here — do not
            # fall through to warehouse because the formula ref explicitly targets
            # a known (filtered) element, not a warehouse table.
            if sheet_matches:
                self._note_chart_ref_miss(
                    _CHART_REF_MISS_UPSTREAM_FILTERED,
                    ref=ref,
                    chart_element_id=chart_element_id,
                    count=count,
                )
                return None

            # sheet_matches is empty: the workbook element is not a registered
            # lineage upstream. The element may share its name with a warehouse table
            # in the chart's data path (e.g. a sibling element whose SQL selects
            # from the same table). Fall through to Step 4 so the ref can still
            # resolve to the warehouse table URN.
            logger.debug(
                "Formula ref source %r matched workbook element names but none were "
                "lineage upstreams for chart element %s; falling through to "
                "warehouse-table resolution.",
                ref.source,
                chart_element_id,
            )
        else:
            # Step 3c: No workbook page element is named ref.source (candidates was
            # empty above), but the formula ref may still point to a DM element that
            # is an upstream of this chart without being exposed as a page element.
            # Check dm_upstream_urn_by_element_name directly before falling through
            # to the warehouse-table short-name index.
            dm_urn = dm_upstream_urn_by_element_name.get(ref.source)
            if dm_urn:
                return (dm_urn, ref.column)

        # Step 4: warehouse-table short-name fallback.
        wh_candidates = element_warehouse_table_index.get(ref.source.upper(), [])
        if len(wh_candidates) == 1:
            return (wh_candidates[0], ref.column)
        elif len(wh_candidates) > 1:
            logger.debug(
                "Ambiguous warehouse table formula ref source %r for field %r; "
                "candidate URNs were %s.",
                ref.source,
                ref.column,
                wh_candidates,
            )
            self._note_chart_ref_miss(
                _CHART_REF_MISS_AMBIGUOUS_WAREHOUSE,
                ref=ref,
                chart_element_id=chart_element_id,
                count=count,
            )
            return None

        # Step 5: the ref names a Data Model element that this chart's own
        # upstream list does not offer. Accepted only when the name is unique
        # run-wide AND that element owns the column -- see the helper.
        #
        # Only on a REAL attempt. A join-chain ref tries up to 2N-3 candidate
        # splits through here, and this step is deliberately the most permissive
        # one: letting it judge a speculative split would let a wrong split
        # validate and be accepted ahead of the right one, which is the failure
        # the split search exists to avoid.
        if count:
            workbook_match = self._resolve_ref_by_workbook_element_name(
                ref,
                candidates=candidates,
                chart_element_id=chart_element_id,
                elementId_to_chart_urn=elementId_to_chart_urn,
                count=count,
            )
            if workbook_match is not None:
                return workbook_match
            global_match = self._resolve_ref_by_global_element_name(
                ref, chart_element_id=chart_element_id, count=count
            )
            if global_match is not None:
                return global_match

        # Nothing matched at any step. ``candidates`` distinguishes the two
        # shapes of this: a workbook element WAS named ref.source but is neither
        # a lineage upstream nor a warehouse table, versus nothing in this
        # workbook is called that at all.
        self._note_chart_ref_miss(
            _CHART_REF_MISS_NAMED_BUT_NOT_AN_UPSTREAM
            if candidates
            else _CHART_REF_MISS_UNKNOWN_SOURCE,
            ref=ref,
            chart_element_id=chart_element_id,
            count=count,
        )
        return None

    def _handle_warehouse_table_upstream(
        self,
        upstream: WarehouseTableUpstream,
        element: Element,
        wb_warehouse_table_index: _WorkbookWarehouseIndex,
        dataset_inputs: Dict[str, List[str]],
    ) -> None:
        # Prefer urlId-based lookup (confident 1:1 match from workbook lineage).
        warehouse_urn = wb_warehouse_table_index.by_url_id.get(upstream.url_id)
        if warehouse_urn is None:
            # Fall back to name-based lookup; skip on ambiguity.
            name_key = upstream.name.upper()
            candidates = wb_warehouse_table_index.by_name.get(name_key, [])
            if not candidates:
                self.reporter.chart_warehouse_table_name_unmatched += 1
                logger.debug(
                    "Sigma chart BFS table %r (url_id=%s) not found in workbook "
                    "warehouse table index for chart %s; treating as unresolved.",
                    upstream.name,
                    upstream.url_id,
                    element.elementId,
                )
                return
            if len(candidates) > 1:
                self.reporter.chart_warehouse_table_name_ambiguous += 1
                if upstream.name not in self._ambiguous_table_name_warned:
                    self._ambiguous_table_name_warned.add(upstream.name)
                    self.reporter.warning(
                        title="Sigma chart warehouse table name is ambiguous",
                        message=(
                            "The table name matched multiple warehouse Dataset URNs "
                            "and the ID-based lookup produced no match; the lineage "
                            "edge was skipped. Set `default_database` in "
                            "`connection_to_platform_map` for the affected connection "
                            "to make table URNs unique."
                        ),
                        context=(
                            f"element={element.elementId}, "
                            f"table={upstream.name!r}, "
                            f"candidates={candidates}"
                        ),
                    )
                return
            warehouse_urn = candidates[0]
        if warehouse_urn not in dataset_inputs:
            # No deduped counter here (unlike element_dm_edge.deduped) — BFS
            # produces at most one type=table node per urlId per element, so
            # duplicates are not expected in practice.
            dataset_inputs[warehouse_urn] = []
            self.reporter.chart_warehouse_upstream_emitted += 1

    def _get_element_input_details(
        self,
        element: Element,
        workbook: Workbook,
        elementId_to_chart_urn: Dict[str, str],
        wb_warehouse_table_index: Optional[_WorkbookWarehouseIndex] = None,
    ) -> Tuple[Dict[str, List[str]], List[str]]:
        """
        Returns (dataset_inputs, chart_input_urns).

        dataset_inputs: Sigma Dataset / warehouse-table / DM element URNs to
            SQL-parsed warehouse URNs (non-empty only for Sigma Dataset
            upstreams matched against the SQL query; empty list otherwise).
        chart_input_urns: sorted list of chart URNs from intra-workbook sheet upstreams.

        wb_warehouse_table_index=None means BFS warehouse-table resolution is
        disabled (extract_lineage=False or pattern blocked); an empty
        _WorkbookWarehouseIndex means enabled but no type=table entries found.
        """
        dataset_inputs: Dict[str, List[str]] = {}
        chart_input_urns: Set[str] = set()
        sql_parser_in_tables: List[str] = []

        data_source_platform_details = self._get_element_data_source_platform_details(
            f"{workbook.path}/{workbook.name}/{element.name}"
        )

        if element.query and data_source_platform_details:
            try:
                sql_parser_in_tables = create_lineage_sql_parsed_result(
                    query=element.query.strip(),
                    default_db=data_source_platform_details.default_db,
                    default_schema=data_source_platform_details.default_schema,
                    platform=data_source_platform_details.data_source_platform,
                    env=data_source_platform_details.env,
                    platform_instance=data_source_platform_details.platform_instance,
                    generate_column_lineage=False,
                ).in_tables
            except Exception:
                logger.debug(f"Unable to parse query of element {element.name}")

        for node_id, upstream in element.upstream_sources.items():
            if isinstance(upstream, DatasetUpstream):
                sigma_dataset_id = node_id.split("-")[-1]
                if not upstream.name:
                    # SQL-bridge cannot run without a name, so no chart-to-
                    # Sigma-dataset edge is emitted for this upstream.
                    # The previous ``DatasetUpstream.name: str`` contract
                    # raised a Pydantic ``ValidationError`` that surfaced
                    # as a ``SourceReport.warning`` with full Pydantic
                    # context. Now that ``name`` is ``Optional[str]``,
                    # surface equivalent context through ``report.warning``
                    # (``LossyList``-backed -- auto-truncates after N
                    # entries, so this is already rate-limited) alongside
                    # the counter so production operators can triage which
                    # upstream / which workbook element triggered the drop.
                    self.reporter.chart_dataset_upstream_name_missing += 1
                    self.reporter.warning(
                        title="Sigma workbook dataset upstream dropped (name missing)",
                        message="A workbook element references a Sigma Dataset "
                        "upstream whose ``name`` field was ``null`` on the "
                        "``/workbooks/{id}/lineage`` payload. No chart-to-"
                        "Sigma-dataset edge can be SQL-correlated for this "
                        "upstream; the edge is skipped. See the "
                        "``chart_dataset_upstream_name_missing`` counter for "
                        "the aggregate drop count.",
                        context=(
                            f"node={node_id}, sigma_dataset_id={sigma_dataset_id}, "
                            f"element={element.name} ({element.elementId}), "
                            f"workbook={workbook.name} ({workbook.workbookId})"
                        ),
                    )
                    continue
                upstream_name_lower = upstream.name.lower()
                for in_table_urn in list(sql_parser_in_tables):
                    # Chart-level SQL lineage uses substring matching because
                    # Sigma dataset upstream names often include the warehouse
                    # table leaf plus extra display context. Formula refs below
                    # use exact short-name matching because formulas reference a
                    # concrete table identifier such as [ORDERS/id].
                    if (
                        DatasetUrn.from_string(in_table_urn).name.split(".")[-1]
                        in upstream_name_lower
                    ):
                        dataset_urn = self._gen_sigma_dataset_urn(sigma_dataset_id)
                        if dataset_urn not in dataset_inputs:
                            dataset_inputs[dataset_urn] = [in_table_urn]
                        else:
                            dataset_inputs[dataset_urn].append(in_table_urn)
                        sql_parser_in_tables.remove(in_table_urn)
            elif isinstance(upstream, SheetUpstream):
                chart_urn = elementId_to_chart_urn.get(upstream.element_id)
                if chart_urn is None:
                    # Target element type not in our allow-list.
                    logger.debug(
                        f"Upstream elementId {upstream.element_id} not in element map "
                        f"for element {element.name}; likely filtered by get_page_elements "
                        # Narrowed when ingest_pivot_and_input_tables is off,
                        # so the line never names a type this run rejected.
                        f"(allowlist: {sorted(self._admitted_element_types())})"
                    )
                    self.reporter.num_filtered_sheet_upstreams += 1
                    continue
                chart_input_urns.add(chart_urn)
            elif isinstance(upstream, DataModelElementUpstream):
                # Workbook element references a DM element. Failures are
                # surfaced through report counters.
                dm_urn, candidates = self._resolve_dm_element_upstream_urn(upstream)
                if dm_urn is not None:
                    if dm_urn in dataset_inputs:
                        # Diamond reference: same DM element reached via
                        # multiple lineage nodeIds on this chart.
                        self.reporter.element_dm_edge.deduped += 1
                    else:
                        dataset_inputs[dm_urn] = []
                        self.reporter.element_dm_edge.resolved += 1
                        # Count ambiguity once per chart-to-DM edge, not
                        # once per diamond sourceId.
                        if len(candidates) > 1:
                            self.reporter.element_dm_edge.ambiguous += 1
                            logger.warning(
                                "Ambiguous DM element name %r in DM %s: "
                                "%d candidates (%s). Picked lowest URN "
                                "%s deterministically.",
                                upstream.name,
                                upstream.data_model_url_id,
                                len(candidates),
                                ", ".join(sorted(candidates)),
                                dm_urn,
                            )
            elif isinstance(upstream, WarehouseTableUpstream):
                if wb_warehouse_table_index is None:
                    continue
                self._handle_warehouse_table_upstream(
                    upstream, element, wb_warehouse_table_index, dataset_inputs
                )

        # Unmatched SQL-parsed warehouse tables become direct dataset inputs.
        # Guard against overlap with BFS-resolved warehouse URNs (same physical
        # table reachable via both paths).
        for in_table_urn in sql_parser_in_tables:
            if in_table_urn not in dataset_inputs:
                dataset_inputs[in_table_urn] = []

        return dataset_inputs, sorted(chart_input_urns)

    def _bridge_warehouse_column_name(
        self,
        *,
        upstream_urn: str,
        sigma_display_name: str,
        column_native_names: Dict[str, str],
        element_id: Optional[str] = None,
    ) -> str:
        """Translate Sigma display name to warehouse-native column name.

        Returns sigma_display_name unchanged when upstream is a Sigma URN (DM
        element, Sigma Dataset) or when column_native_names has no entry for
        the display name. For non-Sigma warehouse Dataset URNs, looks up the
        cased native name built by _gen_elements_workunit.
        """
        if not column_native_names:
            return sigma_display_name
        try:
            platform = (
                DatasetUrn.from_string(upstream_urn)
                .get_data_platform_urn()
                .platform_name
            )
        except InvalidUrnError:
            logger.debug(
                "Could not parse upstream URN %r for column bridge; "
                "returning display name unchanged.",
                upstream_urn,
            )
            return sigma_display_name
        if platform == "sigma":
            return sigma_display_name
        native = column_native_names.get(sigma_display_name)
        if native is not None:
            if native != sigma_display_name:
                self.reporter.chart_input_fields_warehouse_column_bridged += 1
            return native
        self.reporter.chart_input_fields_warehouse_column_bridge_unresolved += 1
        # Print WHAT the map held, not just that the lookup missed. Without it
        # "map was empty", "present under different casing" and "genuinely
        # absent" are indistinguishable, and they need different fixes.
        casefold_hit = next(
            (
                key
                for key in column_native_names
                if key.casefold() == sigma_display_name.casefold()
            ),
            None,
        )
        if casefold_hit is not None:
            self.reporter.chart_input_fields_bridge_case_only_miss += 1
        logger.debug(
            "Column bridge unresolved: display name %r not in native-name map "
            "for upstream %r (element=%s); map has %d entr(ies), "
            "case-insensitive match=%r, keys sample=%r; fieldPath will use the "
            "display name.",
            sigma_display_name,
            upstream_urn,
            element_id,
            len(column_native_names),
            casefold_hit,
            sorted(column_native_names)[:15],
        )
        warn_key = (upstream_urn, sigma_display_name)
        if warn_key not in self._bridge_unresolved_warned:
            self._bridge_unresolved_warned.add(warn_key)
            self.reporter.warning(
                title="Sigma chart column bridge unresolved",
                message=(
                    "A chart column's Sigma display name could not be mapped to a "
                    "warehouse-native column name. The emitted `fieldPath` will use "
                    "the display name, which may not match the warehouse column and "
                    "will silently break column-level lineage."
                ),
                context=(
                    f"element={element_id}, "
                    f"display_name={sigma_display_name!r}, "
                    f"upstream={upstream_urn}"
                ),
            )
        return sigma_display_name

    def _resolve_chart_ref(
        self,
        ref: BracketRef,
        *,
        chart_element_id: str,
        chart_upstream_element_ids: Set[str],
        dm_upstream_urn_by_element_name: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        element_warehouse_table_index: Dict[str, List[str]],
        elementId_to_chart_urn: Dict[str, str],
    ) -> Optional[Tuple[str, str]]:
        """Resolve one formula ref, choosing the strategy by segment count.

        Split out of _build_element_input_fields, which otherwise carries this
        three-way choice inside an already deep loop.
        """
        result = self._resolve_chart_join_chain_ref(
            ref,
            chart_element_id=chart_element_id,
            chart_upstream_element_ids=chart_upstream_element_ids,
            dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
            wb_element_index=wb_element_index,
            element_warehouse_table_index=element_warehouse_table_index,
            elementId_to_chart_urn=elementId_to_chart_urn,
        )
        if result is None and len(ref.parts) <= 2:
            # Single-slash refs never had a candidate search, so
            # the ordinary resolver is their only path.
            result = self._resolve_chart_formula_upstream(
                ref,
                chart_element_id=chart_element_id,
                chart_upstream_element_ids=chart_upstream_element_ids,
                dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                wb_element_index=wb_element_index,
                element_warehouse_table_index=element_warehouse_table_index,
                elementId_to_chart_urn=elementId_to_chart_urn,
            )
        elif result is None:
            # Every split failed schema validation. The legacy
            # first-slash reading would resolve here, but it
            # names the un-split remainder as the column -- a
            # field the upstream provably does not have. The
            # Data Model path drops such refs rather than emit a
            # dangling schemaField URN; this now matches it, and
            # the column falls back to a self-reference so it
            # still appears in the V2 column list.
            self.reporter.chart_join_chain_dangling_suppressed += 1
        return result

    def _count_unresolved_chart_column(
        self,
        *,
        element: Element,
        column: str,
        refs: List[BracketRef],
        all_param: bool,
        all_sibling: bool,
        formulas_incomplete: bool,
    ) -> None:
        """File one self-referential column under the reason it got there.

        The fallback bucket is the largest in the report (~81k on one tenant,
        2026-09) and a single number for it says nothing: a column with no
        formula is expected, a column whose refs failed to resolve may be
        hiding a parse defect, and a column from a workbook whose /columns call
        aborted is neither -- it is our fetch that failed. Only the middle case
        is logged, so the probe cannot flood the log.
        """
        if all_param:
            self.reporter.chart_input_fields_skipped_parameter += 1
            return
        if all_sibling:
            self.reporter.chart_input_fields_skipped_sibling += 1
            return
        self.reporter.chart_input_fields_self_ref_fallback += 1
        if refs:
            self.reporter.chart_input_fields_self_ref_unresolved_refs += 1
            logger.debug(
                "chart element %s column %r: self-ref fallback with "
                "unresolved refs=%r segment_counts=%r",
                element.elementId,
                column,
                [r.raw for r in refs],
                [len(r.parts) for r in refs],
            )
        elif formulas_incomplete:
            # This workbook's /columns fetch aborted, so the absence of a
            # formula says nothing about the column. Counting it as "Sigma
            # reported no formula" made a fetch failure read as an upstream
            # limitation.
            self.reporter.chart_input_fields_formulas_not_fetched += 1
        else:
            self.reporter.chart_input_fields_self_ref_no_formula += 1

    def _build_element_input_fields(
        self,
        *,
        element: Element,
        chart_urn: str,
        chart_upstream_eids: Set[str],
        dm_upstream_urn_by_element_name: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        element_warehouse_table_index: Dict[str, List[str]],
        elementId_to_chart_urn: Dict[str, str],
        wb_only_warehouse_keys: FrozenSet[str] = frozenset(),
        formulas_incomplete: bool = False,
    ) -> List[InputFieldClass]:
        """Emit exactly one InputField per chart column.

        schemaFieldUrn points to the resolved upstream when a formula ref can be
        matched; otherwise falls back to a self-referential URN so the column
        always appears in the V2 column list regardless of formula parseability.

        Counter invariant per element:
          resolved + self_ref_fallback + skipped_parameter + skipped_sibling
          == len(element.columns)

        ``self_ref_fallback`` is split three ways by cause:
        ``self_ref_unresolved_refs`` (a formula existed and its refs did not
        resolve -- the population that can hide a resolver defect),
        ``formulas_not_fetched`` (this workbook's /columns call aborted, so no
        formula was ever retrieved) and ``self_ref_no_formula`` (Sigma really
        reported none). ``formulas_incomplete`` is what separates the middle
        one; without it a fetch failure is indistinguishable from an upstream
        limitation.

        wb_only_warehouse_keys: uppercase table names that are present only in
          the workbook-level index (not in the per-element SQL-parser index).
          Used to sub-categorise chart_input_fields_warehouse_qualified into
          chart_input_fields_warehouse_qualified_via_workbook_index.
        """
        fields: List[InputFieldClass] = []
        for column in element.columns:
            formula = element.column_formulas.get(column)
            # Bound unconditionally: the diagnostic probes below read it even
            # for columns with no formula at all.
            refs: List[BracketRef] = []
            resolved_refs: List[_ResolvedRef] = []
            seen: Set[Tuple[str, str]] = set()
            all_param = False
            all_sibling = False

            if formula is not None:
                refs = extract_bracket_refs(formula)
                if refs:
                    param_count = 0
                    sibling_count = 0
                    for ref in refs:
                        if ref.is_parameter:
                            param_count += 1
                            continue
                        if ref.column is None:
                            sibling_count += 1
                            continue
                        # A join-chain ref gets every split tried and validated
                        # against the candidate upstream's schema.
                        result = self._resolve_chart_ref(
                            ref,
                            chart_element_id=element.elementId,
                            chart_upstream_element_ids=chart_upstream_eids,
                            dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                            wb_element_index=wb_element_index,
                            element_warehouse_table_index=element_warehouse_table_index,
                            elementId_to_chart_urn=elementId_to_chart_urn,
                        )
                        if result is not None:
                            upstream_urn, upstream_field = result
                            key = (upstream_urn, upstream_field)
                            if key not in seen:
                                seen.add(key)
                                resolved_refs.append(
                                    _ResolvedRef(
                                        upstream_urn=upstream_urn,
                                        upstream_field=upstream_field,
                                        ref=ref,
                                    )
                                )
                    if not resolved_refs and refs:
                        total = len(refs)
                        if param_count == total:
                            all_param = True
                        elif sibling_count == total:
                            all_sibling = True

            multi_segment = self._multi_segment_refs(refs)
            if multi_segment:
                # Does the chart path see join-chain refs at all? It shares the
                # parser with the DM path and returns ref.column with no schema
                # check, so a mis-split here emits a wrong or dangling
                # InputField instead of falling back. The counter answers the
                # scoping question in the report; the log line names the refs.
                self.reporter.chart_input_fields_multi_segment_ref += 1
                logger.debug(
                    "chart element %s column %r: join-chain refs %r (resolved=%s)",
                    element.elementId,
                    column,
                    multi_segment,
                    bool(resolved_refs),
                )
            if resolved_refs:
                self.reporter.chart_input_fields_resolved += 1
                self.reporter.chart_input_fields_multi_ref_extra += (
                    len(resolved_refs) - 1
                )
                for rr in resolved_refs:
                    bridged_field = self._bridge_warehouse_column_name(
                        upstream_urn=rr.upstream_urn,
                        sigma_display_name=rr.upstream_field,
                        column_native_names=element.column_native_names,
                        element_id=element.elementId,
                    )
                    schema_field_urn = builder.make_schema_field_urn(
                        rr.upstream_urn, bridged_field
                    )
                    # Sub-category: resolved via warehouse-table short-name index (Step 4).
                    # The resolver (Step 4) returns None for ambiguous (>1 candidate) keys,
                    # so upstream_urn in wh_candidates implies a single-candidate match in
                    # practice, but the membership check is the semantically correct predicate.
                    wh_candidates = element_warehouse_table_index.get(
                        rr.ref.source.upper(), []
                    )
                    if rr.upstream_urn in wh_candidates:
                        self.reporter.chart_input_fields_warehouse_qualified += 1
                        if rr.ref.source.upper() in wb_only_warehouse_keys:
                            self.reporter.chart_input_fields_warehouse_qualified_via_workbook_index += 1
                    fields.append(
                        InputFieldClass(
                            schemaFieldUrn=schema_field_urn,
                            schemaField=self._make_string_schema_field(column),
                        )
                    )
            else:
                schema_field_urn = builder.make_schema_field_urn(chart_urn, column)
                self._count_unresolved_chart_column(
                    element=element,
                    column=column,
                    refs=refs,
                    all_param=all_param,
                    all_sibling=all_sibling,
                    formulas_incomplete=formulas_incomplete,
                )
                fields.append(
                    InputFieldClass(
                        schemaFieldUrn=schema_field_urn,
                        schemaField=self._make_string_schema_field(column),
                    )
                )
        return fields

    def _gen_elements_workunit(
        self,
        elements: List[Element],
        workbook: Workbook,
        all_input_fields: List[InputFieldClass],
        paths: List[str],
        elementId_to_chart_urn: Dict[str, str],
        wb_element_index: Dict[str, List[Element]],
        wb_warehouse_table_index: Optional[_WorkbookWarehouseIndex],
        customsql_extra_inputs: Optional[Dict[str, List[str]]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma page element to Datahub Chart
        """
        for element in elements:
            chart_urn = builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.get_urn_part(),
            )

            custom_properties = {
                "VizualizationType": str(element.vizualizationType),
                "type": str(element.type) if element.type else "Unknown",
            }

            yield self._gen_entity_status_aspect(chart_urn)

            dataset_inputs, chart_input_urns = self._get_element_input_details(
                element, workbook, elementId_to_chart_urn, wb_warehouse_table_index
            )

            # Add warehouse upstream URNs resolved by the workbook-level customSQL registry.
            # These URNs are resolved before pages are emitted so ChartInfo.inputs is complete.
            for warehouse_urn in (customsql_extra_inputs or {}).get(
                element.elementId, []
            ):
                if warehouse_urn not in dataset_inputs:
                    dataset_inputs[warehouse_urn] = []

            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=ChartInfoClass(
                    title=element.name,
                    description="",
                    lastModified=ChangeAuditStampsClass(),
                    customProperties=custom_properties,
                    externalUrl=element.url,
                    inputs=list(dataset_inputs.keys()),
                    inputEdges=(
                        [
                            EdgeClass(destinationUrn=urn, sourceUrn=chart_urn)
                            for urn in chart_input_urns
                        ]
                        if chart_input_urns
                        else None
                    ),
                ),
            ).as_workunit()

            if workbook.workspaceId:
                self.reporter.workspaces.increment_elements_count(workbook.workspaceId)

                yield self._gen_entity_browsepath_aspect(
                    entity_urn=chart_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            # Only Sigma Dataset URNs (with SQL-matched warehouse upstreams) need
            # the cross-entity UpstreamLineage aspect emitted later.
            for dataset_urn, warehouse_urns in dataset_inputs.items():
                if (
                    warehouse_urns
                    and dataset_urn not in self.dataset_upstream_urn_mapping
                ):
                    self.dataset_upstream_urn_mapping[dataset_urn] = warehouse_urns

            # Build per-element context for formula-based InputFields resolution.
            # chart_upstream_eids contains element_ids of SheetUpstream neighbors
            # only. SheetUpstream is the only upstream variant that carries an
            # element_id matching another workbook page element — it represents
            # intra-workbook chart→chart lineage (same workbook, different element).
            # DatasetUpstream (warehouse table) and DataModelElementUpstream (DM
            # element loaded into the workbook) are handled separately below.
            chart_upstream_eids: Set[str] = {
                upstream.element_id
                for upstream in element.upstream_sources.values()
                if isinstance(upstream, SheetUpstream)
            }
            # DataModelElementUpstream: map DM element workbook-page name -> Dataset URN.
            # Look up directly from the name maps without re-incrementing element_dm_edge
            # counters (those were already bumped inside _get_element_input_details).
            dm_upstream_urn_by_element_name: Dict[str, str] = {}
            for upstream in element.upstream_sources.values():
                if isinstance(upstream, DataModelElementUpstream) and upstream.name:
                    name_map = self.dm_element_urn_by_name.get(
                        upstream.data_model_url_id, {}
                    )
                    candidates = name_map.get(upstream.name.lower(), [])
                    if candidates:
                        chosen = sorted(candidates)[0]
                        existing = dm_upstream_urn_by_element_name.get(upstream.name)
                        if existing is not None and existing != chosen:
                            self.reporter.chart_input_fields_dm_upstream_name_collision += 1
                            logger.debug(
                                "DM upstream name collision for element %s: "
                                "name %r maps to both %r and %r; keeping first.",
                                element.elementId,
                                upstream.name,
                                existing,
                                chosen,
                            )
                        else:
                            dm_upstream_urn_by_element_name[upstream.name] = chosen

            element_warehouse_table_index = self._build_element_warehouse_table_index(
                dataset_inputs
            )
            # Merge per-element (SQL-parser) index with per-workbook index.
            # Per-element entries take precedence on key conflict — the SQL parser
            # attributes tables specifically to this chart's own data path.
            wb_idx = (
                wb_warehouse_table_index.by_name if wb_warehouse_table_index else {}
            )
            merged_warehouse_table_index = self._merge_warehouse_table_indices(
                element_warehouse_table_index,
                wb_idx,
            )
            wb_only_warehouse_keys: FrozenSet[str] = frozenset(
                k for k in wb_idx if k not in element_warehouse_table_index
            )

            # Build column_native_names for warehouse-direct charts. For each
            # WarehouseTableUpstream, extract the warehouse-native column name
            # from the columnId ("inode-{urlId}/{NATIVE}") and apply per-connection
            # casing (convert_urns_to_lowercase, default True).
            if element.column_id_by_name:
                element.column_native_names = {}
                for upstream in element.upstream_sources.values():
                    if not isinstance(upstream, WarehouseTableUpstream):
                        continue
                    prefix = f"inode-{upstream.url_id}/"
                    conn_id = self._wb_url_id_to_conn_id.get(upstream.url_id, "")
                    conn_override = self.config.connection_to_platform_map.get(conn_id)
                    lowercase = (
                        conn_override.convert_urns_to_lowercase
                        if conn_override is not None
                        else True
                    )
                    for display_name, col_id in element.column_id_by_name.items():
                        if col_id.startswith(prefix):
                            native_upper = col_id[len(prefix) :]
                            native = native_upper.lower() if lowercase else native_upper
                            existing = element.column_native_names.get(display_name)
                            if existing is not None and existing != native:
                                self.reporter.chart_input_fields_column_native_names_collision += 1
                                logger.debug(
                                    "column_native_names collision for element %s: "
                                    "display name %r maps to both %r and %r across "
                                    "warehouse upstreams; keeping first.",
                                    element.elementId,
                                    display_name,
                                    existing,
                                    native,
                                )
                            else:
                                element.column_native_names[display_name] = native

            element_input_fields = self._build_element_input_fields(
                element=element,
                chart_urn=chart_urn,
                chart_upstream_eids=chart_upstream_eids,
                dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                wb_element_index=wb_element_index,
                element_warehouse_table_index=merged_warehouse_table_index,
                elementId_to_chart_urn=elementId_to_chart_urn,
                wb_only_warehouse_keys=wb_only_warehouse_keys,
                formulas_incomplete=(
                    workbook.workbookId
                    in self.sigma_api.column_formulas_incomplete_workbooks
                ),
            )

            # Stash formula-derived fields for customSQL charts so we can merge at
            # drain time, ensuring warehouse-resolved entries supplement rather than
            # replace computed/unmapped column entries from the formula pass.
            if chart_urn in self._workbook_customsql_registered_urns:
                self._workbook_customsql_formula_fields[chart_urn] = (
                    element_input_fields
                )

            # For customSQL charts a second InputFields MCP is emitted at drain time
            # by _build_workbook_chart_input_fields_mcp; the drain MCP supersedes this
            # one (later in the workunit stream).  The formula-derived fields stashed
            # above are merged into the drain MCP so nothing is silently dropped.
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(fields=element_input_fields),
            ).as_workunit()

            all_input_fields.extend(element_input_fields)

    def _gen_pages_workunit(
        self,
        workbook: Workbook,
        paths: List[str],
        customsql_extra_inputs: Optional[Dict[str, List[str]]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma workbook page to Datahub dashboard
        """
        # Both maps are built once at workbook scope — intra-workbook lineage can
        # cross pages, so all elements must be indexed before processing any page.
        # Keys mirror the chart-emission allow-list in get_page_elements
        # (SigmaAPI.ingested_element_types); filtered types are absent from both.
        elementId_to_chart_urn: Dict[str, str] = {
            element.elementId: builder.make_chart_urn(
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                name=element.get_urn_part(),
            )
            for page in workbook.pages
            for element in page.elements
        }
        wb_element_index = self._build_workbook_element_index(workbook)
        # Build the workbook-level warehouse-table index once per workbook.
        # Gated on extract_lineage + workbook_lineage_pattern to match the
        # analogous gates for formula fetch and element upstream resolution.
        wb_warehouse_table_index: Optional[_WorkbookWarehouseIndex]
        if self.config.extract_lineage and self.config.workbook_lineage_pattern.allowed(
            workbook.name
        ):
            wb_warehouse_table_index = self._build_workbook_warehouse_table_index(
                workbook
            )
        else:
            wb_warehouse_table_index = None

        for page in workbook.pages:
            dashboard_urn = self._gen_dashboard_urn(page.get_urn_part())

            yield self._gen_entity_status_aspect(dashboard_urn)

            yield self._gen_dashboard_info_workunit(page)

            dpi_aspect = self._gen_dataplatform_instance_aspect(dashboard_urn)
            if dpi_aspect:
                yield dpi_aspect

            all_input_fields: List[InputFieldClass] = []

            if workbook.workspaceId:
                self.reporter.workspaces.increment_pages_count(workbook.workspaceId)
                yield self._gen_entity_browsepath_aspect(
                    entity_urn=dashboard_urn,
                    parent_entity_urn=builder.make_container_urn(
                        self._gen_workspace_key(workbook.workspaceId)
                    ),
                    paths=paths + [workbook.name],
                )

            yield from self._gen_elements_workunit(
                page.elements,
                workbook,
                all_input_fields,
                paths,
                elementId_to_chart_urn,
                wb_element_index,
                wb_warehouse_table_index,
                customsql_extra_inputs or {},
            )

            yield MetadataChangeProposalWrapper(
                entityUrn=dashboard_urn,
                aspect=InputFieldsClass(
                    fields=list(
                        {
                            (field.schemaFieldUrn, field.schemaField.fieldPath): field
                            for field in all_input_fields
                            if field.schemaField is not None
                        }.values()
                    )
                ),
            ).as_workunit()

    def _process_workbook_customsql_lineage(
        self, workbook: Workbook
    ) -> Dict[str, List[str]]:
        """Register all workbook customSQL charts with the aggregator.

        Returns element_id → upstream dataset URNs for injection into
        ChartInfo.inputs before page workunits are emitted.
        """
        custom_sql_by_name, element_ids_by_customsql_name = (
            self._build_workbook_customsql_registry(workbook)
        )
        element_by_id: Dict[str, Element] = {
            e.elementId: e for page in workbook.pages for e in page.elements
        }
        customsql_extra_inputs: Dict[str, List[str]] = {}
        # Sorted so the "first wins" behaviour in _process_workbook_customsql_element
        # is deterministic across runs regardless of API response order.
        for csql_name, customsql_entry in sorted(custom_sql_by_name.items()):
            element_ids = element_ids_by_customsql_name.get(csql_name) or []
            if not element_ids:
                self.reporter.workbook_customsql_skipped += 1
                continue
            for element_id in element_ids:
                chart_urn = builder.make_chart_urn(
                    platform=self.platform,
                    platform_instance=self.config.platform_instance,
                    name=element_id,
                )
                element = element_by_id.get(element_id)
                if element is not None:
                    self._build_workbook_customsql_col_mapping(element, chart_urn)
                self._process_workbook_customsql_element(chart_urn, customsql_entry)
                # The upstream_urns here come from a synchronous pre-drain parse;
                # the aggregator produces the same list at drain time from the same SQL.
                # See _parse_customsql_upstream_dataset_urns for details.
                upstream_urns = self._parse_customsql_upstream_dataset_urns(
                    customsql_entry
                )
                if upstream_urns:
                    customsql_extra_inputs[element_id] = upstream_urns
        return customsql_extra_inputs

    def _gen_workbook_workunit(self, workbook: Workbook) -> Iterable[MetadataWorkUnit]:
        """
        Map Sigma Workbook to Datahub container
        """
        owner_username = self.sigma_api.get_user_name(workbook.ownerId)

        dashboard_urn = self._gen_dashboard_urn(workbook.workbookId)

        yield self._gen_entity_status_aspect(dashboard_urn)

        lastModified = AuditStampClass(
            time=int(workbook.updatedAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )
        created = AuditStampClass(
            time=int(workbook.createdAt.timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )

        dashboard_info_cls = DashboardInfoClass(
            title=workbook.name,
            description=workbook.description if workbook.description else "",
            dashboards=[
                EdgeClass(
                    destinationUrn=self._gen_dashboard_urn(page.get_urn_part()),
                    sourceUrn=dashboard_urn,
                )
                for page in workbook.pages
            ],
            externalUrl=workbook.url,
            lastModified=ChangeAuditStampsClass(
                created=created, lastModified=lastModified
            ),
            customProperties={
                "path": workbook.path,
                "latestVersion": str(workbook.latestVersion),
            },
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn, aspect=dashboard_info_cls
        ).as_workunit()

        # Set subtype
        yield MetadataChangeProposalWrapper(
            entityUrn=dashboard_urn,
            aspect=SubTypesClass(typeNames=[BIContainerSubTypes.SIGMA_WORKBOOK]),
        ).as_workunit()

        # Ownership
        owner_urn = (
            builder.make_user_urn(owner_username)
            if self.config.ingest_owner and owner_username
            else None
        )
        if owner_urn:
            yield from add_owner_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                owner_urn=owner_urn,
            )

        # Tags
        tags = [workbook.badge] if workbook.badge else None
        if tags:
            yield from add_tags_to_entity_wu(
                entity_type="dashboard",
                entity_urn=dashboard_urn,
                tags=sorted(tags),
            )

        paths = workbook.path.split("/")[1:]
        if workbook.workspaceId:
            self.reporter.workspaces.increment_workbooks_count(workbook.workspaceId)

            yield self._gen_entity_browsepath_aspect(
                entity_urn=dashboard_urn,
                parent_entity_urn=builder.make_container_urn(
                    self._gen_workspace_key(workbook.workspaceId)
                ),
                paths=paths + [workbook.name],
            )

            if len(paths) == 0:
                yield from add_entity_to_container(
                    container_key=self._gen_workspace_key(workbook.workspaceId),
                    entity_type="dashboard",
                    entity_urn=dashboard_urn,
                )

        # Build customSQL registry before emitting pages so the resolved upstream dataset
        # URNs can be included in ChartInfo.inputs (entity-level lineage for the UI).
        customsql_extra_inputs = (
            self._process_workbook_customsql_lineage(workbook)
            if self.config.extract_lineage
            else {}
        )
        yield from self._gen_pages_workunit(workbook, paths, customsql_extra_inputs)

    def _gen_sigma_dataset_upstream_lineage_workunit(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        for (
            dataset_urn,
            upstream_dataset_urns,
        ) in self.dataset_upstream_urn_mapping.items():
            yield MetadataChangeProposalWrapper(
                entityUrn=dataset_urn,
                aspect=UpstreamLineage(
                    upstreams=[
                        Upstream(
                            dataset=upstream_dataset_urn, type=DatasetLineageType.COPY
                        )
                        for upstream_dataset_urn in upstream_dataset_urns
                    ],
                ),
            ).as_workunit()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:  # noqa: C901
        """DataHub Ingestion framework entry point."""
        logger.info("Sigma plugin execution is started")
        # Reset per-run customSQL state so re-invoking this method on the same
        # instance (e.g. in test harnesses) does not leak state from a prior run.
        # Close existing aggregators before clearing so SQLite tempfiles are released.
        for _agg in self._sql_aggregators.values():
            _agg.close()
        self._sql_aggregators.clear()
        self._customsql_col_mappings.clear()
        self._customsql_passthrough_mappings.clear()
        self._customsql_registered_urns.clear()
        self._customsql_extra_upstreams.clear()
        self._customsql_extra_fgls.clear()
        self._workbook_customsql_registered_urns.clear()
        self._workbook_customsql_formula_fields.clear()
        self.sigma_api.fill_workspaces()

        # Materialize the Sigma Dataset list once and populate the
        # ``url_id -> urn`` bridge map eagerly *before* DM iteration.
        # Previously the map was populated as a side-effect of
        # ``_gen_dataset_workunit`` yielding, which left
        # ``_resolve_dm_element_external_upstream`` dependent on
        # iteration order: the map was only complete because datasets
        # were yielded before DMs and the pipeline framework drained
        # generators sequentially. An eager pre-pass decouples
        # external-upstream resolution from emission order so any
        # future refactor that reorders or parallelizes the yields
        # cannot silently burn through the ``unresolved_external``
        # counter.
        datasets = list(self.sigma_api.get_sigma_datasets())
        for dataset in datasets:
            self.sigma_dataset_urn_by_url_id[dataset.get_urn_part()] = (
                self._gen_sigma_dataset_urn(dataset.get_urn_part())
            )

        for dataset in datasets:
            yield from self._gen_dataset_workunit(dataset)
        # Data Models are emitted before Workbooks so the workbook-to-DM
        # bridge is populated before workbook elements resolve upstreams.
        # Two-pass: populate bridges for every DM first, then emit. Required
        # because DMs can forward-reference each other and ``/dataModels``
        # is not ordered by dependency.
        if self.config.ingest_data_models:
            # Accumulates every DM that will emit: listed DMs plus any
            # personal-space / unlisted DMs reached via discovery below.
            all_data_models: List[SigmaDataModel] = self.sigma_api.get_data_models()
            elementId_maps_by_dm: Dict[str, Dict[str, str]] = {}

            # Discovery loop: prepopulate bridges for every known DM, then
            # fetch any unresolved cross-DM ``<prefix>`` (personal-space
            # DMs absent from /v2/dataModels, or listings that failed),
            # register them, and repeat until stable. Each iteration scans
            # only ``pending`` (newly-added DMs) to keep the walk O(N) in
            # source_ids. ``unresolved_seen`` prevents failed-fetch retries.
            unresolved_seen: Set[str] = set()
            pending = list(all_data_models)
            # Belt-and-braces cap on discovery passes. The loop terminates
            # naturally because ``unresolved_seen`` monotonically grows and
            # ``get_data_model_by_url_id`` returns None on 4xx (filtered DMs
            # are also seen-marked), so the set of reachable ``urlId`` prefixes
            # is finite. The cap exists to protect against pathological
            # Sigma payloads (e.g. a chain of personal-space DMs that each
            # reference newly-discovered personal-space DMs) by degrading
            # gracefully with a ``SourceReport.warning`` rather than looping
            # unbounded.
            max_rounds = self.config.max_personal_dm_discovery_rounds
            discovery_round = 0
            while pending:
                discovery_round += 1
                for dm in pending:
                    # Defensive against re-enqueue via a different urlId
                    # for the same dataModelId.
                    if dm.dataModelId in elementId_maps_by_dm:
                        continue
                    elementId_maps_by_dm[dm.dataModelId] = (
                        self._prepopulate_dm_bridge_maps(dm)
                    )

                # Short-circuit: discovery can't yield an emittable DM when
                # ``ingest_shared_entities=False`` (every candidate is either
                # a personal-space DM or a workspace_pattern-denied one).
                # Skip the per-prefix fetches entirely. Bridges for listed
                # DMs remain populated from the prepopulate step above.
                if not self.config.ingest_shared_entities:
                    break

                new_unresolved = self._collect_unresolved_cross_dm_prefixes(pending)
                # Sort for deterministic discovery order (set iteration is
                # hash-randomized across Python interpreter runs).
                unresolved = sorted(new_unresolved - unresolved_seen)
                unresolved_seen |= new_unresolved
                pending = []

                if not unresolved:
                    # Natural termination: no new prefixes to explore.
                    break

                if discovery_round >= max_rounds:
                    # Defensive: stop before the next API fan-out only when
                    # there are actually unresolved prefixes to abandon.
                    # Checking after collecting unresolved ensures the warning
                    # never fires spuriously when ``max_personal_dm_discovery_rounds``
                    # is set to a low value but the graph was already fully
                    # resolved in earlier rounds (e.g. the common case where
                    # all listed-DM bridges are prepopulated in round 1 and
                    # no orphan prefixes remain).
                    self.reporter.warning(
                        title="personal-space Data Model discovery cap reached",
                        message=(
                            "Personal-space DM discovery reached the "
                            "``max_personal_dm_discovery_rounds`` cap; "
                            "remaining unresolved ``<urlId>`` prefixes will "
                            "not be fetched on this run. Cross-DM edges into "
                            "those DMs will be reported under "
                            "``element_dm_edge.unresolved`` / "
                            "``data_model_element_cross_dm_upstreams_dm_unknown``. "
                            "Raise ``max_personal_dm_discovery_rounds`` if this "
                            "is legitimate, or investigate the DM graph for a "
                            "reference cycle."
                        ),
                        context=(
                            f"cap={max_rounds}, "
                            f"abandoned_count={len(unresolved)}, "
                            f"abandoned_prefixes={unresolved[:10]}"
                        ),
                    )
                    break

                for prefix in unresolved:
                    discovered_dm = self.sigma_api.get_data_model_by_url_id(prefix)
                    if discovered_dm is None:
                        self.reporter.data_model_external_reference_unresolved += 1
                        continue
                    # Re-apply ``workspace_pattern`` / ``data_model_pattern``
                    # so a denied workspace or DM name cannot leak via
                    # cross-DM references. Filtered orphans are treated as
                    # never-registered (``dm_unknown`` at resolution time)
                    # because the operator never opted them in. If the DM
                    # has a workspaceId but the workspace is unreachable
                    # (admin-only / deleted), mirror the listed-DM path:
                    # count under ``data_models_without_workspace``.
                    if discovered_dm.workspaceId:
                        workspace = self.sigma_api.get_workspace(
                            discovered_dm.workspaceId
                        )
                        if workspace and not self.config.workspace_pattern.allowed(
                            workspace.name
                        ):
                            self.reporter.data_models.dropped(
                                f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                                f"in {workspace.name} (discovered, workspace_pattern denied)"
                            )
                            continue
                        if workspace is None:
                            # For discovered DMs, drop rather than emitting
                            # with a phantom parent Container URN that points
                            # at a workspace that was never produced in this
                            # run (403 / deleted workspace). Consistent with
                            # the workspace_pattern-denied path above.
                            # Emit a structured warning so the operator can
                            # see which DM was dropped and why without
                            # tailing stdout — get_workspace() only debugs.
                            self.reporter.warning(
                                title="Sigma discovered Data Model dropped: workspace unreachable",
                                message=(
                                    "A cross-DM reference resolved to a Data Model "
                                    "whose workspace could not be fetched (403, deleted, "
                                    "or exception). The DM is dropped to avoid emitting "
                                    "a phantom parent Container URN. Cross-DM edges into "
                                    "this DM will be counted as "
                                    "``data_model_element_cross_dm_upstreams_dm_unknown``."
                                ),
                                context=(
                                    f"dm={discovered_dm.name} ({discovered_dm.dataModelId}), "
                                    f"workspace_id={discovered_dm.workspaceId}"
                                ),
                            )
                            self.reporter.data_models.dropped(
                                f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                                f"(discovered, workspace {discovered_dm.workspaceId} "
                                f"unreachable or deleted)"
                            )
                            self.reporter.data_models_without_workspace += 1
                            continue
                    if not self.config.data_model_pattern.allowed(discovered_dm.name):
                        self.reporter.data_models.dropped(
                            f"{discovered_dm.name} ({discovered_dm.dataModelId}) "
                            f"(discovered, filtered by data_model_pattern)"
                        )
                        continue
                    self.reporter.data_model_external_references_discovered += 1
                    all_data_models.append(discovered_dm)
                    pending.append(discovered_dm)
                    # Eagerly prepopulate bridge maps so that the alias
                    # (requested prefix != canonical urlId) is registered
                    # immediately. This prevents source_ids carrying the
                    # old slug from resolving as ``dm_unknown`` even when
                    # Sigma returns the DM under a rotated canonical urlId.
                    # The next-round prepopulate loop skips already-registered
                    # DMs via the ``dataModelId in elementId_maps_by_dm`` guard.
                    if discovered_dm.dataModelId not in elementId_maps_by_dm:
                        elementId_maps_by_dm[discovered_dm.dataModelId] = (
                            self._prepopulate_dm_bridge_maps(
                                discovered_dm, requested_alias=prefix
                            )
                        )
                    elif prefix != discovered_dm.get_url_id():
                        # DM is already registered from the listed path, but
                        # the discovery prefix differs from the canonical urlId
                        # (slug rotation). Register the old slug as an alias so
                        # source_ids carrying it still resolve correctly.
                        self._register_dm_alias(prefix, discovered_dm.get_url_id())

            # Cheap insurance against a theoretical duplicate where the
            # same DM lands in ``all_data_models`` via both the listed and
            # discovered paths (relies on Sigma always returning ``urlId``
            # on ``/v2/dataModels``; if that ever changes we still emit
            # once per ``dataModelId``).
            emitted_data_model_ids: Set[str] = set()
            for data_model in all_data_models:
                if data_model.dataModelId in self.dm_collided_data_model_ids:
                    # First DM owns the bridge key; emitting this one would
                    # produce an unlinked orphan. Counter already bumped.
                    continue
                if data_model.dataModelId in emitted_data_model_ids:
                    continue
                emitted_data_model_ids.add(data_model.dataModelId)
                yield from self._gen_data_model_workunit(
                    data_model, elementId_maps_by_dm[data_model.dataModelId]
                )
        for workbook in self.sigma_api.get_sigma_workbooks():
            yield from self._gen_workbook_workunit(workbook)

        for workspace in self._get_allowed_workspaces():
            self.reporter.workspaces.processed(
                f"{workspace.name} ({workspace.workspaceId})"
            )
            yield from self._gen_workspace_workunit(workspace)
            if self.reporter.workspaces.workspace_counts.get(
                workspace.workspaceId, WorkspaceCounts()
            ).is_empty():
                logger.warning(
                    f"Workspace {workspace.name} ({workspace.workspaceId}) is empty. If this is not expected, add the user associated with the Client ID/Secret to each workspace with missing metadata"
                )
                self.reporter.empty_workspaces.append(
                    f"{workspace.name} ({workspace.workspaceId})"
                )
        yield from self._gen_sigma_dataset_upstream_lineage_workunit()
        yield from self._drain_sql_aggregators()

    def get_report(self) -> SourceReport:
        return self.reporter
