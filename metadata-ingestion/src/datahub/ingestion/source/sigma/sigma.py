import logging
from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Iterable, List, Literal, Optional, Set, Tuple

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
    MetadataWorkUnitProcessor,
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
    Workbook,
    WorkbookKey,
    Workspace,
    WorkspaceKey,
)
from datahub.ingestion.source.sigma.formula_parser import (
    BracketRef,
    extract_bracket_refs,
)
from datahub.ingestion.source.sigma.sigma_api import SigmaAPI
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
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
        # "Connection Root/<SCHEMA>"). In that case emit schema.table; otherwise
        # emit the full db.schema.table used by Snowflake and similar platforms.
        name = (
            f"{self.schema}.{self.table}"
            if self.db is None
            else f"{self.db}.{self.schema}.{self.table}"
        )
        if platform.lower() in _WAREHOUSE_LOWERCASE_PLATFORMS and lowercase:
            return name.lower()
        return name


@dataclass
class _CustomSqlRegistration:
    """Carries the per-kind variant parameters for _register_customsql_with_aggregator."""

    urn: str
    registered_set: Set[str]
    counter_prefix: str  # "dm_customsql" or "workbook_customsql"
    label: str  # "DM element" or "workbook chart"


@platform_name("Sigma")
@config_class(SigmaSourceConfig)
@support_status(SupportStatus.INCUBATING)
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
        # Inodes whose /files path already produced an unparseable warning;
        # prevents N identical warnings when the same inode spans N DMs (H3).
        self._files_path_unparseable_seen: Set[str] = set()
        # Connections for which a "default_database not configured" warning has
        # been emitted; dedup so multi-DM tenants don't flood the report.
        self._missing_default_db_warned: Set[str] = set()
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
            return None

        record = self.connection_registry.get(ref.connection_id)
        if record is None or not record.is_mappable:
            # Counter is bumped by caller gated on unresolved_seen to avoid
            # inflating on diamond source_ids.
            logger.debug(
                "inode-%s: connectionId %r not resolvable to a warehouse platform "
                "(missing from registry or is_mappable=False).",
                url_id_suffix,
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

    def _build_customsql_col_mapping(
        self,
        element: SigmaDataModelElement,
        element_dataset_urn: str,
    ) -> None:
        """Populate ``_customsql_col_mappings`` for one customSQL-backed element.

        Scans each column's formula for refs of the form ``[{element.name}/COL]``
        and maps each SQL column name (lowercased) to the Sigma display column
        name.  Only refs whose namespace matches the element's own name are
        registered — cross-element refs (``[OtherElement/COL]``) are skipped so
        they cannot pollute the rewrite map.  ``finditer`` is used so that
        formulas referencing multiple SQL columns register all of them.
        """
        mapping: Dict[str, str] = {}
        passthrough: Dict[str, str] = {}
        for col in element.columns:
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
                if source_id not in unresolved_seen:
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
            return None
        fine_grained = self._build_dm_element_fine_grained_lineages(
            element=element,
            element_dataset_urn=element_dataset_urn,
            element_name_to_eids=element_name_to_eids,
            elementId_to_dataset_urn=elementId_to_dataset_urn,
            entity_level_upstream_urns=set(upstream_urns),
            data_model=data_model,
            warehouse_url_id_map=warehouse_url_id_map,
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
        col_id = column.columnId or ""
        if not col_id.startswith("inode-"):
            return None
        suffix = col_id[len("inode-") :]
        url_id, sep, warehouse_col = suffix.partition("/")
        if not sep or not warehouse_col:
            return None

        # Verify this url_id is one of the element's declared warehouse sources.
        # Mismatches can occur if a column belongs to a different element's inode
        # (shouldn't happen with well-formed API data, but guards against drift).
        if f"inode-{url_id}" not in element.source_ids:
            return None

        # Guard: url_id must resolve in the warehouse map (i.e. /files succeeded).
        wh_ref = warehouse_url_id_map.get(url_id)
        if wh_ref is None:
            return None

        # Resolve the parent Dataset URN.  This is the only allowed path for
        # URN construction — env, platform_instance, and casing all live here,
        # so bypassing it risks orphan schemaFields on casing or instance drift.
        parent_urn = self._resolve_dm_element_warehouse_upstream(
            url_id_suffix=url_id,
            warehouse_map=warehouse_url_id_map,
        )
        if parent_urn is None:
            return None

        # Normalize column casing to match the platform convention used by the
        # warehouse connector.  _resolve_dm_element_warehouse_upstream already
        # resolved the registry record and override, so these lookups are cheap
        # dict hits on the same objects.
        record = self.connection_registry.get(wh_ref.connection_id)
        if record is None:
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
            return None
        canonical_col = upstream_cols.get(ref.column.lower())
        if canonical_col is None:
            self.reporter.data_model_element_fgl_cross_dm_dropped_unknown_upstream_column += 1
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
        data_model: SigmaDataModel,
        fgls: List[FineGrainedLineageClass],
        emitted_pairs: Set[Tuple[str, str]],
    ) -> None:
        """Resolve a formula ref against intra-DM sibling elements.

        Appends to fgls and emitted_pairs on success; bumps the appropriate
        counter on any resolution failure.  Counter bookkeeping and dedup are
        handled here so the caller needs no conditional on the result.
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
            # Intra-DM candidate(s) exist but none appear in /lineage.
            # Rare on tenants where /lineage correctly reports intra-DM edges;
            # keep counter for observability on future tenants.
            self.reporter.data_model_element_fgl_dropped_orphan_upstream += 1
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
        canonical_col = source_cols.get(ref.column.lower())
        if canonical_col is None:
            self.reporter.data_model_element_fgl_dropped_unknown_upstream_column += 1
            logger.debug(
                "DM %s element %s: ref %r column %r not found in upstream "
                "element %s schema winners; dropping FGL entry",
                data_model.dataModelId,
                element.elementId,
                ref.raw,
                ref.column,
                chosen_upstream_urn,
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
    ) -> List[FineGrainedLineageClass]:
        """Build FineGrainedLineage entries for intra-DM [ElementName/col] refs.

        element_name_to_eids maps lowercased element name → list of elementIds.
        Self-references are stripped before resolution: when a DM element is named
        after its warehouse source (e.g., element "data.csv" with formula
        "[data.csv/col]"), the formula ref matches the element's own name and must
        be filtered out to avoid self-referential FGL.  Warehouse-passthrough
        refs are resolved via _try_emit_warehouse_passthrough_fgl; unresolved
        remainder is counted under fgl_warehouse_passthrough_deferred.

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
            if not column.formula:
                continue
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
            for ref in extract_bracket_refs(column.formula):
                if ref.is_parameter or ref.column is None:
                    # [P_*] parameter refs and bare [col] intra-element refs
                    # are not cross-Dataset lineage; skip.
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
                        # All candidates were self-references; actual upstream is the
                        # warehouse table — use the pre-computed warehouse FGL.
                        if not warehouse_consumed:
                            warehouse_consumed = True
                            if warehouse_fgl is None:
                                self.reporter.data_model_element_fgl_warehouse_passthrough_deferred += 1
                            else:
                                assert warehouse_fgl.upstreams
                                pair = (downstream_field, warehouse_fgl.upstreams[0])
                                if pair not in emitted_pairs:
                                    emitted_pairs.add(pair)
                                    fgls.append(warehouse_fgl)
                                    self.reporter.data_model_element_fgl_warehouse_resolved += 1
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
                        if (column.columnId or "").startswith("inode-"):
                            self.reporter.data_model_element_fgl_warehouse_passthrough_deferred += 1
                            continue

                    # Source not in this DM — resolve via cross-DM sources that
                    # Sigma's /lineage explicitly reported for this element.
                    cross_dm_fgl = self._resolve_cross_dm_fgl(
                        ref=ref,
                        element=element,
                        element_dataset_urn=element_dataset_urn,
                        entity_level_upstream_urns=entity_level_upstream_urns,
                        downstream_field=downstream_field,
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
                    data_model=data_model,
                    fgls=fgls,
                    emitted_pairs=emitted_pairs,
                )
        # fgl_emitted is the umbrella count for intra-DM AND warehouse-passthrough
        # FGL (both appended to `fgls`). Cross-DM is tracked separately via
        # fgl_cross_dm_resolved. Warehouse-passthrough is also sub-counted in
        # fgl_warehouse_resolved (overlap intentional for independent triage).
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

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

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
    ) -> Dict[str, List[str]]:
        """Build {uppercase_table_name: [warehouse_dataset_urn, ...]} for one
        workbook from /v2/workbooks/{id}/lineage type=table entries.

        Same key shape as _build_element_warehouse_table_index: uppercase short
        table name → list of warehouse Dataset URNs.  Merged with the per-element
        index in _gen_elements_workunit before passing to _resolve_chart_formula_upstream.

        Reuses _files_cache, _files_path_unparseable_seen, and
        _resolve_dm_element_warehouse_upstream verbatim — URN identity with the
        DM element warehouse upstream path is guaranteed because the same
        construction path is used.
        """
        result: Dict[str, List[str]] = {}
        entries = self.sigma_api.get_workbook_lineage(workbook.workbookId)
        if entries is None:
            self.reporter.chart_input_fields_warehouse_index_lookup_failed += 1
            return result

        # Build a transient urlId → _WarehouseTableRef map from type=table entries,
        # then resolve each to a Dataset URN via _resolve_dm_element_warehouse_upstream
        # so the URN is byte-equal to the entity-level warehouse Dataset URN for the
        # same table.
        transient_map: Dict[str, _WarehouseTableRef] = {}
        url_id_to_table_name: Dict[str, str] = {}

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
            path_invalid = (
                not url_id or not table_name or len(parts) != 3 or not all(parts)
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
                            "Expected exactly 'Connection Root/<DB>/<SCHEMA>' with "
                            "no empty segments and a non-empty urlId. "
                            "Warehouse table index entry skipped for this inode."
                        ),
                        context=(
                            f"workbook={workbook.workbookId}, inode={inode_id}, "
                            f"path={path!r}, url_id={url_id!r}, "
                            f"table_name={table_name!r}"
                        ),
                    )
                continue

            if url_id in transient_map:
                logger.warning(
                    "Workbook %s: two type=table lineage entries share the same "
                    "urlId %r; the earlier entry will be overwritten.",
                    workbook.workbookId,
                    url_id,
                )
            transient_map[url_id] = _WarehouseTableRef(
                connection_id=conn_id,
                db=parts[1],
                schema=parts[2],
                table=table_name,
            )
            url_id_to_table_name[url_id] = table_name

        for url_id in transient_map:
            table_name = url_id_to_table_name[url_id]
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
            result.setdefault(table_name.upper(), []).append(urn)

        return result

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
    ) -> Optional[Tuple[str, str]]:
        """Resolve a single bracket ref to (entity_urn, field_path), or None.

        This method is a pure predicate: it never increments any reporter counter.
        All column-level counting (resolved / self_ref_fallback / skipped_parameter
        / skipped_sibling) happens in the caller (_build_element_input_fields) so
        every chart column lands in exactly one counter bucket regardless of how
        many refs its formula contains.

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

        candidates = wb_element_index.get(ref.source, [])
        if not candidates:
            case_mismatched_names = [
                name for name in wb_element_index if name.lower() == ref.source.lower()
            ]
            if case_mismatched_names:
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
                logger.debug(
                    "No exact-case workbook element match for formula ref source %r; "
                    "falling back to warehouse-table resolution.",
                    ref.source,
                )

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
                return None

            # Step 3b: DataModelElementUpstream match — ref.source is the DM
            # element's workbook-page name; resolve to its Dataset URN.
            dm_urn = dm_upstream_urn_by_element_name.get(ref.source)
            if dm_urn:
                return (dm_urn, ref.column)

            logger.debug(
                "Formula ref source %r matched workbook element names but none were "
                "lineage upstreams for chart element %s; treating as unresolved.",
                ref.source,
                chart_element_id,
            )
            return None

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
            return None

        return None

    def _get_element_input_details(
        self,
        element: Element,
        workbook: Workbook,
        elementId_to_chart_urn: Dict[str, str],
    ) -> Tuple[Dict[str, List[str]], List[str]]:
        """
        Returns (dataset_inputs, chart_input_urns).

        dataset_inputs: Sigma Dataset / warehouse-table / DM element URNs to
            SQL-parsed warehouse URNs (non-empty only for Sigma Dataset
            upstreams matched against the SQL query; empty list otherwise).
        chart_input_urns: sorted list of chart URNs from intra-workbook sheet upstreams.
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
                    # Target element type not in our allow-list (e.g. pivot-table).
                    logger.debug(
                        f"Upstream elementId {upstream.element_id} not in element map "
                        f"for element {element.name}; likely filtered by get_page_elements "
                        f"(allowlist: table, visualization)"
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

        # Unmatched SQL-parsed warehouse tables become direct dataset inputs.
        for in_table_urn in sql_parser_in_tables:
            dataset_inputs[in_table_urn] = []

        return dataset_inputs, sorted(chart_input_urns)

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
    ) -> List[InputFieldClass]:
        """Emit exactly one InputField per chart column.

        schemaFieldUrn points to the resolved upstream when a formula ref can be
        matched; otherwise falls back to a self-referential URN so the column
        always appears in the V2 column list regardless of formula parseability.

        Counter invariant per element:
          resolved + self_ref_fallback + skipped_parameter + skipped_sibling
          == len(element.columns)

        wb_only_warehouse_keys: uppercase table names that are present only in
          the workbook-level index (not in the per-element SQL-parser index).
          Used to sub-categorise chart_input_fields_warehouse_qualified into
          chart_input_fields_warehouse_qualified_via_workbook_index.
        """
        fields: List[InputFieldClass] = []
        for column in element.columns:
            formula = element.column_formulas.get(column)
            resolved: Optional[Tuple[str, str]] = None
            resolving_ref = None
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
                        resolved = self._resolve_chart_formula_upstream(
                            ref,
                            chart_element_id=element.elementId,
                            chart_upstream_element_ids=chart_upstream_eids,
                            dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                            wb_element_index=wb_element_index,
                            element_warehouse_table_index=element_warehouse_table_index,
                            elementId_to_chart_urn=elementId_to_chart_urn,
                        )
                        if resolved is not None:
                            resolving_ref = ref
                            break
                    if resolved is None and refs:
                        total = len(refs)
                        if param_count == total:
                            all_param = True
                        elif sibling_count == total:
                            all_sibling = True

            if resolved is not None:
                upstream_urn, upstream_field = resolved
                schema_field_urn = builder.make_schema_field_urn(
                    upstream_urn, upstream_field
                )
                self.reporter.chart_input_fields_resolved += 1
                # Sub-category: resolved via warehouse-table short-name index (Step 4).
                # The resolver (Step 4) returns None for ambiguous (>1 candidate) keys,
                # so upstream_urn in wh_candidates implies a single-candidate match in
                # practice, but the membership check is the semantically correct predicate.
                # resolving_ref is always set when resolved is set (see loop above),
                # but guard explicitly so -O optimisation doesn't hide the invariant.
                if resolving_ref is not None:
                    wh_candidates = element_warehouse_table_index.get(
                        resolving_ref.source.upper(), []
                    )
                    if upstream_urn in wh_candidates:
                        self.reporter.chart_input_fields_warehouse_qualified += 1
                        if resolving_ref.source.upper() in wb_only_warehouse_keys:
                            self.reporter.chart_input_fields_warehouse_qualified_via_workbook_index += 1
            elif all_param:
                schema_field_urn = builder.make_schema_field_urn(chart_urn, column)
                self.reporter.chart_input_fields_skipped_parameter += 1
            elif all_sibling:
                schema_field_urn = builder.make_schema_field_urn(chart_urn, column)
                self.reporter.chart_input_fields_skipped_sibling += 1
            else:
                schema_field_urn = builder.make_schema_field_urn(chart_urn, column)
                self.reporter.chart_input_fields_self_ref_fallback += 1

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
        wb_warehouse_table_index: Dict[str, List[str]],
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
                element, workbook, elementId_to_chart_urn
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
                        dm_upstream_urn_by_element_name[upstream.name] = sorted(
                            candidates
                        )[0]

            element_warehouse_table_index = self._build_element_warehouse_table_index(
                dataset_inputs
            )
            # Merge per-element (SQL-parser) index with per-workbook index.
            # Per-element entries take precedence on key conflict — the SQL parser
            # attributes tables specifically to this chart's own data path.
            merged_warehouse_table_index = self._merge_warehouse_table_indices(
                element_warehouse_table_index,
                wb_warehouse_table_index,
            )
            wb_only_warehouse_keys: FrozenSet[str] = frozenset(
                k
                for k in wb_warehouse_table_index
                if k not in element_warehouse_table_index
            )

            element_input_fields = self._build_element_input_fields(
                element=element,
                chart_urn=chart_urn,
                chart_upstream_eids=chart_upstream_eids,
                dm_upstream_urn_by_element_name=dm_upstream_urn_by_element_name,
                wb_element_index=wb_element_index,
                element_warehouse_table_index=merged_warehouse_table_index,
                elementId_to_chart_urn=elementId_to_chart_urn,
                wb_only_warehouse_keys=wb_only_warehouse_keys,
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
        # (type in {"table","visualization"}); filtered types are absent from both.
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
        if self.config.extract_lineage and self.config.workbook_lineage_pattern.allowed(
            workbook.name
        ):
            wb_warehouse_table_index = self._build_workbook_warehouse_table_index(
                workbook
            )
        else:
            wb_warehouse_table_index = {}

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
