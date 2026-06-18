"""ThoughtSpot DataHub connector source implementation.

This module implements metadata extraction from ThoughtSpot using REST API v2.0.
It extracts Liveboards (dashboards), Answers (charts), Worksheets, and Tables
organized within Workspace containers.
"""

import logging
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Protocol,
    Set,
    Tuple,
    Union,
)

from datahub.configuration.common import AllowDenyPattern
from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_dashboard_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
    make_user_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import (
    BIAssetSubTypes,
    BIContainerSubTypes,
    DatasetSubTypes,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source.thoughtspot.client import (
    _KEY_BUILDERS,
    _TS_TO_DATAHUB_PLATFORM,
    ThoughtSpotAPIError,
    ThoughtSpotAuthenticationError,
    ThoughtSpotClient,
    ThoughtSpotPermissionError,
)
from datahub.ingestion.source.thoughtspot.config import (
    ExternalConnectionConfig,
    ThoughtSpotConfig,
)
from datahub.ingestion.source.thoughtspot.models import (
    AnswerResponse,
    ColumnResponse,
    ConnectionResponse,
    LiveboardResponse,
    LogicalTableResponse,
    TagRef,
    ThoughtSpotAuthor,
    VisualizationResponse,
    WorkspaceResponse,
)
from datahub.ingestion.source.thoughtspot.report import ThoughtSpotReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    BooleanTypeClass,
    ChartUsageStatisticsClass,
    DashboardUsageStatisticsClass,
    DatasetLineageTypeClass,
    DateTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    GlobalTagsClass,
    InputFieldClass,
    InputFieldsClass,
    NullTypeClass,
    NumberTypeClass,
    OwnershipTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TagAssociationClass,
    TimeTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
    ViewPropertiesClass,
)
from datahub.metadata.urns import CorpUserUrn
from datahub.sdk.chart import Chart
from datahub.sdk.dashboard import Dashboard
from datahub.sdk.dataset import Dataset, UpstreamInputType
from datahub.sql_parsing.sqlglot_lineage import (
    SqlParsingResult,
    create_and_cache_schema_resolver,
    sqlglot_lineage,
)

logger = logging.getLogger(__name__)

# Canonical RFC 4122 GUID: 32 hex digits + 4 hyphens = 36 chars total.
# TS REST v2 occasionally returns a GUID where we expect a human-readable
# login (``author.name`` mirrors ``author.id`` when only the GUID was
# available). ``_looks_like_guid`` lets the resolver skip such values
# rather than emit a ``urn:li:corpuser:<guid>`` placeholder that the
# DataHub UI would render as an unidentifiable owner badge.
#
# Using a strict regex (not just length + hyphen count) so genuinely
# weird logins that happen to be 36 chars with 4 hyphens (e.g.
# ``"username-with-exactly-four-hyphens!!!"``) are correctly accepted as
# real logins rather than misclassified as GUIDs.
_GUID_RE: re.Pattern[str] = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


def _looks_like_guid(s: str) -> bool:
    """Return True iff ``s`` is shaped like a canonical RFC 4122 GUID."""
    return bool(_GUID_RE.match(s))


def _looks_like_login(s: str) -> bool:
    """Return True iff ``s`` could plausibly be a corpuser login.

    TS endpoints sometimes surface the display name ("John Doe") in
    ``author.name`` when the login wasn't separately exposed via
    ``authorName``. Constructing ``urn:li:corpuser:John Doe`` yields a
    malformed URN that DataHub silently drops; reject any candidate with
    whitespace before it reaches ``make_user_urn``.
    """
    return bool(s) and not any(c.isspace() for c in s)


class _HasAuthor(Protocol):
    """Structural type for any TS response that may carry author info.

    Implemented in practice by ``WorkspaceResponse``, ``LiveboardResponse``,
    ``VisualizationResponse``, ``AnswerResponse``, ``LogicalTableResponse``
    (all inherit ``ThoughtSpotMetadataHeader``). Declared as a Protocol so
    ``_resolve_author_login`` doesn't need the concrete Union — and so
    test fixtures can pass duck-typed objects without inheritance.
    """

    author_name: Optional[str]
    author: Optional["ThoughtSpotAuthor"]


def _resolve_author_login(metadata: _HasAuthor) -> Optional[str]:
    """Pick the human-readable login for the metadata's author, skipping
    GUID-shaped values.

    Precedence: wire-level ``author_name`` > ``author.name`` > ``None``.
    A canonical-GUID-shaped login is treated as "no real login" so
    callers can omit the field entirely.
    """
    login = metadata.author_name or (
        metadata.author.name if metadata.author and metadata.author.name else None
    )
    if not login:
        return None
    if _looks_like_guid(login):
        return None
    if not _looks_like_login(login):
        return None
    return login


@dataclass(frozen=True)
class ExternalRef:
    """Resolved external-table reference for one TS Logical Table.

    Computed once per table in ``_resolve_external_upstream`` and reused
    by the table-level and column-level edge emitters.
    """

    platform: str  # DataHub platform name (e.g. "databricks")
    urn: str  # Full DataHub dataset URN
    env: str  # PROD / STAGING / etc.
    platform_instance: Optional[str]
    # When True, column names flow into schemaField URNs as-is (BigQuery,
    # SQL Server, case-quoted Databricks). Default False: lowercased to
    # match the canonical DataHub schemaField convention used by most
    # source connectors (Databricks, Snowflake, Postgres, ...).
    preserve_column_case: bool = False


class _SchemaFieldCacheKey(NamedTuple):
    """Hashable key for ``_schema_field_cache``.

    Identifies a TS column by the fields ``_make_schema_field`` actually
    reads, so two ``ColumnResponse`` objects that differ only in
    irrelevant fields (e.g. ``id``) still hit the same cache slot. Using
    a NamedTuple instead of a bare ``Tuple[Optional[str], ...]`` makes
    the positional contract self-documenting at the call site.
    """

    name: Optional[str]
    data_type: Optional[str]
    description: Optional[str]
    column_type: Optional[str]


@dataclass(frozen=True)
class SqlViewWarehouseRef:
    """Resolved warehouse context for one TS SQL_VIEW.

    SQL views differ from Worksheets in that they don't have a 1:1
    physical mapping (``physical_database_name`` / ``physical_table_name``
    are None) — the upstream tables live inside the SQL statement
    itself. So ``ExternalRef`` (which represents a single resolved
    upstream URN) doesn't fit; we just need the warehouse identity
    so sqlglot can parse the SQL.

    Built by ``_resolve_sql_view_warehouse`` from
    ``LogicalTableResponse.data_source_type`` (for dialect) +
    ``external_connections`` config (for platform_instance / env).
    Crucially, no successful ``/connections/search`` lookup is
    required — both inputs are available without an extra TS API
    call, so SQL views whose connection the principal can't read
    still get parsed lineage.
    """

    platform: str  # DataHub platform name (e.g. "databricks")
    env: str  # PROD / STAGING / etc.
    platform_instance: Optional[str]
    # ``default_db`` is best-effort: populated when the connection
    # lookup happens to succeed (gives sqlglot a default for
    # unqualified table refs). ``None`` is fine — sqlglot handles
    # fully-qualified SQL without it.
    default_db: Optional[str] = None


# Shared empty-override sentinel returned by
# ``_external_connection_overrides[_by_id]`` when no override is
# configured for a connection. ``ExternalConnectionConfig`` is treated
# as immutable by every downstream consumer (``_resolve_external_upstream``,
# ``_resolve_sql_view_warehouse``), so sharing one instance per process
# saves ~N pydantic-model allocations at N-table scale.
_EMPTY_EXTERNAL_CONNECTION_OVERRIDES = ExternalConnectionConfig()


class WorkspaceKey(ContainerKey):
    """Container key for ThoughtSpot workspaces.

    ThoughtSpot organizes content within workspaces (also called Orgs).
    Each workspace has a unique GUID that serves as its identifier.
    """

    workspace_id: str


@platform_name("ThoughtSpot")
@support_status(SupportStatus.INCUBATING)
@config_class(ThoughtSpotConfig)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DESCRIPTIONS, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Workspaces (Orgs) emit as containers")
@capability(
    SourceCapability.OWNERSHIP,
    "Enabled by default, configured via `include_ownership`",
)
@capability(
    SourceCapability.TAGS, "Resolved from `/tags/search` and emitted as GlobalTags"
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Worksheet→Table, Chart→Worksheet, Dashboard→Worksheet, plus cross-platform "
    "Worksheet→external warehouse (Databricks / Snowflake / BigQuery / ...) "
    "configured via `external_connections`",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage from TS's pre-resolved `columns[*].sources` field "
    "on `metadata/search` (TS-internal) and from `physicalColumnName` "
    "(cross-platform external). TML edocs are consulted separately for "
    "chart-layer details (search query, source-table FQNs, chart type), "
    "not for column-level lineage.",
)
@capability(
    SourceCapability.USAGE_STATS,
    "Per-entity cumulative view counts via `metadata_search` `include_stats`; "
    "enabled by `include_usage_stats: true`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled via stateful ingestion",
    supported=True,
)
@capability(SourceCapability.TEST_CONNECTION, "Enabled by default")
class ThoughtSpotSource(StatefulIngestionSourceBase, TestableSource):
    """
    Extract metadata from ThoughtSpot via REST API v2.0.

    This source extracts:
    - Workspaces (as containers)
    - Liveboards (as dashboards)
    - Answers (as charts)
    - Worksheets and Tables (as datasets)

    Implementation follows DataHub SDK V2 patterns for entity creation.
    """

    platform = "thoughtspot"

    def __init__(self, config: ThoughtSpotConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.report: ThoughtSpotReport = ThoughtSpotReport()
        self.client = ThoughtSpotClient(self.config.connection, report=self.report)

        # Lazy cache of {worksheet_id -> {column_name -> ColumnResponse}}.
        # Built on first access by ``_get_worksheet_columns_lookup`` and
        # reused by both ``_process_visualization`` (for Chart InputFields
        # with real SchemaField content) and ``_extract_datasets`` (so we
        # don't refetch worksheets twice from the API).
        self._worksheet_columns_lookup: Optional[
            dict[str, dict[str, "ColumnResponse"]]
        ] = None
        self._logical_tables_cache: Optional[List[LogicalTableResponse]] = None

        # Lazy cache of {tag_id -> tag_name} populated on first access by
        # ``_get_tag_lookup``. Single ``client.get_tags()`` round-trip per
        # ingestion run.
        self._tag_lookup: Optional[Dict[str, str]] = None

        # Lazy {connection_id -> ConnectionResponse} cache populated on
        # first access by ``_get_connection_lookup``. One
        # ``/connections/search`` round-trip per ingestion run.
        self._connection_lookup: Optional[Dict[str, ConnectionResponse]] = None
        self._unresolvable_external_lineage_count: int = 0

        # Per-run memo for ``_make_self_dataset_urn``. At 10K-table scale
        # the URN-builder is hit ~25× per distinct ``table_id`` (column
        # sources, chart inputs, dashboard inputs, lineage upstreams).
        # The three resolution inputs (platform / env / platform_instance)
        # are run-invariant so a simple ``Dict[str, str]`` cache is safe.
        self._self_dataset_urn_cache: Dict[str, str] = {}

        # Per-run memo for ``_make_schema_field``. Keyed by a
        # ``_SchemaFieldCacheKey`` NamedTuple over the column fields the
        # body actually reads, so a worksheet column referenced by N
        # visualisations yields ONE shared ``SchemaFieldClass`` instance
        # instead of N identical ones.
        self._schema_field_cache: Dict[_SchemaFieldCacheKey, SchemaFieldClass] = {}

        # Per-run ``(entity_id, views)`` accumulators populated during
        # ``_process_liveboard`` / ``_process_answer``; consumed by
        # ``_process_usage_stats`` to emit usage aspects from
        # ``entity.stats.views`` without a separate API round-trip.
        #
        # Stored as ``(id, views)`` tuples — not the full Pydantic models —
        # because at 10K+ dashboards the per-entity overhead (header +
        # visualizations + TML-derived data, ~45KB each) would otherwise
        # accumulate to several hundred MB of cache for fields the
        # emitter doesn't read. We filter zero/None views at append time
        # so the consume loop has nothing to skip.
        self._liveboard_usage: List[Tuple[str, int]] = []
        self._answer_usage: List[Tuple[str, int]] = []

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to ThoughtSpot API."""
        test_report = TestConnectionReport()
        try:
            config = ThoughtSpotConfig.model_validate(config_dict)
            client = ThoughtSpotClient(config.connection)

            # Probe API reachability. client.test_connection() raises on
            # failure (no silent-False path); we surface the underlying
            # error class so the failure_reason carries the real cause.
            client.test_connection()
            test_report.basic_connectivity = CapabilityReport(capable=True)
            test_report.capability_report = {
                SourceCapability.DESCRIPTIONS: CapabilityReport(capable=True),
                SourceCapability.PLATFORM_INSTANCE: CapabilityReport(capable=True),
                SourceCapability.OWNERSHIP: CapabilityReport(capable=True),
            }

        except ThoughtSpotAuthenticationError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=(
                    "Authentication failed. Please verify your username and "
                    "either secret_key (trusted auth) or password are correct. "
                    f"Error: {e}"
                ),
            )
        except ThoughtSpotPermissionError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=(
                    "Permission denied. Ensure the API token has 'Can access API' "
                    "permission in ThoughtSpot Security Settings. "
                    f"Error: {e}"
                ),
            )
        except ThoughtSpotAPIError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False,
                failure_reason=f"ThoughtSpot API connection test failed: {e}",
            )
        except Exception as e:
            logger.exception(f"Failed to test connection: {e}")
            test_report.internal_failure = True
            test_report.internal_failure_reason = str(e)
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Unexpected error: {e}"
            )

        return test_report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """
        Main ingestion method.

        Emits entities in topological order:
        1. Workspace containers (parents first)
        2. Liveboards, Answers, Worksheets/Tables (children)
        3. Usage statistics last (depends on liveboard/chart URN sets)
        """
        try:
            # Step 1: workspace containers (parents first)
            logger.info("Extracting ThoughtSpot workspaces")
            workspace_map: Dict[str, WorkspaceResponse] = {}
            with self.report.workspace_extraction_time:
                for workspace in self._safe_extract(
                    self._get_workspaces, entity_label="Workspace"
                ):
                    workspace_map[workspace.id] = workspace
                    yield from self._process_workspace_container(workspace)

            # Step 2: Liveboards (Dashboards)
            if self.config.liveboard_pattern:
                logger.info("Extracting ThoughtSpot Liveboards")
                with self.report.liveboard_extraction_time:
                    yield from self._safe_emit_per_entity(
                        self._get_liveboards,
                        lambda lb: self._process_liveboard(lb, workspace_map),
                        entity_label="Liveboard",
                    )

            # Step 3: Answers (Charts)
            if self.config.answer_pattern:
                logger.info("Extracting ThoughtSpot Answers")
                with self.report.answer_extraction_time:
                    yield from self._safe_emit_per_entity(
                        self._get_answers,
                        lambda a: self._process_answer(a, workspace_map),
                        entity_label="Answer",
                    )

            # Step 4: Worksheets and Tables (Datasets)
            with self.report.dataset_extraction_time:
                yield from self._extract_datasets(workspace_map)

            # Step 5: Usage statistics — consumes entity.stats.views from
            # the caches built during steps 2–3; no additional API call.
            if self.config.include_usage_stats:
                logger.info("Extracting ThoughtSpot usage statistics")
                with self.report.usage_emission_time:
                    yield from self._process_usage_stats()

        finally:
            # Detect config keys that don't match any real connection —
            # almost always a typo. The lookup is only populated if it
            # was accessed during the run (i.e. when external lineage
            # was active). We avoid forcing a fetch here just for
            # validation so disabled-by-config runs don't get a stray
            # /connections/search round-trip.
            if (
                self.config.include_external_lineage
                and self._connection_lookup is not None
                and self.config.external_connections
            ):
                real_ids = set(self._connection_lookup.keys())
                real_names = {c.name for c in self._connection_lookup.values()}
                unmatched_keys = [
                    key
                    for key in self.config.external_connections
                    if key not in real_ids and key not in real_names
                ]
                if unmatched_keys:
                    self.report.warning(
                        title="External Lineage Config Keys Unmatched",
                        message=(
                            "Keys in ``external_connections`` matched no TS "
                            "connection. Likely a typo — check the "
                            "connection's GUID or display name in ThoughtSpot."
                        ),
                        context=f"unmatched_keys={sorted(unmatched_keys)}",
                    )
            if self._unresolvable_external_lineage_count > 0:
                self.report.warning(
                    title="External Lineage Resolution Failed",
                    message=(
                        "TS Logical Tables referenced connections that "
                        "couldn't be resolved (principal lacks read "
                        "access on the connection, or the connection "
                        "was deleted). Cross-platform upstream edges "
                        "for these tables will be missing. Grant the "
                        "ingestion principal read access on the TS "
                        "Connections page to fix."
                    ),
                    context=f"unresolvable_count={self._unresolvable_external_lineage_count}",
                )
            # Aggregated SQL-parser-failure surfacing: per-object DEBUG/INFO
            # logs alone don't surface to operators in the run report.
            # If any SQL view's statement failed to parse, emit one
            # structured warning summarising the breakdown so the gap
            # becomes visible without forcing log triage.
            if self.report.num_sql_parser_failures > 0:
                self.report.warning(
                    title="SQL View Lineage Parse Failures",
                    message=(
                        "SQL view statements could not be parsed by sqlglot. "
                        "Upstream-lineage edges for those views are missing "
                        "or partial; viewLogic still emits. Common causes: "
                        "unsupported warehouse dialect, TS-specific SQL "
                        "extensions, or graph-resolver errors. Check INFO "
                        "logs for per-view error details."
                    ),
                    context=(
                        f"failures={self.report.num_sql_parser_failures}/"
                        f"{self.report.num_sql_parsed}, "
                        f"table_error={self.report.num_sql_parser_table_error}, "
                        f"column_error={self.report.num_sql_parser_column_error}"
                    ),
                )
            self.client.close()

    def _safe_extract(
        self, fetcher: Callable[[], Iterable], *, entity_label: str
    ) -> Iterable:
        """Yield from ``fetcher()`` with one outer try/except so a fetch
        failure surfaces as a structured ``<X> Extraction Failed`` warning
        and the run continues with the remaining steps.
        """
        try:
            yield from fetcher()
        except Exception as e:
            self.report.warning(
                title="Entity Extraction Failed",
                message=("Failed to extract entities. Continuing with other entities."),
                context=f"entity_label={entity_label}",
                exc=e,
            )

    def _safe_emit_per_entity(
        self,
        fetcher: Callable[[], Iterable],
        processor: Callable[[Any], Iterable[MetadataWorkUnit]],
        *,
        entity_label: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Per-entity boundary: fetch entities and yield from ``processor``
        one at a time. Outer try guards the fetch (failure → one structured
        warning, run continues). Inner try guards each entity (failure →
        skip that one, continue with the next).
        """
        try:
            for entity in fetcher():
                try:
                    yield from processor(entity)
                except Exception as e:
                    self.report.warning(
                        title="Failed to Process Entity",
                        message="Skipping this entity.",
                        context=(
                            f"entity_label={entity_label}, "
                            f"entity_id={getattr(entity, 'id', 'unknown')}"
                        ),
                        exc=e,
                    )
        except Exception as e:
            self.report.warning(
                title="Entity Extraction Failed",
                message="Failed to extract entities.",
                context=f"entity_label={entity_label}",
                exc=e,
            )

    def _fetch_and_filter_entities(
        self,
        fetch_fn: Callable[[], Iterable],
        pattern: "AllowDenyPattern",
        entity_type: str,
        tag_filter: Optional[List[str]] = None,
    ) -> Iterator:
        """Stream entities from the API and filter by pattern.

        Generator form (was list-returning): on 100×-scale tenants the
        previous shape held every liveboard/answer in memory before any
        workunit was emitted. Now each entity flows through to its
        processor as soon as it's fetched, then is GC-eligible.

        Auth / permission failures terminate the iterator (and are
        surfaced as report failures); API failures surface a warning and
        the iterator ends cleanly.

        ``tag_filter`` (optional): when set, the entity fetch was
        server-side-filtered by these tag names. If the result is empty
        we surface a ``report.warning`` so a typo'd tag name doesn't
        result in a silent zero-emission run. (TS tag names are
        case-sensitive — ``"Production"`` and ``"production"`` differ.)
        """
        kept = 0
        filtered_out = 0
        try:
            for entity in fetch_fn():
                if pattern.allowed(entity.name):
                    kept += 1
                    yield entity
                else:
                    filtered_out += 1
                    logger.debug(
                        f"{entity_type.rstrip('s')} '{entity.name}' filtered out by pattern"
                    )
        except ThoughtSpotAuthenticationError as e:
            self.report.failure(
                title="Authentication Failed",
                message="Unable to authenticate with ThoughtSpot. Verify username and secret_key (or password).",
                exc=e,
            )
            return
        except ThoughtSpotPermissionError as e:
            self.report.failure(
                title="Permission Denied",
                message=(
                    "Insufficient permissions to access entity list. "
                    "Ensure the API token has 'Can access API' permission."
                ),
                context=f"entity_type={entity_type}",
                exc=e,
            )
            return
        except ThoughtSpotAPIError as e:
            self.report.warning(
                title="Failed to Fetch Entities",
                message="Unable to retrieve entity list from ThoughtSpot API.",
                context=f"entity_type={entity_type}",
                exc=e,
            )
            return
        if tag_filter and kept == 0 and filtered_out == 0:
            # Zero entities AND a tag filter was configured AND no
            # entities were dropped by pattern → the server-side filter
            # matched nothing. Typo / case-mismatch is the most common
            # cause; surface it loudly so operators don't end up with a
            # silent empty run.
            self.report.warning(
                title="Tag Filter Matched Zero Entities",
                message=(
                    "Configured tag filter returned zero entities. "
                    "ThoughtSpot tag names are case-sensitive — check "
                    "for typos."
                ),
                context=f"entity_type={entity_type}, tag_filter={tag_filter}",
            )
        logger.info(
            f"Streamed {kept} {entity_type} to processor ({filtered_out} filtered by pattern)"
        )

    def _get_workspaces(self) -> Iterator[WorkspaceResponse]:
        """Fetch workspaces from ThoughtSpot API."""
        return self._fetch_and_filter_entities(
            self.client.get_workspaces,
            self.config.workspace_pattern,
            "workspaces",
        )

    def _get_liveboards(self) -> Iterator[LiveboardResponse]:
        """Stream Liveboards from ThoughtSpot API."""
        tag_filter = self.config.liveboard_tag_filter
        return self._fetch_and_filter_entities(
            lambda: self.client.iter_liveboards(tag_names=tag_filter),
            self.config.liveboard_pattern,
            "Liveboards",
            tag_filter=tag_filter,
        )

    def _get_answers(self) -> Iterator[AnswerResponse]:
        """Stream Answers from ThoughtSpot API."""
        tag_filter = self.config.answer_tag_filter
        return self._fetch_and_filter_entities(
            lambda: self.client.iter_answers(tag_names=tag_filter),
            self.config.answer_pattern,
            "Answers",
            tag_filter=tag_filter,
        )

    def _get_logical_tables(self) -> Iterator[LogicalTableResponse]:
        """Fetch Worksheets/Tables from ThoughtSpot API.

        Materialises ``_logical_tables_cache`` on first call so a sibling
        ``_get_worksheet_columns_lookup`` (used during Chart InputFields
        emission) doesn't re-fetch from the API. Pattern filtering is
        applied on the stream out — the cache holds the unfiltered set
        so chart-input-field resolution can find worksheets that were
        filtered out of the dataset emission step.
        """
        if self._logical_tables_cache is None:
            try:
                self._logical_tables_cache = list(self.client.get_logical_tables())
            except ThoughtSpotAuthenticationError as e:
                self.report.failure(
                    title="Authentication Failed",
                    message="Unable to authenticate with ThoughtSpot. Verify username and secret_key (or password).",
                    exc=e,
                )
                return
            except ThoughtSpotPermissionError as e:
                self.report.failure(
                    title="Permission Denied",
                    message="Insufficient permissions to access Worksheets/Tables. Ensure the API token has 'Can access API' permission.",
                    exc=e,
                )
                return
            except ThoughtSpotAPIError as e:
                self.report.warning(
                    title="Failed to Fetch Worksheets/Tables",
                    message="Unable to retrieve Worksheets/Tables list from ThoughtSpot API.",
                    exc=e,
                )
                return

        kept = 0
        filtered_out = 0
        for entity in self._logical_tables_cache:
            if self.config.worksheet_pattern.allowed(entity.name):
                kept += 1
                yield entity
            else:
                filtered_out += 1
                logger.debug(f"Worksheet/Table '{entity.name}' filtered out by pattern")
        logger.info(
            f"Streamed {kept} Worksheets/Tables to processor ({filtered_out} filtered by pattern)"
        )

    def _get_worksheet_columns_lookup(
        self,
    ) -> dict[str, dict[str, ColumnResponse]]:
        """Lazy {worksheet_id -> {column_name -> ColumnResponse}} map.

        Built once per ingestion run by reading the LOGICAL_TABLE list from
        the client. Used by Chart InputFields emission so the embedded
        SchemaField matches the dataset's own SchemaField (matching field
        types matters for DataHub's column-level lineage visualizer — a
        stub SchemaField with the wrong type still URN-matches but the UI
        won't draw the column-to-column edge).
        """
        if self._worksheet_columns_lookup is None:
            if self._logical_tables_cache is None:
                # Read directly from the client (skip the pattern filter
                # so we can resolve refs to worksheets that the user
                # filtered out of the main extraction). ``list()`` is
                # defensive — ``get_logical_tables`` returns a real list
                # today, but mirrors the sibling cache-population at
                # ``_get_logical_tables`` so the two paths can't drift.
                self._logical_tables_cache = list(self.client.get_logical_tables())
            self._worksheet_columns_lookup = {
                t.id: {col.name: col for col in (t.columns or []) if col.name}
                for t in self._logical_tables_cache
                if t.id
            }
        return self._worksheet_columns_lookup

    def _get_tag_lookup(self) -> Dict[str, str]:
        """Lazy {tag_id -> tag_name} map. Empty when tags aren't fetchable.

        Failures (most often FORBIDDEN on ``/tags/search`` for a
        principal without read access on tags) emit a single warning
        and fall back to an empty map — entities are still ingested
        but without ``globalTags`` aspects. Without the try/except the
        exception would propagate through the caller's per-entity
        try/except and silently drop the entity.
        """
        if self._tag_lookup is None:
            try:
                self._tag_lookup = {
                    t.id: t.name for t in self.client.get_tags() if t.id and t.name
                }
            except Exception as e:
                self.report.warning(
                    title="Failed to Fetch Tags",
                    message=(
                        "Could not enumerate ThoughtSpot tags. Entities "
                        "will be emitted without ``globalTags`` aspects."
                    ),
                    exc=e,
                )
                self._tag_lookup = {}
        return self._tag_lookup

    def _resolve_entity_tag_urns(self, raw_tags: Optional[List[TagRef]]) -> List[str]:
        """Map an entity's ``metadata_header.tags`` list to DataHub tag URNs.

        ``raw_tags`` is a homogeneous ``List[TagRef]`` after pydantic
        construction, so we just iterate ``.id`` here. Each resolved tag
        name is routed through ``make_tag_urn`` to URL-encode reserved
        characters (e.g. ``"Customer Facing"`` →
        ``urn:li:tag:Customer%20Facing``). Unknown ids are skipped silently
        — emitting a half-resolved URN would be worse than dropping the
        reference.
        """
        if not raw_tags:
            return []
        lookup = self._get_tag_lookup()
        urns: List[str] = []
        seen: set[str] = set()
        for ref in raw_tags:
            name = lookup.get(ref.id)
            if name and name not in seen:
                seen.add(name)
                urns.append(make_tag_urn(name))
        return urns

    def _make_self_dataset_urn(self, name: str) -> str:
        """Build a Dataset URN scoped to *this* connector's platform/env/instance.

        Centralises the ``make_dataset_urn_with_platform_instance(...)``
        call that otherwise repeats with the same three self-references
        at six sites (Liveboard inputs, Chart inputs, Answer URN, Dataset
        URN, internal-lineage upstreams, external-lineage downstream).

        Memoised per-instance via ``_self_dataset_urn_cache``: each
        distinct ``name`` is converted exactly once per ingestion run
        regardless of how many internal references hit it. Safe because
        the three resolution inputs (``platform``, ``env``,
        ``platform_instance``) are run-invariant — set in ``__init__``
        and never reassigned.
        """
        cached = self._self_dataset_urn_cache.get(name)
        if cached is not None:
            return cached
        urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=name,
            env=self.config.env,
            platform_instance=self.config.platform_instance,
        )
        self._self_dataset_urn_cache[name] = urn
        return urn

    def _resolve_workspace_container(
        self,
        owner_id: Optional[str],
        workspace_map: Dict[str, WorkspaceResponse],
    ) -> Optional[WorkspaceKey]:
        """Build a ``WorkspaceKey`` for an entity's parent workspace, or None.

        Centralizes the lookup so liveboard / answer / dataset processors
        share one source of truth.
        """
        logger.debug(f"Resolving workspace container from owner_id={owner_id}")
        if owner_id and owner_id in workspace_map:
            return WorkspaceKey(
                platform=self.platform,
                instance=self.config.platform_instance,
                workspace_id=owner_id,
            )
        return None

    def _apply_entity_ownership(
        self,
        entity: Union[Chart, Dashboard, Dataset],
        author: Optional[ThoughtSpotAuthor],
        author_name: Optional[str] = None,
    ) -> None:
        """Attach a DATAOWNER aspect from a TS author when one is resolvable.

        TS REST v2 returns ``author`` as a GUID string (which our model
        coerces into a minimal ``ThoughtSpotAuthor(id=guid, name=guid)``)
        and the real human-readable login as ``authorName`` (camelCase, aliased
        to ``author_name`` on the model). Prefer ``author_name`` and fall back
        to ``author.name`` only if it doesn't look like a GUID — skipping 36-
        char hyphenated values that are obviously user IDs rather than logins.
        """
        if not self.config.include_ownership:
            return
        owner = author_name or (author.name if author else None)
        if not owner or _looks_like_guid(owner) or not _looks_like_login(owner):
            # No login, canonical GUID, or display-name-with-whitespace
            # (would yield a malformed corpuser URN).
            return
        owner_urn = make_user_urn(owner)
        entity.set_owners([(CorpUserUrn(owner_urn), OwnershipTypeClass.DATAOWNER)])

    @staticmethod
    def _ts_type_to_schema_type(ts_data_type: Optional[str]) -> Any:
        """Map ThoughtSpot's column ``dataType`` to a DataHub schema type.

        Falls back to NullType for unknown types — matches the SDK's
        behavior for unmapped values.
        """
        t = (ts_data_type or "").upper()
        if t in {"INT32", "INT64", "INTEGER", "BIGINT", "SMALLINT", "TINYINT"}:
            return NumberTypeClass()
        if t in {"FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL"}:
            return NumberTypeClass()
        if t in {"VARCHAR", "STRING", "CHAR", "TEXT", "NVARCHAR"}:
            return StringTypeClass()
        if t in {"BOOL", "BOOLEAN"}:
            return BooleanTypeClass()
        if t == "DATE":
            return DateTypeClass()
        if t in {"DATE_TIME", "DATETIME", "TIMESTAMP", "TIME"}:
            return TimeTypeClass()
        return NullTypeClass()

    # Canonical DataHub URNs for column semantic roles, matching the
    # tags Looker emits (``looker_common.py``). Reusing the same URNs
    # means a user filtering for ``urn:li:tag:Measure`` sees TS measures
    # alongside Looker measures.
    _DIMENSION_TAG_URN = "urn:li:tag:Dimension"
    _MEASURE_TAG_URN = "urn:li:tag:Measure"
    _TEMPORAL_TAG_URN = "urn:li:tag:Temporal"

    @classmethod
    def _column_semantic_tags(cls, col: "ColumnResponse") -> List[str]:
        """Resolve column semantic-role tag URNs for a TS column.

        ATTRIBUTE → ``Dimension``, MEASURE → ``Measure``. DATE / TIME
        data types additionally pick up ``Temporal``, matching Looker's
        ``DIMENSION_GROUP`` treatment of time-based fields.
        """
        tags: List[str] = []
        col_type = (col.column_type or "").upper()
        if col_type == "MEASURE":
            tags.append(cls._MEASURE_TAG_URN)
        elif col_type == "ATTRIBUTE":
            tags.append(cls._DIMENSION_TAG_URN)
        data_type = (col.data_type or "").upper()
        if data_type in {"DATE", "DATE_TIME", "DATETIME", "TIMESTAMP", "TIME"}:
            tags.append(cls._TEMPORAL_TAG_URN)
        return tags

    def _make_schema_field(self, col: "ColumnResponse") -> SchemaFieldClass:
        """Build a ``SchemaFieldClass`` from a TS ``ColumnResponse``.

        Single source of truth for SchemaField construction so the worksheet
        ``schemaMetadata`` and the chart ``inputFields`` produce byte-equal
        objects for the same column. The DataHub column-lineage UI compares
        the embedded ``schemaField`` on an InputField against the upstream
        dataset's schemaMetadata; subtle drift (e.g. ``description=None``
        vs ``description=""``) is enough to break the visual match even
        though the URNs resolve. Looker uses the same pattern via its
        ``view_field_to_schema_field`` helper.

        TS's MEASURE / ATTRIBUTE classification is emitted as field-level
        ``globalTags`` using the same canonical URNs Looker uses
        (``urn:li:tag:Measure`` / ``urn:li:tag:Dimension``), so the
        semantic role of a column is cross-source comparable in the UI.

        Memoised per-instance keyed on the column fields actually read
        by the body. Repeated calls for the same column (e.g. one
        worksheet column pinned by N visualizations) return the same
        ``SchemaFieldClass`` instance — ``SchemaFieldClass`` is treated
        as immutable by the emitter so sharing is safe.
        """
        key = _SchemaFieldCacheKey(
            name=col.name,
            data_type=col.data_type,
            description=col.description,
            column_type=col.column_type,
        )
        cached = self._schema_field_cache.get(key)
        if cached is not None:
            return cached
        tag_urns = self._column_semantic_tags(col)
        global_tags = (
            GlobalTagsClass(tags=[TagAssociationClass(tag=urn) for urn in tag_urns])
            if tag_urns
            else None
        )
        sf = SchemaFieldClass(
            fieldPath=col.name,
            type=SchemaFieldDataTypeClass(
                type=self._ts_type_to_schema_type(col.data_type)
            ),
            nativeDataType=col.data_type or "UNKNOWN",
            description=col.description,
            globalTags=global_tags,
        )
        self._schema_field_cache[key] = sf
        return sf

    def _process_workspace_container(
        self, workspace: WorkspaceResponse
    ) -> Iterable[MetadataWorkUnit]:
        """
        Process a workspace and emit as a Container entity.

        Uses gen_containers() helper for proper container hierarchy.
        """
        self.report.report_workspace_scanned()
        try:
            workspace_id = workspace.id
            workspace_name = workspace.name

            logger.debug(f"Processing workspace: {workspace_name} ({workspace_id})")

            # Create WorkspaceKey for workspace
            container_key = WorkspaceKey(
                platform=self.platform,
                instance=self.config.platform_instance,
                workspace_id=workspace_id,
            )

            # Generate container using SDK V2 helper
            containers = list(
                gen_containers(
                    container_key=container_key,
                    name=workspace_name,
                    sub_types=[BIContainerSubTypes.THOUGHTSPOT_WORKSPACE],
                    description=workspace.description,
                    parent_container_key=None,
                )
            )

            yield from containers

        except Exception as e:
            self.report.warning(
                title="Failed to Process Workspace",
                message="Unexpected error processing workspace. Skipping.",
                context=f"workspace_id={getattr(workspace, 'id', 'unknown')}",
                exc=e,
            )

    def _process_liveboard(
        self, liveboard: LiveboardResponse, workspace_map: Dict[str, WorkspaceResponse]
    ) -> Iterable[MetadataWorkUnit]:
        """Process a Liveboard and emit as a Dashboard entity using SDK V2."""
        self.report.report_liveboard_scanned()
        if liveboard.stats is not None and liveboard.stats.views > 0:
            self._liveboard_usage.append((liveboard.id, liveboard.stats.views))
        try:
            liveboard_id = liveboard.id
            liveboard_name = liveboard.name

            logger.debug(f"Processing Liveboard: {liveboard_name} ({liveboard_id})")

            container_key = self._resolve_workspace_container(
                liveboard.owner_id, workspace_map
            )

            # Create Dashboard using SDK V2
            lb_created_dt = self._epoch_ms_to_datetime(liveboard.created)
            lb_modified_dt = self._epoch_ms_to_datetime(liveboard.modified)
            lb_author_urn = self._author_user_urn(liveboard)
            dashboard = Dashboard(
                name=liveboard_id,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                display_name=liveboard_name,
                description=liveboard.description,
                external_url=self._get_liveboard_url(liveboard_id),
                custom_properties=self._extract_custom_properties(liveboard),
                subtype=BIAssetSubTypes.THOUGHTSPOT_LIVEBOARD,
                created_at=lb_created_dt,
                last_modified=lb_modified_dt,
                created_by=lb_author_urn,
                last_modified_by=lb_author_urn,
            )

            if container_key:
                dashboard._set_container(container_key)

            self._apply_entity_ownership(
                dashboard, liveboard.author, liveboard.author_name
            )

            tag_urns = self._resolve_entity_tag_urns(getattr(liveboard, "tags", None))
            if tag_urns:
                dashboard.set_tags(tag_urns)

            # Process visualizations and emit as Chart entities.
            # LiveboardResponse coerces wire dicts/strings to
            # VisualizationResponse via its field validator, so we can
            # iterate directly without per-element type dispatch.
            #
            # TS doesn't tag individual viz tiles — only the parent
            # Liveboard. A viz inside a tagged Liveboard is part of
            # that tagged asset, so we propagate the parent's tags
            # down before processing each viz. Done here (always runs)
            # rather than inside ``_enrich_and_yield_liveboards``
            # (skipped when callers bypass the streaming pipeline).
            chart_urns = []
            for viz in liveboard.visualizations or []:
                if not viz.tags and liveboard.tags:
                    viz.tags = liveboard.tags
                for wu in self._process_visualization(viz, container_key=container_key):
                    yield wu
                    # Filter to chart URNs so we only register the
                    # visualization itself (not e.g. its container or
                    # tag side-effects). Use a typed prefix check —
                    # substring would also match ``urn:li:dashboardChart``
                    # or any future URN type that embeds ``chart``.
                    if wu.get_urn().startswith("urn:li:chart:"):
                        chart_urns.append(wu.get_urn())

            # Set chart references on dashboard
            if chart_urns:
                # Deduplicate URNs (each chart emits multiple workunits)
                unique_chart_urns = list(set(chart_urns))
                dashboard.set_charts(unique_chart_urns)

            # Direct dashboard→dataset lineage from ``reportContent`` filter
            # refs. We only emit this when there's no chart layer to carry
            # the lineage — emitting both would create redundant edges.
            # Reasons the chart layer may be missing:
            #   * Liveboard TML returned FORBIDDEN on this user
            #   * Liveboard TML parsing failed (we already warn)
            #   * Liveboard genuinely has no visualizations
            elif liveboard.source_table_ids:
                upstream_dataset_urns = [
                    self._make_self_dataset_urn(table_id)
                    for table_id in liveboard.source_table_ids
                ]
                if upstream_dataset_urns:
                    dashboard.set_input_datasets(upstream_dataset_urns)

            # Emit dashboard workunits
            yield from dashboard.as_workunits()

        except Exception as e:
            self.report.warning(
                title="Failed to Process Liveboard",
                message="Unexpected error processing liveboard. Skipping.",
                context=f"liveboard_id={getattr(liveboard, 'id', 'unknown')}",
                exc=e,
            )

    def _process_visualization(
        self,
        viz: VisualizationResponse,
        container_key: Optional[WorkspaceKey] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Process a Visualization and emit as a Chart entity using SDK V2.

        Args:
            viz: VisualizationResponse object containing visualization metadata
            container_key: Parent Liveboard's workspace key. A viz lives
                inside the parent Liveboard's workspace, not its own —
                ``VisualizationResponse.owner_id`` is almost always
                ``None``, so we receive the resolved key from
                ``_process_liveboard`` rather than re-resolving here.

        Yields:
            MetadataWorkUnit objects representing the Chart entity and its aspects
        """
        try:
            viz_id = viz.id
            viz_name = viz.name

            logger.debug(f"Processing Visualization: {viz_name} ({viz_id})")

            # Build custom properties from visualization metadata. Keys
            # are prefixed with ``thoughtspot_`` so they're easy to filter
            # in the DataHub UI's custom-properties panel.
            custom_props: Dict[str, str] = {"thoughtspot_id": viz_id}

            if viz.chart_type:
                custom_props["thoughtspot_chart_type"] = viz.chart_type
            if viz.question_text:
                custom_props["thoughtspot_question_text"] = viz.question_text

            viz_created_dt = self._epoch_ms_to_datetime(viz.created)
            if viz_created_dt is not None:
                custom_props["created"] = viz_created_dt.isoformat()
            viz_modified_dt = self._epoch_ms_to_datetime(viz.modified)
            if viz_modified_dt is not None:
                custom_props["modified"] = viz_modified_dt.isoformat()

            # Prefer wire-level ``authorName`` / ``authorDisplayName`` over
            # the nested ``author.*`` (TS often returns ``author`` as a GUID
            # with the real values on the parent metadata header).
            viz_login = _resolve_author_login(viz)
            if viz_login:
                custom_props["author"] = viz_login
            if viz.author_display_name:
                custom_props["author_display_name"] = viz.author_display_name

            viz_stats = getattr(viz, "stats", None)
            if viz_stats is not None:
                if viz_stats.favorites:
                    custom_props["thoughtspot_favorites"] = str(viz_stats.favorites)
                viz_last_accessed_dt = self._epoch_ms_to_datetime(
                    viz_stats.last_accessed
                )
                if viz_last_accessed_dt is not None:
                    custom_props["thoughtspot_last_accessed"] = (
                        viz_last_accessed_dt.isoformat()
                    )

            # Create Chart using SDK V2. Use the ThoughtSpot-specific subtype
            # so dashboard tiles (Visualizations) are distinguishable from
            # standalone saved searches (Answers) in the DataHub UI rather
            # than both rendering with a generic "Chart" badge.
            viz_author_urn = self._author_user_urn(viz)
            chart = Chart(
                name=viz_id,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                display_name=viz_name,
                description=viz.description or "",
                custom_properties=custom_props,
                subtype=BIAssetSubTypes.THOUGHTSPOT_VISUALIZATION,
                created_at=viz_created_dt,
                last_modified=viz_modified_dt,
                created_by=viz_author_urn,
                last_modified_by=viz_author_urn,
            )

            # Container parity. The viz inherits its workspace from
            # the parent Liveboard — the resolved key is passed in by
            # ``_process_liveboard`` so we don't re-resolve.
            if container_key:
                chart._set_container(container_key)

            # Ownership parity with Answer / Liveboard. The viz's
            # ``author`` and ``author_name`` are propagated from the
            # parent liveboard inside ``_enrich_and_yield_liveboards``,
            # so the data is already in hand by the time we get here.
            self._apply_entity_ownership(chart, viz.author, viz.author_name)

            # Tag parity with Answer / Liveboard. ``viz.tags`` is
            # populated by the same enrichment step that propagates
            # author — TS doesn't expose per-viz tags via the API.
            tag_urns = self._resolve_entity_tag_urns(getattr(viz, "tags", None))
            if tag_urns:
                chart.set_tags(tag_urns)

            # Wire upstream lineage. ``source_table_ids`` is populated by the
            # TML-based viz fetch in ``client._get_liveboard_visualizations_via_tml``
            # and lists the LOGICAL_TABLE GUIDs this chart reads from. Each
            # GUID becomes a dataset URN — the same shape ``_process_dataset``
            # uses, so the chart edges resolve to the worksheets we ingest.
            upstream_dataset_urns: List[str] = []
            if viz.source_table_ids:
                upstream_dataset_urns = [
                    self._make_self_dataset_urn(table_id)
                    for table_id in viz.source_table_ids
                ]
                chart.set_input_datasets(upstream_dataset_urns)

            # Emit workunit
            for wu in chart.as_workunits():
                yield wu

            yield from self._emit_chart_input_fields(
                viz_id=viz_id,
                source_table_ids=viz.source_table_ids,
                source_columns=viz.source_columns,
            )

        except Exception as e:
            self.report.warning(
                title="Failed to Process Visualization",
                message="Unexpected error processing visualization. Skipping.",
                context=f"viz_id={getattr(viz, 'id', 'unknown')}",
                exc=e,
            )

    def _emit_chart_input_fields(
        self,
        *,
        viz_id: str,
        source_table_ids: Optional[List[str]],
        source_columns: Optional[List[str]],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a Chart→Dataset column-level lineage ``InputFields`` aspect.

        Source-side cannot disambiguate which source worksheet a column
        lives in without decoding the embedded TML protobuf, so we emit a
        SchemaField URN for every (source worksheet × source column) pair
        — DataHub surfaces the ones that match real fields and ignores
        the others as dangling. The dataset's actual columns are used to
        build ``SchemaField`` instances that match the upstream
        ``schemaMetadata`` exactly; DataHub's column-lineage visualiser
        draws edges only when the InputField's embedded ``schemaField``
        is consistent with the target dataset's SchemaField (URN match
        alone isn't enough — Mode/Looker follow the same pattern).
        """
        if not source_columns or not source_table_ids:
            return

        lookup = self._get_worksheet_columns_lookup()
        fields: List[InputFieldClass] = []
        for table_id in source_table_ids:
            table_cols = lookup.get(table_id, {})
            if not table_cols:
                continue
            dataset_urn = self._make_self_dataset_urn(table_id)
            for col_name in source_columns:
                col = table_cols.get(col_name)
                if col is None:
                    # Bracketed reference doesn't exist on this worksheet —
                    # likely a formula column local to the chart, skip silently.
                    continue
                fields.append(
                    InputFieldClass(
                        schemaFieldUrn=make_schema_field_urn(dataset_urn, col_name),
                        schemaField=self._make_schema_field(col),
                    )
                )

        if fields:
            chart_urn = make_chart_urn(
                platform=self.platform,
                name=viz_id,
                platform_instance=self.config.platform_instance,
            )
            yield MetadataChangeProposalWrapper(
                entityUrn=chart_urn,
                aspect=InputFieldsClass(fields=fields),
            ).as_workunit()

    def _process_answer(
        self, answer: AnswerResponse, workspace_map: dict[str, WorkspaceResponse]
    ) -> Iterable[MetadataWorkUnit]:
        """Process an Answer and emit as a Chart entity using SDK V2."""
        self.report.report_answer_scanned()
        if answer.stats is not None and answer.stats.views > 0:
            self._answer_usage.append((answer.id, answer.stats.views))
        try:
            answer_id = answer.id
            answer_name = answer.name

            logger.debug(f"Processing Answer: {answer_name} ({answer_id})")

            container_key = self._resolve_workspace_container(
                answer.owner_id, workspace_map
            )

            # Create Chart using SDK V2. Answers are standalone saved
            # searches — distinct from Visualizations, which are tiles
            # inside a Liveboard. Both map to the Chart entity but get
            # different subtypes so DataHub search facets and UI badges
            # can tell them apart.
            ans_created_dt = self._epoch_ms_to_datetime(answer.created)
            ans_modified_dt = self._epoch_ms_to_datetime(answer.modified)
            ans_author_urn = self._author_user_urn(answer)
            chart = Chart(
                name=answer_id,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                display_name=answer_name,
                description=answer.description,
                external_url=self._get_answer_url(answer_id),
                custom_properties=self._extract_custom_properties(answer),
                subtype=BIAssetSubTypes.THOUGHTSPOT_ANSWER,
                created_at=ans_created_dt,
                last_modified=ans_modified_dt,
                created_by=ans_author_urn,
                last_modified_by=ans_author_urn,
            )

            if container_key:
                chart._set_container(container_key)

            self._apply_entity_ownership(chart, answer.author, answer.author_name)

            tag_urns = self._resolve_entity_tag_urns(getattr(answer, "tags", None))
            if tag_urns:
                chart.set_tags(tag_urns)

            # Extract upstream lineage (Answer -> Dataset relationships).
            # ``answer.source_tables`` is a homogeneous List[SourceTableRef]
            # after the model validator, so no per-element type dispatch
            # is needed here.
            upstream_urns = [
                self._make_self_dataset_urn(table.id)
                for table in (answer.source_tables or [])
                if table.id
            ]
            if upstream_urns:
                # SDK V2 accepts a list of dataset URNs for input_datasets
                chart.set_input_datasets(upstream_urns)

            # Emit workunit
            yield from chart.as_workunits()

            # Column-level lineage from TML's ``answer.search_query``
            # ``[bracket]`` tokens, mirroring the Visualization path. The
            # source-table ids are the same set that fed the chart's
            # input_datasets above.
            yield from self._emit_chart_input_fields(
                viz_id=answer_id,
                source_table_ids=[t.id for t in (answer.source_tables or []) if t.id],
                source_columns=answer.source_columns,
            )

        except Exception as e:
            self.report.warning(
                title="Failed to Process Answer",
                message="Unexpected error processing answer. Skipping.",
                context=f"answer_id={getattr(answer, 'id', 'unknown')}",
                exc=e,
            )

    def _extract_datasets(
        self,
        workspace_map: dict[str, WorkspaceResponse],
    ) -> Iterable[MetadataWorkUnit]:
        """Extract worksheets, emit them as Datasets, and backfill the base
        tables their column-level lineage references."""
        logger.info("Extracting ThoughtSpot Worksheets and Tables")
        seen_table_ids: Set[str] = set()
        source_table_ids: Set[str] = set()
        try:
            for logical_table in self._get_logical_tables():
                try:
                    seen_table_ids.add(logical_table.id)
                    # Track source tables this worksheet's column-level
                    # lineage references; we'll fetch + emit them after
                    # the main loop so the lineage chain resolves in
                    # DataHub's UI instead of pointing at dangling URNs.
                    if logical_table.columns:
                        for col in logical_table.columns:
                            for src in col.sources or []:
                                if src.table_id:
                                    source_table_ids.add(src.table_id)
                    yield from self._process_dataset(logical_table, workspace_map)
                except Exception as e:
                    self.report.warning(
                        title="Failed to Process Dataset",
                        message="Skipping this dataset.",
                        context=f"table_id={getattr(logical_table, 'id', 'unknown')}",
                        exc=e,
                    )
        except Exception as e:
            self.report.warning(
                title="Dataset Extraction Failed",
                message="Failed to extract datasets.",
                exc=e,
            )

        yield from self._backfill_source_datasets(
            source_table_ids - seen_table_ids, workspace_map
        )

    def _backfill_source_datasets(
        self,
        missing_ids: Set[str],
        workspace_map: dict[str, WorkspaceResponse],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit Dataset entities for base tables referenced by worksheet
        column-level lineage. Per-object visibility rules apply: tables the
        principal can't see come back missing and are silently skipped.
        Without this, fine-grained lineage edges point at URNs DataHub
        treats as unknown and the lineage graph hides them in the UI."""
        if not missing_ids:
            return

        logger.info(
            f"Backfilling {len(missing_ids)} upstream LOGICAL_TABLEs "
            "referenced by worksheet column lineage."
        )
        try:
            backfill = self.client.fetch_logical_tables_by_id(list(missing_ids))
        except Exception as e:
            self.report.warning(
                title="Source Dataset Backfill Failed",
                message=(
                    "Could not fetch upstream base tables. Column-level "
                    "lineage will reference dangling URNs."
                ),
                exc=e,
            )
            return

        logger.info(
            f"Resolved {len(backfill)}/{len(missing_ids)} upstream tables; "
            "the rest are not visible to this user and will appear as "
            "dangling URNs in DataHub."
        )
        for src_table in backfill:
            try:
                yield from self._process_dataset(src_table, workspace_map)
            except Exception as e:
                self.report.warning(
                    title="Failed to Process Source Dataset",
                    message="Skipping this upstream dataset.",
                    context=f"table_id={getattr(src_table, 'id', 'unknown')}",
                    exc=e,
                )

    @staticmethod
    def _logical_table_subtype(ts_type: Optional[str]) -> str:
        """Map a ThoughtSpot ``metadata_header.type`` to a DataHub subtype.

        TS's REST API v2 docs enumerate six subtypes under the
        ``LOGICAL_TABLE`` metadata type (see
        https://developers.thoughtspot.com/docs/rest-apiv2-metadata-search):

        - ``WORKSHEET``: a curated semantic layer over one or more
          underlying tables, with calculated columns, joins, and
          column formulas. (TS Cloud 26.x calls these "Models" in the
          UI but the API still returns ``WORKSHEET``.) Maps to
          ``THOUGHTSPOT_WORKSHEET``.
        - ``PRIVATE_WORKSHEET``: a private Worksheet/Model. Same
          entity, just privacy-flagged. Also maps to
          ``THOUGHTSPOT_WORKSHEET``.
        - ``SQL_VIEW``: a TS-defined SQL view on top of a connection's
          schema. Maps to ``VIEW``.
        - ``ONE_TO_ONE_LOGICAL``: a direct one-to-one mapping of a
          physical source table. Maps to ``TABLE``.
        - ``USER_DEFINED``: data imported from external sources (e.g.
          a CSV file). Tabular row data — maps to ``TABLE``.
        - ``AGGR_WORKSHEET``: a user-saved View, materialised from an
          Answer saved as a View. Intentionally **not** mapped here —
          the unknown-type fallback already routes it to ``VIEW``,
          which is the correct semantic. Adding an explicit mapping
          would change nothing at runtime.

        Anything else (future types or a missing field): fall back to
        ``VIEW`` so any tenant returning a type we haven't catalogued
        still emits a valid subtype rather than crashing.
        """
        mapping = {
            "WORKSHEET": DatasetSubTypes.THOUGHTSPOT_WORKSHEET,
            "PRIVATE_WORKSHEET": DatasetSubTypes.THOUGHTSPOT_WORKSHEET,
            "SQL_VIEW": DatasetSubTypes.VIEW,
            "ONE_TO_ONE_LOGICAL": DatasetSubTypes.TABLE,
            "USER_DEFINED": DatasetSubTypes.TABLE,
        }
        return mapping.get(ts_type or "", DatasetSubTypes.VIEW)

    def _process_dataset(
        self, table: LogicalTableResponse, workspace_map: dict[str, WorkspaceResponse]
    ) -> Iterable[MetadataWorkUnit]:
        """Process a Worksheet/Table and emit as a Dataset entity using SDK V2."""
        self.report.report_dataset_scanned()
        try:
            table_id = table.id
            table_name = table.name

            logger.debug(f"Processing Dataset: {table_name} ({table_id})")

            container_key = self._resolve_workspace_container(
                table.owner_id, workspace_map
            )

            # Create Dataset using SDK V2 — subtype reflects the actual
            # TS-side type so Worksheets (semantic layer), SQL/Logical
            # Views (TS-side SQL definitions), and physical Tables each
            # render as their own kind in the DataHub UI rather than all
            # showing up as "View".
            tbl_created_dt = self._epoch_ms_to_datetime(table.created)
            tbl_modified_dt = self._epoch_ms_to_datetime(table.modified)
            dataset = Dataset(
                name=table_id,
                platform=self.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                display_name=table_name,
                description=table.description,
                external_url=self._get_table_url(table_id),
                custom_properties=self._extract_custom_properties(table),
                subtype=self._logical_table_subtype(table.type),
                created=tbl_created_dt,
                last_modified=tbl_modified_dt,
            )

            if container_key:
                dataset._set_container(container_key)

            self._apply_entity_ownership(dataset, table.author, table.author_name)

            # Global tags from ThoughtSpot's tag system.
            tag_urns = self._resolve_entity_tag_urns(getattr(table, "tags", None))
            if tag_urns:
                dataset.set_tags(tag_urns)

            self._apply_dataset_schema(dataset, table.columns)
            self._apply_dataset_upstreams(dataset, table.columns, table=table)

            # Emit workunit
            yield from dataset.as_workunits()

            # SQL_VIEW datasets: emit viewLogic + augment lineage with
            # sqlglot-parsed edges. No-op for other LOGICAL_TABLE types.
            # Runs after the dataset's own workunits so the parsed
            # UpstreamLineage MCP arrives as a follow-up edit rather
            # than racing with the SDK V2 entity emission.
            if table.type == "SQL_VIEW" and table.sql_view_definition:
                # SQL views don't have ``physical_*`` fields (the
                # upstream tables live inside the SQL statement), so
                # _resolve_external_upstream returns None for them.
                # Use the SQL-view-specific resolver which only needs
                # ``data_source_type`` for dialect — works without a
                # successful /connections/search lookup.
                sv_ref = self._resolve_sql_view_warehouse(table)
                # Build the set of (downstream_col_urn, upstream_col_urn)
                # tuples the existing _apply_dataset_upstreams already
                # emitted onto the dataset, so sqlglot-parsed edges
                # don't duplicate them.
                existing_fgl_edges = self._collect_existing_fgl_edges(dataset)

                yield from self._apply_sql_view_logic(
                    table_id=table.id,
                    sql=table.sql_view_definition,
                    dialect=sv_ref.platform if sv_ref else None,
                )
                yield from self._apply_sql_parsed_upstreams(
                    table_id=table.id,
                    sql=table.sql_view_definition,
                    sv_ref=sv_ref,
                    existing_fgl_edges=existing_fgl_edges or None,
                )

        except Exception as e:
            self.report.warning(
                title="Failed to Process Dataset",
                message="Unexpected error processing dataset. Skipping.",
                context=f"table_id={getattr(table, 'id', 'unknown')}",
                exc=e,
            )

    @staticmethod
    def _collect_existing_fgl_edges(dataset: Dataset) -> Set[Tuple[str, str]]:
        """Return ``(downstream_col_urn, upstream_col_urn)`` pairs already
        on the dataset's ``UpstreamLineage`` aspect.

        Used by the SQL_VIEW parser path to skip column-level edges that
        ``_apply_dataset_upstreams`` already emitted from TS's pre-resolved
        ``columns[*].sources``. The SDK V2 ``Dataset`` exposes the aspect
        via the public ``dataset.upstreams`` property — not the
        ``_upstreams_aspect`` attribute that an earlier revision used (which
        does not exist and silently produced an empty set, disabling dedup).
        """
        edges: Set[Tuple[str, str]] = set()
        upstreams_aspect = dataset.upstreams
        if upstreams_aspect is None:
            return edges
        for fgl in upstreams_aspect.fineGrainedLineages or []:
            for d in fgl.downstreams or []:
                for u in fgl.upstreams or []:
                    edges.add((d, u))
        return edges

    def _apply_sql_view_logic(
        self,
        table_id: str,
        sql: str,
        dialect: Optional[str],
    ) -> Iterable[MetadataWorkUnit]:
        """Emit a ``ViewProperties`` aspect for a SQL_VIEW dataset.

        The viewLogic carries the raw TS SQL statement so users
        browsing the dataset in DataHub can see what the view does
        without leaving the catalog. ``materialized=False`` because
        TS SQL views are virtual (resolved against the warehouse at
        query time). ``viewLanguage`` carries the resolved warehouse
        dialect when available; falls back to ``"SQL"`` so the field
        is never an empty string.

        ``table_id`` is the TS GUID (same value the caller uses for
        ``_make_self_dataset_urn``) — taking it directly keeps this
        helper independent of the SDK V2 ``Dataset`` API surface,
        which doesn't expose the name back as an attribute.

        No-op when ``sql`` is empty / None — emitting an empty
        viewLogic would clobber a real value from a prior ingestion.
        """
        if not sql:
            return
        yield MetadataChangeProposalWrapper(
            entityUrn=str(self._make_self_dataset_urn(table_id)),
            aspect=ViewPropertiesClass(
                materialized=False,
                viewLogic=sql,
                viewLanguage=dialect or "SQL",
            ),
        ).as_workunit()

    def _apply_sql_parsed_upstreams(
        self,
        table_id: str,
        sql: str,
        sv_ref: Optional[SqlViewWarehouseRef],
        existing_fgl_edges: Optional[Set[Tuple[str, str]]] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """Augment the SQL view dataset's UpstreamLineage with
        sqlglot-parsed edges. Mirrors Mode's pattern at
        ``source/mode.py:1267-1303``.

        Runs ``sqlglot_lineage`` against a graph-backed
        ``SchemaResolver`` so upstream warehouse columns can resolve
        to real schemaField URNs. Parse failures increment the
        standard SQL-parser counters but never raise — the existing
        ``columns[*].sources`` edges on this dataset are unaffected
        by any failure here.

        ``existing_fgl_edges`` (optional) carries ``(downstream_col_urn,
        upstream_col_urn)`` pairs already on the dataset so we can drop
        duplicate column-level edges. When ``None``, all parsed edges
        emit verbatim.

        ``sv_ref`` is the resolved warehouse context from
        ``_resolve_sql_view_warehouse``. ``None`` means we couldn't
        identify the warehouse (connection unreadable, deleted
        upstream) — the parser is skipped in that case because
        ``sqlglot_lineage`` requires a known dialect to map upstream
        tables to URNs. The sibling ``_apply_sql_view_logic`` still
        emits viewLogic so the SQL is documented regardless.
        """
        if not sql:
            return
        if sv_ref is None:
            return

        start = time.perf_counter()
        self.report.num_sql_parsed += 1

        try:
            schema_resolver = create_and_cache_schema_resolver(
                platform=sv_ref.platform,
                env=sv_ref.env,
                graph=self.ctx.graph,
                platform_instance=sv_ref.platform_instance,
            )
            parsed: SqlParsingResult = sqlglot_lineage(
                sql=sql,
                schema_resolver=schema_resolver,
                default_db=sv_ref.default_db,
            )
        except Exception as e:
            self.report.num_sql_parser_failures += 1
            self.report.sql_parsing_total_sec += time.perf_counter() - start
            logger.info(f"sqlglot raised for {table_id}: {e}")
            return

        self.report.sql_parsing_total_sec += time.perf_counter() - start

        if parsed.debug_info.table_error:
            self.report.num_sql_parser_table_error += 1
            self.report.num_sql_parser_failures += 1
            logger.info(
                f"sqlglot table_error for {table_id}: {parsed.debug_info.error}"
            )
            return

        if parsed.debug_info.column_error:
            self.report.num_sql_parser_column_error += 1
            self.report.num_sql_parser_failures += 1
            logger.info(
                f"sqlglot column_error for {table_id}: {parsed.debug_info.column_error}"
            )
            # Continue — emit table-level edges only.
        else:
            self.report.num_sql_parser_success += 1

        downstream_urn = str(self._make_self_dataset_urn(table_id))
        audit = AuditStampClass(
            time=int(datetime.now(tz=timezone.utc).timestamp() * 1000),
            actor="urn:li:corpuser:datahub",
        )
        upstream_classes = [
            UpstreamClass(
                dataset=urn,
                type=DatasetLineageTypeClass.TRANSFORMED,
                auditStamp=audit,
            )
            for urn in parsed.in_tables
        ]

        fine_grained: List[FineGrainedLineageClass] = []
        if not parsed.debug_info.column_error:
            for cl in parsed.column_lineage or []:
                downstream_col_urn = make_schema_field_urn(
                    downstream_urn, cl.downstream.column
                )
                upstream_col_urns: List[str] = []
                for u in cl.upstreams:
                    candidate_urn = make_schema_field_urn(u.table, u.column)
                    # Dedup against existing TS-pre-resolved edges so
                    # we don't emit a duplicate column-level edge that
                    # already lives on the dataset's UpstreamLineage.
                    if (
                        existing_fgl_edges is not None
                        and (downstream_col_urn, candidate_urn) in existing_fgl_edges
                    ):
                        continue
                    upstream_col_urns.append(candidate_urn)
                if upstream_col_urns:
                    fine_grained.append(
                        FineGrainedLineageClass(
                            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                            upstreams=upstream_col_urns,
                            downstreams=[downstream_col_urn],
                        )
                    )

        if not upstream_classes and not fine_grained:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=downstream_urn,
            aspect=UpstreamLineageClass(
                upstreams=upstream_classes,
                fineGrainedLineages=fine_grained or None,
            ),
        ).as_workunit()

    def _apply_dataset_schema(
        self, dataset: Dataset, columns: Optional[List["ColumnResponse"]]
    ) -> None:
        """Attach ``schemaMetadata`` to the dataset when columns are known.

        ``get_logical_tables`` fetches with ``include_details=True`` so we
        don't pay a per-table follow-up call. ``columns`` is ``None`` only
        if the list call was made without details, in which case we emit
        the dataset without a schema rather than silently swallowing.

        Builds full ``SchemaFieldClass`` objects here rather than handing
        the SDK 3-tuples — the SDK's tuple-form falls back to
        ``resolve_sql_type``, a SQL-flavoured heuristic that doesn't
        recognise ThoughtSpot's ``DATE_TIME`` (or other snake-case TS
        variants) and resolves them to ``NullType``. Our
        ``_ts_type_to_schema_type`` knows the TS vocabulary, so routing
        through it gives the worksheet schema the same correct
        classification we already emit on chart ``inputFields``.
        """
        if not columns:
            return
        schema_fields = [self._make_schema_field(col) for col in columns if col.name]
        if schema_fields:
            dataset._set_schema(schema_fields)

    def _apply_dataset_upstreams(
        self,
        dataset: Dataset,
        columns: Optional[List["ColumnResponse"]],
        table: Optional["LogicalTableResponse"] = None,
    ) -> None:
        """Attach lineage edges from each TS column with per-edge audit
        stamps.

        Two independent sets of edges, both emitted on the same
        ``UpstreamLineage`` aspect:

        - TS-internal: walks ``col.sources`` (TS's pre-resolved
          leaf-column lineage across joins/formulas) and emits
          column-level edges to each upstream TS column URN.
        - External: when ``table`` resolves to a federated backing
          table, emits one column-level edge per TS column pointing at
          the external column on the upstream physical table. Uses
          ``col.physical_column_name`` (the authoritative external
          name).

        Each table-level ``UpstreamClass`` carries an ``auditStamp``
        sourced from the TS table's own ``modified`` ms-epoch and
        author URN.
        """
        if not columns or table is None:
            return

        external_ref: Optional[ExternalRef] = self._resolve_external_upstream(table)
        per_upstream = self._collect_per_upstream_columns(columns, external_ref)
        if not per_upstream:
            return

        audit = self._build_audit_stamp(table)
        downstream_dataset_urn = str(self._make_self_dataset_urn(table.id))
        upstream_aspects = self._build_upstream_aspects(
            per_upstream, audit, downstream_dataset_urn
        )
        dataset.set_upstreams(upstream_aspects)

    def _collect_per_upstream_columns(
        self,
        columns: List["ColumnResponse"],
        external_ref: Optional[ExternalRef],
    ) -> Dict[str, Dict[str, List[str]]]:
        """Group ``columns`` by ``{upstream_dataset_urn: {downstream_col: [upstream_col]}}``.

        Walks both ``col.sources`` (TS-internal joins/formulas) and the
        resolved external reference when present, so the same dict
        carries both edge classes for the consolidated ``UpstreamLineage``
        aspect emission.

        The dict keys are *resolved DataHub URNs* — TS-internal ids are
        wrapped via ``_make_self_dataset_urn`` here, and external refs
        already carry a full URN. This keeps ``_build_upstream_aspects``
        a pure aspect-builder with no responsibility for URN
        construction.
        """
        per_upstream: Dict[str, Dict[str, List[str]]] = {}
        for col in columns:
            if not col.name:
                continue
            for src in col.sources or []:
                if not src.table_id or not src.column_name:
                    continue
                src_urn = str(self._make_self_dataset_urn(src.table_id))
                per_upstream.setdefault(src_urn, {}).setdefault(col.name, []).append(
                    src.column_name
                )

            if external_ref is not None:
                raw_col = col.physical_column_name or col.name
                # Default lowercase matches DataHub's canonical schemaField URN
                # convention (Databricks/Snowflake/Postgres/MySQL/Redshift/Hive
                # all lowercase column URNs); preserve only when the operator
                # has flagged this connection (e.g. BigQuery, SQL Server).
                external_col = (
                    raw_col if external_ref.preserve_column_case else raw_col.lower()
                )
                per_upstream.setdefault(external_ref.urn, {}).setdefault(
                    col.name, []
                ).append(external_col)
        return per_upstream

    def _build_audit_stamp(self, table: "LogicalTableResponse") -> AuditStampClass:
        """Build the per-edge ``AuditStampClass`` sourced from TS's own
        ``modified`` timestamp and author. Defaults: time=now if the
        wire timestamp is missing or zero; actor=``urn:li:corpuser:datahub``
        service URN when no author resolves. Without these defaults
        DataHub renders ``time=0`` / ``urn:li:corpuser:unknown``
        placeholders in the lineage UI.
        """
        audit_time_ms = (
            table.modified
            if table.modified is not None and table.modified > 0
            else int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        )
        audit_actor = self._author_user_urn(table) or "urn:li:corpuser:datahub"
        return AuditStampClass(time=audit_time_ms, actor=audit_actor)

    @staticmethod
    def _build_upstream_aspects(
        per_upstream: Dict[str, Dict[str, List[str]]],
        audit: AuditStampClass,
        downstream_dataset_urn: str,
    ) -> List[UpstreamInputType]:
        """Build the per-edge ``UpstreamClass`` + ``FineGrainedLineageClass``
        list to hand to ``Dataset.set_upstreams``.

        ``per_upstream`` keys are already-resolved DataHub URNs (see
        ``_collect_per_upstream_columns``). This helper just builds the
        aspect list — no URN wrapping or platform-instance lookup.
        """
        upstream_aspects: List[UpstreamInputType] = []
        for upstream_urn, col_map in per_upstream.items():
            upstream_aspects.append(
                UpstreamClass(
                    dataset=upstream_urn,
                    type=DatasetLineageTypeClass.TRANSFORMED,
                    auditStamp=audit,
                )
            )
            for downstream_col, upstream_cols in col_map.items():
                upstream_aspects.append(
                    FineGrainedLineageClass(
                        upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
                        downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD,
                        upstreams=[
                            make_schema_field_urn(upstream_urn, c)
                            for c in upstream_cols
                        ],
                        downstreams=[
                            make_schema_field_urn(
                                downstream_dataset_urn, downstream_col
                            )
                        ],
                    )
                )
        return upstream_aspects

    def _process_usage_stats(self) -> Iterable[MetadataWorkUnit]:
        """Emit ``DashboardUsageStatistics`` / ``ChartUsageStatistics``
        from each entity's ``stats.views`` field.

        The view count is the global cumulative counter returned by
        ``metadata_search`` when ``include_stats=True`` is sent — same
        number the TS UI shows in its "Views" column. No separate API
        round-trip; the data was already loaded during entity extraction.

        Entities with ``stats=None`` or ``views=0`` emit no aspect.
        Emitting ``viewsCount=0`` would clobber a real count from a prior
        ingestion run.
        """
        if not self.config.include_usage_stats:
            return

        # The skip-zero / skip-None gate ran at append time so each tuple
        # here is guaranteed to have ``views > 0`` — emit unconditionally.
        timestamp_millis = int(datetime.now(tz=timezone.utc).timestamp() * 1000)

        for liveboard_id, views in self._liveboard_usage:
            yield MetadataChangeProposalWrapper(
                entityUrn=make_dashboard_urn(
                    platform=self.platform,
                    name=liveboard_id,
                    platform_instance=self.config.platform_instance,
                ),
                aspect=DashboardUsageStatisticsClass(
                    timestampMillis=timestamp_millis,
                    viewsCount=views,
                ),
            ).as_workunit()

        for answer_id, views in self._answer_usage:
            yield MetadataChangeProposalWrapper(
                entityUrn=make_chart_urn(
                    platform=self.platform,
                    name=answer_id,
                    platform_instance=self.config.platform_instance,
                ),
                aspect=ChartUsageStatisticsClass(
                    timestampMillis=timestamp_millis,
                    viewsCount=views,
                ),
            ).as_workunit()

    def _get_connection_lookup(self) -> Dict[str, "ConnectionResponse"]:
        """Lazy ``{connection_id: ConnectionResponse}`` cache.

        Built once per ingestion run via ``client.get_connections()``.
        Used by ``_resolve_external_upstream`` to map each TS Logical
        Table's ``data_source_id`` back to its connection's platform
        type / default database / default schema. An empty dict here
        (e.g. the principal can't read connections) silently disables
        external lineage — the connector already surfaced a warning.
        """
        if self._connection_lookup is None:
            try:
                self._connection_lookup = {
                    c.id: c for c in self.client.get_connections()
                }
            except Exception as e:
                # Without the try/except the exception propagates
                # through the caller's per-table try/except and
                # silently drops the whole entity. With it, external
                # lineage degrades to "warning + skip" cleanly.
                self.report.warning(
                    title="Failed to Fetch Connections",
                    message=(
                        "Could not enumerate ThoughtSpot connections. "
                        "Cross-platform external lineage will be missing."
                    ),
                    exc=e,
                )
                self._connection_lookup = {}
        return self._connection_lookup

    def _external_connection_overrides(
        self, conn: "ConnectionResponse"
    ) -> "ExternalConnectionConfig":
        """Resolve per-connection overrides for cross-platform lineage.

        Looks up the TS connection in ``external_connections`` by GUID
        first (stable across renames), then by display name. Returns an
        ``ExternalConnectionConfig`` with field defaults when the
        connection isn't configured, so call-sites can dot-access the
        fields without branching on ``None``.
        """
        m = self.config.external_connections
        return (
            m.get(conn.id) or m.get(conn.name) or _EMPTY_EXTERNAL_CONNECTION_OVERRIDES
        )

    def _external_connection_overrides_by_id(
        self, conn_id: Optional[str]
    ) -> "ExternalConnectionConfig":
        """Lookup ``external_connections`` by TS connection GUID only.

        Used by the SQL_VIEW path where we don't have a successful
        connection-lookup (and therefore no ``ConnectionResponse.name``)
        but we do have the data_source_id from the LogicalTableResponse.
        Same fallback shape as ``_external_connection_overrides`` —
        returns the shared ``_EMPTY_EXTERNAL_CONNECTION_OVERRIDES``
        sentinel when no override is configured so the common
        no-overrides path doesn't allocate a fresh pydantic model
        per call.
        """
        if not conn_id:
            return _EMPTY_EXTERNAL_CONNECTION_OVERRIDES
        return (
            self.config.external_connections.get(conn_id)
            or _EMPTY_EXTERNAL_CONNECTION_OVERRIDES
        )

    def _resolve_sql_view_warehouse(
        self, table: "LogicalTableResponse"
    ) -> Optional[SqlViewWarehouseRef]:
        """Derive a warehouse reference for a SQL_VIEW dataset.

        Unlike ``_resolve_external_upstream`` (which targets 1:1
        physical mappings and needs ``physical_*`` fields on the
        table to build a single upstream URN), this helper only
        needs to know the warehouse dialect so sqlglot can parse the
        SQL — the upstream URNs come out of the parsed SQL itself.

        Primary source: the connection lookup. Reading
        ``conn.data_source_type`` is canonical (no ``RDBMS_`` prefix
        to strip), and ``conn.default_database`` feeds sqlglot's
        ``default_db`` for unqualified table refs. The same
        ``_external_connection_overrides(conn)`` path the cross-
        platform lineage uses gives ``platform_instance`` / ``env``.

        Fallback: if the connection isn't in the lookup (rare —
        observed only on tenants with very restrictive ACLs), read
        ``table.data_source_type`` directly. Strip TS's ``RDBMS_``
        prefix so the platform map (keyed by bare names like
        ``DATABRICKS``) resolves. ``default_db`` ends up None on the
        fallback path, which sqlglot handles fine when the SQL is
        fully-qualified.

        Returns ``None`` when the dialect can't be determined or
        ``include_external_lineage`` is disabled — caller skips SQL
        parsing (viewLogic still emits via ``_apply_sql_view_logic``).
        """
        if not self.config.include_external_lineage:
            return None

        # Primary: connection lookup.
        conn = None
        if table.data_source_id:
            conn = self._get_connection_lookup().get(table.data_source_id)

        if conn is not None:
            ts_type = (conn.data_source_type or "").upper()
            platform = _TS_TO_DATAHUB_PLATFORM.get(ts_type)
            if not platform:
                return None  # in-memory, FALCON, or unmapped warehouse
            overrides = self._external_connection_overrides(conn)
            return SqlViewWarehouseRef(
                platform=platform,
                env=overrides.env or self.config.env,
                platform_instance=overrides.platform_instance,
                default_db=conn.default_database,
            )

        # Fallback: connection not in lookup. Read data_source_type
        # off the table directly. Strip the ``RDBMS_`` / ``NOSQL_``
        # prefix TS applies on the LogicalTableResponse so the map
        # (keyed by bare warehouse names) resolves.
        ts_type = (table.data_source_type or "").upper()
        if not ts_type:
            return None
        for prefix in ("RDBMS_", "NOSQL_", "FILE_"):
            if ts_type.startswith(prefix):
                ts_type = ts_type[len(prefix) :]
                break
        platform = _TS_TO_DATAHUB_PLATFORM.get(ts_type)
        if not platform:
            return None

        overrides = self._external_connection_overrides_by_id(table.data_source_id)
        return SqlViewWarehouseRef(
            platform=platform,
            env=overrides.env or self.config.env,
            platform_instance=overrides.platform_instance,
            default_db=None,
        )

    def _resolve_external_upstream(
        self, table: "LogicalTableResponse"
    ) -> Optional[ExternalRef]:
        """Resolve a TS Logical Table to its external (Databricks /
        Snowflake / ...) dataset URN. Returns ``None`` for:

        * In-memory data (``dataSourceTypeEnum == 'DEFAULT'``)
        * Tables backed by a connection type we don't recognise
        * Tables whose connection isn't in the cache (stale TS state)
        * When ``include_external_lineage`` is disabled

        Side effect: populates ``_connection_lookup`` on first call.
        Returns the resolved reference so callers can also build
        column-level edges without re-resolving.
        """
        if not self.config.include_external_lineage:
            return None
        if not table.data_source_id:
            return None
        # Skip silently when the table's own type isn't a known external
        # platform — FALCON / DEFAULT / unmapped types are TS-internal
        # stores (sample data, system tables, CSV uploads) with no
        # external upstream to emit, so a missing connection-lookup
        # entry isn't a real failure to warn about. Without this guard,
        # every TS-internal table on a tenant triggers a false-positive
        # "External Lineage Resolution Failed" warning.
        ts_type = (table.data_source_type or "").upper()
        if ts_type not in _TS_TO_DATAHUB_PLATFORM:
            return None
        conn = self._get_connection_lookup().get(table.data_source_id)
        if conn is None:
            self._unresolvable_external_lineage_count += 1
            return None
        # ``data_source_type`` is Optional on ConnectionResponse — TS
        # occasionally omits it for legacy connections. Mirror the same
        # ``or ""`` guard already used in ``_resolve_sql_view_warehouse``
        # so an unset field falls through to the "platform unknown"
        # warning rather than an AttributeError.
        platform = _TS_TO_DATAHUB_PLATFORM.get((conn.data_source_type or "").upper())
        if not platform:
            return None  # in-memory, FALCON, or unmapped platform

        database = table.physical_database_name or conn.default_database
        schema = table.physical_schema_name or conn.default_schema
        physical_name = table.physical_table_name or table.name
        if not database or not physical_name:
            return None  # incomplete physical mapping

        builder = _KEY_BUILDERS.get(platform)
        if builder is None:
            return None  # platform mapped but no key builder — coverage bug
        key = builder(database, schema, physical_name)

        overrides = self._external_connection_overrides(conn)
        platform_instance = overrides.platform_instance
        env = overrides.env or self.config.env
        # ``platform_instance`` is prepended to the dataset key when set —
        # this is the convention ``make_dataset_urn_with_platform_instance``
        # uses internally. We construct the URN string explicitly so the
        # ExternalRef carries it cleanly.
        full_name = f"{platform_instance}.{key}" if platform_instance else key
        urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{full_name},{env})"
        return ExternalRef(
            platform=platform,
            urn=urn,
            env=env,
            platform_instance=platform_instance,
            preserve_column_case=overrides.preserve_column_case,
        )

    @staticmethod
    def _epoch_ms_to_datetime(ms: Optional[int]) -> Optional[datetime]:
        """Convert TS's ms-since-epoch timestamps to UTC datetimes.

        TS returns ``created`` / ``modified`` as ``Long`` ms since epoch.
        Used for two routes: DataHub SDK V2 entity constructors expect
        ``datetime``, and the user-visible custom-property rendering
        formats the same value as ISO 8601. Single conversion point so
        both call sites stay consistent.
        """
        if ms is None or ms <= 0:
            return None
        return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)

    def _author_user_urn(
        self,
        metadata: Union[
            "WorkspaceResponse",
            "LiveboardResponse",
            "VisualizationResponse",
            "AnswerResponse",
            "LogicalTableResponse",
        ],
    ) -> Optional[str]:
        """Resolve the entity-author CorpUser URN for audit stamps and
        ``last_modified_by`` / ``created_by`` on SDK V2 entities.

        Returns ``None`` when no login resolves, so callers omit the
        field rather than emitting a ``urn:li:corpuser:unknown``
        placeholder.
        """
        login = _resolve_author_login(metadata)
        return make_user_urn(login) if login else None

    def _extract_custom_properties(
        self,
        metadata: WorkspaceResponse
        | LiveboardResponse
        | AnswerResponse
        | LogicalTableResponse,
    ) -> Dict[str, str]:
        """Extract custom properties from ThoughtSpot metadata.

        Keys are prefixed with ``thoughtspot_`` so operators can filter
        them in the DataHub UI's custom-properties panel.
        """
        custom_props: Dict[str, str] = {"thoughtspot_id": metadata.id}

        created_dt = self._epoch_ms_to_datetime(metadata.created)
        if created_dt is not None:
            custom_props["created"] = created_dt.isoformat()
        modified_dt = self._epoch_ms_to_datetime(metadata.modified)
        if modified_dt is not None:
            custom_props["modified"] = modified_dt.isoformat()

        # TS REST v2 returns ``author`` as a user GUID and ``authorName``
        # / ``authorDisplayName`` as the real login + display name. Prefer
        # those top-level fields so the custom property is human-readable
        # — falling back to ``author.name`` only if it doesn't look like
        # a GUID. Same precedence rule as ``_apply_entity_ownership``.
        author_login = _resolve_author_login(metadata)
        if author_login:
            custom_props["author"] = author_login
        if metadata.author_display_name:
            custom_props["author_display_name"] = metadata.author_display_name

        # Chart type / question text are TML-derived and only present on
        # Answers (and on Visualisations, which build custom_props
        # directly in ``_process_visualization``).
        if isinstance(metadata, AnswerResponse):
            if metadata.chart_type:
                custom_props["thoughtspot_chart_type"] = metadata.chart_type
            if metadata.question_text:
                custom_props["thoughtspot_question_text"] = metadata.question_text
        if (
            isinstance(metadata, LogicalTableResponse)
            and metadata.join_count is not None
        ):
            custom_props["thoughtspot_join_count"] = str(metadata.join_count)

        # Surface usage-stats side-channels as custom properties. The headline
        # ``views`` count flows into ``DashboardUsageStatistics`` /
        # ``ChartUsageStatistics`` separately (see _process_usage_stats);
        # ``favorites`` and ``last_accessed`` have no first-class aspect, so
        # they ride on custom properties where the entity-page UI surfaces them.
        stats = getattr(metadata, "stats", None)
        if stats is not None:
            if stats.favorites:
                custom_props["thoughtspot_favorites"] = str(stats.favorites)
            last_accessed_dt = self._epoch_ms_to_datetime(stats.last_accessed)
            if last_accessed_dt is not None:
                custom_props["thoughtspot_last_accessed"] = last_accessed_dt.isoformat()

        return custom_props

    def _get_liveboard_url(self, liveboard_id: str) -> Optional[str]:
        """Generate external URL for a Liveboard."""
        base_url = self.config.connection.base_url
        return f"{base_url}/#/pinboard/{liveboard_id}"

    def _get_answer_url(self, answer_id: str) -> Optional[str]:
        """Generate external URL for an Answer."""
        base_url = self.config.connection.base_url
        return f"{base_url}/#/saved-answer/{answer_id}"

    def _get_table_url(self, table_id: str) -> Optional[str]:
        """Generate external URL for a Worksheet/Table."""
        base_url = self.config.connection.base_url
        return f"{base_url}/#/data/tables/{table_id}"

    def get_report(self):
        """Return ingestion report."""
        return self.report

    @classmethod
    def create(cls, config_dict, ctx):
        """Create source instance from config dictionary."""
        config = ThoughtSpotConfig.model_validate(config_dict)
        return cls(config, ctx)
