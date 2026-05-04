import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Set

from pydantic import Field, SecretStr
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.emitter.mce_builder import make_ts_millis
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.ingestion_job_checkpointing_provider_base import JobId
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.hex.api import HexApi, HexApiReport
from datahub.ingestion.source.hex.constants import (
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
    HEX_INCREMENTAL_JOB_ID,
    HEX_PLATFORM_NAME,
)
from datahub.ingestion.source.hex.document_builder import HexDocumentBuilder
from datahub.ingestion.source.hex.lineage_builder import (
    HexLineageBuilder,
    LineageBuilderReport,
)
from datahub.ingestion.source.hex.mapper import Mapper
from datahub.ingestion.source.hex.model import (
    Component,
    ExploreCell,
    HexIncrementalCheckpointState,
    Project,
    SqlCell,
)
from datahub.ingestion.source.hex.query_fetcher import (
    HexQueryFetcherReport,
)
from datahub.ingestion.source.state.checkpoint import Checkpoint
from datahub.ingestion.source.state.entity_removal_state import GenericCheckpointState
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
    StatefulIngestionUsecaseHandlerBase,
)
from datahub.metadata.schema_classes import StatusClass

logger = logging.getLogger(__name__)


class HexSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    workspace_name: str = Field(
        description="Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>",
    )
    token: SecretStr = Field(
        description=(
            "Hex Workspace Token with the 'Read projects' scope. "
            "Create one at Settings → API → Workspace tokens. "
            "The 'Read projects' scope is required to access project cells for lineage; "
            "tokens without it can enumerate projects but not read their content. "
            "See https://learn.hex.tech/docs/api-integrations/api/overview for token types."
        ),
    )
    base_url: str = Field(
        default=HEX_API_BASE_URL_DEFAULT,
        description="Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. "
        "Single-tenant app users should replace this with the URL they use to access Hex.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )
    include_components: bool = Field(
        default=True,
        description="Include Hex Components in the ingestion",
    )
    page_size: int = Field(
        default=HEX_API_PAGE_SIZE_DEFAULT,
        description="Number of items to fetch per Hex API call.",
    )
    patch_metadata: bool = Field(
        default=False,
        description="Emit metadata as patch events",
    )
    collections_as_tags: bool = Field(
        default=True,
        description="Emit Hex Collections as tags",
    )
    status_as_tag: bool = Field(
        default=True,
        description="Emit Hex Status as tags",
    )
    categories_as_tags: bool = Field(
        default=True,
        description="Emit Hex Category as tags",
    )
    project_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for project titles to filter in ingestion.",
    )
    component_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for component titles to filter in ingestion.",
    )
    set_ownership_from_email: bool = Field(
        default=True,
        description="Set ownership identity from owner/creator email",
    )
    include_lineage: bool = Field(
        default=True,
        description="Extract upstream lineage. Uses queriedTables API (ENTERPRISE workspaces) "
        "or falls back to parsing SQL from cells (all workspaces). No warehouse ingestion "
        "dependency required.",
    )
    connection_platform_map: Dict[str, str] = Field(
        default_factory=dict,
        description="Map connection ID to DataHub platform name for connections that cannot "
        "be resolved automatically (deleted connections, permission gaps, unsupported types). "
        "Lineage is skipped — not guessed — for unmapped connections. "
        'Example: {"<connection_uuid>": "snowflake"}',
    )
    include_run_history: bool = Field(
        default=True,
        description="Emit the most recent scheduled run as an Operation aspect.",
    )
    include_context_documents: bool = Field(
        default=True,
        description="Emit a DataHub Document per project containing SQL sources, "
        "visualisation metadata, and notebook documentation. Documents are hidden from "
        "global search and linked to the project Dashboard for AI agent retrieval.",
    )
    category_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for categories to filter in ingestion. This will exclude any project or component that has any category denied or not explicitly allowed.",
    )

    # Removed fields — emit a clear warning rather than silently ignoring.
    # These were used by the old DataHub-query-fetcher lineage path which searched
    # DataHub for Query entities tagged with Hex metadata comments. Lineage now comes
    # directly from the Hex REST API and none of these fields have any effect.
    _lineage_start_time_removed = pydantic_removed_field(
        "lineage_start_time", month="May", year=2026
    )
    _lineage_end_time_removed = pydantic_removed_field(
        "lineage_end_time", month="May", year=2026
    )
    _datahub_page_size_removed = pydantic_removed_field(
        "datahub_page_size", month="May", year=2026
    )


@dataclass
class HexReport(
    StaleEntityRemovalSourceReport,
    HexApiReport,
    HexQueryFetcherReport,
    LineageBuilderReport,
):
    projects_with_lineage: int = 0
    projects_without_sql_cells: int = 0
    # Incremental ingestion counters — visible in ingestion run summary
    projects_full_refresh: int = 0
    projects_incremental_skip: int = 0


class _HexIncrementalHandler(StatefulIngestionUsecaseHandlerBase):
    """Minimal use-case handler that registers the incremental checkpoint job with the framework."""

    def __init__(self, source: "HexSource") -> None:
        self._source = source
        source.state_provider.register_stateful_ingestion_usecase_handler(self)

    @property
    def job_id(self) -> JobId:
        return HEX_INCREMENTAL_JOB_ID

    def is_checkpointing_enabled(self) -> bool:
        return self._source.state_provider.is_stateful_ingestion_configured()

    def create_checkpoint(self) -> Optional[Checkpoint[HexIncrementalCheckpointState]]:
        assert self._source.ctx.pipeline_name
        return Checkpoint(
            job_name=str(HEX_INCREMENTAL_JOB_ID),
            pipeline_name=self._source.ctx.pipeline_name,
            run_id=self._source.ctx.run_id,
            state=HexIncrementalCheckpointState(),
        )


@platform_name("Hex")
@config_class(HexSourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Supported by default")
@capability(SourceCapability.OWNERSHIP, "Supported by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Supported by default",
    subtype_modifier=[
        SourceCapabilityModifier.HEX_PROJECT,
    ],
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default via queriedTables API (ENTERPRISE) or SQL parsing from cells (all tiers). "
    "No warehouse ingestion dependency required.",
)
@capability(
    SourceCapability.LINEAGE_FINE,
    "Column-level lineage via SQL parsing when datahub-api is configured. "
    "The graph-backed SchemaResolver fetches table schemas from DataHub on demand to expand SELECT * "
    "and resolve column references. Graceful degradation to dataset-level when datahub-api is absent.",
)
@capability(
    SourceCapability.TAGS, "Status, categories, and collections emitted as tags"
)
class HexSource(TestableSource, StatefulIngestionSourceBase):
    def __init__(self, config: HexSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report: HexReport = HexReport()
        self.platform = HEX_PLATFORM_NAME
        self.hex_api = HexApi(
            report=self.report,
            token=self.source_config.token.get_secret_value(),
            base_url=self.source_config.base_url,
            page_size=self.source_config.page_size,
        )
        self.mapper = Mapper(
            workspace_name=self.source_config.workspace_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            base_url=self.source_config.base_url,
            patch_metadata=self.source_config.patch_metadata,
            collections_as_tags=self.source_config.collections_as_tags,
            status_as_tag=self.source_config.status_as_tag,
            categories_as_tags=self.source_config.categories_as_tags,
            set_ownership_from_email=self.source_config.set_ownership_from_email,
        )
        self.project_registry: Dict[str, Project] = {}
        self.component_registry: Dict[str, Component] = {}
        # Projects whose last_edited_at hasn't changed since the last checkpoint.
        # These skip the expensive per-project fetches (cells, lineage, context docs).
        self._light_project_ids: Set[str] = set()
        # Cache of raw cells per entity ID, populated during lineage enrichment and
        # reused by context document generation to avoid fetching twice per project.
        self._cells_cache: Dict[str, List[dict]] = {}
        # Timestamp of the last successful checkpoint (millis). Used to guard
        # against re-emitting run history that was already captured.
        self._last_ingested_at_ms: Optional[int] = None
        # Register the incremental ingestion use-case handler.
        _HexIncrementalHandler(self)

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HexSource":
        config = HexSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    @staticmethod
    def _sample_connections(
        api: "HexApi", base: str, headers: dict
    ) -> "Dict[str, set]":
        """Sample up to 5 projects via export API to discover {conn_id → sample_tables}.

        Uses export (not cells) so it works even when cells is 403.
        Best-effort: exceptions are swallowed so test_connection always completes.
        """
        result: Dict[str, set] = {}
        try:
            import sqlglot as _sqlglot
            import yaml as _yaml

            sample_ids = [
                p.get("id")
                for p in api.session.get(
                    f"{base}/projects",
                    headers=headers,
                    params={"limit": 5},
                    timeout=15,
                )
                .json()
                .get("values", [])
                if p.get("id")
            ]
            for pid in sample_ids:
                exp_r = api.session.post(
                    f"{base}/projects/export",
                    headers=headers,
                    json={"projectId": pid, "version": "draft"},
                    timeout=15,
                )
                if not exp_r.ok:
                    continue
                content = _yaml.safe_load(exp_r.json().get("content", "")) or {}
                for cell in content.get("cells", []):
                    if cell.get("cellType") != "SQL":
                        continue
                    cfg = cell.get("config", {})
                    conn_id = cfg.get("dataConnectionId", "")
                    sql_src = cfg.get("source", "")
                    if not conn_id:
                        continue
                    result.setdefault(conn_id, set())
                    if sql_src and len(result[conn_id]) < 3:
                        for stmt in _sqlglot.parse(sql_src):
                            if stmt:
                                for tbl in stmt.find_all(_sqlglot.exp.Table):
                                    name = str(tbl).split(" ")[0].strip("\"'`")
                                    if "." in name and len(name) < 120:
                                        result[conn_id].add(name)
        except Exception:
            pass
        return result

    @staticmethod
    def _build_conn_failure_msg(
        status_code: int, tables_by_conn: "Dict[str, set]"
    ) -> str:
        lines = [
            f"HTTP {status_code} on /v1/data-connections — token cannot resolve "
            "connection IDs to data platforms, resulting in zero lineage. "
            "Add 'Read data connections' scope to the Workspace Token, OR provide "
            "connection_platform_map in the recipe. "
            f"Discovered {len(tables_by_conn)} connection(s) in sample:"
        ]
        for cid, tables in sorted(tables_by_conn.items()):
            hint = ", ".join(sorted(tables)[:3]) if tables else "(no tables sampled)"
            lines.append(f'  "{cid}": "<platform>"  # e.g. {hint}')
        return "\n".join(lines)

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        report = TestConnectionReport()
        report.capability_report = {}

        try:
            config = HexSourceConfig.parse_obj_allow_extras(config_dict)
            # Use the HexApi retry session — consistent with actual ingestion behaviour
            # (bare requests.get() would fail on rate-limited or flaky responses)
            api = HexApi(
                report=HexReport(),
                token=config.token.get_secret_value(),
                base_url=config.base_url,
            )
            base = config.base_url.rstrip("/")
            headers = {"Authorization": f"Bearer {config.token.get_secret_value()}"}

            # Basic connectivity — GET /v1/users/me (lightweight, just validates token)
            resp = api.session.get(f"{base}/users/me", headers=headers, timeout=15)
            if resp.status_code == 401:
                report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason="Authentication failed — check that your token is valid and not expired.",
                )
                return report
            resp.raise_for_status()
            report.basic_connectivity = CapabilityReport(capable=True)

            # Capabilities that only require a valid token
            for cap in (
                SourceCapability.DESCRIPTIONS,
                SourceCapability.OWNERSHIP,
                SourceCapability.CONTAINERS,
                SourceCapability.USAGE_STATS,
                SourceCapability.TAGS,
            ):
                report.capability_report[cap] = CapabilityReport(capable=True)

            # --- Lineage tier detection ---

            # Fetch first project ID — needed to probe queriedTables
            proj_resp = api.session.get(
                f"{base}/projects",
                headers=headers,
                params={"limit": 1},
                timeout=15,
            )
            conn_resp = api.session.get(
                f"{base}/data-connections", headers=headers, timeout=15
            )

            first_project_id: Optional[str] = None
            if proj_resp.ok:
                projects = proj_resp.json().get("values", [])
                if projects:
                    first_project_id = projects[0].get("id")

            # Tier 1: queriedTables (ENTERPRISE only)
            # Hex returns 403 for non-ENTERPRISE workspaces.
            enterprise_lineage = False
            if first_project_id:
                qt_resp = api.session.get(
                    f"{base}/projects/{first_project_id}/queriedTables",
                    headers=headers,
                    timeout=15,
                )
                if qt_resp.ok:
                    enterprise_lineage = True
                    report.capability_report[
                        "Lineage via queriedTables API (ENTERPRISE)"
                    ] = CapabilityReport(
                        capable=True,
                        mitigation_message="Hex returns the exact list of warehouse tables "
                        "this project queries — no SQL parsing required.",
                    )
                else:
                    report.capability_report[
                        "Lineage via queriedTables API (ENTERPRISE)"
                    ] = CapabilityReport(
                        capable=False,
                        failure_reason=f"HTTP {qt_resp.status_code} — workspace is not on "
                        "the ENTERPRISE tier or token lacks access.",
                        mitigation_message="Upgrade to Hex ENTERPRISE to unlock exact lineage. "
                        "SQL-parsing lineage (Tier 2) is still available.",
                    )

            # Tier 2: cells + SQL parsing (all tiers)
            # Probe the cells endpoint directly — a token may be able to list
            # projects yet return 403 on cells (metadata-only / admin tokens).
            # Also check whether data-connections is accessible; if not, lineage
            # is only possible if the user provided connection_platform_map overrides.
            has_conn_map = bool(config.connection_platform_map)
            cells_accessible = False
            cells_failure_reason: Optional[str] = None
            if not conn_resp.ok and not has_conn_map:
                cells_failure_reason = (
                    f"HTTP {conn_resp.status_code} on /v1/data-connections — token cannot "
                    "resolve connection IDs to data platforms, resulting in zero lineage. "
                    "Add 'Read data connections' scope to the Workspace Token, OR provide "
                    "connection_platform_map in the recipe to manually map connection IDs "
                    'to platform names (e.g. {"<uuid>": "snowflake"}).'
                )
            elif proj_resp.ok and (conn_resp.ok or has_conn_map) and first_project_id:
                cells_probe = api.session.get(
                    f"{base}/cells",
                    headers=headers,
                    params={"projectId": first_project_id, "limit": "1"},
                    timeout=15,
                )
                if cells_probe.ok:
                    cells_accessible = True
                else:
                    cells_failure_reason = (
                        f"HTTP {cells_probe.status_code} on /v1/cells — token cannot "
                        "read project cell content. Create a Workspace Token "
                        "(Settings → API → Workspace tokens) with the 'Read projects' "
                        "scope enabled. Personal tokens and workspace tokens without "
                        "'Read projects' can enumerate projects but not access cells, "
                        "resulting in zero lineage."
                    )
            elif not proj_resp.ok:
                cells_failure_reason = "Could not access /v1/projects."

            if cells_accessible:
                report.capability_report["Lineage via SQL parsing (all tiers)"] = (
                    CapabilityReport(
                        capable=True,
                        mitigation_message="SQL is extracted from each project's cells and parsed "
                        "with sqlglot. Cells with unknown connection IDs are skipped "
                        "rather than emitting wrong lineage.",
                    )
                )
            else:
                report.capability_report["Lineage via SQL parsing (all tiers)"] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=cells_failure_reason
                        or "Could not verify cells access.",
                        mitigation_message="Verify the token has at least 'Can view' access "
                        "on projects in the Hex workspace.",
                    )
                )

            # Sample projects via export to discover unresolvable connection IDs
            # and representative table names per connection. Best-effort only.
            sampled_tables_by_conn = (
                HexSource._sample_connections(api, base, headers)
                if proj_resp.ok
                else {}
            )

            # Enrich failure message with per-connection sample tables
            if sampled_tables_by_conn and not conn_resp.ok and not has_conn_map:
                cells_failure_reason = HexSource._build_conn_failure_msg(
                    conn_resp.status_code, sampled_tables_by_conn
                )
                report.capability_report["Lineage via SQL parsing (all tiers)"] = (
                    CapabilityReport(
                        capable=False,
                        failure_reason=cells_failure_reason,
                        mitigation_message="Verify the token has at least 'Can view' access "
                        "on projects in the Hex workspace.",
                    )
                )

            # Top-level LINEAGE_COARSE — capable if either tier works
            report.capability_report[SourceCapability.LINEAGE_COARSE] = (
                CapabilityReport(
                    capable=enterprise_lineage or cells_accessible,
                    failure_reason=None
                    if (enterprise_lineage or cells_accessible)
                    else "Neither lineage tier is available — check token permissions.",
                )
            )

        except Exception as e:
            logger.exception(f"test_connection failed: {e}")
            report.internal_failure = True
            report.internal_failure_reason = str(e)
            if report.basic_connectivity is None:
                report.basic_connectivity = CapabilityReport(
                    capable=False, failure_reason=str(e)
                )

        return report

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> HexReport:
        return self.report

    def _migrate_legacy_component_dashboards(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Detects and soft-deletes Dashboard entities that were emitted for
        Components by the previous connector version (pre-v1.1).

        In v1.1+ Components are Chart entities; previously they were Dashboard
        entities with subtype 'Component'. If any such Dashboard entities still
        exist in DataHub this method:
          1. Paginates through all of them and emits Status(removed=True) to
             each — ensuring cleanup regardless of whether the stale-entity
             removal handler was tracking them.
          2. Clears _light_project_ids so the current run is forced to fully
             re-process all projects, populating DashboardInfo.charts with the
             new Chart URNs.

        Requires ctx.graph. If the graph is unavailable the migration is
        skipped silently; operators should run once with ignore_old_state=true
        to force a full re-process manually.
        """
        if not self.ctx.graph:
            return

        QUERY = """
        query DetectLegacyHexComponents($start: Int!, $count: Int!) {
            search(input: {
                type: DASHBOARD
                query: "*"
                filters: [
                    {field: "platform", value: "urn:li:dataPlatform:hex"}
                    {field: "typeNames", value: "Component"}
                ]
                start: $start
                count: $count
            }) {
                total
                searchResults { entity { urn } }
            }
        }
        """
        page_size = 200
        start = 0
        legacy_urns: List[str] = []

        try:
            while True:
                result = self.ctx.graph.execute_graphql(
                    QUERY, variables={"start": start, "count": page_size}
                )
                page = result.get("search", {})
                total = page.get("total", 0)
                for item in page.get("searchResults", []):
                    urn = item.get("entity", {}).get("urn")
                    if urn:
                        legacy_urns.append(urn)
                start += page_size
                if start >= total:
                    break
        except Exception as e:
            logger.warning(
                "Failed to query for legacy Dashboard-typed components: %s. "
                "Skipping migration — run with ignore_old_state=true if needed.",
                e,
            )
            return

        if not legacy_urns:
            return

        logger.info(
            "Found %d legacy Dashboard-typed Component entities from a previous "
            "connector version. Soft-deleting and forcing full re-process.",
            len(legacy_urns),
        )
        for urn in legacy_urns:
            yield MetadataWorkUnit(
                id=f"migrate-remove-{urn}",
                mcp=MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=StatusClass(removed=True),
                ),
            )

        # Force all projects to be fully re-processed so DashboardInfo.charts
        # gets populated with the new Chart URNs for components.
        self._light_project_ids.clear()

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Read incremental checkpoint — None on first run or when ignore_old_state=true
        run_start_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
        last_checkpoint = self.state_provider.get_last_checkpoint(
            HEX_INCREMENTAL_JOB_ID, HexIncrementalCheckpointState
        )
        last_ingested_at_ms: Optional[int] = (
            last_checkpoint.state.last_ingested_at_millis
            if last_checkpoint and last_checkpoint.state.last_ingested_at_millis
            else None
        )
        self._last_ingested_at_ms = last_ingested_at_ms
        if last_ingested_at_ms:
            logger.info(
                "Incremental ingestion: last checkpoint at %s. "
                "Projects unchanged since then will skip cells/lineage/context fetches.",
                datetime.fromtimestamp(
                    last_ingested_at_ms / 1000, tz=timezone.utc
                ).isoformat(),
            )
        else:
            logger.info("No incremental checkpoint found — performing full ingestion.")

        # One-time migration: soft-delete legacy Dashboard-typed Component entities
        # from the previous connector version and force a full re-process.
        # No-op after the first successful migration run (search returns 0).
        with self.report.new_stage("Migrate legacy Component entities"):
            yield from self._migrate_legacy_component_dashboards()

        with self.report.new_stage("Fetch Hex projects and components"):
            self._populate_registries(last_ingested_at_ms=last_ingested_at_ms)

        # Build connections map once — used by lineage and context documents
        connections_by_id: Dict[str, Any] = {}
        if (
            self.source_config.include_lineage
            or self.source_config.include_context_documents
        ):
            connections_by_id = self.hex_api.fetch_connections()
            # Apply manual overrides from config. The primary use case is deleted
            # connections that no longer appear in /v1/data-connections — those
            # are inserted with a synthetic name so both lineage and context docs work.
            for conn_id, platform in self.source_config.connection_platform_map.items():
                name = (
                    connections_by_id[conn_id][0]
                    if conn_id in connections_by_id
                    else conn_id
                )
                connections_by_id[conn_id] = (name, platform)

        if self.source_config.include_lineage:
            with self.report.new_stage("Extract lineage via Hex REST API"):
                self._enrich_lineage(connections_by_id)

        if self.source_config.include_run_history:
            with self.report.new_stage("Fetch run history"):
                self._enrich_run_history()

        with self.report.new_stage("Emit"):
            yield from self.mapper.map_workspace()

            for project in self.project_registry.values():
                is_light = project.id in self._light_project_ids
                if is_light:
                    # Project unchanged since last checkpoint — skip all full aspects.
                    # Only emit lastRefreshed PATCH if a COMPLETED run happened AFTER
                    # the last checkpoint (i.e. it's new since the previous ingestion).
                    self.report.projects_incremental_skip += 1
                    new_run_ms = self._new_completed_run_ms(project)
                    if new_run_ms is not None:
                        yield from self.mapper.map_project_last_refreshed(
                            project=project,
                            last_refreshed_ms=new_run_ms,
                        )
                    continue

                self.report.projects_full_refresh += 1
                # upstream_datasets are already embedded in DashboardInfo.datasetEdges
                # via map_project() — the correct lineage mechanism for Dashboard entities.
                # upstreamLineage aspect is NOT registered on dashboard in entity-registry.yml.
                yield from self.mapper.map_project(project=project)
                new_run_ms = self._new_completed_run_ms(project)
                if new_run_ms is not None:
                    yield from self.mapper.map_project_last_refreshed(
                        project=project,
                        last_refreshed_ms=new_run_ms,
                    )

            for component in self.component_registry.values():
                yield from self.mapper.map_component(component=component)

        if self.source_config.include_context_documents:
            with self.report.new_stage("Emit context documents"):
                yield from self._emit_context_documents(connections_by_id)

        # Commit incremental checkpoint. ignore_new_state is honoured by
        # get_current_checkpoint returning None when set.
        cur_checkpoint = self.state_provider.get_current_checkpoint(
            HEX_INCREMENTAL_JOB_ID
        )
        if cur_checkpoint is not None:
            cur_checkpoint.state = HexIncrementalCheckpointState(
                last_ingested_at_millis=run_start_ms
            )

    def _populate_registries(self, last_ingested_at_ms: Optional[int] = None) -> None:
        for item in self.hex_api.fetch_projects(
            include_components=self.source_config.include_components,
            stop_before_ms=last_ingested_at_ms,
        ):
            if item.categories and (
                any(
                    self.source_config.category_pattern.denied(c.name)
                    for c in item.categories
                )
                or not any(
                    self.source_config.category_pattern.allowed(c.name)
                    for c in item.categories
                )
            ):
                continue

            if isinstance(item, Project):
                if self.source_config.project_title_pattern.allowed(item.title):
                    self.project_registry[item.id] = item
                    # Tag as light if unchanged since last checkpoint.
                    # last_edited_at comes from lastEditedAt in the project list response.
                    if (
                        last_ingested_at_ms
                        and item.last_edited_at
                        and int(item.last_edited_at.timestamp() * 1000)
                        <= last_ingested_at_ms
                    ):
                        self._light_project_ids.add(item.id)
            elif isinstance(item, Component):
                if (
                    self.source_config.include_components
                    and self.source_config.component_title_pattern.allowed(item.title)
                ):
                    self.component_registry[item.id] = item
            else:
                assert_never(item)

        # Seed light projects from the previous run's stale-entity-removal checkpoint.
        # Because fetch_projects now sorts by LAST_EDITED_AT DESC and stops early,
        # projects unchanged since the checkpoint never appear in the Hex API response.
        # We add them to project_registry as light stubs so run-history checks and
        # lastRefreshed PATCH emission still work without paginating through all of Hex.
        if last_ingested_at_ms:
            stale_checkpoint = self.state_provider.get_last_checkpoint(
                JobId(f"{HEX_PLATFORM_NAME}_stale_entity_removal"),
                GenericCheckpointState,
            )
            if stale_checkpoint:
                prefix = (
                    f"{self.source_config.platform_instance}."
                    if self.source_config.platform_instance
                    else ""
                )
                for urn in stale_checkpoint.state.urns:
                    if not urn.startswith("urn:li:dashboard:(hex,"):
                        continue
                    # URN format: urn:li:dashboard:(hex,<prefix><project_id>)
                    inner = urn[len("urn:li:dashboard:(hex,") : -1]
                    project_id = (
                        inner[len(prefix) :] if inner.startswith(prefix) else inner
                    )
                    if project_id not in self.project_registry:
                        # Minimal stub — only id is needed for run-history PATCH
                        self.project_registry[project_id] = Project(
                            id=project_id, title="", description=None
                        )
                        self._light_project_ids.add(project_id)
                logger.info(
                    "Seeded %d additional light projects from checkpoint state "
                    "(skipped by early-terminated Hex listing).",
                    sum(
                        1
                        for pid in self._light_project_ids
                        if self.project_registry.get(pid, Project("", "", None)).title
                        == ""
                    ),
                )

        if last_ingested_at_ms:
            light = len(self._light_project_ids)
            full = len(self.project_registry) - light
            logger.info(
                "Incremental: %d projects need full re-processing, %d unchanged (skipping cells/lineage/context).",
                full,
                light,
            )

    def _enrich_lineage(self, connections_by_id: Dict[str, Any]) -> None:
        # {connection_id: connection_type} — connection_platform_map overrides take precedence
        conn_types: Dict[str, str] = {
            cid: ctype for cid, (_, ctype) in connections_by_id.items()
        }
        # Explicit overrides: user-supplied connection_id → platform mappings
        conn_types.update(self.source_config.connection_platform_map)

        lineage_builder = HexLineageBuilder(
            connections=conn_types,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            report=self.report,
            graph=self.ctx.graph,
        )

        # Probe queriedTables on the first project to detect ENTERPRISE availability
        queried_tables_available: Optional[bool] = None

        for project in self.project_registry.values():
            if project.id in self._light_project_ids:
                continue  # unchanged since last run — lineage hasn't changed

            lineage_builder.set_project_id(project.id)
            upstream_urns: List[str] = []

            # Detect whether this project imports any components.
            # The cells API includes COMPONENT_IMPORT cells (without component IDs).
            # If any exist, call the export API to learn which component IDs are used
            # and to obtain only the project's native SQL (not inlined component SQL).
            raw_cells = self.hex_api.fetch_cells(project.id)
            self._cells_cache[project.id] = raw_cells  # reused by context doc stage
            has_component_imports = any(
                c.get("cellType") == "COMPONENT_IMPORT" for c in raw_cells
            )
            native_sql_cells: List[dict] = []
            if has_component_imports:
                native_sql_cells, comp_ids = self.hex_api.fetch_project_export(
                    project.id
                )
                # Only link components that are in the registry (respect title/category filters)
                project.used_component_ids = [
                    cid for cid in comp_ids if cid in self.component_registry
                ]

            # Tier 1: queriedTables (ENTERPRISE) — runtime-proven dataset-level lineage
            if queried_tables_available is not False:
                queried = self.hex_api.fetch_queried_tables(project.id)
                if queried is not None:
                    queried_tables_available = True
                    upstream_urns = lineage_builder.build_from_queried_tables(queried)
                    # Extract column-level lineage from already-cached SQL cells,
                    # cross-validated against queriedTables so only columns whose
                    # parent dataset is runtime-confirmed are emitted. Mismatches
                    # (e.g. unqualified names, view aliases) are recorded in the report.
                    if upstream_urns and self.ctx.graph:
                        sql_cells_for_cll = _extract_sql_cells(raw_cells)
                        if sql_cells_for_cll:
                            project.input_fields = (
                                lineage_builder.build_validated_column_lineage(
                                    sql_cells_for_cll, upstream_urns
                                )
                            )
                elif queried_tables_available is None:
                    queried_tables_available = False
                    logger.info(
                        "queriedTables returned 403 — workspace is not ENTERPRISE tier; "
                        "using cell-based SQL parsing for all projects"
                    )

            # Tier 2: SQL parsing (fallback)
            if not upstream_urns and queried_tables_available is not True:
                if has_component_imports:
                    # Use only native SQL from the export YAML — component SQL belongs
                    # at the component level, not duplicated on the project
                    sql_cells = _extract_sql_cells(native_sql_cells)
                else:
                    sql_cells = _extract_sql_cells(raw_cells)

                if sql_cells:
                    upstream_urns, input_fields = lineage_builder.build_upstream_urns(
                        sql_cells
                    )
                    project.input_fields = input_fields
                elif not has_component_imports:
                    self.report.projects_without_sql_cells += 1

            if upstream_urns:
                project.upstream_datasets = upstream_urns
                self.report.projects_with_lineage += 1

        # Enrich component lineage exactly once per component (component cells → ChartInfo.inputs)
        enriched_component_ids: set = set()
        for project in self.project_registry.values():
            for comp_id in project.used_component_ids:
                if comp_id in enriched_component_ids:
                    continue
                enriched_component_ids.add(comp_id)
                component = self.component_registry.get(comp_id)
                if component is None:
                    continue
                lineage_builder.set_project_id(comp_id)
                comp_cells = self.hex_api.fetch_cells(comp_id)
                self._cells_cache[comp_id] = comp_cells  # reused by context doc stage
                comp_sql_cells = _extract_sql_cells(comp_cells)
                if comp_sql_cells:
                    comp_urns, comp_fields = lineage_builder.build_upstream_urns(
                        comp_sql_cells
                    )
                    component.upstream_datasets = comp_urns
                    component.input_fields = comp_fields

    def _enrich_run_history(self) -> None:
        for project in self.project_registry.values():
            run = self.hex_api.fetch_latest_run(project.id)
            if run:
                project.latest_run = run

    def _new_completed_run_ms(self, project: Project) -> Optional[int]:
        """Return the run start timestamp (ms) if the project has a COMPLETED run
        that happened AFTER the last checkpoint, otherwise None.

        Runs older than the checkpoint were already emitted in the previous
        ingestion — re-emitting would be stale. On the first run (no checkpoint)
        last_ingested_at_ms is None so all COMPLETED runs are considered new.
        """
        if not project.latest_run or project.latest_run.status != "COMPLETED":
            return None
        run_ms = make_ts_millis(project.latest_run.start_time)
        return run_ms if run_ms > (self._last_ingested_at_ms or 0) else None

    def _emit_context_documents(
        self, connections_by_id: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        doc_builder = HexDocumentBuilder(
            workspace_name=self.source_config.workspace_name,
            platform_instance=self.source_config.platform_instance,
            connections=connections_by_id,
        )

        for project in self.project_registry.values():
            if project.id in self._light_project_ids:
                continue  # cells unchanged — skip re-fetching and re-emitting the document

            # Reuse cells fetched during lineage enrichment if available
            raw_cells = self._cells_cache.get(project.id) or self.hex_api.fetch_cells(
                project.id
            )
            sql_cells, explore_cells, section_names, markdown = _parse_cells(raw_cells)
            dashboard_urn = self.mapper._get_dashboard_urn(project.id).urn()
            yield from doc_builder.build_document(
                project=project,
                sql_cells=sql_cells,
                explore_cells=explore_cells,
                section_names=section_names,
                markdown_content=markdown,
                dashboard_urn=dashboard_urn,
            )

        for component in self.component_registry.values():
            raw_cells = self._cells_cache.get(component.id) or self.hex_api.fetch_cells(
                component.id
            )
            sql_cells, explore_cells, section_names, markdown = _parse_cells(raw_cells)
            # Components are Chart entities — related_asset must be the Chart URN
            chart_urn = self.mapper._get_chart_urn(component.id).urn()
            yield from doc_builder.build_document(
                project=component,
                sql_cells=sql_cells,
                explore_cells=explore_cells,
                section_names=section_names,
                markdown_content=markdown,
                dashboard_urn=chart_urn,
            )


# ------------------------------------------------------------------
# Cell-parsing helpers
# ------------------------------------------------------------------


def _extract_sql_cells(raw_cells: List[dict]) -> List[SqlCell]:
    result = []
    for cell in raw_cells:
        if cell.get("cellType") != "SQL":
            continue
        contents = cell.get("contents") or {}
        sql_data = contents.get("sqlCell") or {}
        source = sql_data.get("source") or ""
        conn_id = cell.get("dataConnectionId")
        if source and conn_id:
            result.append(
                SqlCell(
                    cell_id=cell.get("staticId") or cell.get("id", ""),
                    cell_label=cell.get("label"),
                    sql_source=source,
                    data_connection_id=conn_id,
                )
            )
    return result


def _parse_cells(
    raw_cells: List[dict],
) -> "tuple[List[SqlCell], List[ExploreCell], List[str], str]":
    sql_cells, explore_cells, markdown_parts, section_names = [], [], [], []
    for cell in raw_cells:
        ct = cell.get("cellType", "")
        contents = cell.get("contents") or {}
        label = cell.get("label") or ""

        if ct == "SQL":
            sql_data = contents.get("sqlCell") or {}
            source = sql_data.get("source") or ""
            conn_id = cell.get("dataConnectionId")
            if source and conn_id:
                sql_cells.append(
                    SqlCell(
                        cell_id=cell.get("staticId") or cell.get("id", ""),
                        cell_label=label or None,
                        sql_source=source,
                        data_connection_id=conn_id,
                    )
                )
        elif ct == "EXPLORE":
            explore_cells.append(
                ExploreCell(
                    cell_id=cell.get("staticId") or cell.get("id", ""),
                    cell_label=label or None,
                    dataframe=None,
                    chart_type=None,
                )
            )
        elif ct == "MARKDOWN":
            md_data = contents.get("markdownCell") or {}
            source = md_data.get("source") or ""
            if source.strip():
                markdown_parts.append(source)
        elif ct == "COLLAPSIBLE" and label:
            section_names.append(label)

    return (
        sql_cells,
        explore_cells,
        section_names,
        "\n\n".join(p for p in markdown_parts if p.strip()),
    )
