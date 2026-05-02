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
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


class HexSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    workspace_name: str = Field(
        description="Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>",
    )
    token: SecretStr = Field(
        description="Hex API token; either PAT or Workflow token - https://learn.hex.tech/docs/api/api-overview#authentication",
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


class _HexIncrementalHandler:
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
    def test_connection(config_dict: dict) -> TestConnectionReport:
        import requests

        report = TestConnectionReport()
        report.capability_report = {}

        try:
            config = HexSourceConfig.parse_obj_allow_extras(config_dict)
            base = config.base_url.rstrip("/")
            headers = {"Authorization": f"Bearer {config.token.get_secret_value()}"}

            # Basic connectivity — GET /v1/users/me (lightweight, just validates token)
            resp = requests.get(f"{base}/users/me", headers=headers, timeout=15)
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
            proj_resp = requests.get(
                f"{base}/projects",
                headers=headers,
                params={"limit": 1},
                timeout=15,
            )
            conn_resp = requests.get(
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
                qt_resp = requests.get(
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
            cells_accessible = proj_resp.ok and conn_resp.ok
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
                        failure_reason="Could not access /v1/projects or /v1/data-connections.",
                        mitigation_message="Verify the token has at least standard user access.",
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

        with self.report.new_stage("Fetch Hex projects and components"):
            self._populate_registries(last_ingested_at_ms=last_ingested_at_ms)

        # Build connections map once — used by lineage and context documents
        connections_by_id: Dict[str, Any] = {}
        if (
            self.source_config.include_lineage
            or self.source_config.include_context_documents
        ):
            connections_by_id = self.hex_api.fetch_connections()
            # Apply manual overrides from config
            for conn_id, platform in self.source_config.connection_platform_map.items():
                if conn_id in connections_by_id:
                    name, _ = connections_by_id[conn_id]
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
                    if self._is_new_completed_run(project):
                        yield from self.mapper.map_project_last_refreshed(
                            project=project,
                            last_refreshed_ms=make_ts_millis(
                                project.latest_run.start_time  # type: ignore[union-attr]
                            ),
                        )
                    continue

                self.report.projects_full_refresh += 1
                yield from self.mapper.map_project(project=project)
                if project.upstream_datasets:
                    yield from self.mapper.map_project_lineage(
                        project=project,
                        upstream_urns=project.upstream_datasets,
                    )
                if self._is_new_completed_run(project):
                    yield from self.mapper.map_project_last_refreshed(
                        project=project,
                        last_refreshed_ms=make_ts_millis(
                            project.latest_run.start_time  # type: ignore[union-attr]
                        ),
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
        )

        # Probe queriedTables on the first project to detect ENTERPRISE availability
        queried_tables_available: Optional[bool] = None

        for project in self.project_registry.values():
            if project.id in self._light_project_ids:
                continue  # unchanged since last run — lineage hasn't changed

            lineage_builder.set_project_id(project.id)
            upstream_urns: List[str] = []

            # Tier 1: queriedTables (ENTERPRISE)
            if queried_tables_available is not False:
                queried = self.hex_api.fetch_queried_tables(project.id)
                if queried is not None:
                    queried_tables_available = True
                    upstream_urns = lineage_builder.build_from_queried_tables(queried)
                elif queried_tables_available is None:
                    queried_tables_available = False
                    logger.info(
                        "queriedTables returned 403 — workspace is not ENTERPRISE tier; "
                        "using cell-based SQL parsing for all projects"
                    )

            # Tier 2: cells + SQL parsing (fallback)
            if not upstream_urns and queried_tables_available is not True:
                raw_cells = self.hex_api.fetch_cells(project.id)
                sql_cells = _extract_sql_cells(raw_cells)
                if not sql_cells:
                    self.report.projects_without_sql_cells += 1
                    continue
                upstream_urns = lineage_builder.build_upstream_urns(sql_cells)

            if upstream_urns:
                project.upstream_datasets = upstream_urns  # type: ignore[assignment]
                self.report.projects_with_lineage += 1

    def _enrich_run_history(self) -> None:
        for project in self.project_registry.values():
            run = self.hex_api.fetch_latest_run(project.id)
            if run:
                project.latest_run = run  # type: ignore[assignment]

    def _is_new_completed_run(self, project: Project) -> bool:
        """Return True only if this project has a COMPLETED run that happened
        AFTER the last checkpoint — i.e. a genuinely new execution since the
        previous ingestion. Runs older than the checkpoint were already emitted
        and re-emitting them would be stale."""
        if not project.latest_run or project.latest_run.status != "COMPLETED":
            return False
        run_ms = make_ts_millis(project.latest_run.start_time)
        return run_ms > (self._last_ingested_at_ms or 0)

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

            raw_cells = self.hex_api.fetch_cells(project.id)
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
