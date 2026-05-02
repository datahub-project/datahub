import logging
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, SecretStr, model_validator
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.datetimes import parse_user_datetime
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
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
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.hex.api import HexApi, HexApiReport
from datahub.ingestion.source.hex.constants import (
    DATAHUB_API_PAGE_SIZE_DEFAULT,
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
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
    Project,
    SqlCell,
)
from datahub.ingestion.source.hex.query_fetcher import (
    HexQueryFetcherReport,
)
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
    # --- Legacy DataHub-query-fetcher lineage fields (deprecated) ---
    # These were used by the old lineage path that searched DataHub for Query entities.
    # They are kept for backward compatibility but no longer have effect when
    # include_lineage=True (which now uses the REST API approach).
    lineage_start_time: Optional[datetime] = Field(
        default=None,
        description="Deprecated. No longer used; lineage now comes directly from the Hex API.",
    )
    lineage_end_time: Optional[datetime] = Field(
        default=None,
        description="Deprecated. No longer used; lineage now comes directly from the Hex API.",
    )
    datahub_page_size: int = Field(
        default=DATAHUB_API_PAGE_SIZE_DEFAULT,
        description="Deprecated. No longer used; lineage now comes directly from the Hex API.",
    )
    category_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for categories to filter in ingestion. This will exclude any project or component that has any category denied or not explicitly allowed.",
    )

    @model_validator(mode="before")
    @classmethod
    def validate_lineage_times(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # In-place update of the input dict would cause state contamination. This was discovered through test failures
        # in test_hex.py where the same dict is reused.
        # So a deepcopy is performed first.
        data = deepcopy(data)

        if "lineage_end_time" not in data or data["lineage_end_time"] is None:
            data["lineage_end_time"] = datetime.now(tz=timezone.utc)
        # if string is given, parse it
        if isinstance(data["lineage_end_time"], str):
            data["lineage_end_time"] = parse_user_datetime(data["lineage_end_time"])
        # if no timezone is given, assume UTC
        if data["lineage_end_time"].tzinfo is None:
            data["lineage_end_time"] = data["lineage_end_time"].replace(
                tzinfo=timezone.utc
            )
        # at this point, we ensure there is a non null datetime with UTC timezone for lineage_end_time
        assert (
            data["lineage_end_time"]
            and isinstance(data["lineage_end_time"], datetime)
            and data["lineage_end_time"].tzinfo is not None
            and data["lineage_end_time"].tzinfo == timezone.utc
        )

        # lineage_start_time default = lineage_end_time - 1 day
        if "lineage_start_time" not in data or data["lineage_start_time"] is None:
            data["lineage_start_time"] = data["lineage_end_time"] - timedelta(days=1)
        # if string is given, parse it
        if isinstance(data["lineage_start_time"], str):
            data["lineage_start_time"] = parse_user_datetime(data["lineage_start_time"])
        # if no timezone is given, assume UTC
        if data["lineage_start_time"].tzinfo is None:
            data["lineage_start_time"] = data["lineage_start_time"].replace(
                tzinfo=timezone.utc
            )
        # at this point, we ensure there is a non null datetime with UTC timezone for lineage_start_time
        assert (
            data["lineage_start_time"]
            and isinstance(data["lineage_start_time"], datetime)
            and data["lineage_start_time"].tzinfo is not None
            and data["lineage_start_time"].tzinfo == timezone.utc
        )

        return data


@dataclass
class HexReport(
    StaleEntityRemovalSourceReport,
    HexApiReport,
    HexQueryFetcherReport,
    LineageBuilderReport,
):
    projects_with_lineage: int = 0
    projects_without_sql_cells: int = 0


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
class HexSource(StatefulIngestionSourceBase):
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

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HexSource":
        config = HexSourceConfig.model_validate(config_dict)
        return cls(config, ctx)

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
        with self.report.new_stage("Fetch Hex projects and components"):
            self._populate_registries()

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
                yield from self.mapper.map_project(project=project)
                if project.upstream_datasets:
                    yield from self.mapper.map_project_lineage(
                        project=project,
                        upstream_urns=project.upstream_datasets,
                    )
                if project.latest_run:
                    yield from self.mapper.map_project_run(
                        project=project,
                        run=project.latest_run,
                    )

            for component in self.component_registry.values():
                yield from self.mapper.map_component(component=component)

        if self.source_config.include_context_documents:
            with self.report.new_stage("Emit context documents"):
                yield from self._emit_context_documents(connections_by_id)

    def _populate_registries(self) -> None:
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
            elif isinstance(item, Component):
                if (
                    self.source_config.include_components
                    and self.source_config.component_title_pattern.allowed(item.title)
                ):
                    self.component_registry[item.id] = item
            else:
                assert_never(item)

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

    def _emit_context_documents(
        self, connections_by_id: Dict[str, Any]
    ) -> Iterable[MetadataWorkUnit]:
        doc_builder = HexDocumentBuilder(
            workspace_name=self.source_config.workspace_name,
            platform_instance=self.source_config.platform_instance,
            connections=connections_by_id,
        )

        for project in self.project_registry.values():
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
