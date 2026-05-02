import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, SecretStr
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
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
from datahub.ingestion.source.hex_v2.cli_client import HexCliClient, HexCliReport
from datahub.ingestion.source.hex_v2.constants import HEX_PLATFORM_NAME
from datahub.ingestion.source.hex_v2.document_builder import HexDocumentBuilder
from datahub.ingestion.source.hex_v2.lineage_builder import (
    HexLineageBuilder,
    LineageBuilderReport,
)
from datahub.ingestion.source.hex_v2.mapper import HexV2Mapper
from datahub.ingestion.source.hex_v2.model import (
    Collection,
    Component,
    Project,
    SqlCell,
)
from datahub.ingestion.source.hex_v2.yaml_parser import (
    ExploreCell,
    HexYamlParser,
    ParsedProjectYaml,
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


class HexV2SourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    workspace_name: str = Field(
        description="Hex workspace name (or workspace ID UUID). "
        "Visible in your Hex URL: https://app.hex.tech/<workspace_name>",
    )
    token: SecretStr = Field(
        description="Hex API token (PAT or workspace token). "
        "See https://learn.hex.tech/docs/api/api-overview#authentication",
    )
    hex_cli_path: str = Field(
        default="hex",
        description="Path to the Hex CLI binary. Defaults to 'hex' (resolved from PATH). "
        "If not found and auto_install_hex_cli is true, the pinned version is downloaded automatically.",
    )
    auto_install_hex_cli: bool = Field(
        default=True,
        description="Automatically download the pinned Hex CLI release if the binary is not "
        "found on PATH. The binary is cached in ~/.datahub/tools/hex/. "
        "Set to false if you manage the CLI installation yourself.",
    )
    cli_timeout_seconds: int = Field(
        default=120,
        description="Timeout in seconds for each Hex CLI subprocess call.",
    )
    page_size: int = Field(
        default=25,
        description="Number of projects to fetch per CLI call (max 30).",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )
    include_components: bool = Field(
        default=True,
        description="Include Hex Components in the ingestion.",
    )
    include_archived: bool = Field(
        default=False,
        description="Include archived projects.",
    )
    patch_metadata: bool = Field(
        default=False,
        description="Emit metadata as patch events.",
    )
    collections_as_tags: bool = Field(
        default=True,
        description="Emit Hex Collections as tags.",
    )
    status_as_tag: bool = Field(
        default=True,
        description="Emit Hex Status as tags.",
    )
    categories_as_tags: bool = Field(
        default=True,
        description="Emit Hex Categories as tags.",
    )
    set_ownership_from_email: bool = Field(
        default=True,
        description="Set ownership identity from owner/creator email.",
    )
    project_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for project titles to include/exclude.",
    )
    component_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for component titles to include/exclude.",
    )
    category_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for categories. Projects with any denied (or no allowed) "
        "category are excluded. Projects with no categories are always included.",
    )
    include_lineage: bool = Field(
        default=True,
        description="Extract upstream lineage by parsing SQL from project YAML exports.",
    )
    include_run_history: bool = Field(
        default=True,
        description="Emit the most recent run as an Operation aspect.",
    )
    connection_platform_map: Dict[str, str] = Field(
        default_factory=dict,
        description="Override connection_type resolution: map connection ID to DataHub platform name. "
        'Example: {"<connection_uuid>": "snowflake"}',
    )
    sql_parsing_platform_default: str = Field(
        default="snowflake",
        description="Fallback SQL dialect when a cell's connection type cannot be resolved.",
    )
    base_url: str = Field(
        default="https://app.hex.tech",
        description="Base URL for constructing external Hex links.",
    )
    include_context_documents: bool = Field(
        default=True,
        description="Emit a DataHub Document per project containing SQL sources, "
        "visualisation metadata, and notebook documentation. Documents are hidden from "
        "global search (show_in_global_context=False) and linked to the project Dashboard "
        "as related assets — designed for AI agent context retrieval.",
    )


@dataclass
class HexV2Report(
    StaleEntityRemovalSourceReport,
    HexCliReport,
    LineageBuilderReport,
):
    projects_with_lineage: int = 0
    projects_without_sql_cells: int = 0
    projects_export_skipped: int = 0


@platform_name("Hex")
@config_class(HexV2SourceConfig)
@support_status(SupportStatus.INCUBATING)
@capability(SourceCapability.DESCRIPTIONS, "Supported by default")
@capability(SourceCapability.OWNERSHIP, "Supported by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
@capability(
    SourceCapability.USAGE_STATS,
    "Supported by default",
    subtype_modifier=[SourceCapabilityModifier.HEX_PROJECT],
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Enabled by default via project YAML export and SQL parsing. "
    "No warehouse ingestion dependency required.",
)
@capability(
    SourceCapability.TAGS, "Status, categories, and collections emitted as tags"
)
class HexV2Source(StatefulIngestionSourceBase):
    def __init__(self, config: HexV2SourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report: HexV2Report = HexV2Report()
        self.platform = HEX_PLATFORM_NAME

        self.cli_client = HexCliClient(
            token=config.token.get_secret_value(),
            workspace_name=config.workspace_name,
            report=self.report,
            hex_cli_path=config.hex_cli_path,
            timeout_seconds=config.cli_timeout_seconds,
            page_size=config.page_size,
            auto_install=config.auto_install_hex_cli,
        )
        self.mapper = HexV2Mapper(
            workspace_name=config.workspace_name,
            base_url=config.base_url,
            platform_instance=config.platform_instance,
            env=config.env,
            patch_metadata=config.patch_metadata,
            collections_as_tags=config.collections_as_tags,
            status_as_tag=config.status_as_tag,
            categories_as_tags=config.categories_as_tags,
            set_ownership_from_email=config.set_ownership_from_email,
        )
        self.yaml_parser = HexYamlParser()

        self.project_registry: Dict[str, Project] = {}
        self.component_registry: Dict[str, Component] = {}
        # Cached ParsedProjectYaml results keyed by project_id — populated during
        # lineage extraction and reused for document generation, avoiding a double export.
        self._parsed_yamls: Dict[str, ParsedProjectYaml] = {}

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HexV2Source":
        config = HexV2SourceConfig.model_validate(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> HexV2Report:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage("List and enrich projects from Hex CLI"):
            self._populate_registries()

        if self.source_config.include_lineage:
            with self.report.new_stage("Extract SQL lineage via YAML export"):
                self._enrich_lineage()
        elif self.source_config.include_context_documents:
            # Still need YAML for documents even if lineage is disabled
            with self.report.new_stage("Export project YAML for context documents"):
                self._export_yamls_for_documents()

        if self.source_config.include_run_history:
            with self.report.new_stage("Fetch run history"):
                self._enrich_run_history()

        with self.report.new_stage("Emit"):
            yield from self.mapper.map_workspace()
            for project in self.project_registry.values():
                yield from self.mapper.map_project(project=project)
            for component in self.component_registry.values():
                yield from self.mapper.map_component(component=component)

        if self.source_config.include_context_documents:
            with self.report.new_stage("Emit context documents"):
                yield from self._emit_context_documents()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _populate_registries(self) -> None:
        """
        Two-pass population:
        1. list_projects() → minimal items (id, title)
        2. get_project(id) → rich metadata per item
        """
        cfg = self.source_config
        for item in self.cli_client.list_projects(
            include_components=cfg.include_components,
            include_archived=cfg.include_archived,
        ):
            # Category filter — preserves v1 semantics:
            # projects with NO categories are always included.
            if item.categories and (
                any(cfg.category_pattern.denied(c.name) for c in item.categories)
                or not any(
                    cfg.category_pattern.allowed(c.name) for c in item.categories
                )
            ):
                continue

            if isinstance(item, Project):
                if not cfg.project_title_pattern.allowed(item.title):
                    continue
            elif isinstance(item, Component):
                if not (
                    cfg.include_components
                    and cfg.component_title_pattern.allowed(item.title)
                ):
                    continue
            else:
                assert_never(item)

            # Enrich with full metadata from `project get`
            rich = self.cli_client.get_project(item.id)
            if rich is None:
                rich = item  # fall back to list-level data

            if isinstance(rich, Project):
                self.project_registry[rich.id] = rich
            elif isinstance(rich, Component):
                self.component_registry[rich.id] = rich
            else:
                assert_never(rich)

        # Enrich collections via REST (CLI --json omits sharing data)
        all_ids = list(self.project_registry.keys()) + list(
            self.component_registry.keys()
        )
        if all_ids:
            collections_map = self.cli_client.fetch_collections_for_projects(all_ids)
            for pid, coll_names in collections_map.items():
                colls = [Collection(name=n) for n in coll_names]
                if pid in self.project_registry:
                    self.project_registry[pid].collections = colls
                elif pid in self.component_registry:
                    self.component_registry[pid].collections = colls

    def _enrich_lineage(self) -> None:
        """
        Build upstream lineage for each project using a tiered approach:

        Tier 1 — queriedTables API (ENTERPRISE workspaces):
            Hex's own pre-resolved table list. No SQL parsing. Most accurate.
            Returns None (403) for non-ENTERPRISE workspaces → fall through to Tier 2.

        Tier 2 — Cells API + sqlglot SQL parsing (all workspaces):
            Fetches SQL cells via REST, parses with sqlglot per cell.
            Best-effort; may miss complex CTEs or unsupported dialects.
        """
        connections = self.cli_client.list_connections()

        # Override connection_type mapping from config
        for conn_id, platform in self.source_config.connection_platform_map.items():
            if conn_id in connections:
                connections[conn_id].connection_type = platform

        lineage_builder = HexLineageBuilder(
            connections=connections,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            sql_parsing_platform_default=self.source_config.sql_parsing_platform_default,
            report=self.report,
        )

        # Probe queriedTables on the first project to detect ENTERPRISE availability.
        # If it returns None (403), skip the probe for all subsequent projects.
        queried_tables_available: Optional[bool] = None

        for project in self.project_registry.values():
            upstream_urns: List[str] = []

            # --- Tier 1: queriedTables (ENTERPRISE) ---
            if queried_tables_available is not False:
                queried = self.cli_client.fetch_queried_tables(project.id)
                if queried is not None:
                    queried_tables_available = True
                    upstream_urns = lineage_builder.build_from_queried_tables(queried)
                elif queried_tables_available is None:
                    # First 403 — note it and fall through to Tier 2 for all projects
                    queried_tables_available = False
                    logger.info(
                        "queriedTables returned 403 — workspace is not ENTERPRISE tier; "
                        "using cell-based SQL parsing for all projects"
                    )

            # --- Tier 2: Cells API + SQL parsing (fallback) ---
            if not upstream_urns and queried_tables_available is not True:
                raw_cells = self.cli_client.fetch_cells(project.id)
                sql_cells = _extract_sql_cells(raw_cells)

                # Cache parsed cell data for context document generation
                self._parsed_yamls[project.id] = _cells_to_parsed_yaml(
                    project_id=project.id,
                    title=project.title,
                    raw_cells=raw_cells,
                )

                if not sql_cells:
                    self.report.projects_without_sql_cells += 1
                    continue

                upstream_urns = lineage_builder.build_upstream_urns(sql_cells)
            if upstream_urns:
                project.upstream_datasets = upstream_urns
                self.report.projects_with_lineage += 1

    def _export_yamls_for_documents(self) -> None:
        """Fetch and cache cell data for projects not yet populated during lineage."""
        for project in self.project_registry.values():
            if project.id in self._parsed_yamls:
                continue
            raw_cells = self.cli_client.fetch_cells(project.id)
            if raw_cells:
                self._parsed_yamls[project.id] = _cells_to_parsed_yaml(
                    project_id=project.id,
                    title=project.title,
                    raw_cells=raw_cells,
                )

    def _emit_context_documents(self) -> Iterable[MetadataWorkUnit]:
        connections = self.cli_client.list_connections()
        doc_builder = HexDocumentBuilder(
            workspace_name=self.source_config.workspace_name,
            platform_instance=self.source_config.platform_instance,
            connections=connections,
        )
        for project in self.project_registry.values():
            parsed = self._parsed_yamls.get(project.id)
            if parsed is None:
                logger.debug(
                    "Skipping context document for %s — no parsed YAML available",
                    project.id,
                )
                continue

            dashboard_urn = self.mapper._dashboard_urn(project.id).urn()
            yield from doc_builder.build_document(
                project=project,
                parsed_yaml=parsed,
                dashboard_urn=dashboard_urn,
            )

    def _enrich_run_history(self) -> None:
        for project in self.project_registry.values():
            run = self.cli_client.get_latest_run(project.id)
            if run:
                project.latest_run = run


# ------------------------------------------------------------------
# Helpers for converting raw Cells API responses to ParsedProjectYaml
# ------------------------------------------------------------------


def _extract_sql_cells(raw_cells: List[dict]) -> List[SqlCell]:
    """Extract SqlCell objects from raw /v1/cells API responses."""
    result = []
    for cell in raw_cells:
        if cell.get("cellType") != "SQL":
            continue
        contents = cell.get("contents") or {}
        sql_cell_data = contents.get("sqlCell") or {}
        source = sql_cell_data.get("source") or ""
        connection_id = cell.get("dataConnectionId")
        if source and connection_id:
            result.append(
                SqlCell(
                    cell_id=cell.get("staticId") or cell.get("id", ""),
                    cell_label=cell.get("label"),
                    sql_source=source,
                    data_connection_id=connection_id,
                )
            )
    return result


def _cells_to_parsed_yaml(
    project_id: str,
    title: str,
    raw_cells: List[dict],
) -> "ParsedProjectYaml":
    """
    Build a ParsedProjectYaml from raw /v1/cells API responses.

    Replaces YAML export parsing for lineage and context documents.
    EXPLORE chart spec is unavailable via REST (contents are null),
    but label and cell type are preserved.
    """
    sql_cells = []
    explore_cells = []
    markdown_parts = []
    section_names = []

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
                    # dataframe reference not exposed by REST API
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

    return ParsedProjectYaml(
        project_id=project_id,
        title=title,
        sql_cells=sql_cells,
        explore_cells=explore_cells,
        section_names=section_names,
        markdown_content="\n\n".join(p for p in markdown_parts if p.strip()),
    )
