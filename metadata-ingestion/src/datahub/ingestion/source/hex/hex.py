import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Union

import sqlglot
import yaml
from typing_extensions import assert_never

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
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import SourceCapabilityModifier
from datahub.ingestion.source.hex.api import HexApi, HexApiConnection, HexApiReport
from datahub.ingestion.source.hex.config import HexConnectionDetail, HexSourceConfig
from datahub.ingestion.source.hex.constants import (
    CONNECTION_TYPE_TO_DATAHUB_PLATFORM,
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
    HexConnection,
    Project,
    SqlCell,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)

logger = logging.getLogger(__name__)


@dataclass
class HexReport(
    StaleEntityRemovalSourceReport,
    HexApiReport,
    LineageBuilderReport,
):
    projects_with_lineage: int = 0
    components_with_lineage: int = 0
    projects_without_sql_cells: int = 0
    components_without_sql_cells: int = 0


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
    "Enabled by default via queriedTables API (Hex Enterprise workspaces) or SQL "
    "parsing from cells (all Hex tiers). Applied to both projects and components. "
    "Unpublished entities always use SQL parsing. No warehouse ingestion dependency "
    "required.",
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
        # Resolved in get_workunits_internal
        self.workspace_id: Optional[str] = None
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
                content = yaml.safe_load(exp_r.json().get("content", "")) or {}
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
                        for stmt in sqlglot.parse(sql_src):
                            if stmt:
                                for tbl in stmt.find_all(sqlglot.exp.Table):
                                    name = str(tbl).split(" ")[0].strip("\"'`")
                                    if "." in name and len(name) < 120:
                                        result[conn_id].add(name)
        except Exception as e:
            logger.debug("Best-effort connection sampling failed: %s", e)
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
            base = config.base_url
            headers = {"Authorization": f"Bearer {config.token.get_secret_value()}"}

            # Basic connectivity — probe /v1/users/me, then fall back to /v1/projects.
            # Workspace tokens return 500 on /v1/users/me (it's a user-scoped endpoint);
            # in that case we use the project listing as the connectivity check instead.
            resp = api.session.get(f"{base}/users/me", headers=headers, timeout=15)
            if resp.status_code == 401:
                report.basic_connectivity = CapabilityReport(
                    capable=False,
                    failure_reason="Authentication failed — check that your token is valid and not expired.",
                )
                return report
            if resp.ok:
                report.basic_connectivity = CapabilityReport(capable=True)
            else:
                # 500 / other non-401 — workspace token; verify via projects listing
                probe = api.session.get(
                    f"{base}/projects",
                    headers=headers,
                    params={"limit": 1},
                    timeout=15,
                )
                if probe.ok:
                    report.basic_connectivity = CapabilityReport(capable=True)
                else:
                    report.basic_connectivity = CapabilityReport(
                        capable=False,
                        failure_reason=f"HTTP {probe.status_code} on /v1/projects — token is invalid or lacks workspace access.",
                    )
                    return report

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
                        "Lineage via queriedTables API (Hex Enterprise workspaces)"
                    ] = CapabilityReport(
                        capable=True,
                        mitigation_message="Hex returns the exact list of warehouse tables "
                        "this project queries — no SQL parsing required.",
                    )
                else:
                    report.capability_report[
                        "Lineage via queriedTables API (Hex Enterprise workspaces)"
                    ] = CapabilityReport(
                        capable=False,
                        failure_reason=f"HTTP {qt_resp.status_code} — workspace is not "
                        "on Hex's Enterprise tier or the token lacks access.",
                        mitigation_message="Upgrade the Hex workspace to Hex Enterprise "
                        "to unlock exact lineage. SQL-parsing lineage (Tier 2) is still "
                        "available on lower Hex tiers.",
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

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        # Prefer the explicit config value so users can avoid granting the
        # 'Users → Read access' scope just for external-URL building.
        self.workspace_id = (
            self.source_config.workspace_id or self.hex_api.fetch_workspace_id()
        )
        self.mapper.workspace_id = self.workspace_id

        # Resolve connection IDs to HexConnection records once. After this
        # point, no further type→platform translation happens anywhere
        # downstream — the lineage builder and document builder both read this
        # map directly.
        connections: Dict[str, HexConnection] = {}
        if (
            self.source_config.include_lineage
            or self.source_config.include_context_documents
        ):
            connections = self._resolve_connections(
                api_connections=self.hex_api.fetch_connections(),
                connection_overrides=self.source_config.connection_platform_map,
            )

        lineage_builder = HexLineageBuilder(
            connections=connections,
            env=self.source_config.env,
            report=self.report,
            graph=self.ctx.graph,
        )

        # Emit workspace container once before streaming projects
        yield from self.mapper.map_workspace()

        # Single streaming pass: process and emit each item as it arrives.
        projects_processed = 0
        with self.report.new_stage("Stream projects and components"):
            for item in self.hex_api.fetch_projects(
                include_components=self.source_config.include_components,
            ):
                if self._is_filtered(item):
                    continue

                if isinstance(item, Component):
                    if item.id not in self.component_registry:
                        yield from self._stream_component(
                            item, lineage_builder, connections
                        )
                elif isinstance(item, Project):
                    if self.source_config.project_title_pattern.allowed(item.title):
                        yield from self._stream_project(
                            item, lineage_builder, connections
                        )
                        projects_processed += 1
                        if (
                            self.source_config.max_projects is not None
                            and projects_processed >= self.source_config.max_projects
                        ):
                            logger.info(
                                "Reached max_projects limit (%d) — stopping.",
                                self.source_config.max_projects,
                            )
                            break
                else:
                    assert_never(item)

    def _resolve_connections(
        self,
        api_connections: Dict[str, HexApiConnection],
        connection_overrides: Dict[str, HexConnectionDetail],
    ) -> Dict[str, HexConnection]:
        """Merge API connections and user overrides into {conn_id → HexConnection}.

        Per field, the override wins if set; otherwise the API value is used.
        Hex types absent from CONNECTION_TYPE_TO_DATAHUB_PLATFORM and not
        rescued by an override are reported in a single warning.
        """
        connections: Dict[str, HexConnection] = {}
        unmapped_type_counts: Dict[str, int] = {}

        for conn_id in api_connections.keys() | connection_overrides.keys():
            api_conn = api_connections.get(conn_id)
            override = connection_overrides.get(conn_id)
            hex_type = api_conn.type if api_conn else ""

            # API-derived defaults; override (if any) is layered on top per field.
            name = api_conn.name if api_conn else ""
            platform = CONNECTION_TYPE_TO_DATAHUB_PLATFORM.get(hex_type.lower())
            default_database = api_conn.default_database if api_conn else None
            default_schema = api_conn.default_schema if api_conn else None
            platform_instance: Optional[str] = None

            if override is not None:
                platform = override.platform or platform
                platform_instance = override.platform_instance
                if override.default_database is not None:
                    default_database = override.default_database
                if override.default_schema is not None:
                    default_schema = override.default_schema

            if platform is None and hex_type:
                unmapped_type_counts[hex_type] = (
                    unmapped_type_counts.get(hex_type, 0) + 1
                )

            connections[conn_id] = HexConnection(
                name=name or conn_id,
                platform=platform,
                platform_instance=platform_instance,
                default_database=default_database,
                default_schema=default_schema,
            )

        if unmapped_type_counts:
            unmapped_summary = ", ".join(
                f"{t} (x{n})" for t, n in sorted(unmapped_type_counts.items())
            )
            self.report.warning(
                title="Unmapped Hex connection types",
                message=(
                    "Some Hex connection types are not recognised by the "
                    "canonical type-to-platform map. Lineage is skipped for "
                    "cells that reference connections of these types."
                ),
                context=f"Unmapped types: {unmapped_summary}",
            )

        return connections

    def _is_filtered(self, item: Union[Project, Component]) -> bool:
        """Return True if the item should be skipped due to category_pattern.

        Uncategorized items always pass (category_pattern is only consulted
        when the item has at least one category).
        """
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
            return True
        return False

    def _fetch_cells(self, entity_id: str) -> List[dict]:
        """Export-first cell fetch with cells-API fallback."""
        all_cells, _ = self.hex_api.fetch_project_export(entity_id)
        if not all_cells:
            all_cells = self.hex_api.fetch_cells(entity_id)
        return all_cells

    def _apply_cell_based_lineage(
        self,
        hex_item: Union[Project, Component],
        all_cells: List[dict],
        lineage_builder: HexLineageBuilder,
    ) -> List[SqlCell]:
        """Compute Tier 2 (cell-based SQL parsing) lineage.

        Mutates hex_item.upstream_datasets and hex_item.input_fields in place.
        Returns the SQL cells that were found so callers can update
        per-item counters.
        """
        sql_cells = _extract_sql_cells(all_cells)
        if not sql_cells:
            return []
        lineage_builder.set_project_id(hex_item.id)
        upstream_urns, input_fields = lineage_builder.build_upstream_urns(sql_cells)
        hex_item.upstream_datasets = upstream_urns
        hex_item.input_fields = input_fields
        return sql_cells

    def _emit_context_document(
        self,
        hex_item: Union[Project, Component],
        all_cells: List[dict],
        connections: Dict[str, HexConnection],
        parent_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Build and emit the context document MCPs for a Project or Component."""
        sql_cells, explore_cells, section_names, markdown = _parse_cells(all_cells)
        doc_builder = HexDocumentBuilder(
            workspace_name=self.source_config.workspace_name,
            connections=connections,
        )
        yield from doc_builder.build_document(
            project=hex_item,
            sql_cells=sql_cells,
            explore_cells=explore_cells,
            section_names=section_names,
            markdown_content=markdown,
            dashboard_urn=parent_urn,
        )

    def _build_lineage(
        self,
        hex_item: Union[Project, Component],
        all_cells: List[dict],
        lineage_builder: HexLineageBuilder,
    ) -> None:
        """Build lineage for a Project or Component, mutating upstream_datasets
        and input_fields in place.

        Tier 1 (queriedTables) is attempted when:
          - `use_queried_tables_lineage` is enabled
          - the item has been published (drafts skip Tier 1; queriedTables
            is only populated post-publish)
          - HexApi hasn't already cached a 403 from a prior call

        Tier 2 (SQL-cell parsing) runs as fallback or primary. ENTERPRISE
        workspaces with a graph client also extract validated column-level
        lineage from SQL cells using Tier 1's URNs as the truth set.
        """
        used_queried_tables = False
        if (
            self.source_config.use_queried_tables_lineage
            and hex_item.last_published_at is not None
        ):
            queried = self.hex_api.fetch_queried_tables(hex_item.id)
            if queried is not None:
                lineage_builder.set_project_id(hex_item.id)
                upstream_urns = lineage_builder.build_from_queried_tables(queried)
                hex_item.upstream_datasets = upstream_urns
                if upstream_urns and self.ctx.graph:
                    sql_cells = _extract_sql_cells(all_cells)
                    if sql_cells:
                        hex_item.input_fields = (
                            lineage_builder.build_validated_column_lineage(
                                sql_cells, upstream_urns
                            )
                        )
                used_queried_tables = True

        if not used_queried_tables:
            sql_cells = self._apply_cell_based_lineage(
                hex_item, all_cells, lineage_builder
            )
            if not sql_cells:
                if isinstance(hex_item, Project):
                    self.report.projects_without_sql_cells += 1
                else:
                    self.report.components_without_sql_cells += 1

        if hex_item.upstream_datasets:
            if isinstance(hex_item, Project):
                self.report.projects_with_lineage += 1
            else:
                self.report.components_with_lineage += 1

    def _stream_component(
        self,
        component: Component,
        lineage_builder: HexLineageBuilder,
        connections: Dict[str, HexConnection],
    ) -> Iterable[MetadataWorkUnit]:
        """Process and emit a component, adding it to component_registry."""
        if not self.source_config.component_title_pattern.allowed(component.title):
            return
        self.component_registry[component.id] = component

        comp_cells = self._fetch_cells(component.id)
        if self.source_config.include_lineage:
            self._build_lineage(component, comp_cells, lineage_builder)

        yield from self.mapper.map_component(component=component)

        if self.source_config.include_context_documents:
            yield from self._emit_context_document(
                hex_item=component,
                all_cells=comp_cells,
                connections=connections,
                parent_urn=self.mapper.get_chart_urn(component.id).urn(),
            )

    def _stream_project(
        self,
        project: Project,
        lineage_builder: HexLineageBuilder,
        connections: Dict[str, HexConnection],
    ) -> Iterable[MetadataWorkUnit]:
        """Process and emit a single project immediately."""
        self.project_registry[project.id] = project

        # Export: cells + component IDs
        all_cells, comp_ids = self.hex_api.fetch_project_export(project.id)
        if not all_cells:
            all_cells = self.hex_api.fetch_cells(project.id)
            comp_ids = []

        # Resolve component imports — fetch on-demand if not yet in registry
        for comp_id in comp_ids:
            if comp_id not in self.component_registry:
                fetched = self.hex_api.fetch_single_project(comp_id)
                if fetched and isinstance(fetched, Component):
                    if not self._is_filtered(fetched):
                        yield from self._stream_component(
                            fetched, lineage_builder, connections
                        )
        project.used_component_ids = [
            cid for cid in comp_ids if cid in self.component_registry
        ]

        # Lineage
        if self.source_config.include_lineage:
            self._build_lineage(project, all_cells, lineage_builder)

        # Run history — only published projects have runs queryable via the API
        if self.source_config.include_run_history and project.last_published_at:
            run = self.hex_api.fetch_latest_run(project.id)
            if run:
                project.latest_run = run

        # Emit project MCPs
        yield from self.mapper.map_project(project=project)
        if project.latest_run and project.latest_run.status == "COMPLETED":
            yield from self.mapper.map_project_last_refreshed(
                project=project,
                last_refreshed_ms=make_ts_millis(project.latest_run.start_time),
            )

        # Context document
        if self.source_config.include_context_documents:
            yield from self._emit_context_document(
                hex_item=project,
                all_cells=all_cells,
                connections=connections,
                parent_urn=self.mapper.get_dashboard_urn(project.id).urn(),
            )


def _parse_sql_cell(cell: dict) -> Optional[SqlCell]:
    if cell.get("cellType") != "SQL":
        return None
    contents = cell.get("contents") or {}
    source = (contents.get("sqlCell") or {}).get("source") or ""
    conn_id = cell.get("dataConnectionId")
    if not (source and conn_id):
        return None
    return SqlCell(
        cell_id=cell.get("staticId") or cell.get("id", ""),
        cell_label=cell.get("label") or None,
        sql_source=source,
        data_connection_id=conn_id,
    )


def _extract_sql_cells(raw_cells: List[dict]) -> List[SqlCell]:
    return [parsed for cell in raw_cells if (parsed := _parse_sql_cell(cell))]


def _parse_cells(
    raw_cells: List[dict],
) -> "tuple[List[SqlCell], List[ExploreCell], List[str], str]":
    sql_cells, explore_cells, markdown_parts, section_names = [], [], [], []
    for cell in raw_cells:
        ct = cell.get("cellType", "")
        contents = cell.get("contents") or {}
        label = cell.get("label") or ""

        if ct == "SQL":
            parsed_sql = _parse_sql_cell(cell)
            if parsed_sql:
                sql_cells.append(parsed_sql)
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
