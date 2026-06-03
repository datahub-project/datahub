"""
Generates DataHub Document entities for Hex projects and components from the /v1/cells REST API.

Each project/component gets one context document (show_in_global_context=False) linked to
the Dashboard as a related asset — invisible in catalog search, accessible
to AI agents that have the project in scope.
"""

import logging
from typing import Dict, Iterable, List, Union

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hex.model import (
    Component,
    ExploreCell,
    HexConnection,
    Project,
    SqlCell,
)
from datahub.sdk import Document

logger = logging.getLogger(__name__)


class HexDocumentBuilder:
    def __init__(
        self,
        workspace_name: str,
        # {connection_id → HexConnection} for rendering
        # "**Connection:** <name> (<platform>)" alongside each SQL cell.
        connections: Dict[str, HexConnection],
    ):
        self._workspace = workspace_name
        self._connections = connections

    def build_document(
        self,
        project: Union[Project, Component],
        sql_cells: List[SqlCell],
        explore_cells: List[ExploreCell],
        section_names: List[str],
        markdown_content: str,
        dashboard_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        entity_type = "Component" if isinstance(project, Component) else "Project"
        doc = Document.create_document(
            id=f"hex-{project.id}",
            title=f"Hex {entity_type}: {project.title}",
            text=self._render_markdown(
                project, sql_cells, explore_cells, section_names, markdown_content
            ),
            show_in_global_context=False,
            related_assets=[dashboard_urn],
            custom_properties={
                "hex_project_id": project.id,
                "hex_workspace": self._workspace,
                "sql_cell_count": str(len(sql_cells)),
                "explore_cell_count": str(len(explore_cells)),
            },
            created_time=project.created_at,
            last_modified_time=project.last_edited_at,
        )
        yield from doc.as_workunits()

    def _render_markdown(
        self,
        project: Union[Project, Component],
        sql_cells: List[SqlCell],
        explore_cells: List[ExploreCell],
        section_names: List[str],
        markdown_content: str,
    ) -> str:
        parts = [self._render_header(project)]

        if section_names:
            parts.append("## Notebook Structure\n")
            parts.append("\n".join(f"- {s}" for s in section_names))

        if sql_cells:
            parts.append("## SQL Queries\n")
            for cell in sql_cells:
                conn = self._connections.get(cell.data_connection_id or "")
                name = conn.name if conn else "unknown"
                platform = conn.platform if conn and conn.platform else "unknown"
                label = cell.cell_label or cell.cell_id
                sql = _indent_sql(cell.sql_source)
                parts.append(
                    f"### {label}\n\n**Connection:** {name} ({platform})\n\n```sql\n{sql}\n```"
                )

        if explore_cells:
            parts.append("## Visualisations\n")
            for explore_cell in explore_cells:
                label = explore_cell.cell_label or explore_cell.cell_id
                df = f"`{explore_cell.dataframe}`" if explore_cell.dataframe else ""
                chart = (
                    f" ({explore_cell.chart_type})" if explore_cell.chart_type else ""
                )
                suffix = f" — visualises {df}" if df else ""
                parts.append(f"- **{label}**{chart}{suffix}")

        if markdown_content.strip():
            parts.append("## Notebook Documentation\n")
            parts.append(markdown_content)

        return "\n\n".join(parts)

    def _render_header(self, project: Union[Project, Component]) -> str:
        lines = [f"# {project.title}\n"]
        lines.append(f"**Workspace:** {self._workspace}  ")
        if project.owner:
            lines.append(f"**Owner:** {project.owner.email}  ")
        if project.creator and project.creator != project.owner:
            lines.append(f"**Creator:** {project.creator.email}  ")
        if project.status:
            lines.append(f"**Status:** {project.status.name}  ")
        if project.categories:
            lines.append(
                f"**Categories:** {', '.join(c.name for c in project.categories)}  "
            )
        if project.collections:
            lines.append(
                f"**Collections:** {', '.join(c.name for c in project.collections)}  "
            )
        if project.last_edited_at:
            lines.append(
                f"**Last edited:** {project.last_edited_at.strftime('%Y-%m-%d')}  "
            )
        if project.description and project.description.strip():
            lines.append(f"\n## Description\n\n{project.description.strip()}")
        return "\n".join(lines)


def _indent_sql(sql: str) -> str:
    lines = sql.rstrip().splitlines()
    if lines:
        indent = min(
            (len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()),
            default=0,
        )
        lines = [ln[indent:] for ln in lines]
    return "\n".join(lines)
