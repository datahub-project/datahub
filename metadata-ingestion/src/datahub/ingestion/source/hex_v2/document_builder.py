"""
Generates DataHub Document entities for Hex projects.

Each project gets one context document (show_in_global_context=False) that
packages everything the YAML export knows into structured markdown:
SQL cell sources, EXPLORE visualisation metadata, MARKDOWN documentation
from the notebook, and section structure.

The document is linked to the project's Dashboard URN via related_assets,
making it invisible in global search but immediately retrievable by an AI
agent that has the Dashboard in scope.
"""

import logging
from typing import Dict, Iterable, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hex_v2.model import DataConnection, Project
from datahub.ingestion.source.hex_v2.yaml_parser import ParsedProjectYaml
from datahub.sdk import Document

logger = logging.getLogger(__name__)


class HexDocumentBuilder:
    """
    Builds one DataHub Document per Hex project from the parsed YAML export.

    Documents are hidden from global context (AI-only) and linked to the
    project's Dashboard URN as a related asset.
    """

    def __init__(
        self,
        workspace_name: str,
        platform_instance: Optional[str],
        connections: Dict[str, DataConnection],
    ):
        self._workspace = workspace_name
        self._platform_instance = platform_instance
        self._connections = connections

    def build_document(
        self,
        project: Project,
        parsed_yaml: ParsedProjectYaml,
        dashboard_urn: str,
    ) -> Iterable[MetadataWorkUnit]:
        doc_id = f"hex-{project.id}"
        title = f"Hex Project: {project.title}"
        text = self._render_markdown(project, parsed_yaml)

        doc = Document.create_document(
            id=doc_id,
            title=title,
            text=text,
            # Hidden from global search — only accessible to AI agents via the
            # related Dashboard asset.
            show_in_global_context=False,
            related_assets=[dashboard_urn],
            custom_properties={
                "hex_project_id": project.id,
                "hex_workspace": self._workspace,
                "sql_cell_count": str(len(parsed_yaml.sql_cells)),
                "explore_cell_count": str(len(parsed_yaml.explore_cells)),
            },
            created_time=project.created_at,
            last_modified_time=project.last_edited_at,
        )

        yield from doc.as_workunits()

    # ------------------------------------------------------------------
    # Markdown rendering
    # ------------------------------------------------------------------

    def _render_markdown(self, project: Project, yaml: ParsedProjectYaml) -> str:
        parts = []

        # ── Header ──────────────────────────────────────────────────
        parts.append(self._render_header(project))

        # ── Notebook structure ───────────────────────────────────────
        if yaml.section_names:
            parts.append("## Notebook Structure\n")
            parts.append("\n".join(f"- {s}" for s in yaml.section_names))

        # ── SQL queries ──────────────────────────────────────────────
        if yaml.sql_cells:
            parts.append("## SQL Queries\n")
            for cell in yaml.sql_cells:
                conn = self._connections.get(cell.data_connection_id or "")
                conn_label = (
                    f"{conn.name} ({conn.connection_type})"
                    if conn
                    else cell.data_connection_id or "unknown"
                )
                label = cell.cell_label or cell.cell_id
                sql = _indent_sql(cell.sql_source)
                parts.append(
                    f"### {label}\n\n**Connection:** {conn_label}\n\n```sql\n{sql}\n```"
                )

        # ── Visualisations ───────────────────────────────────────────
        if yaml.explore_cells:
            parts.append("## Visualisations\n")
            for cell in yaml.explore_cells:
                label = cell.cell_label or cell.cell_id
                df = f"`{cell.dataframe}`" if cell.dataframe else "_unknown dataframe_"
                chart = f" ({cell.chart_type})" if cell.chart_type else ""
                parts.append(f"- **{label}**{chart} — visualises {df}")

        # ── Notebook documentation ───────────────────────────────────
        if yaml.markdown_content.strip():
            parts.append("## Notebook Documentation\n")
            parts.append(yaml.markdown_content)

        return "\n\n".join(parts)

    def _render_header(self, project: Project) -> str:
        lines = [f"# {project.title}\n"]
        lines.append(f"**Workspace:** {self._workspace}  ")
        if project.owner:
            lines.append(f"**Owner:** {project.owner.email}  ")
        if project.creator and project.creator != project.owner:
            lines.append(f"**Creator:** {project.creator.email}  ")
        if project.status:
            lines.append(f"**Status:** {project.status.name}  ")
        if project.categories:
            names = ", ".join(c.name for c in project.categories)
            lines.append(f"**Categories:** {names}  ")
        if project.collections:
            names = ", ".join(c.name for c in project.collections)
            lines.append(f"**Collections:** {names}  ")
        if project.last_edited_at:
            lines.append(
                f"**Last edited:** {project.last_edited_at.strftime('%Y-%m-%d')}  "
            )
        if project.description and project.description.strip():
            lines.append(f"\n## Description\n\n{project.description.strip()}")
        return "\n".join(lines)


def _indent_sql(sql: str) -> str:
    """Normalize SQL indentation for markdown rendering."""
    lines = sql.rstrip().splitlines()
    # strip common leading whitespace
    if lines:
        indent = min(
            (len(ln) - len(ln.lstrip()) for ln in lines if ln.strip()),
            default=0,
        )
        lines = [ln[indent:] for ln in lines]
    return "\n".join(lines)
