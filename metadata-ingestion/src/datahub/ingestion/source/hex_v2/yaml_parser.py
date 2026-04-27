import logging
from dataclasses import dataclass, field
from typing import List, Optional

import yaml

from datahub.ingestion.source.hex_v2.model import SqlCell

logger = logging.getLogger(__name__)


@dataclass
class ExploreCell:
    """An EXPLORE (visualisation) cell from a project YAML export."""

    cell_id: str
    cell_label: Optional[str]
    # Name of the result-variable (dataframe) this chart visualises
    dataframe: Optional[str]
    # High-level chart type inferred from spec, e.g. "bar", "line", "scatter"
    chart_type: Optional[str]


@dataclass
class ParsedProjectYaml:
    project_id: str
    title: str
    # Connection IDs referenced at the workspace (shared assets) level
    shared_connection_ids: List[str] = field(default_factory=list)
    # SQL cells with an explicit external data connection
    sql_cells: List[SqlCell] = field(default_factory=list)
    # EXPLORE (visualisation) cells
    explore_cells: List[ExploreCell] = field(default_factory=list)
    # Labels from top-level COLLAPSIBLE section containers
    section_names: List[str] = field(default_factory=list)
    # Concatenated plain text from MARKDOWN cells (for AI context)
    markdown_content: str = ""


class HexYamlParser:
    """
    Parses a Hex project YAML export (schema version 3) and extracts SQL cells
    together with their data connection references.

    Only SQL cells that have an explicit `dataConnectionId` are included in
    `sql_cells` — cells with `dataConnectionId: null` operate on in-memory
    dataframes from other cells and do not represent external data reads.
    """

    def parse(self, yaml_content: str) -> Optional[ParsedProjectYaml]:
        try:
            doc = yaml.safe_load(yaml_content)
        except yaml.YAMLError as e:
            logger.warning("Failed to parse project YAML: %s", e)
            return None

        if not isinstance(doc, dict):
            logger.warning("Unexpected YAML root type: %s", type(doc))
            return None

        meta = doc.get("meta", {})
        project_id = meta.get("projectId", "")
        title = meta.get("title", "")

        shared_connection_ids = [
            c["dataConnectionId"]
            for c in (doc.get("sharedAssets", {}).get("dataConnections") or [])
            if isinstance(c, dict) and c.get("dataConnectionId")
        ]

        top_cells = doc.get("cells") or []
        sql_cells: List[SqlCell] = []
        explore_cells: List[ExploreCell] = []
        markdown_parts: List[str] = []
        section_names: List[str] = []

        self._walk_cells(
            top_cells,
            sql_cells=sql_cells,
            explore_cells=explore_cells,
            markdown_parts=markdown_parts,
            section_names=section_names,
            is_top_level=True,
        )

        return ParsedProjectYaml(
            project_id=project_id,
            title=title,
            shared_connection_ids=shared_connection_ids,
            sql_cells=sql_cells,
            explore_cells=explore_cells,
            section_names=section_names,
            markdown_content="\n\n".join(p for p in markdown_parts if p.strip()),
        )

    def _walk_cells(
        self,
        cells: list,
        *,
        sql_cells: List[SqlCell],
        explore_cells: List[ExploreCell],
        markdown_parts: List[str],
        section_names: List[str],
        is_top_level: bool = False,
    ) -> None:
        """Recursively walk cells, populating all output lists in-place."""
        for cell in cells:
            if not isinstance(cell, dict):
                continue

            cell_type = cell.get("cellType", "")
            cfg = cell.get("config") or {}
            cell_id = cell.get("cellId", "")
            cell_label = cell.get("cellLabel")

            if cell_type == "SQL":
                sql_source = cfg.get("source") or ""
                connection_id = cfg.get("dataConnectionId")
                if sql_source and connection_id:
                    sql_cells.append(
                        SqlCell(
                            cell_id=cell_id,
                            cell_label=cell_label,
                            sql_source=sql_source,
                            data_connection_id=connection_id,
                        )
                    )
                elif sql_source:
                    logger.debug(
                        "Skipping SQL cell %s — no dataConnectionId (in-memory transform)",
                        cell_id,
                    )

            elif cell_type == "EXPLORE":
                explore_cells.append(
                    ExploreCell(
                        cell_id=cell_id,
                        cell_label=cell_label,
                        dataframe=cfg.get("dataframe"),
                        chart_type=self._infer_chart_type(cfg.get("spec")),
                    )
                )

            elif cell_type == "MARKDOWN":
                source = cfg.get("source") or ""
                if source.strip():
                    markdown_parts.append(source)

            elif cell_type == "COLLAPSIBLE":
                label = cell_label or ""
                if is_top_level and label:
                    section_names.append(label)

            # Recurse into COLLAPSIBLE children
            if "cells" in cfg and isinstance(cfg["cells"], list):
                self._walk_cells(
                    cfg["cells"],
                    sql_cells=sql_cells,
                    explore_cells=explore_cells,
                    markdown_parts=markdown_parts,
                    section_names=section_names,
                    is_top_level=False,
                )

    @staticmethod
    def _infer_chart_type(spec: Optional[dict]) -> Optional[str]:
        """Best-effort chart type from EXPLORE spec.fields[0].axis config."""
        if not spec or not isinstance(spec, dict):
            return None
        fields = spec.get("fields") or []
        if not fields or not isinstance(fields[0], dict):
            return None
        axis = fields[0].get("axis") or {}
        mark = axis.get("mark") or {}
        if isinstance(mark, dict):
            return mark.get("type")
        return None
