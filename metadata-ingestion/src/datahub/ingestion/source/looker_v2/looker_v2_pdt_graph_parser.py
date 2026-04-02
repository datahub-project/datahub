"""
PDT Dependency Graph Parser.

Parses Graphviz DOT format output from Looker's graph_derived_tables_for_model API
into structured dependency edges.

Looker returns two known DOT formats:

Format 1 (angle brackets, model name in digraph header):
    digraph model_name {
      subgraph { <view_a> -> <view_b> }
      subgraph { <view_c> -> <schema.table> [style=dashed] }
    }

Format 2 (quoted strings with model/view paths):
    digraph g {
      "model_name/view_name" -> "model_name/upstream_view"
      "model_name/view_name" -> "schema.table" [style=dashed]
    }

Dashed edges indicate database table references; solid edges indicate PDT-to-PDT dependencies.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import List, Optional

logger = logging.getLogger(__name__)

# Format 1: <view_a> -> <view_b> [style=dashed]
_ANGLE_EDGE_PATTERN = re.compile(r"<([^>]+)>\s*->\s*<([^>]+)>" r"(?:\s*\[([^\]]*)\])?")

# Format 2: "model/view" -> "model/upstream" [style=dashed]
_QUOTED_EDGE_PATTERN = re.compile(r'"([^"]+)"\s*->\s*"([^"]+)"' r"(?:\s*\[([^\]]*)\])?")

# Extract model name from: digraph model_name {
_DIGRAPH_NAME_PATTERN = re.compile(r"digraph\s+(\S+)\s*\{")


@dataclass
class PDTDependencyEdge:
    """A single directed dependency edge from Looker's PDT graph.

    Represents a dependency from a PDT view to either another PDT view
    (is_database_table=False) or a raw warehouse table (is_database_table=True).

    Attributes:
        view_name: The dependent PDT view.
        model_name: The LookML model that contains the dependent view.
        upstream_name: The name of the upstream node (PDT view or table).
        upstream_model: The model owning the upstream view, or None for raw tables.
        is_database_table: True when the upstream is a warehouse table, not a PDT.
    """

    view_name: str
    model_name: str
    upstream_name: str
    upstream_model: Optional[str]
    is_database_table: bool


def _parse_quoted_node(node: str) -> tuple[str, Optional[str]]:
    """Parse a quoted DOT node into (name, model).

    Nodes with a "/" are model-qualified (e.g. "model_name/view_name").
    Nodes without "/" are raw database tables (e.g. "schema.table").
    """
    if "/" in node:
        parts = node.split("/", 1)
        return parts[1], parts[0]
    return node, None


def parse_pdt_graph(graph_text: str) -> List[PDTDependencyEdge]:
    """Parse DOT graph text from Looker's graph_derived_tables_for_model API.

    Detects Format 1 (angle-bracket nodes) or Format 2 (quoted model/view paths)
    automatically and returns all dependency edges found.

    Args:
        graph_text: Raw DOT format string returned by the Looker API.

    Returns:
        List of dependency edges. Empty list if the input is blank or unparseable.

    Example:
        >>> edges = parse_pdt_graph(
        ...     'digraph my_model { subgraph { <view_a> -> <view_b> } }'
        ... )
        >>> edges[0].view_name
        'view_a'
        >>> edges[0].upstream_name
        'view_b'
    """
    if not graph_text or not graph_text.strip():
        return []

    edges: List[PDTDependencyEdge] = []

    # Try Format 1 (angle brackets) first — this is the common real-world format
    angle_matches = list(_ANGLE_EDGE_PATTERN.finditer(graph_text))
    if angle_matches:
        # Extract model name from digraph header
        model_match = _DIGRAPH_NAME_PATTERN.search(graph_text)
        model_name = model_match.group(1) if model_match else ""

        for match in angle_matches:
            source_name = match.group(1)
            target_name = match.group(2)
            attrs = match.group(3) or ""

            is_database_table = "dashed" in attrs
            # If target contains a dot and no model context, likely a database table
            if "." in target_name and "/" not in target_name:
                is_database_table = True

            target_model: Optional[str] = model_name if not is_database_table else None

            edges.append(
                PDTDependencyEdge(
                    view_name=source_name,
                    model_name=model_name,
                    upstream_name=target_name,
                    upstream_model=target_model,
                    is_database_table=is_database_table,
                )
            )
    else:
        # Try Format 2 (quoted strings with model/view paths)
        for match in _QUOTED_EDGE_PATTERN.finditer(graph_text):
            source_raw = match.group(1)
            target_raw = match.group(2)
            attrs = match.group(3) or ""

            is_database_table = "dashed" in attrs

            source_name, source_model = _parse_quoted_node(source_raw)
            target_name, target_model = _parse_quoted_node(target_raw)

            # If target has no model, it's a raw database table
            if target_model is None:
                is_database_table = True

            edges.append(
                PDTDependencyEdge(
                    view_name=source_name,
                    model_name=source_model or "",
                    upstream_name=target_name,
                    upstream_model=target_model,
                    is_database_table=is_database_table,
                )
            )

    logger.debug(f"Parsed {len(edges)} edges from PDT graph")
    return edges
