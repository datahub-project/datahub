"""Lineage analysis implementation for TMDL."""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

from datahub.ingestion.source.ms_fabric.tmdl.exceptions import ValidationError
from datahub.ingestion.source.ms_fabric.tmdl.models.table import Table


@dataclass
class DependencyNode:
    """Represents a node in the dependency graph."""

    id: str
    type: str  # 'table', 'column', 'measure'
    name: str
    table: Optional[str] = None
    dependencies: Set[str] = field(default_factory=set)


@dataclass
class LineageGraph:
    """Represents the complete lineage graph."""

    nodes: Dict[str, DependencyNode] = field(default_factory=dict)
    edges: Set[Tuple[str, str]] = field(default_factory=set)

    def add_node(self, node: DependencyNode) -> None:
        """Add node to graph."""
        self.nodes[node.id] = node

    def add_edge(self, from_id: str, to_id: str) -> None:
        """Add edge to graph."""
        self.edges.add((from_id, to_id))

    def get_upstream_nodes(self, node_id: str) -> Set[str]:
        """Get IDs of nodes that the given node depends on."""
        return {edge[1] for edge in self.edges if edge[0] == node_id}

    def get_downstream_nodes(self, node_id: str) -> Set[str]:
        """Get IDs of nodes that depend on the given node."""
        return {edge[0] for edge in self.edges if edge[1] == node_id}


class LineageAnalyzer:
    """Analyzer for building and analyzing data lineage."""

    def analyze(self, tables: List[Table], relationships: List) -> LineageGraph:
        """Analyze tables and relationships to build lineage graph."""
        try:
            graph = LineageGraph()

            # Process tables
            for table in tables:
                # Add table node
                table_id = f"table:{table.name}"
                graph.add_node(
                    DependencyNode(id=table_id, type="table", name=table.name)
                )

                # Process columns
                for column in table.columns:
                    if column.expression:
                        column_id = f"column:{table.name}.{column.name}"
                        graph.add_node(
                            DependencyNode(
                                id=column_id,
                                type="column",
                                name=column.name,
                                table=table.name,
                            )
                        )
                        graph.add_edge(column_id, table_id)

                # Process measures
                for measure in table.measures:
                    measure_id = f"measure:{table.name}.{measure.name}"
                    graph.add_node(
                        DependencyNode(
                            id=measure_id,
                            type="measure",
                            name=measure.name,
                            table=table.name,
                        )
                    )
                    graph.add_edge(measure_id, table_id)

            return graph

        except Exception as e:
            raise ValidationError(f"Lineage analysis failed: {str(e)}")
