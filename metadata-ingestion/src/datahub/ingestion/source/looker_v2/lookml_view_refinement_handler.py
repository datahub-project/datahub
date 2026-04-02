"""
View Refinement Handler for Looker V2 Source.

Handles LookML view refinements, tracking each refinement as a separate
lineage node with field attribution.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

from datahub.ingestion.source.looker_v2.lookml_parser import (
    LookMLParser,
    ParsedDimension,
    ParsedMeasure,
    ParsedView,
)

logger = logging.getLogger(__name__)


@dataclass
class FieldAttribution:
    """Tracks which file and project contributed a specific field.

    Attributes:
        operation: One of "added" (first definition) or "modified" (overridden by refinement).
        field_type: One of "dimension", "measure", or "dimension_group".
    """

    field_name: str
    source_file: str
    source_project: str
    is_refinement: bool
    operation: str
    field_type: str


@dataclass
class RefinementNode:
    """Represents a single node in a view refinement chain.

    The first node (order=0) is the base view definition. Subsequent nodes are
    successive +view refinements applied in project order.

    Attributes:
        order: Position in the chain. 0 = base view; 1+ = refinement layers.
    """

    view_name: str
    file_path: str
    project_name: str
    order: int

    # Fields added or modified by this refinement
    added_dimensions: List[ParsedDimension] = field(default_factory=list)
    added_measures: List[ParsedMeasure] = field(default_factory=list)
    modified_fields: List[str] = field(default_factory=list)

    # Upstream lineage (tables referenced by new SQL)
    upstream_tables: List[str] = field(default_factory=list)

    # The parsed view
    parsed_view: Optional[ParsedView] = None

    @property
    def urn_suffix(self) -> str:
        """URN-safe identifier for this node (e.g. "my_view+ref2" for the third node)."""
        if self.order == 0:
            return self.view_name
        return f"{self.view_name}+ref{self.order}"

    @property
    def display_name(self) -> str:
        """Human-readable label showing whether this is the base view or a refinement."""
        if self.order == 0:
            return f"{self.view_name} (base)"
        return f"+{self.view_name} ({self.project_name})"


@dataclass
class RefinementChain:
    """A chain of refinements for a single view."""

    base_view_name: str
    nodes: List[RefinementNode] = field(default_factory=list)

    def add_node(self, node: RefinementNode) -> None:
        """Append a node, automatically assigning its position in the chain."""
        node.order = len(self.nodes)
        self.nodes.append(node)

    @property
    def base(self) -> Optional[RefinementNode]:
        """The base (non-refined) view node, or None if the chain is empty."""
        return self.nodes[0] if self.nodes else None

    @property
    def refinements(self) -> List[RefinementNode]:
        """All nodes beyond the base view (i.e. the +view refinement layers)."""
        return self.nodes[1:] if len(self.nodes) > 1 else []

    def get_all_fields(self) -> Dict[str, FieldAttribution]:
        """
        Get all fields with their attribution across the chain.

        Returns:
            Dictionary mapping field name to its attribution
        """
        fields: Dict[str, FieldAttribution] = {}

        for node in self.nodes:
            if not node.parsed_view:
                continue

            # Reset node mutation targets for idempotency
            node.added_dimensions = []
            node.added_measures = []
            node.modified_fields = []

            # Process dimensions
            for dim in node.parsed_view.dimensions:
                is_new = dim.name not in fields
                fields[dim.name] = FieldAttribution(
                    field_name=dim.name,
                    source_file=node.file_path,
                    source_project=node.project_name,
                    is_refinement=node.order > 0,
                    operation="added" if is_new else "modified",
                    field_type="dimension",
                )
                if is_new:
                    node.added_dimensions.append(dim)
                else:
                    node.modified_fields.append(dim.name)

            # Process measures
            for measure in node.parsed_view.measures:
                is_new = measure.name not in fields
                fields[measure.name] = FieldAttribution(
                    field_name=measure.name,
                    source_file=node.file_path,
                    source_project=node.project_name,
                    is_refinement=node.order > 0,
                    operation="added" if is_new else "modified",
                    field_type="measure",
                )
                if is_new:
                    node.added_measures.append(measure)
                else:
                    node.modified_fields.append(measure.name)

        return fields


class RefinementHandler:
    """
    Handles view refinements and builds refinement chains.

    Processes LookML files to identify base views and their refinements,
    building a chain that tracks field attribution across projects.
    """

    # Regex to identify refinement files (view names starting with +)
    REFINEMENT_PATTERN = re.compile(r"view:\s*\+(\w+)\s*\{", re.MULTILINE)

    def __init__(
        self,
        base_folder: str,
        project_name: str,
        project_dependencies: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the refinement handler.

        Args:
            base_folder: Path to the main LookML project
            project_name: Name of the main project
            project_dependencies: Mapping of project name to path
        """
        self.base_folder = Path(base_folder)
        self.project_name = project_name
        self.project_dependencies = project_dependencies or {}

        self._parser = LookMLParser(template_variables={}, constants={})

        # All project paths including main
        self._all_project_paths: Dict[str, Path] = {project_name: self.base_folder}
        for name, path in self.project_dependencies.items():
            self._all_project_paths[name] = Path(path)

    def find_refinements_for_view(self, view_name: str) -> RefinementChain:
        """
        Find all refinements for a view across all projects.

        Args:
            view_name: Name of the view to find refinements for

        Returns:
            RefinementChain containing base view and all refinements
        """
        chain = RefinementChain(base_view_name=view_name)
        found_files: List[tuple] = []  # (file_path, project_name, is_refinement)

        # Search all projects for the view and its refinements
        for project_name, project_path in self._all_project_paths.items():
            for view_file in project_path.rglob("*.view.lkml"):
                with open(view_file, "r", encoding="utf-8") as f:
                    content = f.read()

                # Check for base view definition
                base_pattern = rf"view:\s+{re.escape(view_name)}\s*\{{"
                if re.search(base_pattern, content):
                    found_files.append((str(view_file), project_name, False))

                # Check for refinement definition
                refine_pattern = rf"view:\s+\+{re.escape(view_name)}\s*\{{"
                if re.search(refine_pattern, content):
                    found_files.append((str(view_file), project_name, True))

        # Sort: base views first, then refinements by project order
        # This is a simplified ordering - in reality, include order matters
        found_files.sort(key=lambda x: (x[2], x[1]))

        # Build chain
        node_order = 0
        for file_path, project_name, is_refinement in found_files:
            parsed_views = self._parser.parse_view_file(file_path, project_name)
            for pv in parsed_views:
                if pv.name == view_name and pv.is_refinement == is_refinement:
                    node = RefinementNode(
                        view_name=view_name,
                        file_path=file_path,
                        project_name=project_name,
                        order=node_order,
                        parsed_view=pv,
                    )
                    chain.add_node(node)
                    node_order += 1
                    break

        return chain

    def find_all_refinement_chains(
        self, view_names: Set[str]
    ) -> Dict[str, RefinementChain]:
        """
        Find refinement chains for multiple views.

        Args:
            view_names: Set of view names to process

        Returns:
            Dictionary mapping view name to its refinement chain
        """
        chains: Dict[str, RefinementChain] = {}

        for view_name in view_names:
            chain = self.find_refinements_for_view(view_name)
            if chain.nodes:
                chains[view_name] = chain
                logger.debug(
                    f"Found refinement chain for {view_name}: {len(chain.nodes)} nodes"
                )

        return chains

    def get_refinement_statistics(
        self, chains: Dict[str, RefinementChain]
    ) -> Dict[str, object]:
        """
        Generate statistics about refinements.

        Args:
            chains: Dictionary of refinement chains

        Returns:
            Statistics dictionary for reporting
        """
        total_chains = len(chains)
        total_refinements = sum(len(c.refinements) for c in chains.values())

        by_project: Dict[str, int] = {}
        fields_added = 0
        fields_modified = 0

        for chain in chains.values():
            # Count fields
            all_fields = chain.get_all_fields()
            fields_added += sum(
                1 for f in all_fields.values() if f.operation == "added"
            )
            fields_modified += sum(
                1 for f in all_fields.values() if f.operation == "modified"
            )

            # Count by project
            for node in chain.refinements:
                by_project[node.project_name] = by_project.get(node.project_name, 0) + 1

        return {
            "refinements_discovered": total_refinements,
            "views_with_refinements": total_chains,
            "refinements_by_project": by_project,
            "fields_added_by_refinement": fields_added,
            "fields_modified_by_refinement": fields_modified,
        }


def merge_additive_parameters(
    base_view: ParsedView, refinement: ParsedView
) -> ParsedView:
    """
    Merge additive parameters from refinement into base view.

    Per Looker docs, these parameters are additive (not replaced):
    - link, drill_fields, tags, sets
    - extends in explore refinements

    Args:
        base_view: The base view being refined
        refinement: The refinement to merge

    Returns:
        New ParsedView with merged content
    """
    from copy import deepcopy

    merged = deepcopy(base_view)
    merged.is_refinement = False  # Result is a merged view

    # Merge dimensions - refinement can override or add
    base_dim_names = {d.name for d in merged.dimensions}
    for dim in refinement.dimensions:
        if dim.name in base_dim_names:
            # Override existing
            for i, base_dim in enumerate(merged.dimensions):
                if base_dim.name == dim.name:
                    # Merge additive tags
                    merged_tags = list(dict.fromkeys(base_dim.tags + dim.tags))
                    new_dim = deepcopy(dim)
                    new_dim.tags = merged_tags
                    merged.dimensions[i] = new_dim
                    break
        else:
            # Add new
            merged.dimensions.append(dim)

    # Merge measures - similar logic
    base_measure_names = {m.name for m in merged.measures}
    for measure in refinement.measures:
        if measure.name in base_measure_names:
            for i, base_measure in enumerate(merged.measures):
                if base_measure.name == measure.name:
                    merged_tags = list(dict.fromkeys(base_measure.tags + measure.tags))
                    new_measure = deepcopy(measure)
                    new_measure.tags = merged_tags
                    merged.measures[i] = new_measure
                    break
        else:
            merged.measures.append(measure)

    # Update sql_table_name if refinement provides one
    if refinement.sql_table_name:
        merged.sql_table_name = refinement.sql_table_name

    # Update derived_table if refinement provides one
    if refinement.derived_table_sql:
        merged.derived_table_sql = refinement.derived_table_sql

    return merged
