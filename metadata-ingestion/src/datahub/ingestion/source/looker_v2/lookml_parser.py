"""
Simplified LookML Parser for Looker V2 Source.

Provides parsing for unreachable views that cannot be accessed via API.
Extracts dimensions, measures, sql_table_name, and derived_table.sql.
Uses LookMLSQLResolver (Jinja2-based) for Liquid template and constant resolution.
"""

from __future__ import annotations

import logging
import re
from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set

import lkml

from datahub.ingestion.source.looker_v2.lookml_sql_resolver import LookMLSQLResolver

logger = logging.getLogger(__name__)


@dataclass
class ParsedDimension:
    """Parsed dimension from LookML."""

    name: str
    type: str = "string"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    primary_key: bool = False
    tags: List[str] = field(default_factory=list)
    group_label: Optional[str] = None


@dataclass
class ParsedMeasure:
    """Parsed measure from LookML."""

    name: str
    type: str = "count"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    group_label: Optional[str] = None


@dataclass
class ParsedDimensionGroup:
    """Parsed dimension group from LookML."""

    name: str
    type: str = "time"
    sql: Optional[str] = None
    label: Optional[str] = None
    description: Optional[str] = None
    timeframes: List[str] = field(default_factory=list)


@dataclass
class ParsedView:
    """Parsed view from LookML."""

    name: str
    file_path: str
    project_name: str

    # SQL sources for lineage
    sql_table_name: Optional[str] = None
    derived_table_sql: Optional[str] = None
    derived_table_explore_source: Optional[str] = None

    # Fields
    dimensions: List[ParsedDimension] = field(default_factory=list)
    measures: List[ParsedMeasure] = field(default_factory=list)
    dimension_groups: List[ParsedDimensionGroup] = field(default_factory=list)

    # Extends and refinements
    extends: List[str] = field(default_factory=list)
    is_refinement: bool = False

    # Raw dict for additional processing
    raw_view: Dict[str, Any] = field(default_factory=dict)

    @property
    def has_sql_source(self) -> bool:
        """Return True if the view has at least one SQL source for lineage extraction."""
        return bool(self.sql_table_name or self.derived_table_sql)


class LookMLParser:
    """
    LookML parser for Looker V2 source.

    Resolves Liquid templates and LookML constants in SQL fields using
    LookMLSQLResolver (Jinja2-based) at parse time, so all SQL values
    in ParsedView are already resolved.
    """

    # Regex patterns for extracting table references from SQL
    TABLE_REF_PATTERN = re.compile(
        r"(?:FROM|JOIN)\s+([`\"\[]?[\w.]+[`\"\]]?)",
        re.IGNORECASE | re.MULTILINE,
    )

    def __init__(
        self,
        template_variables: Dict[str, Any],
        constants: Dict[str, str],
        environment: str = "prod",
    ) -> None:
        self.resolver = LookMLSQLResolver(
            template_variables=template_variables,
            constants=constants,
            environment=environment,
        )

    def parse_view_file(
        self,
        file_path: str,
        project_name: str,
    ) -> List[ParsedView]:
        """
        Parse a LookML view file, resolving Liquid templates and constants.

        Args:
            file_path: Path to the .view.lkml file
            project_name: Name of the project containing the file

        Returns:
            List of parsed views (a file can contain multiple views)
        """
        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            parsed = lkml.load(content)

            views = parsed.get("views", [])

            result = []
            for view_dict in views:
                parsed_view = self._parse_view_dict(view_dict, file_path, project_name)
                if parsed_view:
                    result.append(parsed_view)

            return result

        except Exception as e:
            logger.warning(f"Failed to parse LookML file {file_path}: {e}")
            return []

    def _parse_view_dict(
        self,
        view_dict: Dict[str, Any],
        file_path: str,
        project_name: str,
    ) -> Optional[ParsedView]:
        """Parse a single view dictionary from lkml."""
        name = view_dict.get("name")
        if not name:
            return None

        # Check if this is a refinement (name starts with +)
        is_refinement = name.startswith("+")
        if is_refinement:
            name = name[1:]  # Remove + prefix

        view = ParsedView(
            name=name,
            file_path=file_path,
            project_name=project_name,
            is_refinement=is_refinement,
            raw_view=view_dict,
        )

        # Parse sql_table_name — resolve Liquid templates via LookMLSQLResolver
        if "sql_table_name" in view_dict:
            view.sql_table_name = self.resolver.resolve(view_dict["sql_table_name"])

        # Parse derived_table — resolve Liquid templates in sql field
        derived_table = view_dict.get("derived_table", {})
        if derived_table:
            if "sql" in derived_table:
                view.derived_table_sql = self.resolver.resolve(derived_table["sql"])
            if "explore_source" in derived_table:
                view.derived_table_explore_source = derived_table["explore_source"]

        # Parse extends
        extends = view_dict.get("extends", [])
        if extends:
            view.extends = extends if isinstance(extends, list) else [extends]

        # Parse dimensions
        for dim in view_dict.get("dimensions", []):
            parsed_dim = self._parse_dimension(dim)
            if parsed_dim:
                view.dimensions.append(parsed_dim)

        # Parse measures
        for measure in view_dict.get("measures", []):
            parsed_measure = self._parse_measure(measure)
            if parsed_measure:
                view.measures.append(parsed_measure)

        # Parse dimension groups
        for dim_group in view_dict.get("dimension_groups", []):
            parsed_group = self._parse_dimension_group(dim_group)
            if parsed_group:
                view.dimension_groups.append(parsed_group)

        return view

    def _parse_dimension(self, dim_dict: Dict[str, Any]) -> Optional[ParsedDimension]:
        """Parse a dimension dictionary."""
        name = dim_dict.get("name")
        if not name:
            return None

        return ParsedDimension(
            name=name,
            type=dim_dict.get("type", "string"),
            sql=dim_dict.get("sql"),
            label=dim_dict.get("label"),
            description=dim_dict.get("description"),
            primary_key=dim_dict.get("primary_key", "no") == "yes",
            tags=dim_dict.get("tags", []),
            group_label=dim_dict.get("group_label"),
        )

    def _parse_measure(self, measure_dict: Dict[str, Any]) -> Optional[ParsedMeasure]:
        """Parse a measure dictionary."""
        name = measure_dict.get("name")
        if not name:
            return None

        return ParsedMeasure(
            name=name,
            type=measure_dict.get("type", "count"),
            sql=measure_dict.get("sql"),
            label=measure_dict.get("label"),
            description=measure_dict.get("description"),
            tags=measure_dict.get("tags", []),
            group_label=measure_dict.get("group_label"),
        )

    def _parse_dimension_group(
        self, group_dict: Dict[str, Any]
    ) -> Optional[ParsedDimensionGroup]:
        """Parse a dimension group dictionary."""
        name = group_dict.get("name")
        if not name:
            return None

        return ParsedDimensionGroup(
            name=name,
            type=group_dict.get("type", "time"),
            sql=group_dict.get("sql"),
            label=group_dict.get("label"),
            description=group_dict.get("description"),
            timeframes=group_dict.get("timeframes", []),
        )

    @staticmethod
    def extract_table_refs_from_sql(sql: str) -> List[str]:
        """Extract FROM/JOIN table references from SQL for lineage.

        Strips backtick/quote/bracket delimiters and filters out Looker template
        expressions (${TABLE}, ${...}) that are not real table names.

        Args:
            sql: A raw or already-resolved SQL string.

        Returns:
            Sorted list of unique table reference strings (e.g. "db.schema.table").
        """
        if not sql:
            return []

        refs = set()
        for match in LookMLParser.TABLE_REF_PATTERN.finditer(sql):
            table_ref = match.group(1).strip('`"[]')
            if table_ref.startswith("$") or "${" in table_ref or "{%" in table_ref:
                continue
            refs.add(table_ref)
        return sorted(refs)

    @staticmethod
    def resolve_extends_chain(
        views: Dict[str, ParsedView],
    ) -> Dict[str, ParsedView]:
        """Resolve extends inheritance across all views.

        Walks extends chains recursively, inheriting sql_table_name,
        derived_table_sql, dimensions, measures, and dimension_groups
        from parents. Child values take precedence over parent values.

        Detects and warns about circular extends.
        """
        resolved: Dict[str, ParsedView] = {}

        def resolve(name: str, visited: Set[str]) -> Optional[ParsedView]:
            if name in resolved:
                return resolved[name]
            if name not in views:
                return None
            if name in visited:
                logger.warning(f"Circular extends detected: {name}")
                return views[name]

            visited.add(name)
            view = deepcopy(views[name])

            for parent_name in view.extends:
                parent = resolve(parent_name, visited)
                if not parent:
                    logger.warning(
                        f"Could not resolve extends '{parent_name}' for view '{name}'"
                    )
                    continue
                # Inherit sql_table_name if not defined
                if not view.sql_table_name and parent.sql_table_name:
                    view.sql_table_name = parent.sql_table_name
                # Inherit derived_table_sql if not defined
                if not view.derived_table_sql and parent.derived_table_sql:
                    view.derived_table_sql = parent.derived_table_sql
                # Merge fields: parent fields added if not overridden by child
                child_dim_names = {d.name for d in view.dimensions}
                for dim in parent.dimensions:
                    if dim.name not in child_dim_names:
                        view.dimensions.append(dim)
                child_measure_names = {m.name for m in view.measures}
                for measure in parent.measures:
                    if measure.name not in child_measure_names:
                        view.measures.append(measure)
                child_group_names = {g.name for g in view.dimension_groups}
                for group in parent.dimension_groups:
                    if group.name not in child_group_names:
                        view.dimension_groups.append(group)

            visited.discard(name)
            resolved[name] = view
            return view

        for name in views:
            resolve(name, set())
        return resolved


def parse_views_from_files(
    file_paths: List[str],
    project_name: str,
    template_variables: Optional[Dict[str, Any]] = None,
    constants: Optional[Dict[str, str]] = None,
    environment: str = "prod",
    resolve_extends: bool = True,
) -> Dict[str, ParsedView]:
    """Parse multiple LookML view files into a view dictionary.

    Convenience wrapper around LookMLParser that handles multi-file projects.
    Refinements are stored with a "+" prefix key (e.g. "+my_view") so they do
    not overwrite their base views in the returned mapping.

    Args:
        file_paths: Absolute paths to .view.lkml files to parse.
        project_name: LookML project name, used to populate ParsedView.project_name.
        template_variables: Liquid variables for {{ variable }} substitution in SQL.
        constants: LookML @{constant} values, e.g. from manifest.lkml.
        environment: Controls -- if prod -- / -- if dev -- comment directives.
        resolve_extends: When True, walk the extends chain and inherit parent fields.

    Returns:
        Dictionary mapping view name (or "+view_name" for refinements) to ParsedView.

    Example:
        >>> views = parse_views_from_files(
        ...     ["/path/to/my_view.view.lkml"],
        ...     project_name="my_project",
        ...     constants={"schema": "prod_schema"},
        ... )
        >>> views["my_view"].sql_table_name
        'prod_schema.my_table'
    """
    parser = LookMLParser(
        template_variables=template_variables or {},
        constants=constants or {},
        environment=environment,
    )
    views: Dict[str, ParsedView] = {}

    for file_path in file_paths:
        parsed_views = parser.parse_view_file(file_path, project_name)
        for view in parsed_views:
            # For refinements, store with a special key
            key = f"+{view.name}" if view.is_refinement else view.name
            views[key] = view

    if resolve_extends:
        views = LookMLParser.resolve_extends_chain(views)

    return views
