import importlib.resources as pkg_resources
import json
import logging
import re
import sys
from typing import Any, Dict, List, Optional

import click
from click_default_group import DefaultGroup
from tabulate import tabulate

from datahub.cli.search_filter_parser import parse_filter_string
from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, load_filters
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)

# Exit codes for agent-friendly error differentiation.
EXIT_SUCCESS = 0
EXIT_GENERAL = 1
EXIT_USAGE = 2
EXIT_PERMISSION = 4
EXIT_CONNECTION = 5


class SearchCliError(click.ClickException):
    """Structured error for the search CLI.

    When stderr is not a TTY (agent/pipe context), outputs JSON:
      {"error": "<error_type>", "message": "...", "suggestion": "..."}
    When stderr is a TTY (human context), outputs click's default format.
    """

    def __init__(
        self,
        message: str,
        error_type: str = "search_error",
        suggestion: Optional[str] = None,
        exit_code: int = EXIT_GENERAL,
    ) -> None:
        super().__init__(message)
        self.error_type = error_type
        self.suggestion = suggestion
        self.exit_code = exit_code

    def show(self, file: Any = None) -> None:
        if sys.stderr.isatty():
            super().show(file=file)
        else:
            err: Dict[str, str] = {
                "error": self.error_type,
                "message": self.format_message(),
            }
            if self.suggestion:
                err["suggestion"] = self.suggestion
            click.echo(json.dumps(err), err=True)


def parse_simple_filters(filters_list: List[str]) -> Dict[str, List[str]]:
    """Parse --filter flags into dict of filter_name -> [values].

    Handles:
    - Single value: platform=snowflake
    - Comma-separated: platform=snowflake,bigquery
    - Multiple flags: --filter platform=snowflake --filter env=PROD

    Args:
        filters_list: List of filter strings in key=value format

    Returns:
        Dict mapping filter names to lists of values

    Raises:
        click.UsageError: If filter format is invalid
    """
    parsed: Dict[str, List[str]] = {}
    for filter_str in filters_list:
        if "=" not in filter_str:
            raise click.UsageError(
                f"Invalid filter: {filter_str}. Expected key=value format."
            )

        key, value = filter_str.split("=", 1)
        key = key.strip()
        values = [v.strip() for v in value.split(",")]

        if key in parsed:
            parsed[key].extend(values)
        else:
            parsed[key] = values

    return parsed


def convert_simple_to_filter_dsl(parsed_filters: Dict[str, List[str]]) -> Filter:
    """Convert simple filters to Filter object using SDK.

    Args:
        parsed_filters: Dict of filter_name -> [values]

    Returns:
        Filter object ready for compilation
    """
    if len(parsed_filters) == 1:
        return load_filters(parsed_filters)
    else:
        # Multiple filters = implicit AND
        filter_list = [{k: v} for k, v in parsed_filters.items()]
        return load_filters({"and": filter_list})


def parse_complex_filters(filters_json: str) -> Filter:
    """Parse --filters JSON string into Filter object.

    Args:
        filters_json: JSON string with filter structure

    Returns:
        Filter object ready for compilation

    Raises:
        click.UsageError: If JSON is invalid or filter structure is malformed
    """
    try:
        filters_dict = json.loads(filters_json)
    except json.JSONDecodeError as e:
        raise click.UsageError(f"Invalid JSON in --filters: {e}") from e

    try:
        return load_filters(filters_dict)
    except Exception as e:
        raise click.UsageError(f"Invalid filter structure: {e}") from e


def validate_and_merge_filters(
    simple_filters: List[str],
    complex_filters: Optional[str],
    where_expr: Optional[str] = None,
) -> Optional[Filter]:
    """Ensure --filter, --filters, and --where aren't combined and parse filters.

    Args:
        simple_filters: List of simple filter strings
        complex_filters: JSON string with complex filter structure
        where_expr: SQL-like WHERE expression (e.g. 'platform = snowflake AND env = PROD')

    Returns:
        Filter object or None if no filters provided

    Raises:
        click.UsageError: If multiple filter types are used or parsing fails
    """
    options_used = sum([bool(simple_filters), bool(complex_filters), bool(where_expr)])
    if options_used > 1:
        raise click.UsageError(
            "Cannot combine --filter, --filters, and --where. Use one at a time."
        )

    if where_expr:
        try:
            return parse_filter_string(where_expr)
        except ValueError as e:
            raise click.UsageError(f"Invalid --where expression: {e}") from e
    elif simple_filters:
        parsed = parse_simple_filters(simple_filters)
        return convert_simple_to_filter_dsl(parsed)
    elif complex_filters:
        return parse_complex_filters(complex_filters)
    else:
        return None


_PROJECTION_BLOCKED_PATTERNS = [
    (
        re.compile(r"\b(mutation|subscription)\b", re.IGNORECASE),
        "Projection must be a selection set, not a {match} operation",
    ),
    (
        re.compile(r"\bfragment\b", re.IGNORECASE),
        "--projection cannot define GraphQL fragments. "
        "Use inline spreads (e.g. '... on Dataset {{ properties {{ name }} }}') instead.",
    ),
    (
        re.compile(r"__schema|__type", re.IGNORECASE),
        "Introspection queries not allowed in projection",
    ),
    (re.compile(r"\$"), "Variable definitions ($) not allowed in projection"),
    (re.compile(r"@"), "Directives (@) not allowed in projection"),
    (re.compile(r"#"), "Comments (#) not allowed in projection"),
]
_PROJECTION_MAX_LENGTH = 5000

_PLATFORM_FIELDS_FRAGMENT = """\
fragment PlatformFields on DataPlatform {
  urn
  name
  properties {
    displayName
    logoUrl
  }
}
"""

_SEARCH_QUERY_TEMPLATE = """\
{platform_fragment}query {operation}(
  $types: [EntityType!]
  $query: String!
  $orFilters: [AndFilterInput!]
  $count: Int!
  $start: Int!
  $viewUrn: String
  $sortInput: SearchSortInput
) {{
  {gql_field}(
    input: {{
      query: $query
      count: $count
      start: $start
      types: $types
      orFilters: $orFilters
      viewUrn: $viewUrn
      sortInput: $sortInput
      {extra_input}
    }}
  ) {{
    start
    count
    total
    searchResults {{
      entity {{
        {entity_projection}
      }}
      {extra_result_fields}
    }}
    facets {{
      field
      displayName
      aggregations {{
        value
        count
        displayName
        entity {{
          ...FacetEntityInfo
        }}
      }}
    }}
  }}
}}

fragment FacetEntityInfo on Entity {{
  urn
  type
}}
"""


def _validate_projection(projection: str) -> None:
    """Validate a projection string, rejecting injection attempts and malformed input."""
    if len(projection) > _PROJECTION_MAX_LENGTH:
        raise click.UsageError(
            f"Projection too long ({len(projection)} chars, max {_PROJECTION_MAX_LENGTH})"
        )
    for pattern, msg_template in _PROJECTION_BLOCKED_PATTERNS:
        match = pattern.search(projection)
        if match:
            raise click.UsageError(msg_template.format(match=match.group()))
    if projection.count("{") != projection.count("}"):
        raise click.UsageError("Unbalanced braces in projection")


def _load_projection(value: str) -> str:
    """Load projection from inline string or @file path."""
    if value.startswith("@"):
        file_path = value[1:]
        try:
            with open(file_path, encoding="utf-8") as f:
                return f.read().strip()
        except FileNotFoundError as e:
            raise click.UsageError(f"Projection file not found: {file_path}") from e
        except OSError as e:
            raise click.UsageError(f"Error reading projection file: {e}") from e
    return value


def _strip_outer_braces(projection: str) -> str:
    """Strip outer { } from projection if present, so both '{ urn }' and 'urn' work."""
    stripped = projection.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped[1:-1].strip()
    return stripped


def _build_search_query(
    semantic: bool,
    projection: Optional[str] = None,
) -> str:
    """Build the GraphQL query string.

    When projection is None, returns the full bundled .gql file.
    When projection is provided, uses the template with the user's selection set.
    """
    if projection is None:
        fragments = (
            pkg_resources.files("datahub.cli.gql")
            .joinpath("fragments.gql")
            .read_text(encoding="utf-8")
        )
        op_file = "semantic_search.gql" if semantic else "search.gql"
        operation = (
            pkg_resources.files("datahub.cli.gql")
            .joinpath(op_file)
            .read_text(encoding="utf-8")
        )
        return fragments + "\n" + operation

    entity_projection = _strip_outer_braces(projection)

    if semantic:
        operation = "semanticSearch"
        gql_field = "semanticSearchAcrossEntities"
        extra_input = ""
        extra_result_fields = "matchedFields {\n        name\n        value\n      }"
    else:
        operation = "search"
        gql_field = "searchAcrossEntities"
        extra_input = "searchFlags: { skipHighlighting: true, maxAggValues: 5 }"
        extra_result_fields = ""

    # Only include PlatformFields fragment if the projection references it
    needs_platform = "PlatformFields" in entity_projection
    platform_fragment = _PLATFORM_FIELDS_FRAGMENT if needs_platform else ""

    return _SEARCH_QUERY_TEMPLATE.format(
        platform_fragment=platform_fragment,
        operation=operation,
        gql_field=gql_field,
        extra_input=extra_input,
        entity_projection=entity_projection,
        extra_result_fields=extra_result_fields,
    )


def execute_search(
    query: str,
    filters: Optional[Filter],
    num_results: int,
    sort_by: Optional[str],
    sort_order: str,
    offset: int,
    semantic: bool,
    facets_only: bool,
    view_urn: Optional[str],
    dry_run: bool = False,
    projection: Optional[str] = None,
) -> Dict[str, Any]:
    """Execute search using GraphQL.

    Args:
        query: Search query string
        filters: Filter object to apply
        num_results: Number of results to return (max 50)
        sort_by: Field name to sort by
        sort_order: Sort order (asc/desc)
        offset: Starting position for pagination
        semantic: Use semantic search instead of keyword search
        facets_only: Return only facets, no search results
        view_urn: DataHub View URN to apply
        dry_run: If True, return the compiled query info without executing

    Returns:
        Search results dict, or dry-run info dict when dry_run=True.

    Raises:
        click.ClickException: If search fails or semantic search is not enabled
    """
    # Cap num_results at 50
    num_results = min(num_results, 50)

    # Compile filters to GraphQL format
    types, compiled_filters = compile_filters(filters)

    if semantic:
        operation_name = "semanticSearch"
        graphql_field = "semanticSearchAcrossEntities"
    else:
        operation_name = "search"
        graphql_field = "searchAcrossEntities"

    # Build variables
    variables: Dict[str, Any] = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        # count=1 for facets_only: the backend doesn't support count=0, so we
        # fetch the minimum and discard the result below.
        "count": max(num_results, 1) if not facets_only else 1,
        "start": offset,
        "viewUrn": view_urn,
    }

    # Add sorting if requested
    if sort_by:
        sort_order_enum = "ASCENDING" if sort_order == "asc" else "DESCENDING"
        variables["sortInput"] = {
            "sortCriteria": [{"field": sort_by, "sortOrder": sort_order_enum}]
        }

    if dry_run:
        info: Dict[str, Any] = {
            "operation_name": operation_name,
            "graphql_field": graphql_field,
            "variables": variables,
        }
        if projection is not None:
            info["projection"] = projection
            info["query"] = _build_search_query(
                semantic=semantic, projection=projection
            )
        return info

    # Get graph client
    graph = get_default_graph(ClientMode.CLI)

    # Build the GQL query — uses full bundled file by default, or template with projection.
    search_gql = _build_search_query(semantic=semantic, projection=projection)

    # Execute search
    try:
        response = graph.execute_graphql(
            query=search_gql,
            variables=variables,
            operation_name=operation_name,
        )[graphql_field]
    except Exception as e:
        error_msg = str(e).lower()
        if (
            "semantic search is not enabled" in error_msg
            or "semanticsearchacrossentities" in error_msg
        ):
            raise SearchCliError(
                "Semantic search is not available on this DataHub instance.\n\n"
                "Possible reasons:\n"
                "  • Semantic search is not enabled in the backend configuration\n"
                "  • The entity types you're searching may not be configured for semantic search\n"
                "  • Embeddings may not have been generated for your entities\n\n"
                "Use 'datahub search diagnose' for more details, or use keyword search (default).",
                error_type="semantic_search_unavailable",
                suggestion="datahub search diagnose",
            ) from e
        if "401" in error_msg or "403" in error_msg or "unauthorized" in error_msg:
            raise SearchCliError(
                f"Search failed: {e}",
                error_type="permission_denied",
                suggestion="Check your DataHub credentials and permissions.",
                exit_code=EXIT_PERMISSION,
            ) from e
        if (
            "connection" in error_msg
            or "connect" in error_msg
            or "timed out" in error_msg
        ):
            raise SearchCliError(
                f"Search failed: {e}",
                error_type="connection_error",
                suggestion="Verify DataHub is running: datahub search diagnose",
                exit_code=EXIT_CONNECTION,
            ) from e
        raise SearchCliError(
            f"Search failed: {e}",
            error_type="search_error",
        ) from e

    # Handle facets-only mode
    if facets_only:
        response.pop("searchResults", None)
        response.pop("count", None)

    return response


def format_json_output(results: Dict[str, Any]) -> str:
    """Format results as JSON.

    Args:
        results: Search results dict

    Returns:
        JSON string with pretty formatting
    """
    return json.dumps(results, indent=2, sort_keys=True)


def _extract_entity_name(entity: Dict[str, Any]) -> Optional[str]:
    """Extract display name from entity by resolving DataHub's per-type name field.

    The DataHub schema stores names in different locations depending on entity type.
    We try all known paths in priority order, falling through on None/empty:
      - properties.name          → Dataset, Chart, Dashboard, DataJob, DataFlow,
                                   Domain, Container, GlossaryTerm, Tag
      - properties.displayName   → CorpUser (preferred over username), CorpGroup
      - info.title               → Document
      - username                 → CorpUser (fallback when displayName is null)
      - name (top-level)         → MLModel, MLFeature, MLFeatureTable,
                                   MLPrimaryKey, MLModelGroup, CorpGroup, Tag,
                                   GlossaryTerm, Dataset
    """
    props = entity.get("properties") or {}
    return (
        props.get("name")
        or props.get("displayName")
        or (entity.get("info") or {}).get("title")
        or entity.get("username")
        or entity.get("name")
    )


_DESC_TRUNCATE_MAX_LEN = 80
_DESC_TRUNCATE_AT = 77


def format_table_output(results: Dict[str, Any]) -> str:
    """Format results as a human-readable table.

    Args:
        results: Search results dict

    Returns:
        Formatted table string with summary
    """
    search_results = results.get("searchResults", [])
    if not search_results:
        return "No results found"

    rows = []
    for result in search_results:
        entity = result.get("entity", {})
        urn = entity.get("urn", "")

        # Extract name using pattern-based extraction
        name = _extract_entity_name(entity)

        # Extract platform
        platform = ""
        if "platform" in entity:
            platform = entity["platform"].get("name", "")

        # Extract description (truncated); guard against null from GraphQL
        description = ""
        if "properties" in entity:
            desc = entity["properties"].get("description") or ""
            description = (
                desc[:_DESC_TRUNCATE_AT] + "..."
                if len(desc) > _DESC_TRUNCATE_MAX_LEN
                else desc
            )

        rows.append([urn, name or "(unnamed)", platform, description])

    headers = ["URN", "Name", "Platform", "Description"]
    table = tabulate(
        rows, headers=headers, tablefmt="grid", maxcolwidths=[60, 40, 15, 60]
    )

    # Add summary
    total = results.get("total", 0)
    start = results.get("start", 0)
    count = len(search_results)
    summary = f"\n\nShowing {start + 1}-{start + count} of {total} results"

    return table + summary


def format_urns_output(results: Dict[str, Any]) -> str:
    """Format results as a list of URNs (one per line).

    Args:
        results: Search results dict

    Returns:
        Newline-separated URNs
    """
    search_results = results.get("searchResults", [])
    urns = [result.get("entity", {}).get("urn", "") for result in search_results]
    return "\n".join(urns)


def format_facets_output(results: Dict[str, Any], output_format: str) -> str:
    """Format facets for display.

    Args:
        results: Search results dict with facets
        output_format: Output format (json or table)

    Returns:
        Formatted facets string
    """
    facets = results.get("facets", [])

    if output_format == "json":
        return format_json_output({"facets": facets})

    # Table format
    if not facets:
        return "No facets available"

    output = []
    for facet in facets:
        field = facet.get("field", "")
        display_name = facet.get("displayName", field)
        aggregations = facet.get("aggregations", [])

        output.append(f"\n{display_name} ({field}):")
        if aggregations:
            rows = [
                [agg.get("value", ""), agg.get("count", 0)] for agg in aggregations[:10]
            ]
            output.append(tabulate(rows, headers=["Value", "Count"], tablefmt="simple"))
        else:
            output.append("  (no values)")

    return "\n".join(output)


def list_available_filters() -> None:
    """Display all available filter types with descriptions and examples."""
    filters_info = [
        (
            "entity_type",
            "Filter by entity type",
            "dataset, dashboard, chart, corpuser",
        ),
        ("entity_subtype", "Filter by entity subtype", "Table, View, Model"),
        (
            "platform",
            "Filter by data platform",
            "snowflake, bigquery, looker",
        ),
        ("domain", "Filter by domain (full URN)", "urn:li:domain:marketing"),
        ("container", "Filter by container (full URN)", "urn:li:container:..."),
        ("tag", "Filter by tag (full URN)", "urn:li:tag:PII"),
        (
            "glossary_term",
            "Filter by term (full URN)",
            "urn:li:glossaryTerm:...",
        ),
        ("owner", "Filter by owner (full URN)", "urn:li:corpuser:alice"),
        ("env", "Filter by environment", "PROD, DEV, STAGING"),
        ("status", "Filter by deletion status", "NOT_SOFT_DELETED"),
        (
            "custom",
            "Custom field filter",
            '{"field": "name", "condition": "EQUAL", "values": [...]}',
        ),
        ("and", "Logical AND", "[filter1, filter2]"),
        ("or", "Logical OR", "[filter1, filter2]"),
        ("not", "Logical NOT", "filter"),
    ]

    click.echo("\nAvailable Filters:")
    click.echo(
        tabulate(
            filters_info,
            headers=["Filter", "Description", "Example"],
            tablefmt="grid",
        )
    )

    click.echo("\n\nUsage Examples:")
    click.echo("  Simple filters (implicit AND):")
    click.echo(
        '    datahub search "*" --filter platform=snowflake --filter entity_type=dataset'
    )
    click.echo("\n  Comma-separated OR:")
    click.echo('    datahub search "*" --filter platform=snowflake,bigquery')
    click.echo("\n  Complex filters:")
    click.echo(
        '    datahub search "*" --filters \'{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}\''
    )


def describe_filter_func(filter_name: str) -> None:
    """Display detailed information about a specific filter.

    Args:
        filter_name: Name of the filter to describe
    """
    details = {
        "platform": {
            "description": "Filter entities by their source platform",
            "value_type": "Platform name (string)",
            "examples": ["snowflake", "bigquery", "looker", "tableau"],
            "notes": "Automatically converted to platform URN internally. Case-insensitive.",
        },
        "entity_type": {
            "description": "Filter by the type of entity",
            "value_type": "Entity type name (string)",
            "examples": [
                "dataset",
                "dashboard",
                "chart",
                "corpuser",
                "corpgroup",
                "domain",
            ],
            "notes": "Case-insensitive. Use comma-separated values for OR logic.",
        },
        "entity_subtype": {
            "description": "Filter by the subtype of entity (more specific than entity_type)",
            "value_type": "Subtype name (string)",
            "examples": ["Table", "View", "Model", "Looker Dashboard"],
            "notes": "Available subtypes depend on the platform and entity type.",
        },
        "domain": {
            "description": "Filter entities by their assigned domain",
            "value_type": "Full domain URN (string)",
            "examples": ["urn:li:domain:marketing", "urn:li:domain:finance"],
            "notes": "MUST use full URN. Find domains: datahub search --filter entity_type=domain",
        },
        "container": {
            "description": "Filter entities by their container (e.g., database, schema)",
            "value_type": "Full container URN (string)",
            "examples": [
                "urn:li:container:...",
            ],
            "notes": "MUST use full URN. Find containers: datahub search --filter entity_type=container",
        },
        "tag": {
            "description": "Filter entities by assigned tags",
            "value_type": "Full tag URN (string)",
            "examples": ["urn:li:tag:PII", "urn:li:tag:Sensitive"],
            "notes": "MUST use full URN. Find tags: datahub search --filter entity_type=tag",
        },
        "glossary_term": {
            "description": "Filter entities by assigned glossary terms",
            "value_type": "Full glossary term URN (string)",
            "examples": ["urn:li:glossaryTerm:CustomerData"],
            "notes": "MUST use full URN. Find terms: datahub search --filter entity_type=glossaryterm",
        },
        "owner": {
            "description": "Filter entities by their owner",
            "value_type": "Full user or group URN (string)",
            "examples": ["urn:li:corpuser:alice", "urn:li:corpGroup:data-eng"],
            "notes": "MUST use full URN. Find users: datahub search --filter entity_type=corpuser",
        },
        "env": {
            "description": "Filter by environment designation",
            "value_type": "Environment name (string)",
            "examples": ["PROD", "DEV", "STAGING", "QA"],
            "notes": "Common values are PROD, DEV, STAGING. Case-sensitive.",
        },
        "status": {
            "description": "Filter by deletion status",
            "value_type": "Status value (string)",
            "examples": ["NOT_SOFT_DELETED", "SOFT_DELETED"],
            "notes": "Use NOT_SOFT_DELETED to exclude soft-deleted entities (default in most cases).",
        },
        "custom": {
            "description": "Custom field filter with flexible conditions",
            "value_type": "Object with field, condition, values, negated",
            "examples": [
                '{"field": "name", "condition": "EQUAL", "values": ["my_table"]}',
                '{"field": "description", "condition": "CONTAIN", "values": ["customer"], "negated": false}',
            ],
            "notes": "Conditions: EQUAL, CONTAIN, START_WITH, END_WITH, GREATER_THAN, etc.",
        },
        "and": {
            "description": "Logical AND - all filters must match",
            "value_type": "Array of filter objects",
            "examples": ['{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}'],
            "notes": "Multiple --filter flags implicitly create an AND.",
        },
        "or": {
            "description": "Logical OR - at least one filter must match",
            "value_type": "Array of filter objects",
            "examples": [
                '{"or": [{"entity_type": ["chart"]}, {"entity_type": ["dashboard"]}]}'
            ],
            "notes": "Comma-separated values in --filter create OR for that field.",
        },
        "not": {
            "description": "Logical NOT - negates the filter",
            "value_type": "Single filter object",
            "examples": ['{"not": {"platform": ["snowflake"]}}'],
            "notes": "Excludes entities matching the inner filter.",
        },
    }

    if filter_name not in details:
        click.echo(f"Unknown filter: {filter_name}")
        click.echo("Use --list-filters to see all available filters")
        return

    info = details[filter_name]
    click.echo(f"\nFilter: {filter_name}")
    click.echo(f"Description: {info['description']}")
    click.echo(f"Value Type: {info['value_type']}")
    click.echo("Examples:")
    for example in info["examples"]:
        click.echo(f"  - {example}")
    click.echo(f"Notes: {info['notes']}")


def _print_keyword_diagnostics(kw: Dict[str, Any]) -> None:
    """Print keyword search diagnostics section."""
    click.echo("Keyword Search")
    click.echo("-" * 40)

    if not kw["connected"]:
        click.echo("✗ Cannot connect to DataHub")
        click.echo(f"  Error: {kw['connection_error']}")
        click.echo("\n  Check DATAHUB_GMS_URL and DATAHUB_GMS_TOKEN,")
        click.echo("  or run 'datahub init' to configure.")
        return

    click.echo("✓ Connected to DataHub")

    if kw["search_works"]:
        total = kw["total_entities"]
        click.echo(f"✓ Search is working ({total:,} entities indexed)")
        if total == 0:
            click.echo("  ⚠ No entities found — ingestion may not have run yet")
    else:
        click.echo("✗ Search query failed")
        click.echo(f"  Error: {kw['search_error']}")


def _print_semantic_diagnostics(sem: Dict[str, Any]) -> None:
    """Print semantic search diagnostics section."""
    click.echo("Semantic Search")
    click.echo("-" * 40)

    if sem["semantic_enabled"]:
        click.echo("✓ Semantic search is ENABLED and available")

        config = sem.get("config", {})
        embedding_config = config.get("embedding_config", {})

        if embedding_config and not config.get("error"):
            click.echo("\nEmbedding Configuration:")
            click.echo(f"  • Provider: {embedding_config.get('provider', 'Unknown')}")
            click.echo(f"  • Model ID: {embedding_config.get('modelId', 'Unknown')}")
            click.echo(
                f"  • Embedding Key: {embedding_config.get('modelEmbeddingKey', 'Unknown')}"
            )

            aws_config = embedding_config.get("awsProviderConfig")
            if aws_config and aws_config.get("region"):
                click.echo(f"  • AWS Region: {aws_config['region']}")

            enabled_entities = config.get("enabled_entities", [])
            if enabled_entities:
                click.echo(f"\nEnabled Entity Types ({len(enabled_entities)}):")
                for entity in sorted(enabled_entities):
                    click.echo(f"  • {entity}")
            else:
                click.echo("\nEnabled Entity Types: None configured")
                click.echo(
                    "  ⚠ Warning: No entity types are configured for semantic search"
                )
        else:
            click.echo("\n  Backend: Configured correctly")
            if config.get("error"):
                click.echo(f"  ⚠ Could not fetch detailed config: {config['error']}")

        click.echo("\nNote: Results depend on which entity types are configured")
        click.echo("      and whether embeddings have been generated.")
    elif sem["semantic_available"]:
        click.echo("✗ Semantic search endpoint exists but is NOT enabled")
        click.echo("\nPossible reasons:")
        click.echo("  • Backend configuration missing or incomplete")
        click.echo("  • Embeddings service not running")
        click.echo("  • Entity types not configured for semantic search")
        if sem["semantic_error"]:
            click.echo(f"\nError: {sem['semantic_error']}")
    else:
        click.echo("✗ Semantic search is NOT available")
        click.echo("\nReasons:")
        click.echo("  • Feature not enabled in DataHub backend")
        click.echo("  • GraphQL schema does not include semantic search")
        click.echo("  • May require DataHub upgrade or configuration")


def print_diagnostics() -> None:
    """Print full search diagnostics to console."""
    click.echo("Search Diagnostics")
    click.echo("=" * 50)
    click.echo()

    try:
        results = run_full_diagnostics()
    except Exception as e:
        click.echo(f"✗ Failed to run diagnostics: {e}", err=True)
        raise click.ClickException(
            "Could not connect to DataHub. Check your configuration."
        ) from e

    _print_keyword_diagnostics(results["keyword"])

    # Only show semantic section if we could connect
    if results["keyword"]["connected"]:
        click.echo()
        _print_semantic_diagnostics(results["semantic"])

    click.echo("\n" + "=" * 50)


def diagnose_keyword_search(
    graph: Any,
) -> Dict[str, Any]:
    """Diagnose keyword search health.

    Checks GMS connectivity and runs a basic wildcard query.

    Returns:
        Dict with keyword search diagnostic information
    """
    # Check connectivity
    try:
        graph.execute_graphql(
            query="query ping { appConfig { telemetryConfig { enableThirdPartyLogging } } }"
        )
        connected = True
        connection_error = None
    except Exception as e:
        return {
            "connected": False,
            "connection_error": str(e),
            "search_works": False,
            "total_entities": None,
            "search_error": None,
        }

    # Run a basic wildcard search
    test_query = """
    query testKeywordSearch {
      searchAcrossEntities(input: {query: "*", count: 1, start: 0, searchFlags: {skipHighlighting: true}}) {
        total
      }
    }
    """
    try:
        result = graph.execute_graphql(query=test_query)
        total = result.get("searchAcrossEntities", {}).get("total", 0)
        return {
            "connected": connected,
            "connection_error": connection_error,
            "search_works": True,
            "total_entities": total,
            "search_error": None,
        }
    except Exception as e:
        return {
            "connected": connected,
            "connection_error": connection_error,
            "search_works": False,
            "total_entities": None,
            "search_error": str(e),
        }


def diagnose_semantic_search(
    graph: Any,
) -> Dict[str, Any]:
    """Diagnose semantic search configuration.

    Returns:
        Dict with diagnostic information about semantic search status
    """
    # Test if semantic search endpoint exists
    test_query = """
    query testSemanticSearch {
      __type(name: "Query") {
        fields {
          name
        }
      }
    }
    """

    try:
        introspection = graph.execute_graphql(query=test_query)
        fields = [
            field["name"] for field in introspection.get("__type", {}).get("fields", [])
        ]
        semantic_available = "semanticSearchAcrossEntities" in fields
    except Exception as e:
        logger.debug("Failed introspection: %s", e)
        semantic_available = False

    # Fetch detailed semantic search configuration
    config_query = """
    query getSemanticSearchConfig {
      appConfig {
        semanticSearchConfig {
          enabled
          enabledEntities
          embeddingConfig {
            provider
            modelId
            modelEmbeddingKey
            awsProviderConfig {
              region
            }
          }
        }
      }
    }
    """

    config_data = {}
    try:
        config_result = graph.execute_graphql(query=config_query)
        semantic_config = config_result.get("appConfig", {}).get(
            "semanticSearchConfig", {}
        )
        if semantic_config:
            config_data = {
                "enabled": semantic_config.get("enabled", False),
                "enabled_entities": semantic_config.get("enabledEntities", []),
                "embedding_config": semantic_config.get("embeddingConfig", {}),
            }
    except Exception as e:
        config_data = {"error": str(e)}

    # Try a test semantic search to check if it's truly enabled
    if semantic_available:
        test_semantic_query = """
        query testSemanticSearchEnabled($input: SearchAcrossEntitiesInput!) {
          semanticSearchAcrossEntities(input: $input) {
            total
          }
        }
        """
        try:
            graph.execute_graphql(
                query=test_semantic_query,
                variables={"input": {"query": "test", "types": [], "count": 1}},
            )
            semantic_enabled = True
            semantic_error = None
        except Exception as e:
            semantic_enabled = False
            semantic_error = str(e)
    else:
        semantic_enabled = False
        semantic_error = "Semantic search endpoint not found in GraphQL schema"

    return {
        "semantic_available": semantic_available,
        "semantic_enabled": semantic_enabled,
        "semantic_error": semantic_error,
        "config": config_data,
    }


def run_full_diagnostics() -> Dict[str, Any]:
    """Run all search diagnostics (keyword + semantic).

    Returns:
        Dict with both keyword and semantic diagnostic results
    """
    graph = get_default_graph(ClientMode.CLI)
    return {
        "keyword": diagnose_keyword_search(graph),
        "semantic": diagnose_semantic_search(graph),
    }


class _AgentAwareGroup(DefaultGroup):
    """Group that appends agent context to --help when stdout is not a TTY."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)
        if not sys.stdout.isatty():
            agent_text = (
                pkg_resources.files("datahub.cli.resources")
                .joinpath("SEARCH_AGENT_CONTEXT.md")
                .read_text(encoding="utf-8")
            )
            formatter.write("\n")
            formatter.write(agent_text)


@click.group(cls=_AgentAwareGroup, default="query")
def search() -> None:
    """Search across DataHub entities.

    This is a command group with multiple subcommands for different search operations.
    The default subcommand is 'query', which executes search queries.

    Examples:

    \b
    # Query (default command)
    datahub search "users"
    datahub search query "users" --semantic
    datahub search --list-filters
    datahub search --describe-filter platform

    \b
    # Diagnostics
    datahub search diagnose
    datahub search diagnose --format json

    \b
    # AI agent integration
    datahub search --agent-context

    See https://datahubproject.io/docs/cli for more examples.
    """
    pass


class _AgentAwareCommand(click.Command):
    """Command that appends agent context to --help when stdout is not a TTY."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)
        if not sys.stdout.isatty():
            agent_text = (
                pkg_resources.files("datahub.cli.resources")
                .joinpath("SEARCH_AGENT_CONTEXT.md")
                .read_text(encoding="utf-8")
            )
            formatter.write("\n")
            formatter.write(agent_text)


def _validate_query_inputs(
    limit: int,
    offset: int,
    filters_list: tuple,
    filters: Optional[str],
    projection: Optional[str],
    where_expr: Optional[str] = None,
) -> tuple:
    """Validate and prepare query inputs, returning (limit, filter_obj, loaded_projection)."""
    if limit < 1:
        raise SearchCliError(
            "--limit must be at least 1",
            error_type="usage_error",
            exit_code=EXIT_USAGE,
        )
    if limit > 50:
        click.echo("Warning: limit capped at 50", err=True)
        limit = 50

    if offset < 0:
        raise SearchCliError(
            "--offset must be non-negative",
            error_type="usage_error",
            exit_code=EXIT_USAGE,
        )

    try:
        filter_obj = validate_and_merge_filters(list(filters_list), filters, where_expr)
    except click.UsageError:
        raise
    except Exception as e:
        raise SearchCliError(
            str(e),
            error_type="usage_error",
            exit_code=EXIT_USAGE,
        ) from e

    loaded_projection: Optional[str] = None
    if projection is not None:
        try:
            loaded_projection = _load_projection(projection)
            _validate_projection(loaded_projection)
        except click.UsageError:
            raise
        except Exception as e:
            raise SearchCliError(
                str(e),
                error_type="usage_error",
                exit_code=EXIT_USAGE,
            ) from e

    return limit, filter_obj, loaded_projection


@search.command(name="query", cls=_AgentAwareCommand)
@click.argument("query", default="*")
@click.option(
    "--semantic",
    is_flag=True,
    help="Use semantic search instead of keyword search (BETA: requires backend configuration)",
)
@click.option(
    "--filter",
    "-f",
    "filters_list",
    multiple=True,
    help="Simple filter: key=value (repeatable, use commas for OR on same field)",
)
@click.option("--filters", help="Complex filters as JSON string for AND/OR/NOT logic")
@click.option(
    "--where",
    "where_expr",
    default=None,
    help=(
        "SQL-like filter expression, e.g. 'platform = snowflake AND env = PROD'. "
        "Supports AND, OR, NOT, IN, IS NULL / IS NOT NULL. "
        "Mutually exclusive with --filter and --filters."
    ),
)
@click.option(
    "--limit",
    "-n",
    type=int,
    default=10,
    help="Number of results to return (default: 10, max: 50)",
)
@click.option(
    "--offset",
    type=int,
    default=0,
    help="Starting position for pagination (default: 0)",
)
@click.option("--sort-by", help="Field name to sort by")
@click.option(
    "--sort-order",
    type=click.Choice(["asc", "desc"]),
    default="desc",
    help="Sort order (default: desc)",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["json", "table", "urns"]),
    default="json",
    help="Output format (default: json)",
)
@click.option("--table", is_flag=True, help="Shortcut for --format table")
@click.option("--urns-only", is_flag=True, help="Shortcut for --format urns")
@click.option(
    "--facets-only", is_flag=True, help="Return only facets, no search results"
)
@click.option("--view", help="DataHub View URN to apply")
@click.option(
    "--dry-run",
    is_flag=True,
    help="Show the compiled GraphQL operation and variables without executing the search",
)
@click.option(
    "--projection",
    default=None,
    help="GraphQL selection set for Entity type (inline GQL or @file path)",
)
@upgrade.check_upgrade
def query(
    query: str,
    semantic: bool,
    filters_list: tuple,
    filters: Optional[str],
    where_expr: Optional[str],
    limit: int,
    offset: int,
    sort_by: Optional[str],
    sort_order: str,
    output_format: str,
    table: bool,
    urns_only: bool,
    facets_only: bool,
    view: Optional[str],
    dry_run: bool,
    projection: Optional[str],
) -> None:
    """Execute search query across DataHub entities (default command).

    Supports keyword search (default) and semantic search with flexible filtering.

    QUERY is the search query string (default: "*" for all entities).

    Examples:

    \b
    # Basic searches
    datahub search "users"
    datahub search query "users"
    datahub search "*" --limit 50

    \b
    # Semantic search (BETA)
    datahub search --semantic "financial reports"
    datahub search query --semantic "financial reports"

    \b
    # Simple filters (implicit AND)
    datahub search "*" --filter platform=snowflake --filter entity_type=dataset
    datahub search "customers" --filter platform=snowflake,bigquery  # comma = OR

    \b
    # SQL-like WHERE expression
    datahub search "*" --where "platform = snowflake AND env = PROD"
    datahub search "*" --where "platform IN (snowflake, bigquery)"
    datahub search "*" --where "glossary_term IS NOT NULL"

    \b
    # Complex filters (AND/OR/NOT)
    datahub search "*" --filters '{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}'

    \b
    # Output formats
    datahub search "users" --table
    datahub search "users" --urns-only
    datahub search "users" | jq '.searchResults[].entity.urn'

    \b
    # Discovery
    datahub search list-filters
    datahub search describe-filter platform
    datahub search "*" --filter entity_type=dataset --facets-only

    \b
    # Pagination
    datahub search "users" --limit 50 --offset 100

    See https://datahubproject.io/docs/cli for more examples.
    """
    # Determine output format
    if table:
        output_format = "table"
    elif urns_only:
        output_format = "urns"

    # Validate and prepare inputs
    limit, filter_obj, loaded_projection = _validate_query_inputs(
        limit, offset, filters_list, filters, projection, where_expr
    )

    # Handle dry-run mode: build query info without connecting to DataHub
    if dry_run:
        dry_run_info = execute_search(
            query=query,
            filters=filter_obj,
            num_results=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            offset=offset,
            semantic=semantic,
            facets_only=facets_only,
            view_urn=view,
            dry_run=True,
            projection=loaded_projection,
        )
        click.echo(json.dumps(dry_run_info, indent=2))
        return

    # Execute search
    try:
        results = execute_search(
            query=query,
            filters=filter_obj,
            num_results=limit,
            sort_by=sort_by,
            sort_order=sort_order,
            offset=offset,
            semantic=semantic,
            facets_only=facets_only,
            view_urn=view,
            projection=loaded_projection,
        )
    except SearchCliError:
        raise
    except click.ClickException:
        raise
    except Exception as e:
        raise SearchCliError(f"Search failed: {e}", error_type="search_error") from e

    # Format and output results
    if facets_only:
        output = format_facets_output(results, output_format)
    elif output_format == "json":
        output = format_json_output(results)
    elif output_format == "table":
        output = format_table_output(results)
    elif output_format == "urns":
        output = format_urns_output(results)
    else:
        raise RuntimeError(f"Unexpected output format: {output_format}")

    # Show beta notice for semantic search (non-JSON formats)
    if semantic and output_format != "json":
        click.echo(
            "⚠️  Semantic search is in BETA. Results depend on backend configuration and embeddings.",
            err=True,
        )
        click.echo()

    click.echo(output)


@search.command(name="diagnose")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format (default: text)",
)
@upgrade.check_upgrade
def diagnose(output_format: str) -> None:
    """Diagnose search configuration and availability.

    Checks backend connectivity, keyword search health, semantic search
    configuration, embedding provider, model details, and enabled entity types.

    Examples:

    \b
    # Text format (default)
    datahub search diagnose

    \b
    # JSON format
    datahub search diagnose --format json
    """
    if output_format == "json":
        diagnostics = run_full_diagnostics()
        click.echo(json.dumps(diagnostics, indent=2))
    else:
        print_diagnostics()


@search.command(name="list-filters")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["text", "json"]),
    default="text",
    help="Output format (default: text)",
)
def list_filters_cmd(output_format: str) -> None:
    """List all available filter fields.

    Examples:

    \b
    datahub search list-filters
    datahub search list-filters --format json
    """
    list_available_filters()


@search.command(name="describe-filter")
@click.argument("filter_name")
def describe_filter_cmd(filter_name: str) -> None:
    """Describe a specific filter field.

    FILTER_NAME is the name of the filter to describe (e.g. platform, entity_type).

    Examples:

    \b
    datahub search describe-filter platform
    datahub search describe-filter entity_type
    """
    describe_filter_func(filter_name)
