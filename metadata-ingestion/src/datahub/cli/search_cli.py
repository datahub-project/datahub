import json
import logging
import pathlib
from typing import Any, Dict, List, Optional

import click
from click_default_group import DefaultGroup
from tabulate import tabulate

from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, load_filters
from datahub.upgrade import upgrade

logger = logging.getLogger(__name__)


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
    simple_filters: List[str], complex_filters: Optional[str]
) -> Optional[Filter]:
    """Ensure --filter and --filters aren't used together and parse filters.

    Args:
        simple_filters: List of simple filter strings
        complex_filters: JSON string with complex filter structure

    Returns:
        Filter object or None if no filters provided

    Raises:
        click.UsageError: If both filter types are used or parsing fails
    """
    if simple_filters and complex_filters:
        raise click.UsageError(
            "Cannot use both --filter and --filters. "
            "Use --filter for simple filters or --filters for complex logic."
        )

    if simple_filters:
        parsed = parse_simple_filters(simple_filters)
        return convert_simple_to_filter_dsl(parsed)
    elif complex_filters:
        return parse_complex_filters(complex_filters)
    else:
        return None


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

    Returns:
        Search results dict with searchResults, facets, total, etc.

    Raises:
        click.ClickException: If search fails or semantic search is not enabled
    """
    # Get graph client
    graph = get_default_graph(ClientMode.CLI)

    # Cap num_results at 50
    num_results = min(num_results, 50)

    # Compile filters to GraphQL format
    types, compiled_filters = compile_filters(filters)

    # Load appropriate GraphQL query
    # Navigate from metadata-ingestion/src/datahub/cli/ to datahub-agent-context/
    gql_dir = (
        pathlib.Path(__file__).parent.parent.parent.parent.parent
        / "datahub-agent-context"
        / "src"
        / "datahub_agent_context"
        / "mcp_tools"
        / "gql"
    )

    if semantic:
        search_gql = (gql_dir / "semantic_search.gql").read_text()
        operation_name = "semanticSearch"
        graphql_field = "semanticSearchAcrossEntities"
    else:
        search_gql = (gql_dir / "search.gql").read_text()
        operation_name = "search"
        graphql_field = "searchAcrossEntities"

    # Build variables
    variables: Dict[str, Any] = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
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
            raise click.ClickException(
                "Semantic search is not available on this DataHub instance.\n\n"
                "Possible reasons:\n"
                "  • Semantic search is not enabled in the backend configuration\n"
                "  • The entity types you're searching may not be configured for semantic search\n"
                "  • Embeddings may not have been generated for your entities\n\n"
                "Use 'datahub search diagnose --semantic' for more details, or use keyword search (default)."
            ) from e
        raise click.ClickException(f"Search failed: {e}") from e

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
    """Extract display name from entity following DataHub's entity patterns.

    DataHub entities store names in different locations based on type:
    - Most entities: properties.name or properties.displayName
    - CorpUser: username
    - Document: info.title
    - Some entities: name (top-level)

    Args:
        entity: Entity dict from GraphQL response

    Returns:
        Entity name/title or None if not found
    """
    # Pattern 1: properties.name / properties.displayName (Dataset, Chart, Dashboard, etc.)
    if "properties" in entity:
        return entity["properties"].get("name") or entity["properties"].get(
            "displayName"
        )

    # Pattern 2: info.title (Document entities)
    if "info" in entity and "title" in entity["info"]:
        return entity["info"]["title"]

    # Pattern 3: username (CorpUser entities)
    if "username" in entity:
        return entity["username"]

    # Pattern 4: name (top-level, e.g., CorpGroup, Tag, GlossaryTerm)
    if "name" in entity:
        return entity["name"]

    return None


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

        # Extract description (truncated)
        description = ""
        if "properties" in entity:
            desc = entity["properties"].get("description", "")
            description = desc[:77] + "..." if desc and len(desc) > 80 else desc

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


def print_semantic_diagnostics() -> None:
    """Print semantic search diagnostics to console."""
    click.echo("Checking semantic search configuration...\n")

    try:
        diagnostics = diagnose_semantic_search()

        click.echo("Semantic Search Diagnostics")
        click.echo("=" * 50)

        if diagnostics["semantic_enabled"]:
            click.echo("✓ Semantic search is ENABLED and available")

            config = diagnostics.get("config", {})
            embedding_config = config.get("embedding_config", {})

            if embedding_config and not config.get("error"):
                click.echo("\nEmbedding Configuration:")

                # Provider
                provider = embedding_config.get("provider", "Unknown")
                click.echo(f"  • Provider: {provider}")

                # Model ID
                model_id = embedding_config.get("modelId", "Unknown")
                click.echo(f"  • Model ID: {model_id}")

                # Model Embedding Key
                model_key = embedding_config.get("modelEmbeddingKey", "Unknown")
                click.echo(f"  • Embedding Key: {model_key}")

                # AWS Region (if applicable)
                aws_config = embedding_config.get("awsProviderConfig")
                if aws_config and aws_config.get("region"):
                    click.echo(f"  • AWS Region: {aws_config['region']}")

                # Enabled Entities
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
                click.echo("\nConfiguration:")
                click.echo("  • Backend: Configured correctly")
                click.echo("  • GraphQL endpoint: Available")
                click.echo("  • Status: Ready to use")
                if config.get("error"):
                    click.echo(
                        f"  ⚠ Could not fetch detailed config: {config['error']}"
                    )

            click.echo("\nNote: Results depend on which entity types are configured")
            click.echo("      and whether embeddings have been generated.")
        elif diagnostics["semantic_available"]:
            click.echo("✗ Semantic search endpoint exists but is NOT enabled")
            click.echo("\nPossible reasons:")
            click.echo("  • Backend configuration missing or incomplete")
            click.echo("  • Embeddings service not running")
            click.echo("  • Entity types not configured for semantic search")
            if diagnostics["semantic_error"]:
                click.echo(f"\nError: {diagnostics['semantic_error']}")
        else:
            click.echo("✗ Semantic search is NOT available")
            click.echo("\nReasons:")
            click.echo("  • Feature not enabled in DataHub backend")
            click.echo("  • GraphQL schema does not include semantic search")
            click.echo("  • May require DataHub upgrade or configuration")

        click.echo("\n" + "=" * 50)
        click.echo("\nRecommendations:")

        if not diagnostics["semantic_enabled"]:
            click.echo("  1. Contact your DataHub administrator")
            click.echo("  2. Check DataHub backend configuration")
            click.echo("  3. Verify embeddings service is running")
            click.echo("  4. Use keyword search (default) as alternative")
        else:
            click.echo("  1. Ensure entity types are configured for semantic search")
            click.echo("  2. Verify embeddings have been generated")
            click.echo("  3. Check entity-specific configuration")

    except Exception as e:
        click.echo(f"✗ Failed to diagnose semantic search: {e}", err=True)
        raise click.ClickException(
            "Could not connect to DataHub or check semantic search status"
        ) from e


def diagnose_semantic_search() -> dict:
    """Diagnose semantic search configuration.

    Returns:
        Dict with diagnostic information about semantic search status
    """
    graph = get_default_graph(ClientMode.CLI)

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
    except Exception:
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


@click.group(cls=DefaultGroup, default="query")
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

    See https://datahubproject.io/docs/cli for more examples.
    """
    pass


@search.command(name="query")
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
@click.option("--list-filters", is_flag=True, help="List all available filter fields")
@click.option("--describe-filter", help="Describe a specific filter field")
@click.option(
    "--facets-only", is_flag=True, help="Return only facets, no search results"
)
@click.option("--view", help="DataHub View URN to apply")
@upgrade.check_upgrade
def query(
    query: str,
    semantic: bool,
    filters_list: tuple,
    filters: Optional[str],
    limit: int,
    offset: int,
    sort_by: Optional[str],
    sort_order: str,
    output_format: str,
    table: bool,
    urns_only: bool,
    list_filters: bool,
    describe_filter: Optional[str],
    facets_only: bool,
    view: Optional[str],
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
    # Complex filters (AND/OR/NOT)
    datahub search "*" --filters '{"and": [{"platform": ["snowflake"]}, {"env": ["PROD"]}]}'

    \b
    # Output formats
    datahub search "users" --table
    datahub search "users" --urns-only
    datahub search "users" | jq '.searchResults[].entity.urn'

    \b
    # Discovery
    datahub search --list-filters
    datahub search --describe-filter platform
    datahub search "*" --filter entity_type=dataset --facets-only

    \b
    # Pagination
    datahub search "users" --limit 50 --offset 100

    See https://datahubproject.io/docs/cli for more examples.
    """
    # Handle discovery commands
    if list_filters:
        list_available_filters()
        return

    if describe_filter:
        describe_filter_func(describe_filter)
        return

    # Determine output format
    if table:
        output_format = "table"
    elif urns_only:
        output_format = "urns"

    # Validate arguments
    if limit < 1:
        raise click.UsageError("--limit must be at least 1")
    if limit > 50:
        click.echo("Warning: limit capped at 50", err=True)
        limit = 50

    if offset < 0:
        raise click.UsageError("--offset must be non-negative")

    # Parse and merge filters
    try:
        filter_obj = validate_and_merge_filters(list(filters_list), filters)
    except Exception as e:
        raise click.UsageError(str(e)) from e

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
        )
    except Exception as e:
        raise click.ClickException(str(e)) from e

    # Format and output results
    if facets_only:
        output = format_facets_output(results, output_format)
    elif output_format == "json":
        output = format_json_output(results)
    elif output_format == "table":
        output = format_table_output(results)
    elif output_format == "urns":
        output = format_urns_output(results)

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

    Checks semantic search configuration, backend connectivity,
    embedding provider, model details, and enabled entity types.

    Examples:

    \b
    # Text format (default)
    datahub search diagnose

    \b
    # JSON format
    datahub search diagnose --format json
    """
    if output_format == "json":
        diagnostics = diagnose_semantic_search()
        click.echo(json.dumps(diagnostics, indent=2))
    else:
        print_semantic_diagnostics()
