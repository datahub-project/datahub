import contextlib
import contextvars
import functools
import html
import inspect
import pathlib
import re
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Iterator,
    List,
    Literal,
    Optional,
    ParamSpec,
    TypeVar,
)

import asyncer
import jmespath
from datahub.cli.env_utils import get_boolean_env_variable
from datahub.errors import ItemNotFoundError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub.sdk.main_client import DataHubClient
from datahub.sdk.search_client import compile_filters
from datahub.sdk.search_filters import Filter, FilterDsl, load_filters
from datahub.utilities.ordered_set import OrderedSet
from fastmcp import FastMCP
from loguru import logger
from pydantic import BaseModel

_P = ParamSpec("_P")
_R = TypeVar("_R")
DESCRIPTION_LENGTH_HARD_LIMIT = 1000


def sanitize_html_content(text: str) -> str:
    """Remove HTML tags and decode HTML entities from text."""
    if not text:
        return text

    # Remove HTML tags (including img tags)
    text = re.sub(r"<[^>]+>", "", text)

    # Decode HTML entities
    text = html.unescape(text)

    return text.strip()


def truncate_with_ellipsis(text: str, max_length: int, suffix: str = "...") -> str:
    """Truncate text to max_length and add suffix if truncated."""
    if not text or len(text) <= max_length:
        return text

    # Account for suffix length
    actual_max = max_length - len(suffix)
    return text[:actual_max] + suffix


def sanitize_markdown_content(text: str) -> str:
    """Remove markdown-style embeds that contain encoded data from text, but preserve alt text."""
    if not text:
        return text

    # Remove markdown embeds with data URLs (base64 encoded content) but preserve alt text
    # Pattern: ![alt text](data:image/type;base64,encoded_data) -> alt text
    text = re.sub(r"!\[([^\]]*)\]\(data:[^)]+\)", r"\1", text)

    return text.strip()


def sanitize_and_truncate_description(text: str, max_length: int) -> str:
    """Sanitize HTML content and truncate to specified length."""
    if not text:
        return text

    try:
        # First sanitize HTML content
        sanitized = sanitize_html_content(text)

        # Then sanitize markdown content (preserving alt text)
        sanitized = sanitize_markdown_content(sanitized)

        # Then truncate if needed
        return truncate_with_ellipsis(sanitized, max_length)
    except Exception as e:
        logger.warning(f"Error sanitizing and truncating description: {e}")
        return text[:max_length] if len(text) > max_length else text


def truncate_descriptions(
    data: dict | list, max_length: int = DESCRIPTION_LENGTH_HARD_LIMIT
) -> None:
    """
    Recursively truncates values of keys named 'description' in a dictionary in place.
    """
    # TODO: path-aware truncate, for different length limits per entity type
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "description" and isinstance(value, str):
                data[key] = sanitize_and_truncate_description(value, max_length)
            elif isinstance(value, (dict, list)):
                truncate_descriptions(value)
    elif isinstance(data, list):
        for item in data:
            truncate_descriptions(item)


# See https://github.com/jlowin/fastmcp/issues/864#issuecomment-3103678258
# for why we need to wrap sync functions with asyncify.
def async_background(fn: Callable[_P, _R]) -> Callable[_P, Awaitable[_R]]:
    if inspect.iscoroutinefunction(fn):
        raise RuntimeError("async_background can only be used on non-async functions")

    @functools.wraps(fn)
    async def wrapper(*args: _P.args, **kwargs: _P.kwargs) -> _R:
        return await asyncer.asyncify(fn)(*args, **kwargs)

    return wrapper


mcp = FastMCP[None](name="datahub")


_mcp_dh_client = contextvars.ContextVar[DataHubClient]("_mcp_dh_client")


def get_datahub_client() -> DataHubClient:
    # Will raise a LookupError if no client is set.
    return _mcp_dh_client.get()


def set_datahub_client(client: DataHubClient) -> None:
    _mcp_dh_client.set(client)


@contextlib.contextmanager
def with_datahub_client(client: DataHubClient) -> Iterator[None]:
    token = _mcp_dh_client.set(client)
    try:
        yield
    finally:
        _mcp_dh_client.reset(token)


def _enable_cloud_fields(query: str) -> str:
    return query.replace("#[CLOUD]", "")


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    try:
        # Only DataHub Cloud has a frontend base url.
        _ = graph.frontend_base_url
    except ValueError:
        return False
    return True


def _execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    if _is_datahub_cloud(graph):
        query = _enable_cloud_fields(query)

    return graph.execute_graphql(
        query=query, variables=variables, operation_name=operation_name
    )


def inject_urls_for_urns(
    graph: DataHubGraph, response: Any, json_paths: List[str]
) -> None:
    if not _is_datahub_cloud(graph):
        return

    for path in json_paths:
        for item in jmespath.search(path, response) if path else [response]:
            if isinstance(item, dict) and item.get("urn"):
                # Update item in place with url, ensuring that urn and url are first.
                new_item = {"urn": item["urn"], "url": graph.url_for(item["urn"])}
                new_item.update({k: v for k, v in item.items() if k != "urn"})
                item.clear()
                item.update(new_item)


def maybe_convert_to_schema_field_urn(urn: str, column: Optional[str]) -> str:
    if column is not None:
        maybe_dataset_urn = Urn.from_string(urn)
        if not isinstance(maybe_dataset_urn, DatasetUrn):
            raise ValueError(
                f"Input urn should be a dataset urn if column is provided, but got {urn}."
            )
        urn = str(SchemaFieldUrn(maybe_dataset_urn, column))
    return urn


search_gql = (pathlib.Path(__file__).parent / "gql/search.gql").read_text()
semantic_search_gql = (
    pathlib.Path(__file__).parent / "gql/semantic_search.gql"
).read_text()
entity_details_fragment_gql = (
    pathlib.Path(__file__).parent / "gql/entity_details.gql"
).read_text()

queries_gql = (pathlib.Path(__file__).parent / "gql/queries.gql").read_text()


def _is_semantic_search_enabled() -> bool:
    """Check if semantic search is enabled via environment variable.

    IMPORTANT: Semantic search is an EXPERIMENTAL feature that is ONLY available on
    DataHub Cloud deployments with specific versions and configurations. This feature
    must be explicitly enabled by the DataHub team for your Cloud instance.

    Note:
        This function only checks the environment variable. Actual feature
        availability is validated when the DataHub client is used.
    """
    return get_boolean_env_variable("SEMANTIC_SEARCH_ENABLED", default=False)


def clean_gql_response(response: Any) -> Any:
    if isinstance(response, dict):
        banned_keys = {
            "__typename",
        }

        cleaned_response = {}
        for k, v in response.items():
            if k in banned_keys or v is None or v == []:
                continue
            cleaned_v = clean_gql_response(v)
            if cleaned_v is not None and cleaned_v != {}:
                cleaned_response[k] = cleaned_v

        return cleaned_response
    elif isinstance(response, list):
        return [clean_gql_response(item) for item in response]
    else:
        return response


def clean_get_entity_response(raw_response: dict) -> dict:
    response = clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Remove default field attributes (false values) to keep only meaningful data
        if fields := schema_metadata.get("fields"):
            for field in fields:
                if field.get("recursive") is False:
                    field.pop("recursive", None)
                if field.get("isPartOfKey") is False:
                    field.pop("isPartOfKey", None)

    return response


@mcp.tool(description="Get an entity by its DataHub URN.")
@async_background
def get_entity(urn: str) -> dict:
    client = get_datahub_client()

    if not client._graph.exists(urn):
        # TODO: Ideally we use the `exists` field to check this, and also deal with soft-deleted entities.
        raise ItemNotFoundError(f"Entity {urn} not found")

    # Execute the GraphQL query
    variables = {"urn": urn}
    result = _execute_graphql(
        client._graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    inject_urls_for_urns(client._graph, result, [""])
    truncate_descriptions(result)

    return clean_get_entity_response(result)


def _search_implementation(
    query: str,
    filters: Optional[Filter | str],
    num_results: int,
    search_strategy: Optional[Literal["semantic", "keyword"]] = None,
) -> dict:
    """Core search implementation that can use either semantic or keyword search."""
    client = get_datahub_client()

    # As of 2025-07-25: Our Filter type is a tagged/discriminated union.
    #
    # We've observed that some tools (e.g. Cursor) don't support discriminated
    # unions in their JSON schema validation, and hence reject valid tool calls
    # before they're even passed to our MCP server.
    # Beyond that, older LLMs (e.g. Claude Desktop w/ Sonnet 3.5) have a tendency
    # to pass tool args as JSON-encoded strings instead of proper objects.
    #
    # To work around these issues, we allow stringified JSON filters that we
    # parse on our end. The FastMCP library used to have built-in support for
    # handling this, but removed it in
    # https://github.com/jlowin/fastmcp/commit/7b9696405b1427f4dc5430891166286744b3dab5
    if isinstance(filters, str):
        filters = load_filters(filters)
    types, compiled_filters = compile_filters(filters)
    variables = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
    }

    # Choose GraphQL query and operation based on strategy
    use_semantic = search_strategy == "semantic"
    if use_semantic:
        gql_query = semantic_search_gql
        operation_name = "semanticSearch"
        response_key = "semanticSearchAcrossEntities"
    else:
        gql_query = search_gql
        operation_name = "search"
        response_key = "scrollAcrossEntities"
        # Add scrollId for keyword search (maintaining compatibility)
        variables["scrollId"] = None

    response = _execute_graphql(
        client._graph,
        query=gql_query,
        variables=variables,
        operation_name=operation_name,
    )[response_key]

    if num_results == 0 and isinstance(response, dict):
        # Hack to support num_results=0 without support for it in the backend.
        response.pop("searchResults", None)
        response.pop("count", None)

    return clean_gql_response(response)


# Define enhanced search tool when semantic search is enabled
def enhanced_search(
    query: str = "*",
    search_strategy: Optional[Literal["semantic", "keyword"]] = None,
    filters: Optional[Filter | str] = None,
    num_results: int = 10,
) -> dict:
    """Enhanced search across DataHub entities with semantic and keyword capabilities.

    This tool supports two search strategies with different strengths:

    SEMANTIC SEARCH (search_strategy="semantic"):
    - Uses AI embeddings to understand meaning and concepts, not just exact text matches
    - Finds conceptually related results even when terminology differs
    - Best for: exploratory queries, business-focused searches, finding related concepts
    - Examples: "customer analytics", "financial reporting data", "ML training datasets"
    - Will match: tables named "user_behavior", "client_metrics", "consumer_data" for "customer analytics"

    KEYWORD SEARCH (search_strategy="keyword" or default):
    - Traditional full-text search matching exact words and phrases
    - Fast and precise when you know specific names or technical terms
    - Best for: exact entity names, technical identifiers, known column names
    - Examples: "user_transactions", "revenue_2024", "customer_id"
    - Will match: exact text appearances of these terms

    WHEN TO USE EACH:
    - Use semantic when: user asks conceptual questions ("show me sales data", "find customer information")
    - Use keyword when: user provides specific names ("find table user_events", "show dataset named revenue_jan_2024")
    - Use keyword when: searching for technical terms, column names, or exact identifiers

    Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
    To explore the data catalog and get aggregate statistics, use the wildcard '*' as the query and set `filters: null`. This provides
    facets showing platform distribution, entity types, and other aggregate insights across the entire catalog, plus a representative
    sample of entities.

    A typical workflow will involve multiple calls to this search tool, with each call refining the filters based on the facets/aggregations returned in the previous call.
    After the final search is performed, you'll want to use the other tools to get more details about the relevant entities.

    Here are some example filters:
    - All Looker assets
    ```
    {"platform": ["looker"]}
    ```
    - Production environment warehouse assets
    ```
    {
      "and": [
        {"env": ["PROD"]},
        {"platform": ["snowflake", "bigquery", "redshift"]}
      ]
    }
    ```
    - All non-Snowflake tables
    ```
    {
      "and":[
        {"entity_type": ["DATASET"]},
        {"entity_subtype": ["Table"]},
        {"not": {"platform": ["snowflake"]}}
      ]
    }
    ```

    SEARCH STRATEGY EXAMPLES:
    - Semantic: "customer behavior data" → finds user_analytics, client_metrics, consumer_tracking
    - Keyword: "customer_behavior" → finds tables with exact name "customer_behavior"
    - Semantic: "financial performance metrics" → finds revenue_kpis, profit_analysis, financial_dashboards
    - Keyword: "financial_performance_metrics" → finds exact table name matches
    """
    return _search_implementation(query, filters, num_results, search_strategy)


# Define original search tool for backward compatibility
def search(
    query: str = "*",
    filters: Optional[Filter | str] = None,
    num_results: int = 10,
) -> dict:
    """Search across DataHub entities.

    Returns both a truncated list of results and facets/aggregations that can be used to iteratively refine the search filters.
    To search for all entities, use the wildcard '*' as the query and set `filters: null`.

    A typical workflow will involve multiple calls to this search tool, with each call refining the filters based on the facets/aggregations returned in the previous call.
    After the final search is performed, you'll want to use the other tools to get more details about the relevant entities.

    Here are some example filters:
    - All Looker assets
    ```
    {"platform": ["looker"]}
    ```
    - Production environment warehouse assets
    ```
    {
      "and": [
        {"env": ["PROD"]},
        {"platform": ["snowflake", "bigquery", "redshift"]}
      ]
    }
    ```
    - All non-Snowflake tables
    ```
    {
      "and":[
        {"entity_type": ["DATASET"]},
        {"entity_subtype": ["Table"]},
        {"not": {"platform": ["snowflake"]}}
      ]
    }
    ```
    """
    return _search_implementation(query, filters, num_results, "keyword")


@mcp.tool(
    description="Use this tool to get the SQL queries associated with a dataset or a dataset column."
)
@async_background
def get_dataset_queries(
    urn: str, column: Optional[str] = None, start: int = 0, count: int = 10
) -> dict:
    client = get_datahub_client()

    urn = maybe_convert_to_schema_field_urn(urn, column)

    entities_filter = FilterDsl.custom_filter(
        field="entities", condition="EQUAL", values=[urn]
    )
    _, compiled_filters = compile_filters(entities_filter)

    # Set up variables for the query
    variables = {
        "input": {"start": start, "count": count, "orFilters": compiled_filters}
    }

    # Execute the GraphQL query
    result = _execute_graphql(
        client._graph,
        query=queries_gql,
        variables=variables,
        operation_name="listQueries",
    )["listQueries"]

    for query in result["queries"]:
        if query.get("subjects"):
            query["subjects"] = _deduplicate_subjects(query["subjects"])

    return clean_gql_response(result)


def _deduplicate_subjects(subjects: list[dict]) -> list[str]:
    # The "subjects" field returns every dataset and schema field associated with the query.
    # While this is useful for our backend to have, it's not useful here because
    # we can just look at the query directly. So we'll narrow it down to the unique
    # list of dataset urns.
    updated_subjects: OrderedSet[str] = OrderedSet()
    for subject in subjects:
        with contextlib.suppress(KeyError):
            updated_subjects.add(subject["dataset"]["urn"])
    return list(updated_subjects)


class AssetLineageDirective(BaseModel):
    urn: str
    upstream: bool
    downstream: bool
    max_hops: int
    extra_filters: Optional[Filter]


class AssetLineageAPI:
    def __init__(self, graph: DataHubGraph) -> None:
        self.graph = graph

    def get_degree_filter(self, max_hops: int) -> Filter:
        """
        max_hops: Maximum number of hops to search for lineage
        """
        if max_hops == 1 or max_hops == 2:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=[str(i) for i in range(1, max_hops + 1)],
            )
        elif max_hops >= 3:
            return FilterDsl.custom_filter(
                field="degree",
                condition="EQUAL",
                values=["1", "2", "3+"],
            )
        else:
            raise ValueError(f"Invalid number of hops: {max_hops}")

    def get_lineage(
        self, asset_lineage_directive: AssetLineageDirective
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        filter = self.get_degree_filter(asset_lineage_directive.max_hops)
        if asset_lineage_directive.extra_filters:
            filter = FilterDsl.and_(filter, asset_lineage_directive.extra_filters)
        types, compiled_filters = compile_filters(filter)
        variables = {
            "urn": asset_lineage_directive.urn,
            "start": 0,
            "count": 30,
            "types": types,
            "orFilters": compiled_filters,
            "searchFlags": {"skipHighlighting": True, "maxAggValues": 3},
        }
        if asset_lineage_directive.upstream:
            result["upstreams"] = clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "UPSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )
        if asset_lineage_directive.downstream:
            result["downstreams"] = clean_gql_response(
                _execute_graphql(
                    self.graph,
                    query=entity_details_fragment_gql,
                    variables={
                        "input": {
                            **variables,
                            "direction": "DOWNSTREAM",
                        }
                    },
                    operation_name="GetEntityLineage",
                )["searchAcrossLineage"]
            )

        return result


@mcp.tool(
    description="""\
Use this tool to get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc. \
Set upstream to True for upstream lineage, False for downstream lineage.
Set `column: null` to get lineage for entire dataset or for entity type other than dataset.
Setting max_hops to 3 is equivalent to unlimited hops.
Usage and format of filters is same as that in search tool.
"""
)
@async_background
def get_lineage(
    urn: str,
    column: Optional[str],
    filters: Optional[Filter | str] = None,
    upstream: bool = True,
    max_hops: int = 1,
) -> dict:
    client = get_datahub_client()
    # NOTE: See comment in search tool for why we parse filters as strings.
    if isinstance(filters, str):
        # The Filter type already has a BeforeValidator that parses JSON strings.
        filters = load_filters(filters)

    lineage_api = AssetLineageAPI(client._graph)

    urn = maybe_convert_to_schema_field_urn(urn, column)
    asset_lineage_directive = AssetLineageDirective(
        urn=urn,
        upstream=upstream,
        downstream=not upstream,
        max_hops=max_hops,
        extra_filters=filters,
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive)
    inject_urls_for_urns(client._graph, lineage, ["*.searchResults[].entity"])
    truncate_descriptions(lineage)
    return lineage


def register_search_tools(mcp_instance: FastMCP) -> None:
    """Register the appropriate search tool based on environment configuration."""
    if _is_semantic_search_enabled():
        # Note: Actual semantic search availability is validated at runtime when used
        # This allows the tool to be registered even if validation would fail,
        # but provides clear error messages when semantic search is actually attempted

        # Register enhanced search tool with semantic capabilities (as "search")
        mcp_instance.tool(name="search", description=enhanced_search.__doc__)(
            async_background(enhanced_search)
        )
    else:
        # Register original search tool for backward compatibility (as "search")
        mcp_instance.tool(name="search", description=search.__doc__)(
            async_background(search)
        )


# Register search tools on the global MCP instance
register_search_tools(mcp)
