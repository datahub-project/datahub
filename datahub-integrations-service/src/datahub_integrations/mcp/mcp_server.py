import contextlib
import contextvars
import functools
import html
import inspect
import json
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
import cachetools
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
QUERY_LENGTH_HARD_LIMIT = 5000


def sanitize_html_content(text: str) -> str:
    """
    Remove HTML tags and decode HTML entities from text.

    Uses a bounded regex pattern to prevent ReDoS (Regular Expression Denial of Service)
    attacks. The pattern limits matching to tags with at most 100 characters between < and >,
    which prevents backtracking on malicious input like "<" followed by millions of characters
    without a closing ">".
    """
    if not text:
        return text

    # Use bounded regex to prevent ReDoS (max 100 chars between < and >)
    text = re.sub(r"<[^<>]{0,100}>", "", text)

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


def truncate_query(query: str) -> str:
    """
    Truncate a SQL query if it exceeds the maximum length.
    """
    return truncate_with_ellipsis(
        query, QUERY_LENGTH_HARD_LIMIT, suffix="... [truncated]"
    )


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


def _enable_newer_gms_fields(query: str) -> str:
    """
    Enable newer GMS fields by removing the #[NEWER_GMS] marker suffix.

    Converts:
        someField  #[NEWER_GMS]
    To:
        someField
    """
    lines = query.split("\n")
    cleaned_lines = [
        line.replace(" #[NEWER_GMS]", "").replace("\t#[NEWER_GMS]", "")
        for line in lines
    ]
    return "\n".join(cleaned_lines)


def _disable_newer_gms_fields(query: str) -> str:
    """
    Disable newer GMS fields by commenting out lines with #[NEWER_GMS] marker.

    Converts:
        someField  #[NEWER_GMS]
    To:
        # someField  #[NEWER_GMS]
    """
    lines = query.split("\n")
    processed_lines = []
    for line in lines:
        if "#[NEWER_GMS]" in line:
            # Comment out the line by prefixing with #
            processed_lines.append("# " + line)
        else:
            processed_lines.append(line)
    return "\n".join(processed_lines)


def _enable_cloud_fields(query: str) -> str:
    """
    Enable cloud fields by removing the #[CLOUD] marker suffix.

    Converts:
        someField  #[CLOUD]
    To:
        someField
    """
    lines = query.split("\n")
    cleaned_lines = [
        line.replace(" #[CLOUD]", "").replace("\t#[CLOUD]", "") for line in lines
    ]
    return "\n".join(cleaned_lines)


def _disable_cloud_fields(query: str) -> str:
    """
    Disable cloud fields by commenting out lines with #[CLOUD] marker.

    Converts:
        someField  #[CLOUD]
    To:
        # someField  #[CLOUD]
    """
    lines = query.split("\n")
    processed_lines = []
    for line in lines:
        if "#[CLOUD]" in line:
            # Comment out the line by prefixing with #
            processed_lines.append("# " + line)
        else:
            processed_lines.append(line)
    return "\n".join(processed_lines)


# Cache to track whether newer GMS fields are supported for each graph instance
# Key: id(graph), Value: bool indicating if newer GMS fields are supported
_newer_gms_fields_support_cache: dict[int, bool] = {}


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    """Check if the graph instance is DataHub Cloud.

    Cloud instances typically have newer GMS versions with additional fields.
    This heuristic uses the presence of frontend_base_url to detect Cloud instances.
    """
    # Allow disabling newer GMS field detection via environment variable
    # This is useful when the GMS version doesn't support all newer fields
    if get_boolean_env_variable("DISABLE_NEWER_GMS_FIELD_DETECTION", default=False):
        logger.debug(
            "Newer GMS field detection disabled via DISABLE_NEWER_GMS_FIELD_DETECTION"
        )
        return False

    try:
        # Only DataHub Cloud has a frontend base url.
        # Cloud instances typically run newer GMS versions with additional fields.
        _ = graph.frontend_base_url
    except ValueError:
        return False
    return True


def _is_field_validation_error(error_msg: str) -> bool:
    """Check if the error is a GraphQL field validation error."""
    return "FieldUndefined" in error_msg or "ValidationError" in error_msg


def _execute_graphql(
    graph: DataHubGraph,
    *,
    query: str,
    operation_name: Optional[str] = None,
    variables: Optional[Dict[str, Any]] = None,
) -> Any:
    graph_id = id(graph)
    original_query = query  # Keep original for fallback

    # Detect if this is a DataHub Cloud instance
    is_cloud = _is_datahub_cloud(graph)

    # Process CLOUD tags
    if is_cloud:
        query = _enable_cloud_fields(query)
    else:
        query = _disable_cloud_fields(query)

    # Process NEWER_GMS tags
    # Check if we've already determined newer GMS fields support for this graph
    if graph_id in _newer_gms_fields_support_cache:
        supports_newer_fields = _newer_gms_fields_support_cache[graph_id]
        if supports_newer_fields:
            query = _enable_newer_gms_fields(query)
        else:
            query = _disable_newer_gms_fields(query)
    else:
        # First attempt: try with newer GMS fields if it's detected as cloud
        # (Cloud instances typically run newer GMS versions)
        if is_cloud:
            query = _enable_newer_gms_fields(query)
        else:
            query = _disable_newer_gms_fields(query)
        # Cache the initial detection result
        _newer_gms_fields_support_cache[graph_id] = is_cloud

    try:
        # Execute the GraphQL query
        result = graph.execute_graphql(
            query=query, variables=variables, operation_name=operation_name
        )
        return result

    except Exception as e:
        error_msg = str(e)

        # Check if this is a field validation error and we haven't tried fallback yet
        if _is_field_validation_error(
            error_msg
        ) and _newer_gms_fields_support_cache.get(graph_id, False):
            logger.warning(
                f"GraphQL schema validation error detected. "
                f"Retrying without newer GMS fields as fallback. "
                f"Error: {error_msg}"
            )

            # Update cache to indicate newer GMS fields are NOT supported
            _newer_gms_fields_support_cache[graph_id] = False

            # Retry with newer GMS fields disabled - process both tags again
            try:
                fallback_query = original_query
                # Reprocess CLOUD tags
                if is_cloud:
                    fallback_query = _enable_cloud_fields(fallback_query)
                else:
                    fallback_query = _disable_cloud_fields(fallback_query)
                # Disable newer GMS fields for fallback
                fallback_query = _disable_newer_gms_fields(fallback_query)

                result = graph.execute_graphql(
                    query=fallback_query,
                    variables=variables,
                    operation_name=operation_name,
                )
                logger.info(
                    f"Fallback query succeeded without newer GMS fields for operation: {operation_name}"
                )
                return result
            except Exception as fallback_error:
                logger.exception(
                    f"Fallback query also failed for {operation_name or 'query'}: {fallback_error}"
                )
                raise fallback_error

        # Keep essential error logging for troubleshooting with full stack trace
        logger.exception(
            f"GraphQL {operation_name or 'query'} failed: {e}\n"
            f"Cloud instance: {is_cloud}\n"
            f"Newer GMS fields enabled: {_newer_gms_fields_support_cache.get(graph_id, 'unknown')}\n"
            f"Variables: {variables}"
        )
        raise


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
smart_search_gql = (pathlib.Path(__file__).parent / "gql/smart_search.gql").read_text()
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


# Global View Configuration
DISABLE_DEFAULT_VIEW = get_boolean_env_variable(
    "DATAHUB_MCP_DISABLE_DEFAULT_VIEW", default=False
)
VIEW_CACHE_TTL_SECONDS = 300  # 5 minutes hardcoded

# Log configuration on startup
if not DISABLE_DEFAULT_VIEW:
    logger.info("Default view application ENABLED (cache TTL: 5 minutes)")
else:
    logger.info("Default view application DISABLED")


@cachetools.cached(cache=cachetools.TTLCache(maxsize=1, ttl=VIEW_CACHE_TTL_SECONDS))
def fetch_global_default_view(graph: DataHubGraph) -> Optional[str]:
    """
    Fetch the organization's default global view URN unless disabled.
    Cached for VIEW_CACHE_TTL_SECONDS seconds.
    Returns None if disabled or if no default view is configured.
    """
    # Return None immediately if feature is disabled
    if DISABLE_DEFAULT_VIEW:
        return None

    query = """
    query getGlobalViewsSettings {
        globalViewsSettings {
            defaultView
        }
    }
    """

    result = _execute_graphql(graph, query=query)
    settings = result.get("globalViewsSettings")
    if settings:
        view_urn = settings.get("defaultView")
        if view_urn:
            logger.debug(f"Fetched global default view: {view_urn}")
            return view_urn
    logger.debug("No global default view configured")
    return None


def clean_gql_response(response: Any) -> Any:
    """
    Clean GraphQL response by removing metadata and empty values.

    Recursively removes:
    - __typename fields (GraphQL metadata not useful for consumers)
    - None values
    - Empty arrays []
    - Empty dicts {} (after cleaning)
    - Base64-encoded images from description fields (can be huge - 2MB!)

    Args:
        response: Raw GraphQL response (dict, list, or primitive)

    Returns:
        Cleaned response with same structure but without noise
    """
    if isinstance(response, dict):
        banned_keys = {
            "__typename",
        }

        cleaned_response = {}
        for k, v in response.items():
            if k in banned_keys or v is None or v == []:
                continue
            cleaned_v = clean_gql_response(v)
            # Strip base64 images from description fields
            if (
                k == "description"
                and isinstance(cleaned_v, str)
                and "base64" in cleaned_v
            ):
                import re

                cleaned_v = re.sub(
                    r"data:image/[^;]+;base64,[A-Za-z0-9+/=]+",
                    "[image removed]",
                    cleaned_v,
                )
                cleaned_v = re.sub(
                    r"!\[[^\]]*\]\(data:image/[^)]+\)", "[image removed]", cleaned_v
                )

            if cleaned_v is not None and cleaned_v != {}:
                cleaned_response[k] = cleaned_v

        return cleaned_response
    elif isinstance(response, list):
        return [clean_gql_response(item) for item in response]
    else:
        return response


def clean_get_entities_response(raw_response: dict) -> dict:
    response = clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Clean schemaMetadata.fields to keep important fields while reducing size
        # Keep: fieldPath (required), type, description (truncated), and truthy isPartOfKey/recursive
        if fields := schema_metadata.get("fields"):
            cleaned_fields = []
            for f in fields:
                if not f.get("fieldPath"):  # Skip fields without fieldPath
                    continue

                field_dict = {"fieldPath": f.get("fieldPath")}

                # Add type if present
                if field_type := f.get("type"):
                    field_dict["type"] = field_type

                # Add description if present (truncated)
                if description := f.get("description"):
                    field_dict["description"] = description[:120]

                # Add isPartOfKey only if truthy
                if f.get("isPartOfKey"):
                    field_dict["isPartOfKey"] = True

                # Add recursive only if truthy
                if f.get("recursive"):
                    field_dict["recursive"] = True

                cleaned_fields.append(field_dict)

            schema_metadata["fields"] = cleaned_fields

    # Truncate long view definition to prevent context window issues
    if response and (view_properties := response.get("viewProperties")):
        if view_properties.get("logic"):
            view_properties["logic"] = truncate_query(view_properties["logic"])

    return response


def get_entities(urns: List[str] | str) -> List[dict] | dict:
    """Get detailed information about one or more entities by their DataHub URNs.

    IMPORTANT: Pass an array of URNs to retrieve multiple entities in a single call - this is much
    more efficient than calling this tool multiple times. When examining search results, always pass
    an array with the top 3-10 result URNs to compare and find the best match.

    Accepts an array of URNs or a single URN. Supports all entity types including datasets,
    assertions, incidents, dashboards, charts, users, groups, and more. The response fields vary
    based on the entity type.
    """
    client = get_datahub_client()

    # Handle single URN for backward compatibility
    if isinstance(urns, str):
        urns = [urns]
        return_single = True
    else:
        return_single = False

    results = []
    for urn in urns:
        try:
            # Check if entity exists first
            if not client._graph.exists(urn):
                logger.warning(f"Entity not found during existence check: {urn}")
                if return_single:
                    raise ItemNotFoundError(f"Entity {urn} not found")
                results.append({"error": f"Entity {urn} not found", "urn": urn})
                continue

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

            results.append(clean_get_entities_response(result))

        except Exception as e:
            logger.warning(f"Error fetching entity {urn}: {e}")
            if return_single:
                raise
            results.append({"error": str(e), "urn": urn})

    # Return single dict if single URN was passed, array otherwise
    return results[0] if return_single else results


def _convert_custom_filter_format(filters_obj: Any) -> Any:
    """
    Convert chatbot's intuitive {"custom": {...}} format to the format expected by _CustomCondition.

    Transforms:
    {"custom": {"field": "urn", "condition": "EQUAL", "values": [...]}}

    Into:
    {"field": "urn", "condition": "EQUAL", "values": [...]}

    This allows the discriminator to correctly identify it as _custom.
    """
    if isinstance(filters_obj, dict):
        # Check if this is a "custom" or "custom_condition" wrapper that needs unwrapping
        if len(filters_obj) == 1 and (
            "custom" in filters_obj or "custom_condition" in filters_obj
        ):
            wrapper_key = "custom" if "custom" in filters_obj else "custom_condition"
            custom_content = filters_obj[wrapper_key]
            # Ensure it has the expected structure for _CustomCondition
            if isinstance(custom_content, dict) and "field" in custom_content:
                return custom_content

        # Recursively process nested filters (for "and", "or", etc.)
        result = {}
        for key, value in filters_obj.items():
            if isinstance(value, (list, dict)):
                result[key] = _convert_custom_filter_format(value)
            else:
                result[key] = value
        return result
    elif isinstance(filters_obj, list):
        # Process list of filters
        return [_convert_custom_filter_format(item) for item in filters_obj]
    else:
        # Return primitive values unchanged
        return filters_obj


def _search_implementation(
    query: str,
    filters: Optional[Filter | str],
    num_results: int,
    search_strategy: Optional[Literal["semantic", "keyword", "ersatz_semantic"]] = None,
) -> dict:
    """Core search implementation that can use semantic, keyword, or ersatz_semantic search."""
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
        # Parse JSON first to allow preprocessing
        filters_dict = json.loads(filters)

        # Convert "custom" wrapper to direct _custom format for compatibility
        filters_dict = _convert_custom_filter_format(filters_dict)

        filters = load_filters(filters_dict)

    types, compiled_filters = compile_filters(filters)

    # Fetch and apply default view (returns None if disabled or not configured)
    # Note: If view fetching fails, the search will fail to ensure data governance
    view_urn = fetch_global_default_view(client._graph)
    if view_urn:
        logger.debug(f"Applying default view: {view_urn}")
    else:
        logger.debug("No default view to apply")

    variables = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "viewUrn": view_urn,  # Will be None if disabled or not set
    }

    # Choose GraphQL query and operation based on strategy
    if search_strategy == "semantic":
        gql_query = semantic_search_gql
        operation_name = "semanticSearch"
        response_key = "semanticSearchAcrossEntities"
    elif search_strategy == "ersatz_semantic":
        # Smart search: keyword search with rich entity details for reranking
        gql_query = smart_search_gql
        operation_name = "smartSearch"
        response_key = "scrollAcrossEntities"
        variables["scrollId"] = None
    else:
        # Default: keyword search
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
    Results are ordered by relevance and importance - examine top results first.

    This tool supports two search strategies with different strengths:

    SEMANTIC SEARCH (search_strategy="semantic"):
    - Uses AI embeddings to understand meaning and concepts, not just exact text matches
    - Finds conceptually related results even when terminology differs
    - Best for: exploratory queries, business-focused searches, finding related concepts
    - Examples: "customer analytics", "financial reporting data", "ML training datasets"
    - Will match: tables named "user_behavior", "client_metrics", "consumer_data" for "customer analytics"

    KEYWORD SEARCH (search_strategy="keyword" or default):
    - Structured full-text search - **always start queries with /q**
    - Supports full boolean logic: AND (default), OR, NOT, parentheses, field searches
    - Examples:
      • /q user_transactions → exact terms (AND is default)
      • /q wizard OR pet → entities containing either term
      • /q revenue_* → wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      • /q tag:PII → search by tag name
      • /q "user data table" → exact phrase matching
      • /q (sales OR revenue) AND quarterly → complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    WHEN TO USE EACH:
    - Use semantic when: user asks conceptual questions ("show me sales data", "find customer information")
    - Use keyword when: user provides specific names (/q user_events, /q revenue_jan_2024)
    - Use keyword when: searching for technical terms, boolean logic, or exact identifiers

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
      → Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
       → Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filters={"tag": ["urn:li:tag:pii"]}, num_results=30)
       → Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

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
    - Keyword: /q customer_behavior → finds tables with exact name "customer_behavior"
    - Keyword: /q customer OR user → finds tables with either term
    - Semantic: "financial performance metrics" → finds revenue_kpis, profit_analysis, financial_dashboards
    - Keyword: /q financial_performance_metrics → finds exact table name matches
    - Keyword: /q (financial OR revenue) AND metrics → complex boolean logic

    LIMITATIONS:
    Cannot sort by specific fields like downstream_count or query_count.

    Note: Search results are already ranked by importance - frequently queried and
    high-usage entities appear first. For "most important tables", search by
    importance tags/terms or use the top-ranked results from a broad search.
    """
    return _search_implementation(query, filters, num_results, search_strategy)


# Define original search tool for backward compatibility
def search(
    query: str = "*",
    filters: Optional[Filter | str] = None,
    num_results: int = 10,
) -> dict:
    """Search across DataHub entities using structured full-text search.
    Results are ordered by relevance and importance - examine top results first.

    SEARCH SYNTAX:
    - Structured full-text search - **always start queries with /q**
    - **Recommended: Use + operator for AND** (handles punctuation better than quotes)
    - Supports full boolean logic: AND (default), OR, NOT, parentheses, field searches
    - Examples:
      • /q user+transaction → requires both terms (better for field names with _ or punctuation)
      • /q point+sale+app → requires all terms (works with point_of_sale_app_usage)
      • /q wizard OR pet → entities containing either term
      • /q revenue* → wildcard matching (revenue_2023, revenue_2024, revenue_monthly, etc.)
      • /q tag:PII → search by tag name
      • /q "exact table name" → exact phrase matching (use sparingly)
      • /q (sales OR revenue) AND quarterly → complex boolean combinations
    - Fast and precise for exact matching, technical terms, and complex queries
    - Best for: entity names, identifiers, column names, or any search needing boolean logic

    FACET EXPLORATION - Discover metadata without returning results:
    - Set num_results=0 to get ONLY facets (no search results)
    - Facets show ALL tags, glossaryTerms, platforms, domains used in the catalog
    - Example: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
      → Returns facets showing all tags/glossaryTerms applied to datasets
    - Use this to discover what metadata exists before doing filtered searches

    TYPICAL WORKFLOW:
    1. Facet exploration: search(query="*", filters={"entity_type": ["DATASET"]}, num_results=0)
       → Examine tags/glossaryTerms facets to see what metadata exists
    2. Filtered search: search(query="*", filters={"tag": ["urn:li:tag:pii"]}, num_results=30)
       → Get entities with specific tag using URN from step 1
    3. Get details: Use get_entities() on specific results

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
    - Filter by domain (MUST use full URN format)
    ```
    {"domain": ["urn:li:domain:marketing"]}
    {"domain": ["urn:li:domain:9f8e7d6c-5b4a-3928-1765-432109876543", "urn:li:domain:7c6b5a49-3827-1654-9032-8f7e6d5c4b3a"]}
    ```
    IMPORTANT: Domain filters require full URN format starting with "urn:li:domain:",
    NOT short names like "marketing" or "customer". Domain URNs can be readable names
    or GUIDs. Always search with {"entity_type": ["domain"]}
    to find valid domain URNs first, then use the exact URN from the results.

    SUPPORTED FILTER TYPES (only these will work):
    - entity_type: ["dataset"], ["dashboard", "chart"], ["corp_user"], ["corp_group"]
    - entity_subtype: ["Table"], ["View", "Model"]
    - platform: ["snowflake"], ["looker", "tableau"]
    - domain: ["urn:li:domain:marketing"] (full URN required)
    - container: ["urn:li:container:..."] (full URN required)
    - tag: ["urn:li:tag:PII"] (full tag URN required)
    - glossary_term: ["urn:li:glossaryTerm:uuid"] (full term URN required)
    - owner: ["urn:li:corpuser:alice", "urn:li:corpGroup:marketing"] (full user or group URN required)
    - custom: {"field": "fieldName", "condition": "EQUAL", "values": [...]}
    - status: ["NOT_SOFT_DELETED"] (for non-deleted entities)
    - env: ["PROD"], ["DEV", "STAGING"] (Should not use unless explicitly requested)
    - and: [filter1, filter2] (combines multiple filters)
    - or: [filter1, filter2] (matches any filter)
    - not: {"entity_type": ["dataset"]} (excludes matches)

    CRITICAL: Use only ONE discriminator key per filter object. Never mix
    entity_type with custom, domain, etc. at the same level. Use "and" or "or" to combine.

    SEARCH STRATEGY EXAMPLES:
    - /q customer+behavior → finds tables with both terms (works with customer_behavior fields)
    - /q customer OR user → finds tables with either term
    - /q (financial OR revenue) AND metrics → complex boolean logic

    LIMITATIONS:
    Cannot sort by specific fields like downstream_count or query_count.

    Note: Search results are already ranked by importance - frequently queried and
    high-usage entities appear first. For "most important tables", search by
    importance tags/terms or use the top-ranked results from a broad search.
    """
    return _search_implementation(query, filters, num_results, "keyword")


def get_dataset_queries(
    urn: str,
    column: Optional[str] = None,
    source: Optional[Literal["MANUAL", "SYSTEM"]] = None,
    start: int = 0,
    count: int = 10,
) -> dict:
    """Get SQL queries associated with a dataset or column to understand usage patterns.

    This tool retrieves actual SQL queries that reference a specific dataset or column.
    Useful for understanding how data is used, common JOIN patterns, typical filters,
    and aggregation logic.

    PARAMETERS:

    source - Filter by query origin:
    - "MANUAL": Queries written by users in query editors (real SQL patterns)
    - "SYSTEM": Queries extracted from BI tools/dashboards (production usage)
    - null: Return both types (default)

    COMMON USE CASES:

    1. SQL Generation - Learn real query patterns:
       get_dataset_queries(urn, source="MANUAL", count=5-10)
       → See how users actually write SQL against this table
       → Discover common JOINs, aggregations, filters
       → Match organizational SQL conventions and patterns

    2. Production usage analysis:
       get_dataset_queries(urn, source="SYSTEM", count=20)
       → See how dashboards and reports query this data
       → Understand which queries run in production
       → Identify critical query patterns

    3. Column usage patterns:
       get_dataset_queries(urn, column="customer_id", source="MANUAL", count=5)
       → See how a specific column is used in queries
       → Learn filtering and grouping patterns for that column
       → Discover relationships via JOIN patterns

    4. General usage exploration:
       get_dataset_queries(urn, count=10)
       → Get mix of manual and system queries
       → Understand overall table usage

    EXAMPLES:

    - Get manual queries for SQL generation:
      get_dataset_queries(
          urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)",
          source="MANUAL",
          count=10
      )

    - Get dashboard queries (production usage):
      get_dataset_queries(
          urn="urn:li:dataset:(...)",
          source="SYSTEM",
          count=20
      )

    - Column-specific query patterns:
      get_dataset_queries(
          urn="urn:li:dataset:(...)",
          column="created_at",
          source="MANUAL",
          count=5
      )

    RESPONSE STRUCTURE:
    - total: Total number of queries matching criteria
    - start: Starting offset
    - count: Number of results returned
    - queries: Array of query objects with:
      - urn: Query identifier
      - properties.statement.value: The actual SQL text
      - properties.statement.language: Query language (SQL, etc.)
      - properties.source: MANUAL or SYSTEM
      - properties.name: Optional query name
      - platform: Source platform
      - subjects: Referenced datasets/columns (deduplicated to dataset URNs)

    ANALYZING RETRIEVED QUERIES:
    Once you retrieve queries, examine the SQL statements to identify:
    - JOIN patterns: Which tables are joined? On what keys?
    - Aggregations: Common SUM, COUNT, AVG, GROUP BY patterns
    - Filters: Typical WHERE clauses, date range logic
    - Column usage: Which columns appear frequently vs rarely
    - CTEs and subqueries: Complex query structures

    BEST PRACTICES:
    - For SQL generation: Use source="MANUAL" (count=5-10) to see real user patterns
    - For production analysis: Use source="SYSTEM" to see dashboard/report queries
    - Start with moderate count (5-10) to avoid overwhelming context
    - If no queries found (total=0), proceed without query examples - not all tables have queries
    - Parse the SQL statements yourself to find patterns - they are not full-text searchable
    """
    client = get_datahub_client()

    urn = maybe_convert_to_schema_field_urn(urn, column)

    entities_filter = FilterDsl.custom_filter(
        field="entities", condition="EQUAL", values=[urn]
    )
    _, compiled_filters = compile_filters(entities_filter)

    # Set up variables for the query
    variables = {
        "input": {
            "start": start,
            "count": count,
            "orFilters": compiled_filters,
        }
    }

    # Add optional source filter
    if source is not None:
        variables["input"]["source"] = source

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

        # Truncate long SQL queries to prevent context window issues
        if queryProperties := query.get("properties"):
            queryProperties["statement"]["value"] = truncate_query(
                queryProperties["statement"]["value"]
            )

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
    max_results: int


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
        self,
        asset_lineage_directive: AssetLineageDirective,
        query: Optional[str] = None,
    ) -> Dict[str, Any]:
        result: Dict[str, Any] = {}

        filter = self.get_degree_filter(asset_lineage_directive.max_hops)
        if asset_lineage_directive.extra_filters:
            filter = FilterDsl.and_(filter, asset_lineage_directive.extra_filters)
        types, compiled_filters = compile_filters(filter)
        variables = {
            "urn": asset_lineage_directive.urn,
            "query": query or "*",
            "start": 0,
            "count": asset_lineage_directive.max_results,
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


def get_lineage(
    urn: str,
    column: Optional[str],
    query: Optional[str] = None,
    filters: Optional[Filter | str] = None,
    upstream: bool = True,
    max_hops: int = 1,
    max_results: int = 30,
) -> dict:
    """Get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc.

    Set upstream to True for upstream lineage, False for downstream lineage.
    Set `column: null` to get lineage for entire dataset or for entity type other than dataset.
    Setting max_hops to 3 is equivalent to unlimited hops.
    Usage and format of filters is same as that in search tool.

    QUERY PARAMETER - Search within lineage results:
    You can filter lineage results using the `query` parameter with same /q syntax as search tool:
    - /q workspace.growthgestaofin → find tables in specific schema
    - /q customer+transactions → find entities with both terms
    - /q looker OR tableau → find dashboards on either platform
    - /q * → get all lineage results (default)

    Examples:
    - Find specific table in 643 downstreams: query="workspace.growthgestaofin.qs_retention"
    - Find Looker dashboards in lineage: query="/q tag:looker"
    - Get all results: query="*" or omit parameter

    COUNT PARAMETER - Control result size:
    - Default: 30 results
    - For aggregation: count=30 is sufficient (facets computed on ALL items server-side)
    - For finding specific item: Increase count or use query to filter
    - Example: count=100 for larger result sets

    WHEN TO USE QUERY vs COUNT:
    - User asks "is X affected?" → Use query to filter for X specifically
    - Large lineage (>30 items) → Keep count=30, use facets for aggregation
    - Need complete list → Increase count only if total ≤100
    """
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
        max_results=max_results,
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive, query=query)
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

# Register get_lineage tool
mcp.tool(name="get_lineage", description=get_lineage.__doc__)(
    async_background(get_lineage)
)

# Register get_dataset_queries tool
mcp.tool(name="get_dataset_queries", description=get_dataset_queries.__doc__)(
    async_background(get_dataset_queries)
)

# Register get_entities tool
mcp.tool(name="get_entities", description=get_entities.__doc__)(
    async_background(get_entities)
)
