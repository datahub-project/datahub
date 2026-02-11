"""DataHub MCP Server Implementation.

IMPORTANT: This file is kept in sync between two repositories.

When making changes, ensure both versions remain identical. Use relative imports
(e.g., `from ._token_estimator import ...`) instead of absolute imports to maintain
compatibility across both repositories.
"""

import contextlib
import contextvars
import functools
import html
import inspect
import json
import os
import pathlib
import re
import string
import threading
from enum import Enum
from typing import (
    Any,
    Awaitable,
    Callable,
    Dict,
    Generator,
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
from fastmcp.tools.tool import Tool as FastMCPTool
from json_repair import repair_json
from loguru import logger
from pydantic import BaseModel

# IMPORTANT: Use relative imports to maintain compatibility across repositories
from ._token_estimator import TokenCountEstimator
from .tools.descriptions import update_description
from .tools.documents import grep_documents, search_documents
from .tools.domains import remove_domains, set_domains
from .tools.get_me import get_me
from .tools.owners import add_owners, remove_owners
from .tools.save_document import is_save_document_enabled, save_document
from .tools.structured_properties import (
    add_structured_properties,
    remove_structured_properties,
)
from .tools.tags import add_tags, remove_tags
from .tools.terms import (
    add_glossary_terms,
    remove_glossary_terms,
)
from .version_requirements import TOOL_VERSION_REQUIREMENTS

_P = ParamSpec("_P")
_R = TypeVar("_R")
T = TypeVar("T")
DESCRIPTION_LENGTH_HARD_LIMIT = 1000
QUERY_LENGTH_HARD_LIMIT = 5000
DOCUMENT_CONTENT_CHAR_LIMIT = 8000

# Maximum token count for tool responses to prevent context window issues
# As per telemetry tool result length goes upto
TOOL_RESPONSE_TOKEN_LIMIT = int(os.getenv("TOOL_RESPONSE_TOKEN_LIMIT", 80000))

# Per-entity schema token budget for field truncation
# Assumes ~5 entities per response: 80K total / 5 = 16K per entity
ENTITY_SCHEMA_TOKEN_BUDGET = int(os.getenv("ENTITY_SCHEMA_TOKEN_BUDGET", "16000"))


class ToolType(Enum):
    """Tool type enumeration for different tool types."""

    SEARCH = "search"  # Datahub search tools
    MUTATION = "mutation"  # Datahub mutation tools
    USER = "user"  # Datahub user tools
    DEFAULT = "default"  # Fallback tag


def _select_results_within_budget(
    results: Iterator[T],
    fetch_entity: Callable[[T], dict],
    max_results: int = 10,
    token_budget: Optional[int] = None,
) -> Generator[T, None, None]:
    """
    Generator that yields results within token budget.

    Generic helper that works for any result structure. Caller provides a function
    to extract/clean entity for token counting (can mutate the result).

    Yields results until:
    - max_results reached, OR
    - token_budget would be exceeded (and we have at least 1 result)

    Args:
        results: Iterator of result objects of any type T (memory efficient)
        fetch_entity: Function that extracts entity dict from result for token counting.
                   Can mutate the result to clean/update entity in place.
                   Signature: T -> dict (entity for token counting)
                   Example: lambda r: (r.__setitem__("entity", clean(r["entity"])), r["entity"])[1]
        max_results: Maximum number of results to return
        token_budget: Token budget (defaults to 90% of TOOL_RESPONSE_TOKEN_LIMIT)

    Yields:
        Original result objects of type T (possibly mutated by fetch_entity)
    """
    if token_budget is None:
        # Use 90% of limit as safety buffer:
        # - Token estimation is approximate, not exact
        # - Response wrapper adds overhead
        # - Better to return fewer results that fit than exceed limit
        token_budget = int(TOOL_RESPONSE_TOKEN_LIMIT * 0.9)

    total_tokens = 0
    results_count = 0

    # Consume iterator up to max_results
    for i, result in enumerate(results):
        if i >= max_results:
            break
        # Extract (and possibly clean) entity using caller's lambda
        # Note: fetch_entity may mutate result to clean/update entity in place
        entity = fetch_entity(result)

        # Estimate token cost
        entity_tokens = TokenCountEstimator.estimate_dict_tokens(entity)

        # Check if adding this entity would exceed budget
        if total_tokens + entity_tokens > token_budget:
            if results_count == 0:
                # Always yield at least 1 result
                logger.warning(
                    f"First result ({entity_tokens:,} tokens) exceeds budget ({token_budget:,}), "
                    "yielding it anyway"
                )
                yield result  # Yield original result structure
                results_count += 1
                total_tokens += entity_tokens
            else:
                # Have at least 1 result, stop here to stay within budget
                logger.info(
                    f"Stopping at {results_count} results (next would exceed {token_budget:,} token budget)"
                )
                break
        else:
            yield result  # Yield original result structure
            results_count += 1
            total_tokens += entity_tokens

    logger.info(
        f"Selected {results_count} results using {total_tokens:,} tokens "
        f"(budget: {token_budget:,})"
    )


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
        try:
            return await asyncer.asyncify(fn)(*args, **kwargs)
        except Exception:
            # Log with full stack trace before FastMCP catches it
            logger.exception(
                f"Tool function {fn.__name__} failed with args={args}, kwargs={kwargs}"
            )
            raise

    return wrapper


def _register_tool(
    mcp_instance: FastMCP,
    name: str,
    fn: Callable,
    *,
    description: Optional[str] = None,
    tags: Optional[set] = None,
) -> None:
    """Register a tool on the MCP instance and capture its version requirement.

    This is a convenience wrapper that:
    1. Wraps the sync function with async_background
    2. Registers it on the MCP instance
    3. Reads the _version_requirement attribute (set by @min_version decorator)
       and populates TOOL_VERSION_REQUIREMENTS

    Args:
        mcp_instance: The FastMCP instance to register on.
        name: The tool name (may differ from fn.__name__).
        fn: The tool function (sync).
        description: Tool description. Defaults to fn.__doc__.
        tags: Optional set of tag strings.
    """
    mcp_instance.tool(
        name=name,
        description=description or fn.__doc__,
        tags=tags,
    )(async_background(fn))

    req = getattr(fn, "_version_requirement", None)
    if req is not None:
        TOOL_VERSION_REQUIREMENTS[name] = req


mcp = FastMCP[None](
    name="datahub",
)


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
    """Check if the error is a GraphQL field/type validation or syntax error.

    Includes InvalidSyntax because unknown types (like Document on older GMS)
    cause syntax errors rather than validation errors.
    """
    return (
        "FieldUndefined" in error_msg
        or "ValidationError" in error_msg
        or "InvalidSyntax" in error_msg
    )


def execute_graphql(
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
    newer_gms_enabled_for_this_query = False
    if graph_id in _newer_gms_fields_support_cache:
        supports_newer_fields = _newer_gms_fields_support_cache[graph_id]
        if supports_newer_fields:
            query = _enable_newer_gms_fields(query)
            newer_gms_enabled_for_this_query = True
        else:
            query = _disable_newer_gms_fields(query)
    else:
        # First attempt: try with newer GMS fields if it's detected as cloud
        # (Cloud instances typically run newer GMS versions)
        if is_cloud:
            query = _enable_newer_gms_fields(query)
            newer_gms_enabled_for_this_query = True
        else:
            query = _disable_newer_gms_fields(query)
        # Cache the initial detection result
        _newer_gms_fields_support_cache[graph_id] = is_cloud

    logger.debug(
        f"Executing GraphQL {operation_name or 'query'}: "
        f"is_cloud={is_cloud}, newer_gms_enabled={newer_gms_enabled_for_this_query}"
    )
    logger.debug(
        f"GraphQL query for {operation_name or 'query'}:\n{query}\nVariables: {variables}"
    )

    try:
        # Execute the GraphQL query
        result = graph.execute_graphql(
            query=query, variables=variables, operation_name=operation_name
        )
        return result

    except Exception as e:
        error_msg = str(e)

        # Check if this is a field validation error and we tried with newer GMS fields enabled
        # Only retry if we had newer GMS fields enabled in the query that just failed
        if _is_field_validation_error(error_msg) and newer_gms_enabled_for_this_query:
            logger.warning(
                f"GraphQL schema validation error detected for {operation_name or 'query'}. "
                f"Retrying without newer GMS fields as fallback."
            )
            logger.exception(e)

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

                logger.debug(
                    f"Retry {operation_name or 'query'} with NEWER_GMS fields disabled: "
                    f"is_cloud={is_cloud}"
                )

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
        elif (
            _is_field_validation_error(error_msg)
            and not newer_gms_enabled_for_this_query
        ):
            # Field validation error but NEWER_GMS fields were already disabled
            logger.error(
                f"GraphQL schema validation error for {operation_name or 'query'} "
                f"but NEWER_GMS fields were already disabled (is_cloud={is_cloud}). "
                f"This may indicate a CLOUD-only field being used on a non-cloud instance, "
                f"or a field that's unavailable in this GMS version."
            )
            logger.exception(e)

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
    if column:
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
query_entity_gql = (pathlib.Path(__file__).parent / "gql/query_entity.gql").read_text()
related_documents_gql = (
    pathlib.Path(__file__).parent / "gql/related_documents.gql"
).read_text()


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

    result = execute_graphql(graph, query=query)
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


def _sort_fields_by_priority(fields: List[dict]) -> Iterator[dict]:
    """
    Yield schema fields sorted by priority for deterministic truncation.

    Priority order:
    1. Primary/partition keys (isPartOfKey, isPartitioningKey)
    2. Fields with descriptions
    3. Fields with tags or glossary terms
    4. Alphabetically by fieldPath

    Each field gets a score tuple for sorting:
    - key_score: 2 if isPartOfKey, 1 if isPartitioningKey, 0 otherwise
    - has_description: 1 if description exists, 0 otherwise
    - has_tags: 1 if tags or glossary terms exist, 0 otherwise
    - fieldPath: for alphabetical tiebreaker

    Sorted in descending order by score components, then ascending by fieldPath.

    Args:
        fields: List of field dicts from GraphQL response

    Yields:
        Fields in priority order (generator for memory efficiency)
    """
    # Score each field with tuple: (key_score, has_description, has_tags, fieldPath, index)
    scored_fields = []
    for idx, field in enumerate(fields):
        # Score key fields (highest priority)
        key_score = 0
        if field.get("isPartOfKey"):
            key_score = 2
        elif field.get("isPartitioningKey"):
            key_score = 1

        # Score fields with descriptions
        has_description = 1 if field.get("description") else 0

        # Score fields with tags or glossary terms
        has_tags_or_terms = 0
        if field.get("tags") or field.get("glossaryTerms"):
            has_tags_or_terms = 1

        # Get fieldPath for alphabetical sorting (tiebreaker)
        field_path = field.get("fieldPath", "")

        # Store as (score_tuple, original_index, field)
        # Sort descending by scores, ascending by fieldPath
        score_tuple = (-key_score, -has_description, -has_tags_or_terms, field_path)
        scored_fields.append((score_tuple, idx, field))

    # Sort by score tuple
    scored_fields.sort(key=lambda x: x[0])

    # Yield fields in sorted order
    for _, _, field in scored_fields:
        yield field


def _clean_schema_fields(
    sorted_fields: Iterator[dict], editable_map: dict[str, dict]
) -> Iterator[dict]:
    """
    Clean and normalize schema fields for response.

    Yields cleaned field dicts with only essential properties for SQL generation
    and understanding schema structure. Merges user-edited metadata (descriptions,
    tags, glossary terms) into fields with "edited*" prefix when they differ.

    Note: All fields are expected to have fieldPath (always requested in GraphQL).
    If fieldPath is missing, it indicates a data quality issue.

    Args:
        sorted_fields: Iterator of fields in priority order
        editable_map: Map of fieldPath -> editable field data for merging

    Yields:
        Cleaned field dicts with merged editable data (generator for memory efficiency)
    """
    for f in sorted_fields:
        # fieldPath is required - it's always requested in GraphQL and is essential
        # for identifying the field. If missing, fail fast rather than silently skipping.
        field_dict = {"fieldPath": f["fieldPath"]}

        # Add type if present (essential for SQL)
        if field_type := f.get("type"):
            field_dict["type"] = field_type

        # Add nativeDataType if present (important for SQL type casting)
        if native_type := f.get("nativeDataType"):
            field_dict["nativeDataType"] = native_type

        # Add description if present (truncated)
        if description := f.get("description"):
            field_dict["description"] = description[:120]

        # Add nullable if present (important for SQL NULL handling)
        if f.get("nullable") is not None:
            field_dict["nullable"] = f.get("nullable")

        # Add label if present (useful for human-readable names)
        if label := f.get("label"):
            field_dict["label"] = label

        # Add isPartOfKey only if truthy (important for joins)
        if f.get("isPartOfKey"):
            field_dict["isPartOfKey"] = True

        # Add isPartitioningKey only if truthy (important for query optimization)
        if f.get("isPartitioningKey"):
            field_dict["isPartitioningKey"] = True

        # Add recursive only if truthy
        if f.get("recursive"):
            field_dict["recursive"] = True

        # Add deprecation status if present (warn about deprecated fields)
        if schema_field_entity := f.get("schemaFieldEntity"):
            if deprecation := schema_field_entity.get("deprecation"):
                if deprecation.get("deprecated"):
                    field_dict["deprecated"] = {
                        "deprecated": True,
                        "note": deprecation.get("note", "")[:120],  # Truncate note
                    }

        # Add tags if present (keep minimal info for classification context)
        if tags := f.get("tags"):
            if tag_list := tags.get("tags"):
                # Keep just tag names for context
                field_dict["tags"] = [
                    t["tag"]["properties"]["name"]
                    for t in tag_list
                    if t.get("tag", {}).get("properties")
                    and t["tag"]["properties"].get("name")
                ]

        # Add glossary terms if present (keep minimal info for business context)
        if glossary_terms := f.get("glossaryTerms"):
            if terms_list := glossary_terms.get("terms"):
                # Keep just term names for context
                field_dict["glossaryTerms"] = [
                    t["term"]["properties"]["name"]
                    for t in terms_list
                    if t.get("term", {}).get("properties")
                    and t["term"]["properties"].get("name")
                ]

        # Merge editable metadata if available for this field
        field_path = f["fieldPath"]
        if editable := editable_map.get(field_path):
            # Add editedDescription if it differs from system description
            if editable_desc := editable.get("description"):
                system_desc = field_dict.get("description", "")
                # Only add if different (token optimization)
                if editable_desc[:120] != system_desc:  # Compare truncated versions
                    field_dict["editedDescription"] = editable_desc[:120]

            # Add editedTags if present and different
            if editable_tags := editable.get("tags"):
                if tag_list := editable_tags.get("tags"):
                    edited_tag_names = [
                        t["tag"]["properties"]["name"]
                        for t in tag_list
                        if t.get("tag", {}).get("properties")
                        and t["tag"]["properties"].get("name")
                    ]
                    if edited_tag_names:
                        system_tags = field_dict.get("tags", [])
                        if edited_tag_names != system_tags:
                            field_dict["editedTags"] = edited_tag_names

            # Add editedGlossaryTerms if present and different
            if editable_terms := editable.get("glossaryTerms"):
                if terms_list := editable_terms.get("terms"):
                    edited_term_names = [
                        t["term"]["properties"]["name"]
                        for t in terms_list
                        if t.get("term", {}).get("properties")
                        and t["term"]["properties"].get("name")
                    ]
                    if edited_term_names:
                        system_terms = field_dict.get("glossaryTerms", [])
                        if edited_term_names != system_terms:
                            field_dict["editedGlossaryTerms"] = edited_term_names

        yield field_dict


def clean_get_entities_response(
    raw_response: dict,
    *,
    sort_fn: Optional[Callable[[List[dict]], Iterator[dict]]] = None,
    offset: int = 0,
    limit: Optional[int] = None,
) -> dict:
    """
    Clean and optimize entity responses for LLM consumption.

    Performs several transformations to reduce token usage while preserving essential information:

    1. **Clean GraphQL artifacts**: Removes __typename, null values, empty objects/arrays
       (via clean_gql_response)

    2. **Schema field processing** (if schemaMetadata.fields exists):
       - Sorts fields using sort_fn (defaults to _sort_fields_by_priority)
       - Cleans each field to keep only essential properties (fieldPath, type, description, etc.)
       - Merges editableSchemaMetadata into fields with "edited*" prefix (editedDescription,
         editedTags, editedGlossaryTerms) - only included when they differ from system values
       - Applies pagination (offset/limit) with token budget constraint
       - Field selection stops when EITHER limit is reached OR ENTITY_SCHEMA_TOKEN_BUDGET is exceeded
       - Adds schemaFieldsTruncated metadata when fields are cut

    3. **Remove duplicates**: Deletes editableSchemaMetadata after merging into schemaMetadata

    4. **Truncate view definitions**: Limits SQL view logic to QUERY_LENGTH_HARD_LIMIT

    The result is optimized for LLM tool responses: reduced token usage, no duplication,
    clear distinction between system-generated and user-curated content.

    Args:
        raw_response: Raw entity dict from GraphQL query
        sort_fn: Optional custom function to sort fields. If None, uses _sort_fields_by_priority.
                 Should take a list of field dicts and return an iterator of sorted fields.
        offset: Number of fields to skip after sorting (default: 0)
        limit: Maximum number of fields to include after offset (default: None = unlimited)

    Returns:
        Cleaned entity dict optimized for LLM consumption
    """
    response = clean_gql_response(raw_response)

    if response and (schema_metadata := response.get("schemaMetadata")):
        # Remove empty platformSchema to reduce response clutter
        if platform_schema := schema_metadata.get("platformSchema"):
            schema_value = platform_schema.get("schema")
            if not schema_value or schema_value == "":
                del schema_metadata["platformSchema"]

        # Clean schemaMetadata.fields to keep important fields while reducing size
        # Keep fields essential for SQL generation and understanding schema structure
        if fields := schema_metadata.get("fields"):
            total_fields = len(fields)  # Use original count before any filtering

            # Build editable map from editableSchemaMetadata for merging
            # Make this safe - if duplicate fieldPaths exist, last one wins (no failure)
            editable_map = {}
            if editable_schema := response.get("editableSchemaMetadata"):
                if editable_fields := editable_schema.get("editableSchemaFieldInfo"):
                    for editable_field in editable_fields:
                        if field_path := editable_field.get("fieldPath"):
                            editable_map[field_path] = editable_field

            # Sort fields using custom function or default priority sorting
            sort_function = sort_fn if sort_fn is not None else _sort_fields_by_priority
            sorted_fields = sort_function(fields)
            cleaned_fields = _clean_schema_fields(sorted_fields, editable_map)

            # Apply offset, limit, and token budget to select fields
            selected_fields: list[dict] = []
            accumulated_tokens = 0
            fields_remaining = limit  # None means unlimited

            for idx, field in enumerate(cleaned_fields):
                # Skip fields before offset
                if idx < offset:
                    continue

                field_tokens = TokenCountEstimator.estimate_dict_tokens(field)

                # Stop if we exceed token budget (keep at least 1 field after offset)
                if (
                    accumulated_tokens + field_tokens > ENTITY_SCHEMA_TOKEN_BUDGET
                    and selected_fields
                ):
                    logger.info(
                        f"Truncating schema fields: {len(selected_fields)}/{total_fields - offset} "
                        f"fields fit in {ENTITY_SCHEMA_TOKEN_BUDGET:,} token budget "
                        f"(accumulated {accumulated_tokens:,} tokens, offset={offset})"
                    )
                    break

                # Stop if we've hit the limit
                if fields_remaining is not None and fields_remaining <= 0:
                    logger.info(
                        f"Reached limit: {len(selected_fields)} fields selected (limit={limit}, offset={offset})"
                    )
                    break

                selected_fields.append(field)
                accumulated_tokens += field_tokens
                if fields_remaining is not None:
                    fields_remaining -= 1

            # Add truncation metadata if fields were cut
            # Truncation occurs if we have fewer fields than (total - offset)
            fields_after_offset = total_fields - offset
            if len(selected_fields) < fields_after_offset:
                schema_metadata["schemaFieldsTruncated"] = {
                    "totalFields": total_fields,
                    "includedFields": len(selected_fields),
                    "offset": offset,
                }
                logger.warning(
                    f"Schema fields truncated: included {len(selected_fields)}/{fields_after_offset} fields "
                    f"(offset={offset}, {accumulated_tokens:,} tokens, budget: {ENTITY_SCHEMA_TOKEN_BUDGET:,})"
                )

            schema_metadata["fields"] = selected_fields

    # Remove editableSchemaMetadata - data has been merged into schemaMetadata fields
    if "editableSchemaMetadata" in response:
        del response["editableSchemaMetadata"]

    # Truncate long view definition to prevent context window issues
    if response and (view_properties := response.get("viewProperties")):
        if view_properties.get("logic"):
            view_properties["logic"] = truncate_query(view_properties["logic"])

    # Truncate document content to prevent context window issues
    if response and (info := response.get("info")):
        if contents := info.get("contents"):
            if text := contents.get("text"):
                if len(text) > DOCUMENT_CONTENT_CHAR_LIMIT:
                    original_length = len(text)
                    truncate_at = DOCUMENT_CONTENT_CHAR_LIMIT
                    contents["text"] = (
                        text[:truncate_at]
                        + "\n\n[Content truncated. Use grep_documents(start_offset={}) to continue.]".format(
                            truncate_at
                        )
                    )
                    contents["_truncated"] = True
                    contents["_originalLengthChars"] = original_length
                    contents["_truncatedAtChar"] = truncate_at
                    logger.info(
                        f"Document content truncated: {original_length:,} -> {truncate_at:,} chars"
                    )

    return response


def clean_related_documents_response(raw_response: dict) -> dict:
    """
    Clean and optimize related documents response for LLM consumption.

    Applies basic GraphQL cleaning to remove __typename, null values, empty objects/arrays.
    This is a simpler version of clean_get_entities_response focused on related documents.

    Args:
        raw_response: Raw related documents dict from GraphQL query (RelatedDocumentsResult)

    Returns:
        Cleaned related documents dict optimized for LLM consumption
    """
    return clean_gql_response(raw_response)


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

    # Handle JSON-stringified arrays (same issue as filters in search tool)
    # Some MCP clients/LLMs pass arrays as JSON strings instead of proper lists
    if isinstance(urns, str):
        urns_str = urns.strip()  # Remove leading/trailing whitespace

        # Try to parse as JSON array first
        if urns_str.startswith("["):
            try:
                # Use json_repair to handle malformed JSON from LLMs
                urns = json.loads(repair_json(urns_str))
                return_single = False
            except (json.JSONDecodeError, Exception) as e:
                logger.warning(
                    f"Failed to parse URNs as JSON array: {e}. Treating as single URN."
                )
                # Not valid JSON, treat as single URN string
                urns = [urns_str]
                return_single = True
        else:
            # Single URN string
            urns = [urns_str]
            return_single = True
    else:
        return_single = False

    # Trim whitespace from each URN (defensive against string concatenation issues)
    urns = [urn.strip() for urn in urns]

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

            # Special handling for Query entities (not part of Entity union type)
            is_query = urn.startswith("urn:li:query:")

            # Execute the appropriate GraphQL query
            variables = {"urn": urn}
            if is_query:
                result = execute_graphql(
                    client._graph,
                    query=query_entity_gql,
                    variables=variables,
                    operation_name="GetQueryEntity",
                )["entity"]
            else:
                result = execute_graphql(
                    client._graph,
                    query=entity_details_fragment_gql,
                    variables=variables,
                    operation_name="GetEntity",
                )["entity"]

            # Check if entity data was returned
            if result is None:
                raise ItemNotFoundError(
                    f"Entity {urn} exists but no data could be retrieved. "
                    f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
                )

            # Fetch related documents for supported entity types
            try:
                related_docs_input = {"start": 0, "count": 10}
                related_docs_result = execute_graphql(
                    client._graph,
                    query=related_documents_gql,
                    variables={"urn": urn, "input": related_docs_input},
                    operation_name="getRelatedDocuments",
                )
                if (
                    related_docs_result
                    and related_docs_result.get("entity")
                    and related_docs_result["entity"].get("relatedDocuments")
                ):
                    result["relatedDocuments"] = clean_related_documents_response(
                        related_docs_result["entity"]["relatedDocuments"]
                    )
            except Exception as e:
                logger.debug(
                    f"Could not fetch related documents for {urn}: {e}. This entity type may not support related documents."
                )

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


def list_schema_fields(
    urn: str,
    keywords: Optional[List[str] | str] = None,
    limit: int = 100,
    offset: int = 0,
) -> dict:
    """List schema fields for a dataset, with optional keyword filtering and pagination.

    Useful when schema fields were truncated in search results (schemaFieldsTruncated present)
    and you need to explore specific columns. Supports pagination for large schemas.

    Args:
        urn: Dataset URN
        keywords: Optional keywords to filter schema fields (OR matching).
                 - Single string: Treated as one keyword (NOT split on whitespace). Use for field names or exact phrases.
                 - List of strings: Multiple keywords, matches any (OR logic).
                 - None or empty list: Returns all fields in priority order (same as get_entities).
                 Matches against fieldPath, description, label, tags, and glossary terms.
                 Matching fields are returned first, sorted by match count.
        limit: Maximum number of fields to return (default: 100)
        offset: Number of fields to skip for pagination (default: 0)

    Returns:
        Dictionary with:
        - urn: The dataset URN
        - fields: List of schema fields (paginated)
        - totalFields: Total number of fields in the schema
        - returned: Number of fields actually returned
        - remainingCount: Number of fields not included after offset (accounts for limit and token budget)
        - matchingCount: Number of fields that matched keywords (if keywords provided, None otherwise)
        - offset: The offset used

    Examples:
        # Single keyword (string) - search for exact field name or phrase
        list_schema_fields(urn="urn:li:dataset:(...)", keywords="user_email")
        # Returns fields matching "user_email" (like user_email_address, primary_user_email)

        # Multiple keywords (list) - OR matching
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["email", "user"])
        # Returns fields containing "email" OR "user" (user_email, contact_email, user_id, etc.)

        # Pagination through all fields
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=0)   # First 100
        list_schema_fields(urn="urn:li:dataset:(...)", limit=100, offset=100) # Next 100

        # Combine filtering + pagination
        list_schema_fields(urn="urn:li:dataset:(...)", keywords=["user"], limit=50, offset=0)
    """
    client = get_datahub_client()

    # Normalize keywords to list (None means no filtering)
    keywords_lower = None
    if keywords is not None:
        if isinstance(keywords, str):
            keywords = [keywords]
        keywords_lower = [kw.lower() for kw in keywords]

    # Fetch entity
    if not client._graph.exists(urn):
        raise ItemNotFoundError(f"Entity {urn} not found")

    # Execute GraphQL query to get full schema
    variables = {"urn": urn}
    result = execute_graphql(
        client._graph,
        query=entity_details_fragment_gql,
        variables=variables,
        operation_name="GetEntity",
    )["entity"]

    # Check if entity data was returned
    if result is None:
        raise ItemNotFoundError(
            f"Entity {urn} exists but no data could be retrieved. "
            f"This can happen if the entity has no aspects ingested yet, or if there's a permissions issue."
        )

    # Apply same preprocessing as get_entities
    inject_urls_for_urns(client._graph, result, [""])
    truncate_descriptions(result)

    # Extract total field count before processing
    total_fields = len(result.get("schemaMetadata", {}).get("fields", []))

    if total_fields == 0:
        return {
            "urn": urn,
            "fields": [],
            "totalFields": 0,
            "returned": 0,
            "remainingCount": 0,
            "matchingCount": None,
            "offset": offset,
        }

    # Define custom sorting function for keyword matching
    sort_fn = None
    matching_count = None

    if keywords_lower:
        # Helper function to score a field by keyword matches
        def score_field_by_keywords(field: dict) -> int:
            """
            Score a field by counting keyword match coverage across its metadata.

            Scoring logic (OR matching):
            - Each keyword gets +1 if it appears in ANY searchable text (substring match)
            - Multiple occurrences of the same keyword in one text still count as +1
            - Higher score = more aspects of the field match the keywords

            Searchable texts (in order of priority):
            1. fieldPath (column name)
            2. description
            3. label
            4. tag names
            5. glossary term names

            Example:
                keywords = ["email", "user"]
                field = {
                    "fieldPath": "user_email",        # matches both
                    "description": "User's email",    # matches both
                    "tags": ["PII"]                   # matches neither
                }
                Score = 4 (email in fieldPath + email in desc + user in fieldPath + user in desc)

            Returns:
                Integer score (0 = no matches, higher = more coverage)
            """
            searchable_texts = [
                field.get("fieldPath", ""),
                field.get("description", ""),
                field.get("label", ""),
            ]

            # Add tag names
            if tags := field.get("tags"):
                if tag_list := tags.get("tags"):
                    searchable_texts.extend(
                        [
                            (t.get("tag", {}).get("properties") or {}).get("name", "")
                            for t in tag_list
                        ]
                    )

            # Add glossary term names
            if glossary_terms := field.get("glossaryTerms"):
                if terms_list := glossary_terms.get("terms"):
                    searchable_texts.extend(
                        [
                            (t.get("term", {}).get("properties") or {}).get("name", "")
                            for t in terms_list
                        ]
                    )

            # Count keyword coverage: +1 for each (keyword, text) pair that matches
            # Note: Substring matching, case-insensitive
            return sum(
                1
                for kw in keywords_lower
                for text in searchable_texts
                if text and kw in text.lower()
            )

        # Pre-compute matching count (need all fields for this)
        fields_for_counting = result.get("schemaMetadata", {}).get("fields", [])
        matching_count = sum(
            1 for field in fields_for_counting if score_field_by_keywords(field) > 0
        )

        # Define sort function for clean_get_entities_response
        def sort_by_keyword_match(fields: List[dict]) -> Iterator[dict]:
            """Sort fields by keyword match count (descending), then alphabetically."""
            scored_fields = [
                (score_field_by_keywords(field), field) for field in fields
            ]
            scored_fields.sort(key=lambda x: (-x[0], x[1].get("fieldPath", "")))
            return iter(field for _, field in scored_fields)

        sort_fn = sort_by_keyword_match

    # Use clean_get_entities_response for consistent processing
    cleaned_entity = clean_get_entities_response(
        result,
        sort_fn=sort_fn,
        offset=offset,
        limit=limit,
    )

    # Extract the cleaned fields and metadata
    schema_metadata = cleaned_entity.get("schemaMetadata", {})
    cleaned_fields = schema_metadata.get("fields", [])

    # Calculate how many fields remain after what we returned
    # This accounts for both pagination and token budget constraints
    remaining_count = total_fields - offset - len(cleaned_fields)

    return {
        "urn": urn,
        "fields": cleaned_fields,
        "totalFields": total_fields,
        "returned": len(cleaned_fields),
        "remainingCount": remaining_count,
        "matchingCount": matching_count,
        "offset": offset,
    }


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
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
) -> dict:
    """Core search implementation that can use semantic, keyword, or ersatz_semantic search."""
    client = get_datahub_client()

    # Cap num_results at 50 to prevent excessive requests
    num_results = min(num_results, 50)

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

    variables: Dict[str, Any] = {
        "query": query,
        "types": types,
        "orFilters": compiled_filters,
        "count": max(num_results, 1),  # 0 is not a valid value for count.
        "start": offset,
        "viewUrn": view_urn,  # Will be None if disabled or not set
    }

    # Add sorting if requested
    if sort_by is not None:
        sort_order_enum = "ASCENDING" if sort_order == "asc" else "DESCENDING"
        variables["sortInput"] = {
            "sortCriteria": [{"field": sort_by, "sortOrder": sort_order_enum}]
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
        response_key = "searchAcrossEntities"
    else:
        # Default: keyword search
        gql_query = search_gql
        operation_name = "search"
        response_key = "searchAcrossEntities"

    response = execute_graphql(
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
# TODO: Consider adding sorting support (sort_by, sort_order parameters) similar to search() tool if needed.
def enhanced_search(
    query: str = "*",
    search_strategy: Optional[Literal["semantic", "keyword"]] = None,
    filters: Optional[Filter | str] = None,
    num_results: int = 10,
    offset: int = 0,
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

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      • First page: offset=0, num_results=10
      • Second page: offset=10, num_results=10
      • Third page: offset=20, num_results=10

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
    return _search_implementation(
        query, filters, num_results, search_strategy, offset=offset
    )


# Define original search tool for backward compatibility
def search(
    query: str = "*",
    filters: Optional[Filter | str] = None,
    num_results: int = 10,
    sort_by: Optional[str] = None,
    sort_order: Optional[Literal["asc", "desc"]] = "desc",
    offset: int = 0,
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

    PAGINATION:
    - num_results: Number of results to return per page (max: 50)
    - offset: Starting position in results (default: 0)
    - Examples:
      • First page: offset=0, num_results=10
      • Second page: offset=10, num_results=10
      • Third page: offset=20, num_results=10

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

    SORTING - Order results by specific fields:
    - sort_by: Field name to sort by (optional)
    - sort_order: "desc" (default) or "asc"

    $SORTING_FIELDS_DOCS

    Note: If sort_by is not provided, search results use default ranking by relevance and
    importance. When using sort_by, results are strictly ordered by that field.
    """
    return _search_implementation(
        query, filters, num_results, "keyword", sort_by, sort_order, offset
    )


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
    result = execute_graphql(
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
                execute_graphql(
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
                execute_graphql(
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


def _extract_lineage_columns_from_paths(search_results: List[dict]) -> List[dict]:
    """
    Extract column information from paths field for column-level lineage results.

    When querying column-level lineage (e.g., get_lineage(urn, column="user_id")),
    the GraphQL response returns DATASET entities (not individual columns) with a
    'paths' field containing the column-level lineage chains.

    Each path shows the column flow, e.g.:
      source_table.user_id -> intermediate_table.uid -> target_table.customer_id

    The LAST entity in each path is a SchemaFieldEntity representing a column in the
    target dataset. This function extracts those column names into a 'lineageColumns' field.

    Args:
        search_results: List of lineage search results where entities are DATASET

    Returns:
        Same list with 'lineageColumns' field added to each result:
        - entity: Dataset entity (unchanged)
        - lineageColumns: List of unique column names (fieldPath) from path endpoints
        - degree: Degree value (unchanged)
        - paths: Removed to reduce response size (column info extracted to lineageColumns)

    Example transformation:
        Input: [
            {
                entity: {type: "DATASET", name: "target_table"},
                paths: [
                    {path: [
                        {type: "SCHEMA_FIELD", fieldPath: "user_id"},
                        {type: "SCHEMA_FIELD", fieldPath: "customer_id"}  # <- target column
                    ]},
                    {path: [
                        {type: "SCHEMA_FIELD", fieldPath: "user_id"},
                        {type: "SCHEMA_FIELD", fieldPath: "uid"}  # <- another target column
                    ]}
                ],
                degree: 1
            }
        ]
        Output: [
            {
                entity: {type: "DATASET", name: "target_table"},
                lineageColumns: ["customer_id", "uid"],
                degree: 1
            }
        ]
    """
    if not search_results:
        return search_results

    # Check if this is column-level lineage by looking for paths
    # (Dataset-level lineage may have empty/missing paths)
    has_column_paths = any(
        result.get("paths") and len(result.get("paths", [])) > 0
        for result in search_results
    )

    if not has_column_paths:
        # Not column-level lineage (or no paths available), return as-is
        return search_results

    processed_results = []
    for result in search_results:
        paths = result.get("paths", [])

        if not paths:
            # No paths for this result, keep as-is
            processed_results.append(result)
            continue

        # Extract column names from the LAST entity in each path
        # (that's the target column in this dataset)
        lineage_columns = []
        for path_obj in paths:
            path = path_obj.get("path", [])
            if not path:
                continue

            # Get the last entity in the path (target column)
            last_entity = path[-1]
            if last_entity.get("type") == "SCHEMA_FIELD":
                field_path = last_entity.get("fieldPath")
                if field_path and field_path not in lineage_columns:
                    lineage_columns.append(field_path)

        # Create new result with lineageColumns
        new_result = {
            "entity": result["entity"],
            "degree": result.get("degree", 0),
        }

        if lineage_columns:
            new_result["lineageColumns"] = lineage_columns

        # Keep other fields that might exist (explored, truncatedChildren, etc.)
        for key in ["explored", "truncatedChildren", "ignoredAsHop"]:
            if key in result:
                new_result[key] = result[key]

        processed_results.append(new_result)

    logger.info(
        f"Extracted lineageColumns from {len(search_results)} column-level lineage results"
    )

    return processed_results


# TODO: Consider adding sorting support (sort_by, sort_order parameters) similar to search() tool.
# GraphQL SearchAcrossLineageInput supports sortInput parameter.
def get_lineage(
    urn: str,
    column: Optional[str] = None,
    query: Optional[str] = None,
    filters: Optional[Filter | str] = None,
    upstream: bool = True,
    max_hops: int = 1,
    max_results: int = 30,
    offset: int = 0,
) -> dict:
    """Get upstream or downstream lineage for any entity, including datasets, schemaFields, dashboards, charts, etc.

    Set upstream to True for upstream lineage, False for downstream lineage.
    Set `column: null` to get lineage for entire dataset or for entity type other than dataset.
    Setting max_hops to 3 is equivalent to unlimited hops.
    Usage and format of filters is same as that in search tool.

    PAGINATION:
    Use offset to paginate through large lineage graphs:
    - offset=0, max_results=30 → first 30 entities
    - offset=30, max_results=30 → next 30 entities

    Note: Token budget constraints may return fewer entities than max_results.
    Check the returned metadata (hasMore, returned, etc.) to understand truncation.

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
    # Normalize column parameter: Some LLMs pass the string "null" instead of JSON null.
    # Note: This means columns literally named "null" cannot be queried.
    # If this becomes a problem, we could add an escape mechanism (e.g., "column_name:null" prefix).
    if column == "null" or column == "":
        column = None

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

    # Track if this is column-level lineage for metadata
    is_column_level_lineage = column is not None

    # Apply offset, entity-level truncation, and cleaning to upstreams/downstreams
    for direction in ["upstreams", "downstreams"]:
        if direction_results := lineage.get(direction):
            if search_results := direction_results.get("searchResults"):
                # Extract lineageColumns from paths for column-level lineage
                search_results = _extract_lineage_columns_from_paths(search_results)
                direction_results["searchResults"] = search_results

                total_available = len(search_results)

                # Apply offset (skip first N entities)
                if offset >= total_available:
                    direction_results["searchResults"] = []
                    direction_results["offset"] = offset
                    direction_results["returned"] = 0
                    direction_results["hasMore"] = False
                    continue

                # Skip offset and apply token budget using generic helper
                results_after_offset = search_results[offset:]

                # Lambda to clean entity in place and return it for token counting
                def get_cleaned_entity(result_item: dict) -> dict:
                    entity = result_item.get("entity", {})
                    cleaned = clean_get_entities_response(entity)
                    result_item["entity"] = cleaned  # Mutate in place
                    return cleaned  # Return for token counting

                # Get results within budget (entities cleaned in place, degree preserved)
                selected_results = list(
                    _select_results_within_budget(
                        results=iter(results_after_offset),
                        fetch_entity=get_cleaned_entity,
                        max_results=max_results,
                    )
                )

                # Update results and add metadata
                direction_results["searchResults"] = selected_results
                direction_results["offset"] = offset
                direction_results["returned"] = len(selected_results)
                direction_results["hasMore"] = (
                    offset + len(selected_results)
                ) < total_available

                if len(selected_results) < len(results_after_offset):
                    direction_results["truncatedDueToTokenBudget"] = True

                logger.info(
                    f"get_lineage {direction}: Returned {len(selected_results)}/{total_available} entities "
                    f"(offset={offset}, hasMore={direction_results['hasMore']})"
                )

    # Add metadata for column-level lineage responses
    if is_column_level_lineage:
        lineage["metadata"] = {
            "queryType": "column-level-lineage",
            "groupedBy": "dataset",
            "fields": {
                "lineageColumns": {
                    "description": "Columns in each dataset that have a lineage relationship with the source column",
                    "semantics": {
                        "downstream": "Columns derived from the source column",
                        "upstream": "Columns that the source column depends on",
                    },
                }
            },
        }

    return lineage


def get_lineage_paths_between(
    source_urn: str,
    target_urn: str,
    source_column: Optional[str] = None,
    target_column: Optional[str] = None,
    direction: Optional[Literal["upstream", "downstream"]] = None,
) -> dict:
    """Get detailed lineage path(s) between two specific entities or columns.

    Returns the paths array from searchAcrossLineage, showing the exact transformation
    chain(s) including intermediate entities, columns, and transformation query URNs.

    Unlike get_lineage() which returns all lineage targets with compact lineageColumns,
    this tool focuses on ONE specific target and returns detailed path information.

    Args:
        source_urn: URN of the source dataset
        target_urn: URN of the target dataset
        source_column: Optional column name in source dataset
        target_column: Optional column name in target dataset (required if source_column provided)
        direction: Optional direction to search. If None (default), automatically discovers
                  the path by trying downstream first, then upstream. Specify "downstream" or
                  "upstream" explicitly for better performance if you know the direction.

    Returns:
        Dictionary with:
        - source: Source entity/column info
        - target: Target entity/column info
        - paths: Array of path objects from GraphQL (with QUERY URNs)
        - pathCount: Number of paths found

    Examples:
        # Column-level paths
        paths_result = get_lineage_paths_between(
            source_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.base_table,PROD)",
            target_urn="urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.final_table,PROD)",
            source_column="user_id",
            target_column="customer_id"
        )
        # Returns paths with QUERY URNs showing transformation chain

        # Fetch SQL for specific query of interest
        query_details = get_entities(paths_result["paths"][0]["path"][1]["urn"])

        # Dataset-level paths (auto-discover direction)
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table"
        )

        # Explicit direction for better performance
        get_lineage_paths_between(
            source_urn="urn:li:dataset:(...):base_table",
            target_urn="urn:li:dataset:(...):final_table",
            direction="downstream"
        )
    """
    # Normalize column parameters
    if source_column == "null" or source_column == "":
        source_column = None
    if target_column == "null" or target_column == "":
        target_column = None

    # Validate: if either column is specified, must be column-level lineage
    if (source_column is None) != (target_column is None):
        raise ValueError(
            "Both source_column and target_column must be provided for column-level lineage, "
            "or both must be None for dataset-level lineage"
        )

    client = get_datahub_client()

    # Convert to schema field URN if column specified
    query_urn = maybe_convert_to_schema_field_urn(source_urn, source_column)
    target_full_urn = maybe_convert_to_schema_field_urn(target_urn, target_column)

    # Auto-discover direction if not specified
    if direction is None:
        # Try downstream first (more common use case)
        try:
            result = _find_lineage_path(
                client=client,
                query_urn=query_urn,
                target_full_urn=target_full_urn,
                source_urn=source_urn,
                target_urn=target_urn,
                source_column=source_column,
                target_column=target_column,
                direction="downstream",
            )
            result["metadata"]["direction"] = "auto-discovered-downstream"
            result["metadata"]["note"] = (
                "Direction was automatically discovered. Specify direction='downstream' or 'upstream' explicitly for better performance."
            )
            return result
        except ItemNotFoundError:
            # Try upstream as fallback
            try:
                result = _find_lineage_path(
                    client=client,
                    query_urn=query_urn,
                    target_full_urn=target_full_urn,
                    source_urn=source_urn,
                    target_urn=target_urn,
                    source_column=source_column,
                    target_column=target_column,
                    direction="upstream",
                )
                result["metadata"]["direction"] = "auto-discovered-upstream"
                result["metadata"]["note"] = (
                    "Direction was automatically discovered. Specify direction='downstream' or 'upstream' explicitly for better performance."
                )
                return result
            except ItemNotFoundError:
                # Not found in either direction
                raise ItemNotFoundError(
                    f"No lineage path found between {source_urn}"
                    + (f".{source_column}" if source_column else "")
                    + f" and {target_urn}"
                    + (f".{target_column}" if target_column else "")
                    + " in either upstream or downstream direction"
                ) from None
    else:
        # User specified direction explicitly
        return _find_lineage_path(
            client=client,
            query_urn=query_urn,
            target_full_urn=target_full_urn,
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            direction=direction,
        )


def _find_lineage_path(
    client: DataHubClient,
    query_urn: str,
    target_full_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    direction: Literal["upstream", "downstream"],
) -> dict:
    """
    Internal helper to find lineage path in a specific direction.

    Always queries upstream internally (more efficient), but maintains the
    semantic direction in the API. For downstream queries, swaps source/target
    and reverses the path.

    Separated from main function to support auto-discovery logic.
    """

    if direction == "downstream":
        # User semantic: source flows TO target (source → target)
        # Implementation: Query target's upstream to find source
        # Then reverse the path to show source → target
        result = _find_upstream_lineage_path(
            client=client,
            query_urn=target_full_urn,  # Query from target
            search_for_urn=query_urn,  # Search for source
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            semantic_direction=direction,
        )

        # Reverse paths to show source → target order
        # Structure: result["paths"] = [{"path": [entity1, entity2, ...]}, ...]
        # Each path is an array of entities (SCHEMA_FIELD/DATASET/QUERY)
        # Upstream query returns: [target, query, intermediate, query, source]
        # We reverse to: [source, query, intermediate, query, target]
        for path_obj in result.get("paths", []):
            if path_obj and "path" in path_obj:
                path_obj["path"] = list(reversed(path_obj["path"]))

        return result
    else:
        # User semantic: source depends ON target (target → source)
        # Implementation: Query source's upstream to find target
        # Path is already in correct order (target → source)
        return _find_upstream_lineage_path(
            client=client,
            query_urn=query_urn,  # Query from source
            search_for_urn=target_full_urn,  # Search for target
            source_urn=source_urn,
            target_urn=target_urn,
            source_column=source_column,
            target_column=target_column,
            semantic_direction=direction,
        )


def _find_result_with_target_urn(
    search_results: List[dict],
    target_urn: str,
    is_column_level: bool,
) -> Optional[dict]:
    """
    Find the search result that contains the target URN.

    For column-level lineage: Searches paths to find one ending with target column URN
    For dataset-level lineage: Matches entity URN directly

    Args:
        search_results: List of lineage search results
        target_urn: URN to search for (dataset or schemaField)
        is_column_level: Whether this is column-level lineage

    Returns:
        The search result containing the target, or None if not found
    """
    for result in search_results:
        if is_column_level:
            # Column-level: Check if any path ends with target column URN
            paths = result.get("paths") or []
            for path_obj in paths:
                if not path_obj:
                    continue
                path = path_obj.get("path") or []
                if path and path[-1].get("urn") == target_urn:
                    return result
        else:
            # Dataset-level: Match entity URN directly
            if result.get("entity", {}).get("urn") == target_urn:
                return result

    return None


def _find_upstream_lineage_path(
    client: DataHubClient,
    query_urn: str,
    search_for_urn: str,
    source_urn: str,
    target_urn: str,
    source_column: Optional[str],
    target_column: Optional[str],
    semantic_direction: Literal["upstream", "downstream"],
) -> dict:
    """
    Internal helper to find upstream lineage path.

    Always queries upstream lineage (more bounded than downstream).

    KEY INSIGHT: Lineage is isotropic (symmetric):
    - If B is in A's downstream, then A is in B's upstream
    - The path is the same, just viewed from different ends
    - Therefore, we can always query upstream and reverse the path for downstream queries

    This optimization significantly reduces response sizes:
    - Upstream: Typically 10-100 results (bounded by data sources)
    - Downstream: Can be 1000s of results (unlimited consumers)

    Args:
        query_urn: URN to query lineage from (could be source or target depending on semantic direction)
        search_for_urn: URN to search for in results
        source_urn: Original source URN (for response metadata)
        target_urn: Original target URN (for response metadata)
        source_column: Original source column (for response metadata)
        target_column: Original target column (for response metadata)
        semantic_direction: User's requested direction (for metadata, not query direction)
    """

    # Get lineage with paths using the API directly (always upstream)
    lineage_api = AssetLineageAPI(client._graph)
    asset_lineage_directive = AssetLineageDirective(
        urn=query_urn,
        upstream=True,  # Always upstream
        downstream=False,
        max_hops=10,  # Higher to ensure we find target
        extra_filters=None,
        max_results=100,  # Need enough results to find target
    )
    lineage = lineage_api.get_lineage(asset_lineage_directive, query="*")

    # Clean up the response
    inject_urls_for_urns(client._graph, lineage, ["*.searchResults[].entity"])
    truncate_descriptions(lineage)

    # Get upstream results (always querying upstream)
    search_results = lineage.get("upstreams", {}).get("searchResults", [])

    if not search_results:
        raise ItemNotFoundError(
            f"No lineage found from {source_urn}"
            + (f".{source_column}" if source_column else "")
        )

    # Find the result containing the target URN
    target_result = _find_result_with_target_urn(
        search_results=search_results,
        target_urn=search_for_urn,
        is_column_level=(target_column is not None),
    )

    if not target_result:
        raise ItemNotFoundError(
            f"No lineage path found from {source_urn}"
            + (f".{source_column}" if source_column else "")
            + f" to {target_urn}"
            + (f".{target_column}" if target_column else "")
        )

    # Extract paths array (with QUERY URNs as-is)
    paths = target_result.get("paths", [])
    if not paths:
        raise ValueError(
            "Target found but no path information available. "
            "This may indicate the entities are directly connected without intermediate steps."
        )

    # Clean the paths response
    cleaned_paths = clean_gql_response(paths)

    # Check if any paths contain QUERY entities (with safe null handling)
    has_queries = any(
        any(
            entity.get("type") == "QUERY"
            for entity in (path_obj.get("path") or [])
            if entity  # Skip None entities
        )
        for path_obj in cleaned_paths
        if path_obj  # Skip None path objects
    )

    # Build metadata
    paths_metadata: dict[str, str] = {
        "description": "Array of lineage paths showing transformation chains from source to target",
        "structure": "Each path contains alternating entities (SCHEMA_FIELD or DATASET) and optional transformation QUERY entities",
    }

    # Add query enrichment note only when queries are present
    if has_queries:
        paths_metadata["queryEntities"] = (
            "QUERY entities are returned as URNs only. "
            "Use get_entities(query_urn) to fetch SQL statement and other query details."
        )

    metadata = {
        "queryType": "lineage-path-trace",
        "direction": semantic_direction,
        "pathType": "column-level" if source_column else "dataset-level",
        "fields": {"paths": paths_metadata},
    }

    # Build response with metadata
    return {
        "metadata": metadata,
        "source": {
            "urn": source_urn,
            **({"column": source_column} if source_column else {}),
        },
        "target": {
            "urn": target_urn,
            **({"column": target_column} if target_column else {}),
        },
        "pathCount": len(cleaned_paths),
        "paths": cleaned_paths,
    }


# Track if tools have been registered to prevent duplicate registration
_tools_registered = False
_tools_registration_lock = threading.Lock()


def register_mutation_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register mutation tools on an MCP instance.

    This is the core registration logic that can be used by both production code
    (via register_all_tools) and tests (with isolated MCP instances).

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).
    """

    enabled = get_boolean_env_variable("TOOLS_IS_MUTATION_ENABLED")

    logger.info(f"Mutation Tools {'ENABLED' if enabled else 'DISABLED'} MCP Server.")

    if not enabled:
        return

    _register_tool(mcp_instance, "add_tags", add_tags, tags={ToolType.MUTATION.value})
    _register_tool(
        mcp_instance, "remove_tags", remove_tags, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "add_terms", add_glossary_terms, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance,
        "remove_terms",
        remove_glossary_terms,
        tags={ToolType.MUTATION.value},
    )
    _register_tool(
        mcp_instance, "add_owners", add_owners, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "remove_owners", remove_owners, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "set_domains", set_domains, tags={ToolType.MUTATION.value}
    )
    _register_tool(
        mcp_instance, "remove_domains", remove_domains, tags={ToolType.MUTATION.value}
    )
    _register_tool(mcp_instance, "update_description", update_description)
    _register_tool(mcp_instance, "add_structured_properties", add_structured_properties)
    _register_tool(
        mcp_instance, "remove_structured_properties", remove_structured_properties
    )

    # Register save_document tool (only if enabled via environment variable)
    if is_save_document_enabled():
        logger.info("Save Document ENABLED - registering save_document tool")
        _register_tool(
            mcp_instance, "save_document", save_document, tags={ToolType.MUTATION.value}
        )
    else:
        logger.info("Save Document DISABLED - save_document tool not registered")


def register_user_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register user information tools on an MCP instance.

    This includes tools for fetching authenticated user information.

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions.
                If False, use Cloud descriptions.
    """

    enabled = get_boolean_env_variable("TOOLS_IS_USER_ENABLED")
    logger.info(f"User Tools {'ENABLED' if enabled else 'DISABLED'} MCP Server.")

    if not enabled:
        return

    _register_tool(mcp_instance, "get_me", get_me, tags={ToolType.USER.value})


def register_search_tools(mcp_instance: FastMCP, is_oss: bool = False) -> None:
    """Register search and entity tools on an MCP instance.

    This is the core registration logic that can be used by both production code
    (via register_all_tools) and tests (with isolated MCP instances).

    Args:
        mcp_instance: The FastMCP instance to register tools on
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).
    """
    # Choose sorting documentation based on deployment type
    if not is_oss:
        sorting_docs = """Available sort fields for datasets:
    - queryCountLast30DaysFeature: Number of queries in last 30 days
    - rowCountFeature: Table row count
    - sizeInBytesFeature: Table size in bytes
    - writeCountLast30DaysFeature: Number of writes/updates in last 30 days

    Sorting examples:
    - Most queried datasets:
      search(query="*", filters={"entity_type": ["DATASET"]}, sort_by="queryCountLast30DaysFeature", num_results=10)
    - Largest tables:
      search(query="*", filters={"entity_type": ["DATASET"]}, sort_by="sizeInBytesFeature", num_results=10)
    - Smallest tables first:
      search(query="*", filters={"entity_type": ["DATASET"]}, sort_by="sizeInBytesFeature", sort_order="asc", num_results=10)"""
    else:
        sorting_docs = """Available sort fields:
    - lastOperationTime: Last modified timestamp in source system

    Sorting examples:
    - Most recently updated:
      search(query="*", filters={"entity_type": ["DATASET"]}, sort_by="lastOperationTime", sort_order="desc", num_results=10)"""

    # Build full description with interpolated sorting docs using Template
    if search.__doc__ is None:
        raise ValueError("search function must have a docstring")
    search_description = string.Template(search.__doc__).substitute(
        SORTING_FIELDS_DOCS=sorting_docs
    )

    # Register search tool
    if _is_semantic_search_enabled():
        # Note: Actual semantic search availability is validated at runtime when used
        # This allows the tool to be registered even if validation would fail,
        # but provides clear error messages when semantic search is actually attempted
        _register_tool(
            mcp_instance, "search", enhanced_search, tags={ToolType.SEARCH.value}
        )
    else:
        # Register original search tool with deployment-specific description
        _register_tool(
            mcp_instance,
            "search",
            search,
            description=search_description,
            tags={ToolType.SEARCH.value},
        )

    _register_tool(
        mcp_instance, "get_lineage", get_lineage, tags={ToolType.SEARCH.value}
    )
    _register_tool(
        mcp_instance,
        "get_dataset_queries",
        get_dataset_queries,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(
        mcp_instance, "get_entities", get_entities, tags={ToolType.SEARCH.value}
    )
    _register_tool(
        mcp_instance,
        "list_schema_fields",
        list_schema_fields,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(
        mcp_instance,
        "get_lineage_paths_between",
        get_lineage_paths_between,
        tags={ToolType.SEARCH.value},
    )
    _register_tool(mcp_instance, "search_documents", search_documents)
    _register_tool(mcp_instance, "grep_documents", grep_documents)


def register_all_tools(is_oss: bool = False) -> None:
    """Register all MCP tools on the global mcp instance.

    Args:
        is_oss: If True, use OSS-compatible tool descriptions (limited sorting fields).
                If False, use Cloud descriptions (full sorting features).

    Note: Thread-safe. Can be called multiple times from different threads.
          Only the first call will register tools, subsequent calls are no-ops.
    """
    global _tools_registered

    # Thread-safe check-and-set using lock
    with _tools_registration_lock:
        if _tools_registered:
            logger.debug("Tools already registered, skipping duplicate registration")
            return

        _tools_registered = True
        logger.info(f"Registering MCP tools (is_oss={is_oss})")

    # Call the core registration logic on the global mcp instance
    register_search_tools(mcp, is_oss)

    register_mutation_tools(mcp, is_oss)

    register_user_tools(mcp, is_oss)


def get_valid_tools_from_mcp(
    filter_fn: Optional[Callable[[FastMCPTool], bool]] = None,
) -> List[FastMCPTool]:
    """Get valid tools from MCP, optionally filtered.

    Args:
        filter_fn: Optional function to filter tools. Receives a Tool and returns True to include it.

    Returns:
        List of Tool objects that pass the filter (or all tools if no filter provided).

    Example filtering by tag values:
        # Filter tools that have the "mutation" tag
        tools = get_valid_tools_from_mcp(
            filter_fn=lambda tool: "mutation" in (tool.tags or set())
        )

        # Filter tools that have either "search" or "user" tags
        tools = get_valid_tools_from_mcp(
            filter_fn=lambda tool: bool((tool.tags or set()) & {"search", "user"})
        )
    """
    tools = list(mcp._tool_manager._tools.values())
    if filter_fn:
        return [tool for tool in tools if filter_fn(tool)]
    return tools
