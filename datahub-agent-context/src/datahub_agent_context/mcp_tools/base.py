import logging
import re
from typing import Any, Dict, Optional

import cachetools

from datahub.cli.env_utils import get_boolean_env_variable
from datahub.ingestion.graph.client import DataHubGraph

logger = logging.getLogger(__name__)

# Cache to track whether newer GMS fields are supported for each graph instance
# Key: id(graph), Value: bool indicating if newer GMS fields are supported
_newer_gms_fields_support_cache: dict[int, bool] = {}

# Default view configuration
DISABLE_DEFAULT_VIEW = get_boolean_env_variable(
    "DATAHUB_MCP_DISABLE_DEFAULT_VIEW", default=False
)
VIEW_CACHE_TTL_SECONDS = 300  # 5 minutes


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
    query = _enable_cloud_fields(query) if is_cloud else _disable_cloud_fields(query)

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


def _is_datahub_cloud(graph: DataHubGraph) -> bool:
    """Check if the graph instance is DataHub Cloud.

    Cloud instances typically have newer GMS versions with additional fields.
    This heuristic uses the presence of frontend_base_url to detect Cloud instances.
    """
    if get_boolean_env_variable("DISABLE_NEWER_GMS_FIELD_DETECTION", default=False):
        logger.debug(
            "Newer GMS field detection is disabled via DISABLE_NEWER_GMS_FIELD_DETECTION"
        )
        return False

    is_cloud = hasattr(graph, "frontend_base_url") and graph.frontend_base_url
    logger.debug(f"Cloud detection: {is_cloud}")
    return bool(is_cloud)


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
