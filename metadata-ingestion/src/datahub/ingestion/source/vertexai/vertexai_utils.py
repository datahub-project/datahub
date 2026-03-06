import functools
import inspect
import logging
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterable, Iterator, List, Optional, TypeVar, Union

from google.api_core import retry as api_retry
from google.api_core.exceptions import (
    DeadlineExceeded,
    GoogleAPICallError,
    NotFound,
    PermissionDenied,
    ResourceExhausted,
    ServiceUnavailable,
    Unauthenticated,
)

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp_builder import ContainerKey, ProjectIdKey, gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PROGRESS_LOG_INTERVAL,
    SDK_CLASSES_TO_PATCH_FOR_RETRY,
    MLMetadataDefaults,
    ResourceCategoryType,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    VertexAIResourceCategoryKey,
)
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

T = TypeVar("T")


def create_vertex_retry_without_429() -> api_retry.Retry:
    """
    Create a retry policy that excludes ResourceExhausted (429) errors.

    The default Vertex AI SDK retry behavior retries 429 errors for up to 120s,
    which makes quota problems worse. This retry policy fails fast on 429 errors
    and lets the rate limiter control the request rate instead.
    """

    def should_retry(exception):
        if isinstance(exception, ResourceExhausted):
            return False
        return api_retry.if_transient_error(exception)

    return api_retry.Retry(
        predicate=should_retry,
        initial=MLMetadataDefaults.RETRY_INITIAL_WAIT_SECS,
        maximum=MLMetadataDefaults.RETRY_MAXIMUM_WAIT_SECS,
        multiplier=MLMetadataDefaults.RETRY_MULTIPLIER,
        deadline=MLMetadataDefaults.RETRY_DEADLINE_SECS,
    )


def patch_vertex_sdk_retry(custom_retry: api_retry.Retry) -> None:
    """
    Patch Vertex AI SDK list methods to use custom retry.

    The SDK's default retry behavior retries 429s for up to 120s, which
    makes quota problems worse. This patches SDK classes to use a custom
    retry policy that fails fast on 429 errors, letting the rate limiter
    control the request rate instead.

    Args:
        custom_retry: Custom retry policy to inject into SDK list methods
    """
    for cls in SDK_CLASSES_TO_PATCH_FOR_RETRY:
        try:
            if not hasattr(cls, "list"):
                continue

            original_list = (
                cls.list.__func__ if hasattr(cls.list, "__func__") else cls.list
            )

            try:
                sig = inspect.signature(original_list)
                supports_retry = "retry" in sig.parameters
            except (ValueError, TypeError):
                supports_retry = False

            if supports_retry:

                @functools.wraps(original_list)
                def patched_list(
                    *args, _orig=original_list, _retry=custom_retry, **kwargs
                ):
                    if "retry" not in kwargs:
                        kwargs["retry"] = _retry
                    return _orig(*args, **kwargs)

                cls.list = classmethod(patched_list)
        except (AttributeError, TypeError):
            pass


def log_progress(
    current: int,
    total: Optional[int],
    item_type: str,
    interval: int = PROGRESS_LOG_INTERVAL,
) -> None:
    if current % interval == 0:
        logger.info(f"Processed {current} {item_type} from Vertex AI")
    if total and current == total:
        logger.info(f"Finished processing {total} {item_type} from Vertex AI")


def get_actor_from_labels(labels: Optional[Dict[str, str]]) -> Optional[str]:
    if not labels:
        return None

    actor_keys = ["created_by", "creator", "owner"]
    for key in actor_keys:
        if key in labels and labels[key]:
            return builder.make_user_urn(labels[key])

    return None


def get_project_container(
    project_id: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
) -> ProjectIdKey:
    return ProjectIdKey(
        project_id=project_id,
        platform=platform,
        instance=platform_instance,
        env=env,
    )


def get_resource_category_container(
    project_id: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    category: ResourceCategoryType,
) -> VertexAIResourceCategoryKey:
    return VertexAIResourceCategoryKey(
        project_id=project_id,
        platform=platform,
        instance=platform_instance,
        env=env,
        category=category,
    )


def gen_resource_subfolder_container(
    project_id: str,
    platform: str,
    platform_instance: Optional[str],
    env: str,
    resource_category: ResourceCategoryType,
    container_key: ContainerKey,
    name: str,
    sub_types: List[str],
    extra_properties: Optional[Dict[str, str]] = None,
    external_url: Optional[str] = None,
) -> Iterable[MetadataWorkUnit]:
    """Generate subfolder container: Project → Category → Subfolder."""
    category_key = get_resource_category_container(
        project_id=project_id,
        platform=platform,
        platform_instance=platform_instance,
        env=env,
        category=resource_category,
    )

    yield from gen_containers(
        parent_container_key=category_key,
        container_key=container_key,
        name=name,
        sub_types=sub_types,
        extra_properties=extra_properties,
        external_url=external_url,
    )


def gen_browse_path_from_container(container_key: ContainerKey) -> BrowsePathsV2Class:
    browse_path: List[BrowsePathEntryClass] = []
    current_container: Optional[ContainerKey] = container_key
    while current_container is not None:
        container_urn = current_container.as_urn()
        browse_path.insert(0, BrowsePathEntryClass(id=container_urn, urn=container_urn))
        current_container = current_container.parent_key()
    return BrowsePathsV2Class(path=browse_path)


def format_api_error_message(
    error: Exception,
    operation: str,
    resource_type: Optional[str] = None,
    resource_name: Optional[str] = None,
    *,
    custom_message: Optional[str] = None,
) -> str:
    """
    Format a consistent error message for Google API errors.

    If custom_message is provided, it's used as-is. Otherwise, a default message
    is generated based on the error type and operation.
    """
    error_type = type(error).__name__

    if custom_message:
        msg = custom_message
    elif isinstance(error, (PermissionDenied, Unauthenticated)):
        msg = f"Permission denied while {operation}"
    elif isinstance(error, ResourceExhausted):
        msg = f"API quota exceeded while {operation}"
    elif isinstance(error, (DeadlineExceeded, ServiceUnavailable)):
        msg = f"Timeout or service unavailable while {operation}"
    elif isinstance(error, NotFound):
        msg = f"{operation} not found"
    else:
        msg = f"Error while {operation}"

    if resource_type and resource_name:
        msg += f" | resource_type={resource_type} | resource_name={resource_name}"
    msg += f" | cause={error_type}: {error}"

    return msg


@contextmanager
def handle_google_api_errors(
    operation: str,
    resource_type: Optional[str] = None,
    resource_name: Optional[str] = None,
    log_level: str = "warning",
) -> Iterator[None]:
    """
    Context manager for Google API error handling. Suppresses exceptions and logs them.

    For returning specific values on error, use format_api_error_message() directly.
    """
    try:
        yield
    except NotFound as e:
        logger.debug(
            format_api_error_message(e, operation, resource_type, resource_name)
        )
    except GoogleAPICallError as e:
        msg = format_api_error_message(e, operation, resource_type, resource_name)
        if log_level == "debug":
            logger.debug(msg)
        else:
            logger.warning(msg)


def log_checkpoint_time(checkpoint_millis: int, resource_type: str) -> None:
    """Log checkpoint information for incremental mode."""
    checkpoint_time = datetime.fromtimestamp(
        checkpoint_millis / 1000, tz=timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S")
    logger.info(
        f"Incremental mode: Only processing {resource_type}s updated after {checkpoint_time} UTC"
    )


def filter_by_update_time(
    items: List[T],
    checkpoint_millis: int,
    time_field: str = "update_time",
    resource_type: str = "items",
) -> List[T]:
    """Filter items to only include those updated after the checkpoint time."""
    original_count = len(items)
    filtered_items = [
        item
        for item in items
        if hasattr(item, time_field)
        and getattr(item, time_field)
        and int(getattr(item, time_field).timestamp() * 1000) > checkpoint_millis
    ]

    if original_count > len(filtered_items):
        logger.debug(
            f"Filtered to {len(filtered_items)} new {resource_type} "
            f"(out of {original_count} total)"
        )

    return filtered_items


def sort_by_update_time(
    items: List[T], time_field: str = "update_time", reverse: bool = True
) -> None:
    """Sort items by update time in-place, with fallback for None values."""
    items.sort(
        key=lambda item: getattr(item, time_field, None)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=reverse,
    )


def paginated_list_with_rate_limit(
    pager: Iterable[T],
    rate_limiter: Union[RateLimiter, AbstractContextManager[None]],
) -> List[T]:
    """Collect all items from a pager, consuming one rate-limit token before each page fetch."""
    if hasattr(pager, "pages"):
        results: List[T] = []
        page_iter = iter(pager.pages)  # type: ignore[union-attr]
        while True:
            with rate_limiter:
                try:
                    page = next(page_iter)
                except StopIteration:
                    break
            results.extend(page)
        return results
    else:
        with rate_limiter:
            return list(pager)


def iterate_pager_with_rate_limit(
    pager: Iterable[T],
    rate_limiter: Union[RateLimiter, AbstractContextManager[None]],
) -> Iterator[T]:
    """Yield items from a pager lazily, consuming one rate-limit token before each page fetch."""
    if hasattr(pager, "pages"):
        page_iter = iter(pager.pages)  # type: ignore[union-attr]
        while True:
            with rate_limiter:
                try:
                    page = next(page_iter)
                except StopIteration:
                    break
            yield from page
    else:
        with rate_limiter:
            yield from pager
