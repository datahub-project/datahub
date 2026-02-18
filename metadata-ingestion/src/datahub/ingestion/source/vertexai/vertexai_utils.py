import logging
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Iterator, List, Optional, TypeVar

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
from datahub.emitter.mcp_builder import ProjectIdKey
from datahub.ingestion.source.vertexai.vertexai_constants import (
    PROGRESS_LOG_INTERVAL,
    ResourceCategoryType,
)
from datahub.ingestion.source.vertexai.vertexai_models import (
    VertexAIResourceCategoryKey,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


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


@contextmanager
def handle_google_api_errors(
    operation: str,
    resource_type: Optional[str] = None,
    resource_name: Optional[str] = None,
    log_level: str = "warning",
) -> Iterator[None]:
    """
    Context manager for consistent Google API error handling across Vertex AI extractors.

    Args:
        operation: Description of the operation being performed (e.g., "fetch model evaluations")
        resource_type: Type of resource being accessed (e.g., "model", "experiment")
        resource_name: Name/ID of the specific resource
        log_level: Logging level - "debug" for expected errors (like NotFound), "warning" for unexpected

    Example:
        with handle_google_api_errors("fetch evaluations", "model", model.name):
            evaluations = list(model.list_model_evaluations())
    """
    try:
        yield
    except (PermissionDenied, Unauthenticated) as e:
        error_type = type(e).__name__
        msg = f"Permission denied for {operation}"
        if resource_type and resource_name:
            msg += f" | resource_type={resource_type} | resource_name={resource_name}"
        msg += f" | cause={error_type}: {e}"
        if log_level == "debug":
            logger.debug(msg)
        else:
            logger.warning(msg)
    except ResourceExhausted as e:
        error_type = type(e).__name__
        msg = f"Quota exceeded for {operation}"
        if resource_type and resource_name:
            msg += f" | resource_type={resource_type} | resource_name={resource_name}"
        msg += f" | cause={error_type}: {e}"
        if log_level == "debug":
            logger.debug(msg)
        else:
            logger.warning(msg)
    except (DeadlineExceeded, ServiceUnavailable) as e:
        error_type = type(e).__name__
        msg = f"Timeout or service unavailable for {operation}"
        if resource_type and resource_name:
            msg += f" | resource_type={resource_type} | resource_name={resource_name}"
        msg += f" | cause={error_type}: {e}"
        if log_level == "debug":
            logger.debug(msg)
        else:
            logger.warning(msg)
    except NotFound as e:
        error_type = type(e).__name__
        msg = f"Resource not found for {operation}"
        if resource_type and resource_name:
            msg += f" | resource_type={resource_type} | resource_name={resource_name}"
        msg += f" | cause={error_type}: {e}"
        logger.debug(msg)
    except GoogleAPICallError as e:
        error_type = type(e).__name__
        msg = f"Google API error for {operation}"
        if resource_type and resource_name:
            msg += f" | resource_type={resource_type} | resource_name={resource_name}"
        msg += f" | cause={error_type}: {e}"
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
    """
    Filter items to only include those updated after the checkpoint time.

    Args:
        items: List of objects to filter
        checkpoint_millis: Checkpoint timestamp in milliseconds since epoch
        time_field: Name of the timestamp field on each item (default: "update_time")
        resource_type: Name of resource type for logging (default: "items")

    Returns:
        Filtered list containing only items updated after checkpoint
    """
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
    """
    Sort items by update time in-place, with fallback for None values.

    Args:
        items: List of objects to sort
        time_field: Name of the timestamp field on each item (default: "update_time")
        reverse: Sort in descending order (newest first) if True (default: True)
    """
    items.sort(
        key=lambda item: getattr(item, time_field, None)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=reverse,
    )
