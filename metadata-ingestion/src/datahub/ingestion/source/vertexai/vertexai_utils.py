import functools
import inspect
import logging
from contextlib import AbstractContextManager, contextmanager
from datetime import datetime, timezone
from typing import (
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Protocol,
    Type,
    TypeVar,
    Union,
)

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
from google.cloud.aiplatform import initializer as vertex_initializer

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
    GapicListRequest,
    VertexAIResourceCategoryKey,
)
from datahub.metadata.schema_classes import BrowsePathEntryClass, BrowsePathsV2Class
from datahub.utilities.ratelimiter import RateLimiter

logger = logging.getLogger(__name__)

T = TypeVar("T")
T_co = TypeVar("T_co", covariant=True)


def create_vertex_retry_without_429() -> api_retry.Retry:
    """Retry policy that fails fast on 429s instead of retrying for 120s."""

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
    """Patch SDK list() methods to use a retry policy that fails fast on 429s."""
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


class _GapicPagedFn(Protocol[T_co]):
    def __call__(self, *, request: object) -> Iterable[T_co]:
        pass


def rate_limited_paged_call(
    gapic_fn: _GapicPagedFn[T],
    request: object,
    rate_limiter: Union[RateLimiter, AbstractContextManager[None]],
) -> Iterable[T]:
    with rate_limiter:
        pager = gapic_fn(request=request)
    if hasattr(pager, "_method"):
        _orig = pager._method

        def _rate_limited_next(*args: Any, _orig: Any = _orig, **kwargs: Any) -> Any:
            with rate_limiter:
                return _orig(*args, **kwargs)

        pager._method = _rate_limited_next
    return pager


def rate_limited_gapic_list(
    cls: Type[T],
    rate_limiter: Union[RateLimiter, AbstractContextManager[None]],
    order_by: Optional[str] = None,
    filter_str: Optional[str] = None,
    parent: Optional[str] = None,
) -> List[T]:
    """List SDK resources via GAPIC with one rate-limit token per page fetch."""
    # sdk_cls typed as Any to access SDK private class attributes (_empty_constructor,
    # _list_method, _construct_sdk_resource_from_gapic) that are not in the public type.
    sdk_cls: Any = cls

    if not hasattr(sdk_cls, "_list_method"):
        with rate_limiter:
            return sdk_cls.list(order_by=order_by) if order_by else sdk_cls.list()

    supported_schemas = getattr(sdk_cls, "_supported_training_schemas", None)
    supported_uris = getattr(sdk_cls, "_supported_metadata_schema_uris", None)

    def proto_filter(p: Any) -> bool:
        if supported_schemas is not None:
            return p.training_task_definition in supported_schemas
        if supported_uris is not None:
            return p.metadata_schema_uri in supported_uris
        return True

    try:
        resource = sdk_cls._empty_constructor()
        creds = resource.credentials
        gapic_fn = getattr(resource.api_client, sdk_cls._list_method)

        request = GapicListRequest(
            parent=parent or vertex_initializer.global_config.common_location_path(),
            filter=filter_str,
            order_by=order_by,
        )

        try:
            pager = rate_limited_paged_call(
                gapic_fn, request.model_dump(exclude_none=True), rate_limiter
            )
        except ValueError:
            # list_training_pipelines and similar don't support order_by via gRPC
            pager = rate_limited_paged_call(
                gapic_fn,
                request.model_copy(update={"order_by": None}).model_dump(
                    exclude_none=True
                ),
                rate_limiter,
            )

        return [
            sdk_cls._construct_sdk_resource_from_gapic(proto, credentials=creds)
            for proto in pager
            if proto_filter(proto)
        ]
    except (AttributeError, TypeError):
        logger.debug(
            f"Per-page rate limiting unavailable for {cls.__name__}, falling back"
        )
        with rate_limiter:
            return sdk_cls.list(order_by=order_by) if order_by else sdk_cls.list()


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
    """Format a Google API error message; custom_message overrides the default."""
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
    """Suppress Google API exceptions and log them."""
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
    items.sort(
        key=lambda item: getattr(item, time_field, None)
        or datetime.min.replace(tzinfo=timezone.utc),
        reverse=reverse,
    )
