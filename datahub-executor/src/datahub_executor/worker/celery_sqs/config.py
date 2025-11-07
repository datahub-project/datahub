import io
import logging
import pickle
import types
from typing import Any, Dict, List

from celery import Celery
from kombu import Queue
from kombu.serialization import register
from kombu.utils.url import safequote

from datahub_executor.common.client.config.resolver import ExecutorConfigResolver
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import ExecutorConfig
from datahub_executor.config import (
    DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE,
    DATAHUB_EXECUTOR_POOL_ID,
    DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT,
)

logger = logging.getLogger(__name__)


class SchemaPickler(pickle._Pickler):
    def save(self, obj: Any, save_persistent_id: bool = True) -> None:
        patched = False
        original_getstate = None

        try:
            if DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE:
                getstate = getattr(obj, "__getstate__", None)
                if self._is_pydantic_v2_model(obj) and getstate is not None:
                    # Translate v2 state to v1
                    def __getstate__(self):
                        _state = getstate()
                        _state["__private_attribute_values__"] = (
                            _state.get("__pydantic_private__", {}) or {}
                        )
                        _state["__fields_set__"] = (
                            _state.get("__pydantic_fields_set__", set()) or set()
                        )
                        return _state

                    # Patch getstate
                    original_getstate = getstate
                    type(obj).__getstate__ = types.MethodType(__getstate__, obj)
                    patched = True

            return super().save(obj, save_persistent_id=save_persistent_id)  # type: ignore
        finally:
            # Only restore if we actually patched it
            if patched and original_getstate is not None:
                type(obj).__getstate__ = original_getstate

    def _is_pydantic_v2_model(self, obj) -> bool:
        return (
            hasattr(obj, "__pydantic_fields_set__")
            and hasattr(obj, "model_dump")
            and callable(obj.model_dump)
            and hasattr(obj, "model_fields")
        )

    def save_global(self, obj, name=None):
        original_module = getattr(obj, "__module__", None)
        try:
            if DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE and original_module is not None:
                # Module name translation for compatibility
                new_module = original_module.replace("acryl_datahub_cloud", "datahub")
                obj.__module__ = new_module
                logger.debug(
                    f"Translated module name: {original_module} -> {new_module}"
                )
            return super().save_global(obj, name=name)  # type: ignore[misc]
        except Exception as e:
            logger.error(f"Error in SchemaPickler.save_global for {obj}: {e}")
            raise
        finally:
            # Always restore original module
            if original_module is not None:
                obj.__module__ = original_module


def schema_pickle_dumps(obj):
    """
    Serialize an object using the custom SchemaPickler.

    This function provides Pydantic v2 to v1 compatibility during serialization
    when DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE is enabled.

    Args:
        obj: The object to serialize (typically ExecutionRequest or MetadataChangeLogClass)

    Returns:
        bytes: The pickled object data
    """
    buf = io.BytesIO()
    SchemaPickler(buf, protocol=pickle.HIGHEST_PROTOCOL).dump(obj)
    return buf.getvalue()


def schema_pickle_loads(s):
    """
    Deserialize an object from pickled data.

    This function uses standard pickle.loads for deserialization. The compatibility
    translation happens during pickling, so no special handling is needed here.

    Args:
        s: The pickled data bytes

    Returns:
        The deserialized object
    """
    return pickle.loads(s)


register(
    "pickle_custom",
    schema_pickle_dumps,
    schema_pickle_loads,
    content_type="application/x-python-serialize",  # Must match CeleryConfig.accept_content below
    content_encoding="binary",
)


class CeleryConfig:
    task_serializer = "pickle_custom"
    result_serializer = "pickle_custom"
    event_serializer = "json"
    accept_content = ["application/json", "application/x-python-serialize"]
    result_accept_content = ["application/json", "application/x-python-serialize"]
    task_create_missing_queues = False
    task_default_queue = "default"
    broker_connection_retry_on_startup = True
    broker_url = ""
    task_queues = ()
    broker_transport_options: Dict[str, Any] = {}


def update_celery_config(
    config: CeleryConfig, executor_configs: List[ExecutorConfig]
) -> CeleryConfig:
    queues = {}
    routing_queues = []
    for executor_config in executor_configs:
        queues[f"{executor_config.executor_id}"] = {
            "url": executor_config.queue_url,
            "access_key_id": safequote(executor_config.access_key),
            "secret_access_key": safequote(executor_config.secret_key),
        }
        routing_queues.append(
            Queue(
                executor_config.executor_id,
                routing_key=f"{executor_config.executor_id}.#",
            )
        )

    config.task_queues = tuple(routing_queues)  # type: ignore
    config.task_create_missing_queues = False

    if executor_configs:
        config.task_default_queue = executor_configs[0].executor_id
        config.broker_url = f"sqs://{safequote(executor_configs[0].access_key)}:{safequote(executor_configs[0].secret_key)}@"
        config.broker_transport_options = {
            "region": executor_configs[0].region,
            "predefined_queues": queues,
            "sts_role_arn": "redirect-to-patched-handle-sts-session",
            "visibility_timeout": DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT,
        }
    return config


def update_celery_credentials(app: Celery, is_startup: bool, queue_name: str) -> bool:
    executor_config_resolver = ExecutorConfigResolver()

    if is_startup:
        METRIC(
            "WORKER_CREDENTIALS_REFRESH_REQUESTS", pool_name=DATAHUB_EXECUTOR_POOL_ID
        ).inc()

        executor_configs = executor_config_resolver.get_executor_configs()
        config = update_celery_config(CeleryConfig(), executor_configs)
        app.config_from_object(config)
    else:
        current_queues = (
            app.conf.broker_transport_options["predefined_queues"].keys()
            if "predefined_queues" in app.conf.broker_transport_options
            else []
        )
        # if we don't know about this queue, we force a refresh of the config
        if queue_name and queue_name not in current_queues:
            did_refresh = True
            executor_configs = executor_config_resolver.fetch_executor_configs()
        else:
            (
                did_refresh,
                executor_configs,
            ) = executor_config_resolver.refresh_executor_configs()

        if did_refresh:
            METRIC(
                "WORKER_CREDENTIALS_REFRESH_REQUESTS",
                pool_name=DATAHUB_EXECUTOR_POOL_ID,
            ).inc()

            config = update_celery_config(CeleryConfig(), executor_configs)
            app.config_from_object(config)

            # Celery permanently caches queues and router object using @cached_property
            # decorator. This requires application restart when a new queue is created
            # at runtime. To work around this issue, we delete internal cache entries
            # in Celery every time new config is received.
            app.amqp.__dict__.pop("queues", None)
            app.amqp.__dict__.pop("router", None)
            app.amqp._producer_pool = None  # type: ignore[attr-defined]

            if "predefined_queues" in app.conf.broker_transport_options:
                for q in app.conf.broker_transport_options["predefined_queues"].keys():
                    if q not in current_queues:
                        # this is a newly added queue, let's tell celery!
                        logger.info(f"Adding new queue to celery config {q}")
                        app.control.add_consumer(q)

    if queue_name:
        updated_queues = (
            app.conf.broker_transport_options["predefined_queues"].keys()
            if "predefined_queues" in app.conf.broker_transport_options
            else []
        )

        if queue_name not in updated_queues:
            METRIC(
                "WORKER_CREDENTIALS_REFRESH_ERRORS",
                exception="NoQueue",
                pool_name=DATAHUB_EXECUTOR_POOL_ID,
            ).inc()
            logger.error(f"SQS qeueue {queue_name} does not exist or misconfigured.")
            return False

    return True
