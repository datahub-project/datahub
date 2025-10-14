import io
import logging
import pickle
import re
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

_SCHEMA_RE = re.compile(r"schema_classes")


class SchemaPickler(pickle._Pickler):
    """
    Custom pickler for DataHub executor that provides Pydantic v2 to v1 backwards compatibility.

    This pickler ensures that Pydantic v2 models can be correctly deserialized by systems
    expecting Pydantic v1 model state format. This is essential for coordinator-worker
    communication where different environments may have different Pydantic versions.

    Key compatibility features:
    - Translates Pydantic v2 model state to v1 format during pickling
    - Handles field alias mapping from v2 to v1 naming conventions
    - Converts private attributes (__pydantic_private__ -> __private_attribute_values__)
    - Translates fields_set tracking (__pydantic_fields_set__ -> __fields_set__)
    - Provides module name translation for acryl_datahub_cloud -> datahub

    Environment Variables:
    - DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE: Enable/disable compatibility mode
      * "True" or "1": Enable compatibility translation (default for mixed environments)
      * "False" or "0": Disable compatibility (for homogeneous v2 environments)

    Supported Models:
    - ExecutionRequest: Primary model for Celery task communication
    - MetadataChangeLogClass: Used for ingestion tasks
    - Any Pydantic v2 model with __pydantic_fields_set__ attribute

    Usage:
    The pickler is automatically used by Celery for task serialization when configured
    with the 'pickle_custom' serializer. No manual instantiation required.
    """

    def save(self, obj, save_persistent_id=True):
        getstate = getattr(obj, "__getstate__", None)
        original_getstate = getstate
        try:
            if DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE and self._is_pydantic_v2_model(obj):
                # Translate v2 state to v1 for backward compatibility
                # Capture reference to pickler instance
                self_ref = self

                def __getstate__(self):
                    try:
                        # Get original state first
                        _state = (
                            original_getstate()
                            if original_getstate is not None
                            else {
                                "__dict__": obj.__dict__.copy(),
                                "__pydantic_fields_set__": getattr(
                                    obj, "__pydantic_fields_set__", set()
                                ),
                                "__pydantic_extra__": getattr(
                                    obj, "__pydantic_extra__", None
                                ),
                                "__pydantic_private__": getattr(
                                    obj, "__pydantic_private__", None
                                ),
                            }
                        )

                        return self_ref._translate_pydantic_v2_to_v1_state(_state, obj)
                    except Exception as e:
                        logger.warning(
                            f"Failed to translate Pydantic v2 to v1 state for {type(obj).__name__}: {e}"
                        )
                        # Fallback to original getstate or default behavior
                        return (
                            original_getstate()
                            if original_getstate is not None
                            else obj.__dict__.copy()
                        )

                # Patch getstate temporarily on the instance
                obj.__getstate__ = types.MethodType(__getstate__, obj)

            return super().save(obj, save_persistent_id=save_persistent_id)  # type: ignore[misc]
        except Exception as e:
            logger.error(f"Error in SchemaPickler.save for {type(obj).__name__}: {e}")
            raise
        finally:
            # Always restore original getstate on the instance
            if original_getstate is not None:
                obj.__getstate__ = original_getstate
            elif hasattr(obj, "__getstate__"):
                # Remove the patched method if we added it
                delattr(obj, "__getstate__")

    def _is_pydantic_v2_model(self, obj) -> bool:
        """
        Check if object is a Pydantic v2 model.

        Pydantic v2 models are identified by the presence of:
        - __pydantic_fields_set__: Set of fields that have been explicitly set
        - model_dump: Method for serializing model data
        - model_fields: Class attribute containing field definitions
        """
        return (
            hasattr(obj, "__pydantic_fields_set__")
            and hasattr(obj, "model_dump")
            and callable(obj.model_dump)
            and hasattr(obj, "model_fields")
        )

    def _translate_pydantic_v2_to_v1_state(
        self, state: Dict[str, Any], obj
    ) -> Dict[str, Any]:
        """
        Translate Pydantic v2 model state to v1 format.

        This method performs the following translations:
        1. Field alias mapping: Maps v2 field names to their v1 aliases if defined
        2. Private attributes: __pydantic_private__ -> __private_attribute_values__
        3. Fields set tracking: __pydantic_fields_set__ -> __fields_set__
        4. Cleanup: Removes v2-only attributes that don't exist in v1

        Args:
            state: The Pydantic v2 model state dictionary
            obj: The original Pydantic v2 model object

        Returns:
            Dict containing the translated state compatible with Pydantic v1
        """
        # Create alias mapping for field names
        alias_map = {}
        if hasattr(obj, "model_fields"):
            try:
                alias_map = {
                    name: field.alias
                    for name, field in obj.model_fields.items()
                    if hasattr(field, "alias") and field.alias and field.alias != name
                }
            except Exception as e:
                logger.debug(f"Could not build alias map for {type(obj).__name__}: {e}")

        # Apply field name remapping if we have aliases and __dict__
        if alias_map and "__dict__" in state and isinstance(state["__dict__"], dict):
            state["__dict__"] = {
                alias_map.get(k, k): v for k, v in state["__dict__"].items()
            }

        # Translate v2 private attributes to v1 format
        if "__pydantic_private__" in state:
            pv = state.pop("__pydantic_private__")
            state["__private_attribute_values__"] = pv if pv is not None else {}

        # Translate v2 fields_set to v1 format
        if "__pydantic_fields_set__" in state:
            fields = state.pop("__pydantic_fields_set__")
            if isinstance(fields, set):
                # Apply alias mapping to field names in fields_set
                state["__fields_set__"] = {
                    alias_map.get(field, field) for field in fields
                }
            else:
                state["__fields_set__"] = set()

        # Remove v2-specific attributes that don't exist in v1
        state.pop("__pydantic_extra__", None)

        return state

    def save_global(self, obj, name=None):
        module = getattr(obj, "__module__", None)
        original_module = module
        try:
            if DATAHUB_EXECUTOR_PICKLE_COMPAT_MODE and module is not None:
                # Module name translation for compatibility
                new_module = module.replace("acryl_datahub_cloud", "datahub")
                if new_module != module:
                    obj.__module__ = new_module
                    logger.debug(f"Translated module name: {module} -> {new_module}")
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
