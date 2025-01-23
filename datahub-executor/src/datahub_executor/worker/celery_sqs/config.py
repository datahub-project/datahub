import logging
from typing import Any, Dict, List

from celery import Celery
from kombu import Queue
from kombu.utils.url import safequote

from datahub_executor.common.client.config.resolver import ExecutorConfigResolver
from datahub_executor.common.monitoring.metrics import (
    STATS_CREDENTIALS_REFRESH_ERRORS,
    STATS_CREDENTIALS_REFRESH_REQUESTS,
)
from datahub_executor.common.types import ExecutorConfig
from datahub_executor.config import (
    DATAHUB_EXECUTOR_POOL_NAME,
    DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT,
)

logger = logging.getLogger(__name__)


class CeleryConfig:
    task_serializer = "pickle"
    result_serializer = "pickle"
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
        STATS_CREDENTIALS_REFRESH_REQUESTS.labels(DATAHUB_EXECUTOR_POOL_NAME).inc()

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
            STATS_CREDENTIALS_REFRESH_REQUESTS.labels(DATAHUB_EXECUTOR_POOL_NAME).inc()

            config = update_celery_config(CeleryConfig(), executor_configs)
            app.config_from_object(config)

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
            STATS_CREDENTIALS_REFRESH_ERRORS.labels(
                "NoQueue", DATAHUB_EXECUTOR_POOL_NAME
            ).inc()
            logger.error(f"SQS qeueue {queue_name} does not exist or misconfigured.")
            return False

    return True
