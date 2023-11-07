from typing import Any, Dict, List

from kombu import Queue
from kombu.utils.url import safequote

from datahub_monitors.workers.types import ExecutorConfig


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
    transport_options: Dict[str, Any] = {}


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
        config.broker_url = f"sqs://{safequote(executor_configs[0].access_key)}:{safequote(executor_configs[0].secret_key)}@"
        config.transport_options = {
            "region": executor_configs[0].region,
            "predefined_queues": queues,
        }
    return config
