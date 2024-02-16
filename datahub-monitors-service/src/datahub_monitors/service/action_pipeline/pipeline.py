import asyncio
from threading import Thread
from typing import Callable, List

from datahub.configuration.config_loader import load_config_file
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub_actions.pipeline.pipeline import (
    DEFAULT_FAILED_EVENTS_DIR,
    DEFAULT_FAILURE_MODE,
    DEFAULT_RETRY_COUNT,
    Pipeline,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)

from datahub_monitors.common.helpers import create_datahub_graph
from datahub_monitors.config import (
    ACTIONS_PIPELINE_CONFIG_PATH,
    EMBEDDED_WORKER_ENABLED,
    EMBEDDED_WORKER_ID,
    INGESTION_ENABLED,
)

from .action import MonitorServiceAction


def start_async_action_pipeline(sighandler: List[Callable]) -> None:
    config_dict = load_config_file(ACTIONS_PIPELINE_CONFIG_PATH)
    connection = config_dict.get("connection", {})

    source = KafkaEventSource(
        config=KafkaEventSourceConfig(
            connection=KafkaConsumerConnectionConfig(
                bootstrap=connection.get("bootstrap", "broker:9092"),
                schema_registry_url=connection.get(
                    "schema_registry_url", "http://schema-registry:8081"
                ),
                consumer_config=connection.get("consumer_config", {}),
            ),
            topic_routes=config_dict.get("topic_routes", None),
        ),
        ctx=PipelineContext(
            pipeline_name="monitors-service-action-pipeline",
            graph=create_datahub_graph(),
        ),
    )

    action = MonitorServiceAction(
        EMBEDDED_WORKER_ENABLED, INGESTION_ENABLED, EMBEDDED_WORKER_ID
    )

    pipeline = Pipeline(
        "monitors-service-action-pipeline",
        source,
        [],
        action,
        retry_count=DEFAULT_RETRY_COUNT,
        failure_mode=DEFAULT_FAILURE_MODE,
        failed_events_dir=DEFAULT_FAILED_EVENTS_DIR,
    )

    # uvicorn overrides global event loop implementation to uvloop. uvloop does not work correctly with create_subprocess_exec(),
    # which is used in acryl-executor. See https://github.com/MagicStack/uvloop/issues/508 for repro case. As a workaround,
    # we reset event_loop implementation back to python's native.
    asyncio.set_event_loop_policy(None)

    # setup sigint handlers
    sighandler.append(action.shutdown)
    sighandler.append(pipeline.stop)

    pt = Thread(target=pipeline.run)
    pt.start()
