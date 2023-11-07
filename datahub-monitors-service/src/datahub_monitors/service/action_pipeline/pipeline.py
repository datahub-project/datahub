import asyncio
import os
import threading

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
    EMBEDDED_WORKER_ENABLED,
    EMBEDDED_WORKER_ID,
    INGESTION_ENABLED,
)

from .action import MonitorServiceAction


def start_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()


def start_action_pipeline() -> None:
    source = KafkaEventSource(
        config=KafkaEventSourceConfig(
            connection=KafkaConsumerConnectionConfig(
                bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092"),
                schema_registry_url=os.getenv(
                    "SCHEMA_REGISTRY_URL", "http://schema-registry:8081"
                ),
            )
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

    # thanks chatGPT!
    new_loop = asyncio.new_event_loop()

    # Start the event loop in a new thread
    t = threading.Thread(target=start_loop, args=(new_loop,))
    t.start()

    def start_action_pipeline() -> None:
        asyncio.run_coroutine_threadsafe(pipeline.start(), new_loop)

    # Call the function to run the async function
    start_action_pipeline()
