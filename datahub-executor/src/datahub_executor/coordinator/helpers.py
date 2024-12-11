import asyncio
import logging
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

from datahub_executor.common.client.fetcher.ingestion.fetcher import IngestionFetcher
from datahub_executor.common.client.fetcher.monitors.fetcher import MonitorFetcher
from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.config import (
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_INGESTION_ENABLED,
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_CONFIG_PATH,
    DATAHUB_EXECUTOR_WORKER_ID,
)
from datahub_executor.coordinator.config import get_ingestion_config, get_monitor_config
from datahub_executor.coordinator.ingestion import IngestionAction
from datahub_executor.coordinator.manager import ExecutionRequestManager
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

from .assertion_handlers import async_queue_start, async_queue_stop

logger = logging.getLogger(__name__)
manager = None


def start_ingestion_pipeline(sighandler: List[Callable]) -> None:
    config_dict = load_config_file(DATAHUB_EXECUTOR_INGESTION_PIPELINE_CONFIG_PATH)
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
            async_commit_enabled=True,
        ),
        ctx=PipelineContext(
            pipeline_name="datahub-executor-ingestion-pipeline",
            graph=create_datahub_graph(),
        ),
    )

    action = IngestionAction(
        DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
        DATAHUB_EXECUTOR_INGESTION_ENABLED,
        DATAHUB_EXECUTOR_WORKER_ID,
    )

    pipeline = Pipeline(
        "datahub-executor-ingestion-pipeline",
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


def start_scheduler(sighandler: List[Callable]) -> None:
    try:
        global manager

        # Create DataHub Client
        graph = create_datahub_graph()

        if DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED:
            discovery = DatahubExecutorDiscovery(graph)
            discovery.start()

        # Create a fetcher
        monitor_fetcher = MonitorFetcher(graph, get_monitor_config())
        ingestion_fetcher = IngestionFetcher(graph, get_ingestion_config())

        # Create a scheduler
        scheduler = ExecutionRequestScheduler(None, None, None, None)

        # Create a manager
        manager = ExecutionRequestManager(
            [monitor_fetcher, ingestion_fetcher], scheduler
        )

        if DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED:
            sighandler.append(discovery.stop)

        sighandler.append(scheduler.shutdown)
        sighandler.append(manager.shutdown)

        logger.info("Successfully created fetcher scheduler.")

        async_queue_start()
        sighandler.append(async_queue_stop)

        # Start the monitors
        manager.start()

    except Exception as e:
        logger.exception(f"Failed to create fetcher scheduler: {str(e)}")
        raise
