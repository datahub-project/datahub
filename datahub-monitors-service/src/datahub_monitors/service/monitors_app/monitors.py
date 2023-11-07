import logging
import os

from datahub_monitors.common.constants import (
    LIST_INGESTION_REFRESH_INTERVAL_MINUTES,
    LIST_MONITORS_REFRESH_INTERVAL_MINUTES,
)
from datahub_monitors.common.helpers import create_datahub_graph
from datahub_monitors.config import INGESTION_ENABLED, MONITORS_ENABLED
from datahub_monitors.service.scheduler.fetcher.config import FetcherConfig, FetcherMode
from datahub_monitors.service.scheduler.ingestion.fetcher.fetcher import (
    IngestionFetcher,
)
from datahub_monitors.service.scheduler.manager import ExecutionRequestManager
from datahub_monitors.service.scheduler.monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.service.scheduler.scheduler import ExecutionRequestScheduler

logger = logging.getLogger(__name__)

manager = None


def get_monitor_config() -> FetcherConfig:
    # Configure the monitor configs
    # TODO - fetch this from somewhere?
    executor_ids_str = os.getenv("DATAHUB_MONITORS_EXECUTOR_IDS", None)
    executor_ids = executor_ids_str.split(",") if executor_ids_str else None
    return FetcherConfig(
        id="monitors-assertions",
        enabled=MONITORS_ENABLED,
        mode=FetcherMode.DEFAULT,
        executor_ids=executor_ids,
        refresh_interval=LIST_MONITORS_REFRESH_INTERVAL_MINUTES,
    )


def get_ingestion_config() -> FetcherConfig:
    # Configure the ingestion configs
    # TODO - fetch this from somewhere?
    executor_ids_str = os.getenv("DATAHUB_MONITORS_EXECUTOR_IDS", None)
    executor_ids = executor_ids_str.split(",") if executor_ids_str else None
    return FetcherConfig(
        id="ingestion-sources",
        enabled=INGESTION_ENABLED,
        mode=FetcherMode.DEFAULT,
        executor_ids=executor_ids,
        refresh_interval=LIST_INGESTION_REFRESH_INTERVAL_MINUTES,
    )


def start_async_execution_request_manager() -> None:
    try:
        # Create DataHub Client
        graph = create_datahub_graph()

        # Create a fetcher
        monitor_fetcher = MonitorFetcher(graph, get_monitor_config())
        ingestion_fetcher = IngestionFetcher(graph, get_ingestion_config())

        # Create a scheduler
        scheduler = ExecutionRequestScheduler(None, None, None)

        # Create a manager
        manager = ExecutionRequestManager(
            [monitor_fetcher, ingestion_fetcher], scheduler
        )

        logger.info("Successfully created monitor manager! Starting the monitors...")

        # Start the monitors
        manager.start()

    except Exception:
        logger.exception("Failed to create monitor manager! Cannot start up monitors.")
