import os

from datahub_executor.common.types import FetcherConfig, FetcherMode
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INGESTION_ENABLED,
    DATAHUB_EXECUTOR_INGESTION_FETCHER_REFRESH_INTERVAL,
    DATAHUB_EXECUTOR_MONITORS_ENABLED,
    DATAHUB_EXECUTOR_MONITORS_FETCHER_REFRESH_INTERVAL,
)


def get_monitor_config() -> FetcherConfig:
    # Configure the monitor configs
    # TODO - fetch this from somewhere?
    executor_ids_str = os.getenv("DATAHUB_EXECUTOR_POOL_NAMES", None)
    executor_ids = executor_ids_str.split(",") if executor_ids_str else None
    return FetcherConfig(
        id="monitors-assertions",
        enabled=DATAHUB_EXECUTOR_MONITORS_ENABLED,
        mode=FetcherMode.DEFAULT,
        executor_ids=executor_ids,
        refresh_interval=DATAHUB_EXECUTOR_MONITORS_FETCHER_REFRESH_INTERVAL,
    )


def get_ingestion_config() -> FetcherConfig:
    # Configure the ingestion configs
    # TODO - fetch this from somewhere?
    executor_ids_str = os.getenv("DATAHUB_EXECUTOR_POOL_NAMES", None)
    executor_ids = executor_ids_str.split(",") if executor_ids_str else None
    return FetcherConfig(
        id="ingestion-sources",
        enabled=DATAHUB_EXECUTOR_INGESTION_ENABLED,
        mode=FetcherMode.DEFAULT,
        executor_ids=executor_ids,
        refresh_interval=DATAHUB_EXECUTOR_INGESTION_FETCHER_REFRESH_INTERVAL,
    )
