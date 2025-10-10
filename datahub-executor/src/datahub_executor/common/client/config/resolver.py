import datetime
import logging
from typing import Callable, List, Optional, Tuple, Type, TypeVar

from datahub.ingestion.graph.client import DataHubGraph
from tenacity import retry, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_executor.common.client.config.graphql.query import (
    GRAPHQL_FETCH_EXECUTOR_CONFIGS,
)
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import ExecutorConfig
from datahub_executor.config import DATAHUB_EXECUTOR_MODE

CREDENTIAL_EXPIRY_DELTA = 5

logger = logging.getLogger(__name__)


T = TypeVar("T")


def singleton(cls: Type[T]) -> Callable[..., T]:
    instances = {}

    def get_instance(*args, **kwargs) -> T:  # type: ignore
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)  # type: ignore
        return instances[cls]

    return get_instance


@singleton
class ExecutorConfigResolver:
    """Resolver class responsible for resolving/fetching executor configuration"""

    graph: DataHubGraph
    executor_configs: List[ExecutorConfig]

    def __init__(self, graph: Optional[DataHubGraph] = None) -> None:
        if graph:
            self.graph = graph
        else:
            self.graph = create_datahub_graph()
        self.executor_configs = []

    def get_executor_configs(self) -> List[ExecutorConfig]:
        if not self.executor_configs:
            self.executor_configs = self.fetch_executor_configs()
        return self.executor_configs

    def refresh_executor_configs(self) -> Tuple[bool, List[ExecutorConfig]]:
        expiring = False
        for creds in self.executor_configs:
            if (
                creds.expiration
                and datetime.datetime.now(datetime.timezone.utc)
                + datetime.timedelta(minutes=CREDENTIAL_EXPIRY_DELTA)
                > creds.expiration
            ):
                expiring = True

        if expiring is True or not self.executor_configs:
            logger.info("Refreshing executor_configs - fetching new from GMS")
            self.executor_configs = self.fetch_executor_configs()
            expiring = True

        return expiring, self.executor_configs

    def fetch_executor_configs(self) -> List[ExecutorConfig]:
        return self._fetch_executor_configs()

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=60),
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
        retry_error_callback=lambda x: METRIC(
            "WORKER_CONFIG_FETCHER_ERRORS", exception="Retry"
        ).inc(),
    )
    @METRIC("WORKER_CONFIG_FETCHER_REQUESTS").time()  # type: ignore
    def _fetch_executor_configs(self) -> List[ExecutorConfig]:
        result = self.graph.execute_graphql(GRAPHQL_FETCH_EXECUTOR_CONFIGS)

        if "error" in result and result["error"] is not None:
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="GmsError").inc()
            raise Exception(
                f"Received error while fetching executor_configs from GMS! {result.get('error')}"
            )

        if (
            "listExecutorConfigs" not in result
            or "executorConfigs" not in result["listExecutorConfigs"]
        ):
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="IncompleteResults").inc()
            raise Exception(
                "Found incomplete search results when fetching executor_configs from GMS!"
            )

        # In worker mode, having no queues means that worker never has work to do, so we have to retry until we get any
        if (
            DATAHUB_EXECUTOR_MODE != "coordinator"
            and len(result["listExecutorConfigs"]["executorConfigs"]) == 0
        ):
            METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="EmptyConfig").inc()
            raise Exception("GMS returned no executor configs, unable to proceed.")

        executor_configs = []
        for credential in result["listExecutorConfigs"]["executorConfigs"]:
            try:
                executor_configs.append(ExecutorConfig.model_validate(credential))
            except Exception:
                METRIC("WORKER_CONFIG_FETCHER_ERRORS", exception="ParseError").inc()
                raise Exception(
                    f"Failed to convert ExecutorConfig object to Python object. {credential}"
                )

        self.executor_configs = executor_configs

        return self.executor_configs
