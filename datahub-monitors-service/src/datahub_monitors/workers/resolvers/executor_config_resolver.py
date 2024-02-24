import datetime
import logging
from typing import Callable, List, Optional, Tuple, Type, TypeVar

from tenacity import retry, stop_after_attempt, wait_exponential
from tenacity.before_sleep import before_sleep_log

from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.common.helpers import create_datahub_graph
from datahub_monitors.workers.graphql.query import GRAPHQL_FETCH_EXECUTOR_CONFIGS
from datahub_monitors.workers.types import ExecutorConfig

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

    graph: DataHubAssertionGraph
    executor_configs: List[ExecutorConfig]

    def __init__(self, graph: Optional[DataHubAssertionGraph] = None) -> None:
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
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=4, max=10),
        reraise=True,
        before_sleep=before_sleep_log(logger, logging.ERROR, True),
    )
    def _fetch_executor_configs(self) -> List[ExecutorConfig]:
        result = self.graph.execute_graphql(GRAPHQL_FETCH_EXECUTOR_CONFIGS)
        if "error" in result and result["error"] is not None:
            logger.error(
                f"Received error while fetching executor_configs from GMS! {result.get('error')}"
            )
            return []

        if (
            "listExecutorConfigs" not in result
            or "executorConfigs" not in result["listExecutorConfigs"]
        ):
            logger.error(
                "Found incomplete search results when fetching executor_configs from GMS!"
            )
            return []

        executor_configs = []
        for credential in result["listExecutorConfigs"]["executorConfigs"]:
            try:
                executor_configs.append(ExecutorConfig.parse_obj(credential))
            except Exception:
                logger.exception(
                    f"Failed to convert ExecutorConfig object to Python object. {credential}"
                )

        self.executor_configs = executor_configs
        return self.executor_configs
