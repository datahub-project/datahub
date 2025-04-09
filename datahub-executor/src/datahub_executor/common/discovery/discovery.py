import logging
import time
from threading import Event, Thread

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import RemoteExecutorStatusClass

from datahub_executor.common.identity.base import (
    DATAHUB_EXECUTOR_IDENTITY,
    DATAHUB_EXECUTOR_IDENTITY_ADDRESS,
    DATAHUB_EXECUTOR_IDENTITY_BUILD_INFO,
    DATAHUB_EXECUTOR_IDENTITY_HOSTNAME,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.config import (
    DATAHUB_EXECUTOR_DISCOVERY_INTERVAL,
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_INTERNAL_WORKER,
    DATAHUB_EXECUTOR_MODE,
    DATAHUB_EXECUTOR_POOL_ID,
)

from .utils import get_backend_revision, get_utc_timestamp, send_remote_executor_status

logger = logging.getLogger(__name__)

# Due to rename of poolName to poolId, this code requires minimum backend revision of 2
REQUIRED_MIN_BACKEND_REVISION = 2


class DatahubExecutorDiscovery:
    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        self.start_time = time.time()
        self.enabled = False

        self.stop_event = Event()
        self.stop_flag = False
        self.loop = Thread(target=self._loop_handler)

    def _get_uptime(self) -> float:
        return time.time() - self.start_time

    def _ping(self) -> None:
        if not self.enabled:
            if not self.is_backend_discovery_capable():
                logger.error("Discovery disabled: backend is not discovery-capable.")
                return
            self.enabled = True

        with METRIC(
            "WORKER_DISCOVERY_PING_REQUESTS", pool_name=DATAHUB_EXECUTOR_POOL_ID
        ).time():
            try:
                status = RemoteExecutorStatusClass(
                    executorPoolId=DATAHUB_EXECUTOR_POOL_ID,
                    executorReleaseVersion=DATAHUB_EXECUTOR_IDENTITY_BUILD_INFO.get_version(),
                    executorAddress=DATAHUB_EXECUTOR_IDENTITY_ADDRESS,
                    executorHostname=DATAHUB_EXECUTOR_IDENTITY_HOSTNAME,
                    executorUptime=self._get_uptime(),
                    executorStopped=self.stop_flag,
                    executorEmbedded=(
                        DATAHUB_EXECUTOR_MODE == "coordinator"
                        and DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED
                    ),
                    executorInternal=DATAHUB_EXECUTOR_INTERNAL_WORKER,
                    logDeliveryEnabled=False,
                    reportedAt=get_utc_timestamp(),
                )
                logger.info("Discovery: sending status update.")

                send_remote_executor_status(
                    self.graph, DATAHUB_EXECUTOR_IDENTITY, status
                )
            except Exception as e:
                METRIC(
                    "WORKER_DISCOVERY_PING_ERRORS", pool_name=DATAHUB_EXECUTOR_POOL_ID
                ).inc()
                logger.error(f"Discovery: failed to sending status to GMS: {e}")
            return

    def _loop_handler(self) -> None:
        while not self.stop_flag:
            try:
                self.stop_event.clear()
                self.stop_event.wait(timeout=DATAHUB_EXECUTOR_DISCOVERY_INTERVAL)
                if not self.stop_flag:
                    self._ping()
            except Exception as e:
                logger.error(f"Discovery: error in discovery loop: {e}")

    def is_backend_discovery_capable(self) -> bool:
        revision = get_backend_revision(self.graph)
        return revision >= REQUIRED_MIN_BACKEND_REVISION

    def start(self) -> None:
        version = DATAHUB_EXECUTOR_IDENTITY_BUILD_INFO.get_version()
        logger.warning(
            f"Discovery: starting discovery loop; Instance ID = {DATAHUB_EXECUTOR_IDENTITY}; Version = {version}; Update interval = {DATAHUB_EXECUTOR_DISCOVERY_INTERVAL}"
        )

        # Register itself with GMS before starting the loop
        self._ping()
        self.loop.start()

    def stop(self) -> None:
        logger.info("Discovery: shutting down")

        self.stop_flag = True
        self.stop_event.set()
        self.loop.join()

        # Send final "deregister" event to GMS
        self._ping()
