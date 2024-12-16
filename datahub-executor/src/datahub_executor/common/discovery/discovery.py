import logging
import socket
import time
from threading import Event, Thread

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import RemoteExecutorStatusClass

from datahub_executor.common.constants import (
    DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME,
    DATAHUB_URN_MAX_LEN,
)
from datahub_executor.config import (
    DATAHUB_EXECUTOR_DISCOVERY_INTERVAL,
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_INTERNAL_WORKER,
    DATAHUB_EXECUTOR_NAMESPACE,
    DATAHUB_EXECUTOR_WORKER_ID,
    DATAHUB_GMS_URL,
)

from .build_info import BuildInfo
from .utils import (
    get_hostname_from_url,
    get_random_string,
    get_string_hash,
    get_utc_timestamp,
    send_remote_executor_status,
)

logger = logging.getLogger(__name__)


class DatahubExecutorDiscovery:
    def __init__(self, graph: DataHubGraph):
        self.graph = graph
        self.start_time = time.time()

        self.stop_event = Event()
        self.stop_flag = False
        self.loop = Thread(target=self._loop_handler)

        self.my_hostname = socket.gethostname()
        self.my_address = socket.gethostbyname(self.my_hostname)
        self.my_address_hash = get_string_hash(self.my_address)

        self.build_info = BuildInfo()
        self.instance_id = self._generate_instance_id()

    def _generate_instance_id(self) -> str:
        # InstanceID has the following format:
        #     {address-hash8}-{random4}.{executor-id}.{gms-host}
        #
        # For internal use cases, gms-host is always set to "dh-datahub-gms",
        # which is non-descriptive and thus namespace is used instead.

        if DATAHUB_EXECUTOR_INTERNAL_WORKER and DATAHUB_EXECUTOR_NAMESPACE is not None:
            hostname = DATAHUB_EXECUTOR_NAMESPACE
        else:
            hostname = get_hostname_from_url(DATAHUB_GMS_URL)

        instance_id = "{}-{}.{}.{}".format(
            self.my_address_hash,
            get_random_string(8),
            DATAHUB_EXECUTOR_WORKER_ID,
            hostname,
        )

        # Max URN length is 500 chars, including prefix. Truncate if ID length exceeds the limit
        # to make it consistent across fields that do not enforce the limit. Keep the left-most part
        # of the ID intact.
        max_len = DATAHUB_URN_MAX_LEN - len(DATAHUB_REMOTE_EXECUTOR_ENTITY_NAME) - 10
        if len(instance_id) > max_len:
            logger.warning(
                f"Discovery: instance_id is longer than {max_len} and will be truncated."
            )
            instance_id = instance_id[:max_len]

        return instance_id

    def _ping(self) -> None:
        status = RemoteExecutorStatusClass(
            executorId=DATAHUB_EXECUTOR_WORKER_ID,
            executorReleaseVersion=self.build_info.get_version(),
            executorAddress=self.my_address,
            executorHostname=self.my_hostname,
            executorUptime=self.get_uptime(),
            executorStopped=self.stop_flag,
            executorEmbedded=DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
            executorInternal=DATAHUB_EXECUTOR_INTERNAL_WORKER,
            logDeliveryEnabled=False,
            reportedAt=get_utc_timestamp(),
        )
        logger.info("Discovery: sending status update.")
        send_remote_executor_status(self.graph, self.instance_id, status)
        return

    def _loop_handler(self) -> None:
        while not self.stop_flag:
            self.stop_event.clear()
            self.stop_event.wait(timeout=DATAHUB_EXECUTOR_DISCOVERY_INTERVAL)
            if not self.stop_flag:
                self._ping()

    def get_instance_id(self) -> str:
        return self.instance_id

    def get_build_info(self) -> BuildInfo:
        return self.build_info

    def get_uptime(self) -> float:
        return time.time() - self.start_time

    def start(self) -> None:
        version = self.build_info.get_version()
        logger.warning(
            f"Discovery: starting discovery loop; Instance ID = {self.instance_id}; Version = {version}; Update interval = {DATAHUB_EXECUTOR_DISCOVERY_INTERVAL}"
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
