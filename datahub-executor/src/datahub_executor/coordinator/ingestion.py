import json
import logging
import time
from threading import Thread
from typing import Optional, cast

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
)
from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
from datahub_executor.common.ingestion.helpers import (
    extract_execution_request,
    extract_execution_request_weight,
    handle_ingestion_signal_requests,
    setup_ingestion_executor,
)
from datahub_executor.common.monitoring.metrics import (
    STATS_INGESTION_HANDLER_ERRORS,
    STATS_INGESTION_HANDLER_REQUESTS,
    STATS_INGESTION_KAFKA_EXEC_EVENTS,
    STATS_INGESTION_KAFKA_MCL_EVENTS,
)
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS,
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_SIGNAL_POLL_INTERVAL,
    DATAHUB_EXECUTOR_POOL_NAME,
)
from datahub_executor.worker.remote import apply_remote_ingestion_request

logger = logging.getLogger(__name__)


class IngestionAction(Action):
    graph: DataHubGraph
    discovery: Optional[DatahubExecutorDiscovery]
    ingestion_enabled: bool
    embedded_worker_enabled: bool
    embedded_worker_id: str = DATAHUB_EXECUTOR_POOL_NAME

    def __init__(
        self,
        graph: DataHubGraph,
        discovery: Optional[DatahubExecutorDiscovery],
        embedded_worker_enabled: bool,
        ingestion_enabled: bool,
        embedded_worker_id: str = DATAHUB_EXECUTOR_POOL_NAME,
    ) -> None:
        self.ingestion_enabled = ingestion_enabled
        self.embedded_worker_id = embedded_worker_id
        self.embedded_worker_enabled = embedded_worker_enabled
        self.shutdown_flag = False
        self.graph = graph

        if self.embedded_worker_enabled:
            if discovery is None:
                self.ingestion_executor = setup_ingestion_executor()
            else:
                self.ingestion_executor = setup_ingestion_executor(
                    executor_instance_id=discovery.get_instance_id(),
                    executor_version=discovery.get_build_info().get_version(),
                )
            self.tp = ThreadPoolExecutorWithQueueSizeLimit(
                max_workers=DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS,
                name="ingestions",
            )
            self.signal_thread = Thread(target=self._signal_thread_worker)
            self.signal_thread.start()

    @classmethod
    def create(
        cls,
        graph: DataHubGraph,
        discovery: Optional[DatahubExecutorDiscovery],
        embedded_worker_enabled: bool,
        ingestion_enabled: bool,
        embedded_worker_id: str = DATAHUB_EXECUTOR_POOL_NAME,
    ) -> Action:
        return cls(
            graph,
            discovery,
            embedded_worker_enabled,
            ingestion_enabled,
            embedded_worker_id,
        )

    def close(self) -> None:
        return super().close()

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_flag = True

        if self.embedded_worker_enabled:
            self.tp.shutdown(wait)
            self.signal_thread.join()

    def act(self, event: EventEnvelope) -> bool:
        """This method listens for ExecutionRequest changes to execute in schedule and trigger events"""
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            STATS_INGESTION_KAFKA_MCL_EVENTS.inc()
            if (
                orig_event.get("entityType") == DATAHUB_EXECUTION_REQUEST_ENTITY_NAME
                and orig_event.get("changeType") == "UPSERT"
            ):
                if (
                    orig_event.get("aspectName")
                    == DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME
                ):
                    STATS_INGESTION_KAFKA_EXEC_EVENTS.inc()
                    logger.debug("Received execution request input. Processing...")
                    self._handle_execution_request_input(orig_event)
                    return True
        return False

    def _handle_execution_request_input(
        self, orig_event: MetadataChangeLogClass
    ) -> None:
        if self.ingestion_enabled is False:
            STATS_INGESTION_HANDLER_ERRORS.labels("IngestionsDisabled").inc()
            logger.info(f"Ingestion disabled, ignoring {orig_event.entityUrn}")
            return

        if not orig_event.aspect:
            STATS_INGESTION_HANDLER_ERRORS.labels("ParseError").inc()
            logger.error(
                f"Unable to parse Execution Request Input, no aspect {orig_event.entityUrn}.."
            )
            return

        aspect_dict = json.loads(orig_event.aspect.value)

        executor_id = aspect_dict["executorId"]

        if self.embedded_worker_enabled and (
            executor_id is None or executor_id == self.embedded_worker_id
        ):
            STATS_INGESTION_HANDLER_REQUESTS.labels(executor_id, "true").inc()
            self._apply_ingestion_request(orig_event)
        else:
            STATS_INGESTION_HANDLER_REQUESTS.labels(executor_id, "false").inc()
            task = apply_remote_ingestion_request(orig_event, aspect_dict["executorId"])
            logger.info(f"started task ingestion_request for exec_id = {task}")

    def _execute_wrapper(self, request: ExecutionRequest) -> None:
        try:
            self.ingestion_executor.execute(request)
        except Exception:
            logger.exception("Error executing ingestion: ")

    def _apply_ingestion_request(self, orig_event: MetadataChangeLogClass) -> None:
        execution_request = extract_execution_request(orig_event, self.graph)
        if execution_request:
            weight = extract_execution_request_weight(execution_request)
            logger.info(
                f"Starting ingestion task {execution_request.exec_id} on a local thread with weight = {weight}"
            )

            self.tp.submit_weighted(weight, self._execute_wrapper, execution_request)

    def _signal_thread_worker(self) -> None:
        if self.embedded_worker_enabled:
            while not self.shutdown_flag:
                handle_ingestion_signal_requests(self.graph, self.ingestion_executor)
                time.sleep(DATAHUB_EXECUTOR_INGESTION_PIPELINE_SIGNAL_POLL_INTERVAL)
