import json
import logging
import time
from threading import Thread
from typing import cast

from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE

from datahub_executor.common.constants import (
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME,
)
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.ingestion.helpers import (
    extract_execution_request,
    handle_ingestion_signal_requests,
    setup_ingestion_executor,
)
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS,
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_SIGNAL_POLL_INTERVAL,
)
from datahub_executor.worker.remote import apply_remote_ingestion_request

logger = logging.getLogger(__name__)


class IngestionAction(Action):
    ingestion_enabled: bool
    embedded_worker_enabled: bool
    embedded_worker_id: str = ""

    def __init__(
        self,
        embedded_worker_enabled: bool,
        ingestion_enabled: bool,
        embedded_worker_id: str = "",
    ) -> None:
        self.ingestion_enabled = ingestion_enabled
        self.embedded_worker_id = embedded_worker_id
        self.embedded_worker_enabled = embedded_worker_enabled
        self.shutdown_flag = False

        if self.embedded_worker_enabled:
            self.ingestion_executor = setup_ingestion_executor()
            self.tp = ThreadPoolExecutorWithQueueSizeLimit(
                max_workers=DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS
            )
            self.signal_thread = Thread(target=self._signal_thread_worker)
            self.signal_thread.start()

    @classmethod
    def create(
        cls,
        embedded_worker_enabled: bool,
        ingestion_enabled: bool,
        embedded_worker_id: str = "",
    ) -> Action:
        return cls(embedded_worker_enabled, ingestion_enabled, embedded_worker_id)

    def close(self) -> None:
        return super().close()

    def shutdown(self, wait: bool = True) -> None:
        self.shutdown_flag = True

        if self.embedded_worker_enabled:
            self.tp.shutdown(wait)
            self.signal_thread.join()

    def act(self, event: EventEnvelope) -> None:
        """This method listens for ExecutionRequest changes to execute in schedule and trigger events"""
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            if (
                orig_event.get("entityType") == DATAHUB_EXECUTION_REQUEST_ENTITY_NAME
                and orig_event.get("changeType") == "UPSERT"
            ):
                if (
                    orig_event.get("aspectName")
                    == DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME
                ):
                    logger.debug("Received execution request input. Processing...")
                    self._handle_execution_request_input(orig_event)

    def _handle_execution_request_input(
        self, orig_event: MetadataChangeLogClass
    ) -> None:
        if self.ingestion_enabled is False:
            logger.info(f"Ingestion disabled, ignoring {orig_event.entityUrn}")
            return

        if not orig_event.aspect:
            logger.error(
                f"Unable to parse Execution Request Input, no aspect {orig_event.entityUrn}.."
            )
            return

        aspect_dict = json.loads(orig_event.aspect.value)

        if (
            self.embedded_worker_enabled
            and aspect_dict["executorId"] == self.embedded_worker_id
        ):
            # submit will block if queue size > worker_count
            self.tp.submit(self._apply_ingestion_request, orig_event)

            logger.info("started task ingetsion_request on a local thread")
        else:
            task = apply_remote_ingestion_request(orig_event, aspect_dict["executorId"])

            logger.info(f"started task ingestion_request task_id = {task.id}")

    def _apply_ingestion_request(self, orig_event: MetadataChangeLogClass) -> None:
        execution_request = extract_execution_request(orig_event)
        if execution_request:
            self.ingestion_executor.execute(execution_request)

    def _signal_thread_worker(self) -> None:
        if self.embedded_worker_enabled:
            graph = create_datahub_graph()

            while not self.shutdown_flag:
                handle_ingestion_signal_requests(graph, self.ingestion_executor)
                time.sleep(DATAHUB_EXECUTOR_INGESTION_PIPELINE_SIGNAL_POLL_INTERVAL)
