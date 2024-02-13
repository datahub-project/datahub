import json
import logging
import threading
from typing import cast

from datahub.metadata.schema_classes import MetadataChangeLogClass
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import METADATA_CHANGE_LOG_EVENT_V1_TYPE

from datahub_monitors.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_monitors.workers.helpers import (
    extract_execution_request,
    extract_execution_request_signal,
    setup_ingestion_executor,
    update_celery_credentials,
)
from datahub_monitors.workers.tasks import app, evaluate_execution_request_input

from datahub_monitors.config import (
    ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS,
)

DATAHUB_EXECUTION_REQUEST_ENTITY_NAME = "dataHubExecutionRequest"
DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME = "dataHubExecutionRequestInput"
DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME = "dataHubExecutionRequestSignal"

logger = logging.getLogger(__name__)


class MonitorServiceAction(Action):
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
        if self.embedded_worker_enabled:
            self.ingestion_executor = setup_ingestion_executor()
            self.tp = ThreadPoolExecutorWithQueueSizeLimit(max_workers = ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS)

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

    def shutdown(self, wait = True) -> None:
        if self.embedded_worker_enabled:
            self.tp.shutdown(wait)

    def act(self, event: EventEnvelope) -> None:
        """This method listens for ExecutionRequest changes to execute in schedule and trigger events"""
        if event.event_type is METADATA_CHANGE_LOG_EVENT_V1_TYPE:
            orig_event = cast(MetadataChangeLogClass, event.event)
            if (orig_event.get("entityType") == DATAHUB_EXECUTION_REQUEST_ENTITY_NAME and
                  orig_event.get("changeType") == "UPSERT"):
                if (orig_event.get("aspectName") == DATAHUB_EXECUTION_REQUEST_INPUT_ASPECT_NAME):
                    logger.debug("Received execution request input. Processing...")
                    self._handle_execution_request_input(orig_event)
                elif (orig_event.get("aspectName") == DATAHUB_EXECUTION_REQUEST_SIGNAL_ASPECT_NAME):
                    if self.embedded_worker_enabled:
                        logger.info("Received execution request signal. Processing...")
                        self._handle_embedded_worker_enabled_execution_request_signal(orig_event)

    def _handle_execution_request_input(self, orig_event: MetadataChangeLogClass) -> None:
        if self.ingestion_enabled is False:
            logger.info(f"Ingestion disabled, ignoring {orig_event.entityUrn}")
            return

        if not orig_event.aspect:
            logger.error(f"Unable to parse Execution Request Input, no aspect {orig_event.entityUrn}..")
            return

        aspect_dict = json.loads(orig_event.aspect.value)

        if (self.embedded_worker_enabled and aspect_dict["executorId"] == self.embedded_worker_id):
            # submit will block if queue size > worker_count
            self.tp.submit(self._evaluate_execution_request_input, orig_event)

            logger.info("started task evaluate_execution_request_input on a local thread")
        else:
            # before we try to send a task over celery, we make sure we have valid SQS creds
            update_celery_credentials(app, False, aspect_dict["executorId"])

            task = evaluate_execution_request_input.apply_async(
                args=[orig_event],
                task_id=orig_event.entityUrn,
                queue=aspect_dict["executorId"],
                routing_key=f'{aspect_dict["executorId"]}.evaluate_execution_request_input',
            )
            logger.info(f"started task evaluate_execution_request_input task_id = {task.id}")

    def _handle_embedded_worker_enabled_execution_request_signal(self, orig_event: MetadataChangeLogClass) -> None:
        if not orig_event.aspect:
            logger.error(f"Unable to parse Execution Request Signal, no aspect {orig_event.entityUrn}.."
            )
            return None

        aspect_dict = json.loads(orig_event.aspect.value)
        if aspect_dict["signal"] == "KILL":
            signal, exec_id = extract_execution_request_signal(orig_event)
            if signal and exec_id and exec_id in self.ingestion_executor.task_futures:
                self.ingestion_executor.signal(signal)

    def _evaluate_execution_request_input(
        self, orig_event: MetadataChangeLogClass
    ) -> None:
        execution_request = extract_execution_request(orig_event)
        if execution_request:
            self.ingestion_executor.execute(execution_request)
