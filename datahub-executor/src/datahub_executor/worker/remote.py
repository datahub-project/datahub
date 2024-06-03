import logging
from typing import Any

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.metadata.schema_classes import MetadataChangeLogClass

from datahub_executor.common.constants import SQS_MESSAGE_MAX_LENGTH
from datahub_executor.common.monitoring.metrics import (
    STATS_SCHEDULER_SQS_LIMIT_EXCEEDED,
)
from datahub_executor.config import DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION
from datahub_executor.worker.celery_sqs.app import assertion_request, ingestion_request
from datahub_executor.worker.celery_sqs.config import update_celery_credentials
from datahub_executor.worker.celery_sqs.init import app

logger = logging.getLogger(__name__)


def apply_remote_assertion_request(
    execution_request: ExecutionRequest, executor_id: str
) -> Any:
    if DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION == "default":
        logger.info(
            f"Going to submit SQS assertion execution request {execution_request.args['urn']} via {executor_id}"
        )

        # before we try to send a task over celery, we make sure we have valid SQS creds
        if not update_celery_credentials(app, False, executor_id):
            return execution_request.args["urn"]

        message_size = len(execution_request.json())
        if message_size > SQS_MESSAGE_MAX_LENGTH:
            STATS_SCHEDULER_SQS_LIMIT_EXCEEDED.inc()
            logger.error(
                f"Assertion ExecutionRequest {execution_request.args['urn']} is too big ({message_size}) to send via SQS and will be dropped."
            )
            return execution_request.args["urn"]

        # for others (monitors/assertions) we directly trigger the task run.
        assertion_request.apply_async(
            args=[execution_request],
            task_id=execution_request.args["urn"],
            queue=executor_id,
            routing_key=f"{executor_id}.assertion_request",
        )
        return execution_request.args["urn"]
    else:
        # not implemented
        logger.error(
            f"Worker implementation {DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION} is not available."
        )


def apply_remote_ingestion_request(
    event: MetadataChangeLogClass, executor_id: str
) -> Any:
    if DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION == "default":
        logger.info(
            f"Going to submit SQS ingestion execution request {event.entityUrn} via {executor_id}"
        )

        # before we try to send a task over celery, we make sure we have valid SQS creds
        if not update_celery_credentials(app, False, executor_id):
            return event.entityUrn

        if event.aspect is None:
            logger.error(
                f"Ingestion ExecutionRequest {event.entityUrn} has no aspect and will be dropped."
            )
            return event.entityUrn

        message_size = len(event.aspect.value)
        if message_size > SQS_MESSAGE_MAX_LENGTH:
            STATS_SCHEDULER_SQS_LIMIT_EXCEEDED.inc()
            logger.error(
                f"Ingestion ExecutionRequest {event.entityUrn} is too big ({message_size}) to send via SQS and will be dropped."
            )
            return event.entityUrn

        # for others (monitors/assertions) we directly trigger the task run.
        ingestion_request.apply_async(
            args=[event],
            task_id=event.entityUrn,
            queue=executor_id,
            routing_key=f"{executor_id}.ingestion_request",
        )
        return event.entityUrn
    else:
        # not implemented
        logger.error(
            f"Worker implementation {DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION} is not available."
        )
