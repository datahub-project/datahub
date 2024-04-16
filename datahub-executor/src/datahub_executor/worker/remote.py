import logging
from typing import Any

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.metadata.schema_classes import MetadataChangeLogClass

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
            return

        # for others (monitors/assertions) we directly trigger the task run.
        task = assertion_request.apply_async(
            args=[execution_request],
            task_id=execution_request.args["urn"],
            queue=executor_id,
            routing_key=f"{executor_id}.assertion_request",
        )
        return task
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
            return

        # for others (monitors/assertions) we directly trigger the task run.
        task = ingestion_request.apply_async(
            args=[event],
            task_id=event.entityUrn,
            queue=executor_id,
            routing_key=f"{executor_id}.ingestion_request",
        )
        return task
    else:
        # not implemented
        logger.error(
            f"Worker implementation {DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION} is not available."
        )
