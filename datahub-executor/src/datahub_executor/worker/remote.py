import json
import logging
from typing import Any

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import GenericAspectClass, MetadataChangeLogClass

from datahub_executor.common.constants import (
    CLI_EXECUTOR_ID,
    DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
    DATAHUB_EXECUTION_REQUEST_SQS_TRUNCATED_ASPECT_NAME,
)
from datahub_executor.common.helpers import (
    fetch_cloud_logging_config,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.types import CloudLoggingConfig
from datahub_executor.config import (
    DATAHUB_EXECUTOR_SQS_MESSAGE_MAX_LENGTH,
    DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION,
)
from datahub_executor.worker.celery_sqs.app import (
    assertion_request,
    ingestion_request,
    monitor_training_request,
)
from datahub_executor.worker.celery_sqs.config import update_celery_credentials
from datahub_executor.worker.celery_sqs.init import app

logger = logging.getLogger(__name__)


def apply_remote_assertion_request(
    execution_request: ExecutionRequest, executor_id: str
) -> Any:
    if executor_id == CLI_EXECUTOR_ID:
        return
    if DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION == "default":
        logger.info(
            f"Going to submit SQS assertion execution request {execution_request.args['urn']} via {executor_id}"
        )

        # before we try to send a task over celery, we make sure we have valid SQS creds
        if not update_celery_credentials(app, False, executor_id):
            return execution_request.args["urn"]

        message_size = len(execution_request.model_dump_json())
        if message_size > DATAHUB_EXECUTOR_SQS_MESSAGE_MAX_LENGTH:
            METRIC("SCHEDULER_MESSAGE_SIZE_EXCEEDED").inc()
            logger.warning(
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


def add_cloud_logging_configs(
    aspect: GenericAspectClass | None, cloud_logging_config: CloudLoggingConfig | None
) -> None:
    if aspect is None:
        return

    if (
        cloud_logging_config is None
        or "null" in cloud_logging_config.s3_bucket
        or "null" in cloud_logging_config.s3_prefix
        or not cloud_logging_config.s3_bucket
        or not cloud_logging_config.s3_prefix
    ):
        return

    aspect_value = json.loads(aspect.value)
    aspect_value_args = aspect_value["args"]
    if aspect_value_args is None:
        aspect_value_args = {}
        aspect_value["args"] = aspect_value_args
    extra_env_vars = aspect_value_args.get("extra_env_vars")
    if extra_env_vars is None:
        extra_env_vars = "{}"
        aspect_value_args["extra_env_vars"] = extra_env_vars
    extra_env_vars_obj = json.loads(extra_env_vars)
    extra_env_vars_obj["DATAHUB_CLOUD_LOG_BUCKET"] = cloud_logging_config.s3_bucket
    extra_env_vars_obj["DATAHUB_CLOUD_LOG_PATH"] = cloud_logging_config.s3_prefix
    extra_env_vars = json.dumps(extra_env_vars_obj)
    aspect_value_args["extra_env_vars"] = extra_env_vars
    aspect.value = json.dumps(aspect_value).encode()


def apply_remote_monitor_training_request(
    execution_request: ExecutionRequest, executor_id: str
) -> Any:
    if executor_id == CLI_EXECUTOR_ID:
        return
    if DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION == "default":
        logger.info(
            f"Going to submit SQS assertion execution request {execution_request.args['urn']} via {executor_id}"
        )

        # before we try to send a task over celery, we make sure we have valid SQS creds
        if not update_celery_credentials(app, False, executor_id):
            return execution_request.args["urn"]

        message_size = len(execution_request.model_dump_json())
        if message_size > DATAHUB_EXECUTOR_SQS_MESSAGE_MAX_LENGTH:
            METRIC("SCHEDULER_MESSAGE_SIZE_EXCEEDED").inc()
            logger.warning(
                f"Monitor Training ExecutionRequest {execution_request.args['urn']} is too big ({message_size}) to send via SQS and will be dropped."
            )
            return execution_request.args["urn"]

        # for others (monitors/assertions) we directly trigger the task run.
        monitor_training_request.apply_async(
            args=[execution_request],
            task_id=execution_request.exec_id,
            queue=executor_id,
            routing_key=f"{executor_id}.monitor_training",
        )
        return execution_request.exec_id
    else:
        # not implemented
        logger.error(
            f"Worker implementation {DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION} is not available."
        )


def truncate_remote_ingestion_request(
    event: MetadataChangeLogClass,
) -> MetadataChangeLogClass:
    if event.aspect is None:
        return event

    message_size = len(event.aspect.value)
    if message_size <= DATAHUB_EXECUTOR_SQS_MESSAGE_MAX_LENGTH:
        return event

    METRIC("SCHEDULER_MESSAGE_SIZE_EXCEEDED").inc()
    logger.error(
        f"Ingestion ExecutionRequest {event.entityUrn} is too big ({message_size}) to send via SQS and will be truncated."
    )

    # strip event off of recipe data
    new_event = MetadataChangeLogClass(
        entityType=DATAHUB_EXECUTION_REQUEST_ENTITY_NAME,
        changeType="UPSERT",
        entityUrn=event.entityUrn,
        aspectName=DATAHUB_EXECUTION_REQUEST_SQS_TRUNCATED_ASPECT_NAME,
    )

    return new_event


def apply_remote_ingestion_request(
    event: MetadataChangeLogClass, executor_id: str, graph: DataHubGraph
) -> Any:
    if executor_id == CLI_EXECUTOR_ID:
        return
    if DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION == "default":
        try:
            # Adding in try catch to handle executor running with older gms for backward compatibility
            cloud_logging_config = fetch_cloud_logging_config(graph)
            if cloud_logging_config.remote_executor_logging_enabled:
                add_cloud_logging_configs(event.aspect, cloud_logging_config)
        except Exception as e:
            logger.error(f"Error fetching cloud logging config: {e}")

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

        # Truncate ingestion event if it's too big to send over SQS
        new_event = truncate_remote_ingestion_request(event)

        # for others (monitors/assertions) we directly trigger the task run.
        ingestion_request.apply_async(
            args=[new_event],
            task_id=new_event.entityUrn,
            queue=executor_id,
            routing_key=f"{executor_id}.ingestion_request",
        )
        return event.entityUrn
    else:
        # not implemented
        logger.error(
            f"Worker implementation {DATAHUB_EXECUTOR_WORKER_IMPLEMENTATION} is not available."
        )
