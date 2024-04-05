import logging
import typing

from acryl.executor.request.execution_request import ExecutionRequest
from celery.signals import celeryd_init, heartbeat_sent
from datahub.metadata.schema_classes import MetadataChangeLogClass
from kombu.transport.SQS import Channel

from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.assertion.helpers import handle_assertions_signal_requests
from datahub_executor.common.constants import RUN_ASSERTION_TASK_NAME
from datahub_executor.common.helpers import create_datahub_graph
from datahub_executor.common.ingestion.helpers import (
    extract_execution_request,
    handle_ingestion_signal_requests,
    setup_ingestion_executor,
)
from datahub_executor.common.monitoring.base import monitoring_start
from datahub_executor.common.monitoring.metrics import (
    STATS_WORKER_ASSERTION_ERRORS,
    STATS_WORKER_ASSERTION_REQUESTS,
    STATS_WORKER_INGESTION_REQUESTS,
)
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.config import (
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS,
    DATAHUB_EXECUTOR_WORKER_ID,
)

from .config import update_celery_credentials
from .health import kube_health_check
from .init import app
from .kombu_patch import patched_handle_sts_session

logger = logging.getLogger(__name__)

# Start prometheus server
monitoring_start()

# Kombu credentials patch
Channel._handle_sts_session = patched_handle_sts_session

update_celery_credentials(app, True, "")

tp = None
ingestion_executor = None
assertion_executor = None
graph = None


@typing.no_type_check
@celeryd_init.connect
def worker_startup(*args, **kwargs):
    global graph
    graph = create_datahub_graph()

    global ingestion_executor
    ingestion_executor = setup_ingestion_executor()

    global assertion_executor
    assertion_executor = AssertionExecutor()

    global tp
    tp = ThreadPoolExecutorWithQueueSizeLimit(
        max_workers=DATAHUB_EXECUTOR_INGESTION_PIPELINE_MAX_WORKERS, name="ingestions"
    )

    logger.info("celery worker initialization finished")


# Note - had to add this so mypy would stop throwing this error
# error: Untyped decorator makes function "assertion_request" untyped  [misc]
@typing.no_type_check
@app.task
def assertion_request(execution_request: ExecutionRequest) -> None:
    if execution_request.name == RUN_ASSERTION_TASK_NAME:
        STATS_WORKER_ASSERTION_REQUESTS.labels(DATAHUB_EXECUTOR_WORKER_ID).inc()

        global assertion_executor
        assertion_executor.execute(execution_request)
    else:
        STATS_WORKER_ASSERTION_ERRORS.labels(
            DATAHUB_EXECUTOR_WORKER_ID, "UnsupportedRequest"
        ).inc()
        logger.error(
            f"Unsupported ExecutionRequest type {execution_request.name} provided. Skipping execution of {execution_request.exec_id}.."
        )
        return


@typing.no_type_check
@app.task
def ingestion_request(event: MetadataChangeLogClass) -> None:
    execution_request = extract_execution_request(event)
    if execution_request:
        global tp
        global ingestion_executor
        if ingestion_executor and tp:
            STATS_WORKER_INGESTION_REQUESTS.labels(DATAHUB_EXECUTOR_WORKER_ID).inc()
            tp.submit(ingestion_executor.execute, execution_request)


@typing.no_type_check
@heartbeat_sent.connect
def poll_signals(**kwargs):
    global graph
    global ingestion_executor
    global assertion_executor

    # update health check file
    kube_health_check()

    # check for any signal requests on running tasks
    if ingestion_executor is not None:
        handle_ingestion_signal_requests(graph, ingestion_executor)

    if assertion_executor is not None:
        handle_assertions_signal_requests(graph, assertion_executor)
