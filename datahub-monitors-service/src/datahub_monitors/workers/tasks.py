import logging
import typing

from acryl.executor.request.execution_request import ExecutionRequest
from celery import Celery
from celery.signals import celeryd_init, heartbeat_sent
from datahub.metadata.schema_classes import MetadataChangeLogClass
from kombu.transport.SQS import Channel

from datahub_monitors.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_monitors.common.helpers import create_datahub_graph
from datahub_monitors.service.scheduler.types import RUN_ASSERTION_TASK_NAME
from datahub_monitors.workers.kombu_patch import patched_new_sqs_client

from datahub_monitors.config import (
    ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS,
)

from .assertion_executor import AssertionExecutor
from .helpers import (
    extract_execution_request,
    setup_ingestion_executor,
    update_celery_credentials,
    handle_assertions_signal_requests,
    handle_ingestion_signal_requests,
)

logger = logging.getLogger(__name__)

### KOMBU PATCH
Channel.new_sqs_client = patched_new_sqs_client

app = Celery("tasks")
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
    tp = ThreadPoolExecutorWithQueueSizeLimit(max_workers = ACTIONS_PIPELINE_EXECUTOR_MAX_WORKERS)

# Note - had to add this so mypy would stop throwing this error
# error: Untyped decorator makes function "evaluate_execution_request" untyped  [misc]
@typing.no_type_check
@app.task
def evaluate_execution_request(execution_request: ExecutionRequest) -> None:
    if execution_request.name == RUN_ASSERTION_TASK_NAME:
        global assertion_executor
        assertion_executor.execute(execution_request)
    else:
        logger.error(
            f"Unsupported ExecutionRequest type {execution_request.name} provided. Skipping execution of {execution_request.exec_id}.."
        )
        return

@typing.no_type_check
@app.task
def evaluate_execution_request_input(event: MetadataChangeLogClass) -> None:
    execution_request = extract_execution_request(event)
    if execution_request:
        global tp
        global ingestion_executor
        if ingestion_executor and tp:
            tp.submit(ingestion_executor.execute, execution_request)

@typing.no_type_check
@heartbeat_sent.connect
def poll_signals(**kwargs):
    global graph
    global ingestion_executor
    global assertion_executor

    # check for any signal requests on running tasks
    handle_ingestion_signal_requests(graph, ingestion_executor)
    handle_assertions_signal_requests(graph, assertion_executor)
