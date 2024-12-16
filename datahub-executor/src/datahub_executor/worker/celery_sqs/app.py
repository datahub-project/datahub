import logging
import time
import typing
from pathlib import Path
from threading import Thread

from acryl.executor.request.execution_request import ExecutionRequest
from celery.signals import celeryd_init, heartbeat_sent, worker_shutting_down
from datahub.metadata.schema_classes import MetadataChangeLogClass
from kombu.transport.SQS import Channel

from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.assertion.helpers import handle_assertions_signal_requests
from datahub_executor.common.constants import RUN_ASSERTION_TASK_NAME
from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
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
    DATAHUB_EXECUTOR_LIVENESS_HEARTBEAT_FILE,
    DATAHUB_EXECUTOR_READINESS_HEARTBEAT_FILE,
    DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT,
    DATAHUB_EXECUTOR_WORKER_ID,
    DATAHUB_EXECUTOR_WORKER_MONITOR_INTERVAL,
)

from .config import update_celery_credentials
from .init import app
from .kombu_patch import patched_handle_sts_session

logger = logging.getLogger(__name__)

# Start prometheus server
monitoring_start()

# Kombu credentials patch
Channel._handle_sts_session = patched_handle_sts_session

update_celery_credentials(app, True, "")

tp = None
monitor = None
discovery = None
ingestion_executor = None
assertion_executor = None
graph = None

# Health check status
celery_liveness = 0
celery_readiness = False
celery_shutdown = False


def touch_heartbeat_file(path: str) -> None:
    try:
        Path(path).touch()
    except Exception as e:
        logger.error(f"Failed to update heartbeat file {path}: {e}")


# These methods improve handling of SQS visibility timeout: since thread pool submit operation is blocking,
# celery can not delete the message it just read from SQS when it blocks. If it's blocked for longer than
# the visibility timeout, the message is sent back to the queue and another worker may pick it up. However,
# the worker that originally picked it up will resume processing the message when thread pool becomes
# available again, so the message may be processed more than once, thus wasting resources and in worst case
# may create an infinite loop.


def is_visibility_timeout_exceeded(submitted_at: float) -> bool:
    discard_threshold = DATAHUB_EXECUTOR_SQS_VISIBILITY_TIMEOUT * 0.9
    wait_time = time.time() - submitted_at
    return wait_time > discard_threshold


def safe_execute_ingestion(er: ExecutionRequest, submitted_at: float) -> None:
    global ingestion_executor

    if ingestion_executor and not is_visibility_timeout_exceeded(submitted_at):
        ingestion_executor.execute(er)


def monitor_thread() -> None:
    global graph
    global ingestion_executor, assertion_executor
    global celery_liveness, celery_readiness, celery_shutdown

    max_allowed_staleness = DATAHUB_EXECUTOR_WORKER_MONITOR_INTERVAL * 3
    warning_last_logged = 0
    failure_last_logged = 0
    log_frequency = 60

    while not celery_shutdown:
        ingestions_running = False
        assertions_running = False

        # check for any signal requests on running tasks
        if ingestion_executor is not None:
            ingestions_running = handle_ingestion_signal_requests(
                graph, ingestion_executor
            )
        if assertion_executor is not None:
            assertions_running = handle_assertions_signal_requests(
                graph, assertion_executor
            )

        # Update readiness file if worker is up
        if celery_readiness:
            touch_heartbeat_file(DATAHUB_EXECUTOR_READINESS_HEARTBEAT_FILE)

        now = int(time.time())
        staleness = now - celery_liveness

        # Update readiness file if celery heartbeat is getting updated or GMS communication is ok
        if staleness <= max_allowed_staleness:
            touch_heartbeat_file(DATAHUB_EXECUTOR_LIVENESS_HEARTBEAT_FILE)
            if warning_last_logged > 0 or failure_last_logged > 0:
                logger.warning("monitor: celery liveness check recovered.")
                warning_last_logged = 0
                failure_last_logged = 0
        elif ingestions_running or assertions_running:
            touch_heartbeat_file(DATAHUB_EXECUTOR_LIVENESS_HEARTBEAT_FILE)
            if (now - warning_last_logged) > log_frequency:
                logger.warning(
                    "monitor: celery liveness check is blocked, using other signals"
                )
                warning_last_logged = now
        else:
            if (now - failure_last_logged) > log_frequency:
                logger.warning(
                    f"monitor: celery liveness check failed: last updated {staleness} seconds ago."
                )
                failure_last_logged = now

        time.sleep(DATAHUB_EXECUTOR_WORKER_MONITOR_INTERVAL)


@typing.no_type_check
@celeryd_init.connect
def worker_startup(*args, **kwargs):
    global graph, monitor, discovery
    graph = create_datahub_graph()

    discovery = DatahubExecutorDiscovery(graph)
    discovery.start()

    global ingestion_executor
    ingestion_executor = setup_ingestion_executor(
        executor_instance_id=discovery.get_instance_id(),
        executor_version=discovery.get_build_info().get_version(),
    )

    global assertion_executor
    assertion_executor = AssertionExecutor()

    # Start monitoring thread
    monitor = Thread(target=monitor_thread)
    monitor.start()

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
    global graph
    execution_request = extract_execution_request(event, graph)
    if execution_request:
        global tp
        global ingestion_executor
        if ingestion_executor and tp:
            submitted_at = time.time()
            tp.submit(safe_execute_ingestion, execution_request, submitted_at)

            # Do not delete message if visibility timeout exceeded -- another worker will handle it after it's re-enqueued.
            if is_visibility_timeout_exceeded(submitted_at):
                raise RuntimeError(
                    f"ExecutionRequest {execution_request.exec_id} dropped due to exceeded SQS visibility timeout."
                )
            STATS_WORKER_INGESTION_REQUESTS.labels(DATAHUB_EXECUTOR_WORKER_ID).inc()


@typing.no_type_check
@heartbeat_sent.connect
def heartbeat(**kwargs):
    global celery_liveness, celery_readiness

    celery_readiness = True
    celery_liveness = int(time.time())


@typing.no_type_check
@worker_shutting_down.connect
def shutdown(**kwargs):
    global celery_shutdown, celery_readiness
    global discovery, tp, monitor, assertion_executor

    celery_shutdown = True
    celery_readiness = False

    if discovery is not None:
        discovery.stop()
    if monitor is not None:
        monitor.join()
    if tp is not None:
        tp.shutdown()
    if assertion_executor is not None:
        assertion_executor.shutdown()
