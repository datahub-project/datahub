import logging

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.constants import RUN_MONITOR_TRAINING_TASK_NAME
from datahub_executor.common.helpers import (
    create_datahub_graph,
    create_monitor_training_engine,
)
from datahub_executor.common.monitor.inference.monitor_training_engine import (
    MonitorTrainingEngine,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    Monitor,
)
from datahub_executor.config import DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS

logger = logging.getLogger(__name__)


class MonitorExecutor:
    graph: DataHubGraph
    engine: MonitorTrainingEngine
    tp: ThreadPoolExecutorWithQueueSizeLimit

    def __init__(self) -> None:
        self.graph = create_datahub_graph()
        self.engine = create_monitor_training_engine(self.graph)
        self.stop = False
        self.tp = ThreadPoolExecutorWithQueueSizeLimit(
            max_workers=DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS,  # Same limit as assertion evaluation, for now.
            name="monitors",
        )

    def get_active_thread_count(self) -> int:
        return self.tp.get_active_thread_count()

    def execute(self, request: ExecutionRequest) -> None:
        # submit will block if queue size > max_workers
        if not self.stop:
            self.tp.submit(self.worker, request)

    def worker(self, request: ExecutionRequest) -> None:
        try:
            if not self.stop:
                self.evaluate_monitor_training(request)
        except Exception as e:
            METRIC(
                "WORKER_MONITOR_TRAINING_EXECUTOR_ERRORS", exception="exception"
            ).inc()
            logger.exception(
                f"MonitorExecutor: error executing {request.exec_id}: %s", e
            )
            return

    def shutdown(self, wait: bool = True) -> None:
        self.stop = True
        self.tp.shutdown(wait)

    @METRIC("WORKER_MONITOR_TRAINING_EXECUTOR_REQUESTS").time()  # type: ignore
    def evaluate_monitor_training(self, execution_request: ExecutionRequest) -> None:
        if execution_request.name == RUN_MONITOR_TRAINING_TASK_NAME:
            monitor = Monitor.parse_obj(execution_request.args["monitor"])
            self.engine.train(monitor)
        else:
            raise Exception(
                f"Failed to evaluate monitor training. Was provided unrecognized task with name {execution_request.name}"
            )
