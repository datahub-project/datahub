import logging

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    AssertionEvaluationContext,
    AssertionEvaluationSpec,
)
from datahub_executor.config import DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS

logger = logging.getLogger(__name__)


class AssertionExecutor:
    graph: DataHubGraph
    engine: AssertionEngine
    tp: ThreadPoolExecutorWithQueueSizeLimit

    def __init__(self) -> None:
        self.graph = create_datahub_graph()
        self.engine = create_assertion_engine(self.graph)
        self.stop = False
        self.tp = ThreadPoolExecutorWithQueueSizeLimit(
            max_workers=DATAHUB_EXECUTOR_MONITORS_MAX_WORKERS,
            name="assertions",
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
                self.evaluate_assertion(request)
        except Exception as e:
            METRIC("ASSERTION_EVALUATE_ERRORS", exception="exception").inc()
            logger.exception(
                f"AssertionExecutor: error executing {request.exec_id}: %s", e
            )
            return

    def shutdown(self, wait: bool = True) -> None:
        self.stop = True
        self.tp.shutdown(wait)

    @METRIC("ASSERTION_EVALUATE_REQUESTS").time()  # type: ignore
    def evaluate_assertion(self, execution_request: ExecutionRequest) -> None:
        assertion_spec = AssertionEvaluationSpec.parse_obj(
            execution_request.args["assertion_spec"]
        )

        context = AssertionEvaluationContext(
            dry_run=execution_request.args["context"]["dry_run"],
            monitor_urn=execution_request.args["context"]["monitor_urn"],
            assertion_evaluation_spec=assertion_spec,
        )

        self.engine.evaluate(
            assertion_spec.assertion, assertion_spec.parameters, context
        )
