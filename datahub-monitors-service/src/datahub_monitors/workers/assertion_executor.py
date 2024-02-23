import logging

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_monitors.common.assertion.engine.engine import AssertionEngine
from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_monitors.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_monitors.common.types import (
    AssertionEvaluationContext,
    AssertionEvaluationSpec,
)
from datahub_monitors.config import MONITORS_EXECUTOR_MAX_WORKERS

logger = logging.getLogger(__name__)


class AssertionExecutor:
    graph: DataHubAssertionGraph
    engine: AssertionEngine
    tp: ThreadPoolExecutorWithQueueSizeLimit

    def __init__(self) -> None:
        self.graph = create_datahub_graph()
        self.engine = create_assertion_engine(self.graph)
        self.tp = ThreadPoolExecutorWithQueueSizeLimit(
            max_workers=MONITORS_EXECUTOR_MAX_WORKERS
        )

    def execute(self, request: ExecutionRequest) -> None:
        # submit will block if queue size > max_workers
        self.tp.submit(self.worker, request)

    def worker(self, request: ExecutionRequest) -> None:
        try:
            self.evaluate_assertion(request)
        except Exception as e:
            logger.error(e)
            return

    def shutdown(self, wait: bool = True) -> None:
        self.tp.shutdown(wait)

    def evaluate_assertion(self, execution_request: ExecutionRequest) -> None:
        assertion_spec = AssertionEvaluationSpec.parse_obj(
            execution_request.args["assertion_spec"]
        )
        context = AssertionEvaluationContext(
            dry_run=execution_request.args["context"]["dry_run"],
            monitor_urn=execution_request.args["context"]["monitor_urn"],
        )

        self.engine.evaluate(
            assertion_spec.assertion, assertion_spec.parameters, context
        )
