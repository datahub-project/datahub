import logging

from acryl.executor.request.execution_request import ExecutionRequest
from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.assertion.engine.evaluator.utils.errors import (
    extract_assertion_evaluation_result_error,
)
from datahub_executor.common.exceptions import (
    AssertionResultException,
    MissingEvaluationParametersException,
    ResultEmissionException,
)
from datahub_executor.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.tp import ThreadPoolExecutorWithQueueSizeLimit
from datahub_executor.common.types import (
    AssertionEvaluationContext,
    AssertionEvaluationParameters,
    AssertionEvaluationParametersType,
    AssertionEvaluationResult,
    AssertionEvaluationSpec,
    AssertionResultType,
    AssertionType,
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
            METRIC("WORKER_ASSERTION_EVALUATE_ERRORS", exception="exception").inc()
            logger.exception(
                f"AssertionExecutor: error executing {request.exec_id}: %s", e
            )
            return

    def shutdown(self, wait: bool = True) -> None:
        self.stop = True
        self.tp.shutdown(wait)

    @METRIC("WORKER_ASSERTION_EVALUATE_REQUESTS").time()  # type: ignore
    def evaluate_assertion(self, execution_request: ExecutionRequest) -> None:
        assertion_spec = AssertionEvaluationSpec.model_validate(
            execution_request.args["assertion_spec"]
        )

        ctx_args = execution_request.args.get("context", {})
        context = AssertionEvaluationContext(
            dry_run=ctx_args.get("dry_run", False),
            online_smart_assertions=ctx_args.get("online_smart_assertions", False),
            monitor_urn=ctx_args.get("monitor_urn"),
            assertion_evaluation_spec=assertion_spec,
            runtime_parameters=ctx_args.get("runtime_parameters"),
        )

        try:
            if assertion_spec.parameters is None:
                raise MissingEvaluationParametersException(
                    message=(
                        f"Assertion {assertion_spec.assertion.urn} has no evaluation parameters"
                    )
                )

            self.engine.evaluate(
                assertion_spec.assertion, assertion_spec.parameters, context
            )
        except AssertionResultException as e:
            # Convert known assertion errors into an AssertionEvaluationResult and emit via handlers.
            if isinstance(e, ResultEmissionException):
                logger.exception(
                    "AssertionExecutor: error emitting assertion result for %s: %s",
                    assertion_spec.assertion.urn,
                    e,
                )
                return

            logger.exception(
                "AssertionExecutor: assertion evaluation error for %s: %s",
                assertion_spec.assertion.urn,
                e,
            )
            error = extract_assertion_evaluation_result_error(e)
            result = AssertionEvaluationResult(
                AssertionResultType.ERROR,
                error=error,
            )
            parameters = assertion_spec.parameters
            if parameters is None:
                type_mapping = {
                    AssertionType.FRESHNESS: AssertionEvaluationParametersType.DATASET_FRESHNESS,
                    AssertionType.VOLUME: AssertionEvaluationParametersType.DATASET_VOLUME,
                    AssertionType.SQL: AssertionEvaluationParametersType.DATASET_SQL,
                    AssertionType.FIELD: AssertionEvaluationParametersType.DATASET_FIELD,
                    AssertionType.DATA_SCHEMA: AssertionEvaluationParametersType.DATASET_SCHEMA,
                }
                inferred_type = type_mapping.get(assertion_spec.assertion.type)
                if inferred_type is None:
                    logger.warning(
                        "AssertionExecutor: unable to infer parameters type for %s",
                        assertion_spec.assertion.urn,
                    )
                    return
                parameters = AssertionEvaluationParameters(type=inferred_type)

            for result_handler in self.engine.result_handlers:
                result_handler.handle(
                    assertion_spec.assertion,
                    parameters,
                    result,
                    context,
                )
