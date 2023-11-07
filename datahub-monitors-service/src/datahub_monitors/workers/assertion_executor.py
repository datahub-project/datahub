import asyncio
import logging
import traceback
from typing import Dict

from acryl.executor.request.execution_request import ExecutionRequest

from datahub_monitors.common.assertion.engine.engine import AssertionEngine
from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.common.helpers import (
    create_assertion_engine,
    create_datahub_graph,
)
from datahub_monitors.common.types import (
    AssertionEvaluationContext,
    AssertionEvaluationSpec,
)

logger = logging.getLogger(__name__)


class AssertionExecutor:
    task_event_loops: Dict[str, asyncio.AbstractEventLoop] = {}
    task_futures: Dict[str, asyncio.Task] = {}
    graph: DataHubAssertionGraph
    engine: AssertionEngine

    def __init__(self) -> None:
        self.graph = create_datahub_graph()
        self.engine = create_assertion_engine(self.graph)

    def execute(self, request: ExecutionRequest) -> None:
        try:
            task_event_loop = asyncio.new_event_loop()
            self.task_event_loops[request.exec_id] = task_event_loop
            asyncio.set_event_loop(task_event_loop)

            task = task_event_loop.create_task(self.evaluate_assertion(request))
            self.task_futures[request.exec_id] = task

            task_event_loop.run_until_complete(task)
        except Exception as e:
            logger.error(e)
            return
        except asyncio.exceptions.CancelledError:
            logger.error(
                f"Execution cancelled while EXECUTING task_id={request.exec_id}, name={request.name}, stacktrace={traceback.format_exc(limit=3)}"
            )
            return
        finally:
            if task_event_loop:
                task_event_loop.close()
            if request.exec_id in self.task_futures:
                del self.task_futures[request.exec_id]
            if request.exec_id in self.task_event_loops:
                del self.task_event_loops[request.exec_id]

    async def evaluate_assertion(self, execution_request: ExecutionRequest) -> None:
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

    def cancel(self, exec_id: str) -> None:
        if exec_id in self.task_futures:
            task_future = self.task_futures[exec_id]
            event_loop = self.task_event_loops[exec_id]

            # Cancel the task if not complete.
            if not task_future.done():
                logger.debug(f"Trying to cancel {task_future}")
                event_loop.call_soon_threadsafe(task_future.cancel)
