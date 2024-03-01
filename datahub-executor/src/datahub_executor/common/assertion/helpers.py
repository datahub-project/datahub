from datahub_executor.common.assertion.executor import AssertionExecutor
from datahub_executor.common.graph import DataHubAssertionGraph


def handle_assertions_signal_requests(
    graph: DataHubAssertionGraph,
    assertion_executor: AssertionExecutor,
) -> None:
    # TODO - Query a GMS API to see if these task_ids have any signals for them
    # this doesn't exist yet.
    return
