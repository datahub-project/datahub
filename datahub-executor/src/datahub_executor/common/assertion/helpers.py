from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.executor import AssertionExecutor


def handle_assertions_signal_requests(
    graph: DataHubGraph,
    assertion_executor: AssertionExecutor,
) -> None:
    # TODO - Query a GMS API to see if these task_ids have any signals for them
    # this doesn't exist yet.
    return
