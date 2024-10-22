from datahub.ingestion.graph.client import DataHubGraph

from datahub_executor.common.assertion.executor import AssertionExecutor


def handle_assertions_signal_requests(
    graph: DataHubGraph,
    assertion_executor: AssertionExecutor,
) -> bool:
    return assertion_executor.get_active_thread_count() > 0
