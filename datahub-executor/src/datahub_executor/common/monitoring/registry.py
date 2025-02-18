from prometheus_client import Gauge, Summary

REGISTRY = {
    "MONITORING_PUSH_PAYLOAD_SIZE": [
        Gauge,
        "Compressed payload size of the prometheus metrics pushed to the backend",
    ],
    "MONITORING_PUSH_ERRORS": [
        Gauge,
        "Number of errors occured while pushing prometheus metrics to the backend",
    ],
    "MONITORING_PUSH_REQUESTS": [
        Summary,
        "Number of requests to push prometheus metrics to the backend",
    ],
    "CONFIG_FETCHER_ERRORS": [
        Gauge,
        "Number of errors occurred while fetching executor configs from GMS",
        ["exception"],
    ],
    "CONFIG_FETCHER_REQUESTS": [
        Summary,
        "Count/time spent fetching executor configs from GMS",
    ],
    "INGESTION_FETCHER_ERRORS": [
        Gauge,
        "Number of errors occurred while fetching ingestion execution requests",
        ["exception"],
    ],
    "INGESTION_FETCHER_REQUESTS": [
        Summary,
        "Time spent fetching ingestion execution requests",
    ],
    "INGESTION_FETCHER_ITEMS_MAPPED": [
        Gauge,
        "Number of items fetched/processed by the mapper",
    ],
    "INGESTION_FETCHER_ITEMS_ERRORED": [
        Gauge,
        "Number of items the mapper failed to processed",
        ["exception"],
    ],
    "ASSERTION_FETCHER_ERRORS": [
        Gauge,
        "Number of errors occurred while fetching assertion execution requests",
        ["exception"],
    ],
    "ASSERTION_FETCHER_REQUESTS": [
        Summary,
        "Time spent fetching assertion execution requests",
    ],
    "ASSERTION_FETCHER_ITEMS_MAPPED": [
        Gauge,
        "Number of items fetched/processed by the mapper",
    ],
    "ASSERTION_FETCHER_ITEMS_ERRORED": [
        Gauge,
        "Number of items the mapper failed to processed",
        ["exception"],
    ],
    "SCHEDULER_INGESTION_REQUESTS": [
        Gauge,
        "Number of ingestion requests submitted from scheduler",
        ["pool_name"],
    ],
    "SCHEDULER_ASSERTION_REQUESTS": [
        Gauge,
        "Number of assertion requests submitted from scheduler",
        ["pool_name", "embedded"],
    ],
    "SCHEDULER_SUBMISSION_ERRORS": [
        Gauge,
        "Number of execution submissions resulted in a failure",
        ["pool_name", "embedded", "exception"],
    ],
    "INGESTION_HANDLER_REQUESTS": [
        Gauge,
        "Number of execution requests submitted by ingestion handler",
        ["pool_name", "embedded"],
    ],
    "INGESTION_HANDLER_ERRORS": [
        Gauge,
        "Number of errors occurred while handling ingestion requests",
        ["exception"],
    ],
    "INGESTION_KAFKA_MCL_EVENTS": [
        Gauge,
        "Number of Kafka MCL events received by ingestion handler",
    ],
    "INGESTION_KAFKA_EXEC_EVENTS": [
        Gauge,
        "Number of Kafka execution request events received by ingestion handler",
    ],
    "INGESTION_CANCEL_REQUESTS": [
        Gauge,
        "Number of cancel requests received by ingestion handler",
    ],
    "INGESTION_FETCH_SIGNAL_REQUESTS": [
        Summary,
        "Count/time spent fetching ingestion singal/cancel requests",
    ],
    "INGESTION_FALLBACK_FETCH_REQUESTS": [
        Summary,
        "Count/time spent fetching ingestion execution requests as a result of GMS-fallback",
    ],
    "INGESTION_FETCH_SIGNAL_ERRORS": [
        Gauge,
        "Number of errors occurred while fetching ingestion signal requests",
        ["exception"],
    ],
    "INGESTION_EMIT_MCP_EVENTS": [
        Gauge,
        "Number of MCP events emitted by ingestion handler",
    ],
    "INGESTION_EMIT_MCP_ERRORS": [
        Gauge,
        "Number of errors occurred while emitting MCP event",
        ["exception"],
    ],
    "CREDENTIALS_REFRESH_REQUESTS": [
        Gauge,
        "Number of requests to refresh credentials",
        ["pool_name"],
    ],
    "CREDENTIALS_REFRESH_ERRORS": [
        Gauge,
        "Number of errors occurred when refreshing credentials",
        ["pool_name", "exception"],
    ],
    "WORKER_INGESTION_REQUESTS": [
        Gauge,
        "Number of ingestion requests received/processed by worker",
        ["pool_name"],
    ],
    "WORKER_ASSERTION_REQUESTS": [
        Gauge,
        "Number of assertion requests received/processed by worker",
        ["pool_name"],
    ],
    "WORKER_ASSERTION_ERRORS": [
        Gauge,
        "Number of errors occurred while processing assertion requests in worker",
        ["pool_name", "exception"],
    ],
    "THREAD_POOL_MAX_WORKERS": [
        Gauge,
        "Max number of worker threads in a thread pool",
        ["thread_pool_name"],
    ],
    "THREAD_POOL_ACTIVE_WORKERS": [
        Gauge,
        "Number of active worker threads in a thread pool",
        ["thread_pool_name"],
    ],
    "THREAD_POOL_ACTIVE_WEIGHT": [
        Gauge,
        "Total weight of all active tasks executing in the thread pool",
        ["thread_pool_name"],
    ],
    "THREAD_POOL_MAX_WEIGHT": [
        Gauge,
        "Max weight of all tasks in the thread pool",
        ["thread_pool_name"],
    ],
    "ASSERTION_EVALUATE_REQUESTS": [
        Summary,
        "Number of evaluate requests handled by assertions executor",
    ],
    "ASSERTION_EVALUATE_ERRORS": [
        Gauge,
        "Number of errors occurred while evaluating assertions",
        ["exception"],
    ],
    "SCHEDULER_MESSAGE_SIZE_EXCEEDED": [
        Gauge,
        "Number of requests that failed to send over SQS due to excessive size",
    ],
    "SWEEPER_ACTIONS_PLANNED": [
        Gauge,
        "Number of actions scheduled for execution by the sweeper",
        ["action"],
    ],
    "SWEEPER_ACTIONS_EXECUTED": [
        Gauge,
        "Number of actions executed by the sweeper",
        ["action"],
    ],
    "SWEEPER_EXECUTION_REQUESTS": [
        Gauge,
        "Number of execution requests processed by the sweeper grouped by status",
        ["status"],
    ],
    "DISCOVERY_PING_REQUESTS": [
        Summary,
        "Number of ping requests made by discovery thread",
        ["pool_name"],
    ],
    "DISCOVERY_PING_ERRORS": [
        Gauge,
        "Number of errors occurred while making discovery ping requests",
        ["pool_name"],
    ],
}
