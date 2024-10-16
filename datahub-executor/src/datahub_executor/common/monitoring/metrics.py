from prometheus_client import Gauge, Summary

STATS_CONFIG_FETCHER_ERRORS = Gauge(
    "datahub_executor_config_fetcher_errors",
    "Number of errors occurred while fetching executor configs from GMS",
    ["exception"],
)
STATS_CONFIG_FETCHER_REQUESTS = Summary(
    "datahub_executor_config_fetcher_requests",
    "Count/time spent fetching executor configs from GMS",
)


STATS_INGESTION_FETCHER_ERRORS = Gauge(
    "datahub_executor_ingestion_fetcher_errors",
    "Number of errors occurred while fetching ingestion execution requests",
    ["exception"],
)
STATS_INGESTION_FETCHER_REQUESTS = Summary(
    "datahub_executor_ingestion_fetcher_requests",
    "Time spent fetching ingestion execution requests",
)
STATS_INGESTION_FETCHER_ITEMS_MAPPED = Gauge(
    "datahub_executor_ingestion_fetcher_items_mapped",
    "Number of items fetched/processed by the mapper",
)
STATS_INGESTION_FETCHER_ITEMS_ERRORED = Gauge(
    "datahub_executor_ingestion_fetcher_items_errored",
    "Number of items the mapper failed to processed",
    ["exception"],
)


STATS_ASSERTION_FETCHER_ERRORS = Gauge(
    "datahub_executor_assertion_fetcher_errors",
    "Number of errors occurred while fetching assertion execution requests",
    ["exception"],
)
STATS_ASSERTION_FETCHER_REQUESTS = Summary(
    "datahub_executor_assertion_fetcher_requests",
    "Time spent fetching assertion execution requests",
)
STATS_ASSERTION_FETCHER_ITEMS_MAPPED = Gauge(
    "datahub_executor_assertion_fetcher_items_mapped",
    "Number of items fetched/processed by the mapper",
)
STATS_ASSERTION_FETCHER_ITEMS_ERRORED = Gauge(
    "datahub_executor_assertion_fetcher_items_errored",
    "Number of items the mapper failed to processed",
    ["exception"],
)


STATS_SCHEDULER_INGESTION_REQUESTS = Gauge(
    "datahub_executor_scheduler_ingestion_requests",
    "Number of ingestion requests submitted from scheduler",
    ["executor_id"],
)

STATS_SCHEDULER_ASSERTION_REQUESTS = Gauge(
    "datahub_executor_scheduler_assertion_requests",
    "Number of assertion requests submitted from scheduler",
    ["executor_id", "embedded"],
)

STATS_SCHEDULER_SUBMISSION_ERRORS = Gauge(
    "datahub_executor_scheduler_submission_errors",
    "Number of execution submissions resulted in a failure",
    ["executor_id", "embedded", "exception"],
)

STATS_INGESTION_HANDLER_REQUESTS = Gauge(
    "datahub_executor_ingestion_handler_requests",
    "Number of execution requests submitted by ingestion handler",
    ["executor_id", "embedded"],
)

STATS_INGESTION_HANDLER_ERRORS = Gauge(
    "datahub_executor_ingestion_handler_errors",
    "Number of errors occurred while handling ingestion requests",
    ["exception"],
)

STATS_INGESTION_KAFKA_MCL_EVENTS = Gauge(
    "datahub_executor_ingestions_kafka_mcl_events",
    "Number of Kafka MCL events received by ingestion handler",
)
STATS_INGESTION_KAFKA_EXEC_EVENTS = Gauge(
    "datahub_executor_ingestion_kafka_exec_events",
    "Number of Kafka execution request events received by ingestion handler",
)

STATS_INGESTION_HANDLER_CANCEL_REQUESTS = Gauge(
    "datahub_executor_ingestion_cancel_requests",
    "Number of cancel requests received by ingestion handler",
)

STATS_EXECUTION_FETCH_SIGNAL_REQUESTS = Summary(
    "datahub_executor_ingestion_fetch_singal_requests",
    "Count/time spent fetching ingestion singal/cancel requests",
)
STATS_EXECUTION_FETCH_REQUESTS = Summary(
    "datahub_executor_ingestion_fetch_requests",
    "Count/time spent fetching ingestion execution requests",
)
STATS_EXECUTION_FETCH_SIGNAL_ERRORS = Gauge(
    "datahub_executor_ingestion_fetch_singal_errors",
    "Number of errors occurred while fetching ingestion signal requests",
    ["exception"],
)

STATS_MCP_EMIT_EVENTS = Gauge(
    "datahub_executor_ingestion_emit_mcp_events",
    "Number of MCP events emitted by ingestion handler",
)
STATS_MCP_EMIT_ERRORS = Gauge(
    "datahub_executor_ingestion_emit_mcp_errors",
    "Number of errors occurred while emitting MCP event",
    ["exception"],
)

STATS_CREDENTIALS_REFRESH_REQUESTS = Gauge(
    "datahub_executor_credentials_refresh_requests",
    "Number of requests to refresh credentials",
    ["executor_id"],
)
STATS_CREDENTIALS_REFRESH_ERRORS = Gauge(
    "datahub_executor_credentials_refresh_errors",
    "Number of errors occurred when refreshing credentials",
    ["exception", "executor_id"],
)


STATS_WORKER_INGESTION_REQUESTS = Gauge(
    "datahub_executor_worker_ingestion_requests",
    "Number of ingestion requests received/processed by worker",
    ["executor_id"],
)
STATS_WORKER_INGESTION_ERRORS = Gauge(
    "datahub_executor_worker_ingestion_errors",
    "Number of errors occurred while processing ingestion requests in worker",
    ["executor_id"],
)

STATS_WORKER_ASSERTION_REQUESTS = Gauge(
    "datahub_executor_worker_assertion_requests",
    "Number of assertion requests received/processed by worker",
    ["executor_id"],
)
STATS_WORKER_ASSERTION_ERRORS = Gauge(
    "datahub_executor_worker_assertion_errors",
    "Number of errors occurred while processing assertion requests in worker",
    ["executor_id"],
)


STATS_THREAD_POOL_MAX_WORKERS = Gauge(
    "datahub_executor_thread_pool_max_workers",
    "Max number of worker threads in a thread pool",
    ["thread_pool_name"],
)
STATS_THREAD_POOL_ACTIVE_WORKERS = Gauge(
    "datahub_executor_thread_pool_active_workers",
    "Number of active worker threads in a thread pool",
    ["thread_pool_name"],
)


STATS_ASSERTION_EXECUTOR_EVALUATE_REQUESTS = Summary(
    "datahub_executor_assertion_evaluate_requests",
    "Number of evaluate requests handled by assertions executor",
)
STATS_ASSERTION_EXECUTOR_EVALUATE_ERRORS = Gauge(
    "datahub_executor_assertion_evaluate_errors",
    "Number of errors occurred while evaluating assertions",
    ["exception"],
)

STATS_SCHEDULER_SQS_LIMIT_EXCEEDED = Gauge(
    "datahub_executor_scheduler_message_size_exceeded",
    "Number of requests that failed to send over SQS due to excessive size",
)


STATS_SWEEPER_ACTIONS_PLANNED = Gauge(
    "datahub_executor_sweeper_actions_planned",
    "Number of actions scheduled for execution by the sweeper",
    ["action_type"],
)

STATS_SWEEPER_ACTIONS_EXECUTED = Gauge(
    "datahub_executor_sweeper_actions_executed",
    "Number of actions executed by the sweeper",
    ["action_type"],
)

STATS_SWEEPER_EXECUTION_REQUESTS_COUNT = Gauge(
    "datahub_executor_sweeper_actions",
    "Number of execution requests processed by the sweeper grouped by status",
    ["status"],
)
