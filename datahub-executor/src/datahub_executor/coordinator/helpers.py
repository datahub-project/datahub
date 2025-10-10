import asyncio
import logging
import os
from threading import Thread
from typing import Any, Callable, Dict, List

from datahub.configuration.config_loader import load_config_file
from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.ingestion.graph.client import DataHubGraph
from datahub_actions.pipeline.pipeline import (
    DEFAULT_FAILED_EVENTS_DIR,
    DEFAULT_FAILURE_MODE,
    DEFAULT_RETRY_COUNT,
    Pipeline,
)
from datahub_actions.pipeline.pipeline_context import PipelineContext
from datahub_actions.plugin.source.kafka.kafka_event_source import (
    KafkaEventSource,
    KafkaEventSourceConfig,
)

from datahub_executor.common.client.fetcher.ingestion.fetcher import IngestionFetcher
from datahub_executor.common.client.fetcher.monitors.fetcher import MonitorFetcher
from datahub_executor.common.discovery.discovery import DatahubExecutorDiscovery
from datahub_executor.config import (
    DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
    DATAHUB_EXECUTOR_INGESTION_ENABLED,
    DATAHUB_EXECUTOR_INGESTION_PIPELINE_CONFIG_PATH,
    DATAHUB_EXECUTOR_POOL_ID,
)
from datahub_executor.coordinator.config import get_ingestion_config, get_monitor_config
from datahub_executor.coordinator.ingestion import IngestionAction
from datahub_executor.coordinator.manager import ExecutionRequestManager
from datahub_executor.coordinator.scheduler import ExecutionRequestScheduler

from . import scheduler_handlers
from .assertion_handlers import (
    async_queue_start,
    async_queue_stop,
)

logger = logging.getLogger(__name__)
manager = None


def _build_oauth_consumer_config() -> Dict[str, Any]:
    """
    Build OAuth consumer configuration based on environment variables.

    Supports:
    - AWS MSK IAM authentication (OAUTHBEARER)
    - Azure Event Hubs OAuth (OAUTHBEARER)
    - SASL PLAIN authentication (username/password)

    Returns:
        Dict with OAuth-specific Kafka consumer configuration
    """
    sasl_mechanism = os.environ.get("KAFKA_PROPERTIES_SASL_MECHANISM")

    if not sasl_mechanism:
        return {}

    # For OAUTHBEARER (MSK IAM or Azure Event Hubs)
    if sasl_mechanism == "OAUTHBEARER":
        oauth_callback = os.environ.get(
            "KAFKA_PROPERTIES_OAUTH_CALLBACK",
            "datahub_executor.common.kafka_msk_iam:oauth_cb",
        )

        config = {
            "sasl.mechanism": "OAUTHBEARER",
            "sasl.oauthbearer.method": os.environ.get(
                "KAFKA_PROPERTIES_SASL_OAUTHBEARER_METHOD", "default"
            ),
            "oauth_cb": oauth_callback,
        }

        logger.info(
            f"Configured OAUTHBEARER authentication with callback: {oauth_callback}"
        )
        return config

    # For PLAIN mechanism (username/password)
    sasl_username = os.environ.get("KAFKA_PROPERTIES_SASL_USERNAME")
    sasl_password = os.environ.get("KAFKA_PROPERTIES_SASL_PASSWORD")

    if sasl_username and sasl_password:
        logger.info("Configured SASL PLAIN authentication")
        return {
            "sasl.mechanism": sasl_mechanism,
            "sasl.username": sasl_username,
            "sasl.password": sasl_password,
        }

    # If SASL mechanism is set but credentials are missing
    logger.warning(
        f"SASL mechanism '{sasl_mechanism}' is set but credentials are missing. "
        "Set KAFKA_PROPERTIES_SASL_USERNAME and KAFKA_PROPERTIES_SASL_PASSWORD."
    )
    return {"sasl.mechanism": sasl_mechanism}


def start_ingestion_pipeline(
    graph: DataHubGraph,
    discovery: DatahubExecutorDiscovery,
    sighandler: List[Callable],
) -> None:
    config_dict = load_config_file(DATAHUB_EXECUTOR_INGESTION_PIPELINE_CONFIG_PATH)
    connection = config_dict.get("connection", {})

    # Get base consumer config from file
    consumer_config = connection.get("consumer_config", {}).copy()

    # Override with environment variables if set
    security_protocol = os.environ.get("KAFKA_PROPERTIES_SECURITY_PROTOCOL")
    if security_protocol:
        consumer_config["security.protocol"] = security_protocol
        logger.info(f"Using security protocol from env: {security_protocol}")

    # Add OAuth configuration if SASL mechanism is configured
    oauth_config = _build_oauth_consumer_config()
    if oauth_config:
        consumer_config.update(oauth_config)
        logger.info("OAuth configuration added to Kafka consumer config")

    # Override bootstrap and schema registry from environment if set
    bootstrap = os.environ.get(
        "KAFKA_BOOTSTRAP_SERVER", connection.get("bootstrap", "broker:9092")
    )
    schema_registry_url = os.environ.get(
        "SCHEMA_REGISTRY_URL",
        connection.get("schema_registry_url", "http://schema-registry:8081"),
    )

    logger.info(f"Kafka bootstrap: {bootstrap}")
    logger.info(f"Schema registry URL: {schema_registry_url}")

    source = KafkaEventSource(
        config=KafkaEventSourceConfig(
            connection=KafkaConsumerConnectionConfig(
                bootstrap=bootstrap,
                schema_registry_url=schema_registry_url,
                consumer_config=consumer_config,
            ),
            topic_routes=config_dict.get("topic_routes", None),
            async_commit_enabled=True,
        ),
        ctx=PipelineContext(
            pipeline_name="datahub-executor-ingestion-pipeline",
            graph=graph,
        ),
    )

    action = IngestionAction(
        graph,
        discovery,
        DATAHUB_EXECUTOR_EMBEDDED_WORKER_ENABLED,
        DATAHUB_EXECUTOR_INGESTION_ENABLED,
        DATAHUB_EXECUTOR_POOL_ID,
    )

    pipeline = Pipeline(
        "datahub-executor-ingestion-pipeline",
        source,
        [],
        action,
        retry_count=DEFAULT_RETRY_COUNT,
        failure_mode=DEFAULT_FAILURE_MODE,
        failed_events_dir=DEFAULT_FAILED_EVENTS_DIR,
    )

    # uvicorn overrides global event loop implementation to uvloop. uvloop does not work correctly with create_subprocess_exec(),
    # which is used in acryl-executor. See https://github.com/MagicStack/uvloop/issues/508 for repro case. As a workaround,
    # we reset event_loop implementation back to python's native.
    asyncio.set_event_loop_policy(None)

    # setup sigint handlers
    sighandler.append(action.shutdown)
    sighandler.append(pipeline.stop)

    pt = Thread(target=pipeline.run)
    pt.start()


def start_scheduler(graph: DataHubGraph, sighandler: List[Callable]) -> None:
    try:
        global manager

        # Create a fetcher
        monitor_fetcher = MonitorFetcher(graph, get_monitor_config())
        ingestion_fetcher = IngestionFetcher(graph, get_ingestion_config())

        # Create a scheduler
        scheduler = ExecutionRequestScheduler(None, None, None, None, None)
        scheduler_handlers.scheduler = scheduler

        # Create a manager
        manager = ExecutionRequestManager(
            [monitor_fetcher, ingestion_fetcher], scheduler
        )

        sighandler.append(scheduler.shutdown)
        sighandler.append(manager.shutdown)

        logger.debug("Successfully created execution request scheduler.")

        async_queue_start()
        sighandler.append(async_queue_stop)

        # Start the monitors
        manager.start()

    except Exception as e:
        logger.exception(f"Failed to create fetcher scheduler: {str(e)}")
        raise
