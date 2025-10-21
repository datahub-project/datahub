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


def get_kafka_consumer_config_from_env() -> Dict[str, Any]:
    """
    Automatically extract Kafka consumer configuration from KAFKA_PROPERTIES_* environment variables.

    This function provides a flexible way to configure any Kafka client property without code changes.
    All KAFKA_PROPERTIES_* environment variables are automatically mapped to Kafka consumer properties.

    Environment variable format: KAFKA_PROPERTIES_{PROPERTY_NAME} -> {property.name}
    - Underscores in the property name are converted to dots
    - Property names are lowercased
    - Empty values are excluded

    Examples:
        KAFKA_PROPERTIES_SECURITY_PROTOCOL=SASL_SSL -> security.protocol=SASL_SSL
        KAFKA_PROPERTIES_SASL_USERNAME=myuser -> sasl.username=myuser
        KAFKA_PROPERTIES_SSL_CA_LOCATION=/path/to/ca.pem -> ssl.ca.location=/path/to/ca.pem

    Special case:
        KAFKA_PROPERTIES_OAUTH_CB is mapped to oauth_cb (not converted to dots)

    Supported authentication mechanisms (non-exhaustive):
    - SASL/PLAIN (username/password)
    - SASL/SCRAM-SHA-256, SASL/SCRAM-SHA-512
    - SASL/OAUTHBEARER (AWS MSK IAM, Azure Event Hubs)
    - SASL/GSSAPI (Kerberos)
    - SSL/TLS with certificates
    - Any other Kafka client property

    Returns:
        Dictionary of Kafka consumer configuration properties extracted from environment variables
    """
    prefix = "KAFKA_PROPERTIES_"
    consumer_config: Dict[str, Any] = {}

    for env_var, value in os.environ.items():
        if env_var.startswith(prefix) and value:  # Only include non-empty values
            # Extract the parameter name (e.g., SECURITY_PROTOCOL)
            param_name = env_var[len(prefix) :]

            # Convert to Kafka property format (lowercase, underscores to dots)
            # Special case: oauth_cb should remain as-is (callback parameter, not a dotted property)
            if param_name == "OAUTH_CB":
                kafka_prop = "oauth_cb"
            else:
                kafka_prop = param_name.lower().replace("_", ".")

            consumer_config[kafka_prop] = value
            logger.debug(f"Mapped {env_var} -> {kafka_prop}={value}")

    if consumer_config:
        logger.info(
            f"Loaded {len(consumer_config)} Kafka properties from KAFKA_PROPERTIES_* environment variables"
        )

    return consumer_config


def start_ingestion_pipeline(
    graph: DataHubGraph,
    discovery: DatahubExecutorDiscovery,
    sighandler: List[Callable],
) -> None:
    config_dict = load_config_file(DATAHUB_EXECUTOR_INGESTION_PIPELINE_CONFIG_PATH)
    connection = config_dict.get("connection", {})

    # Step 1: Load environment variables as base config (lowest priority)
    # This allows setting defaults via KAFKA_PROPERTIES_* environment variables
    consumer_config = get_kafka_consumer_config_from_env()

    # Step 2: Override with YAML config (highest priority)
    # YAML configuration takes precedence over environment variables
    yaml_consumer_config = connection.get("consumer_config", {})
    consumer_config.update(yaml_consumer_config)

    logger.info(f"Final Kafka consumer config has {len(consumer_config)} properties")

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
