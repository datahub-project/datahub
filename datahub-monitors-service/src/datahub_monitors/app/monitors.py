import logging
import os

from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.secret.datahub_secret_store import DataHubSecretStore

from datahub_monitors.assertion.engine.engine import AssertionEngine
from datahub_monitors.assertion.engine.evaluator.freshness_evaluator import (
    FreshnessAssertionEvaluator,
)
from datahub_monitors.assertion.engine.evaluator.volume_evaluator import (
    VolumeAssertionEvaluator,
)
from datahub_monitors.assertion.result.assertion_run_event_handler import (
    AssertionRunEventResultHandler,
)
from datahub_monitors.assertion.scheduler.scheduler import AssertionScheduler
from datahub_monitors.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_monitors.fetcher.fetcher import MonitorFetcher
from datahub_monitors.fetcher.types import MonitorFetcherConfig
from datahub_monitors.manager.manager import MonitorManager
from datahub_monitors.source.provider import SourceProvider
from datahub_monitors.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)

logger = logging.getLogger(__name__)

manager = None


def start_async_monitors(config: MonitorFetcherConfig) -> None:
    try:
        # Create DataHub Client
        DATAHUB_SERVER = f"{os.environ.get('DATAHUB_GMS_PROTOCOL', 'http')}://{os.environ.get('DATAHUB_GMS_HOST', 'localhost')}:{os.environ.get('DATAHUB_GMS_PORT', 8080)}"
        graph = DataHubGraph(
            DatahubClientConfig(
                server=DATAHUB_SERVER,
                # When token is not set, the client will automatically try to use
                # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
                token=None,
            )
        )

        # Create a fetcher
        fetcher = MonitorFetcher(graph, config)

        # Create secret store for resolving recipe credentials
        datahub_secret_store = DataHubSecretStore.create({"graph_client": graph})

        # Create assertion result handler
        datahub_assertion_event_result_handler = AssertionRunEventResultHandler(graph)

        # setup state provider
        state_provider = DataHubMonitorStateProvider(graph)

        # Create assertion evaluators
        evaluators = [
            FreshnessAssertionEvaluator(
                DataHubIngestionSourceConnectionProvider(graph, [datahub_secret_store]),
                state_provider,
                SourceProvider(),
            ),
            VolumeAssertionEvaluator(
                DataHubIngestionSourceConnectionProvider(graph, [datahub_secret_store]),
                state_provider,
                SourceProvider(),
            ),
        ]

        # Create assertion engine
        engine = AssertionEngine(evaluators, [datahub_assertion_event_result_handler])

        # Create a scheduler
        scheduler = AssertionScheduler(engine, None, None, None)

        # Create a manager
        manager = MonitorManager(fetcher, scheduler, engine)

        logger.info("Successfully created monitor manager! Starting the monitors...")

        # Start the monitors
        manager.start()

    except Exception:
        logger.exception("Failed to create monitor manager! Cannot start up monitors.")
