from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.secret.datahub_secret_store import DataHubSecretStore

from datahub_monitors.common.assertion.engine.engine import AssertionEngine
from datahub_monitors.common.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_monitors.common.assertion.engine.evaluator.freshness_evaluator import (
    FreshnessAssertionEvaluator,
)
from datahub_monitors.common.assertion.engine.evaluator.sql_evaluator import (
    SQLAssertionEvaluator,
)
from datahub_monitors.common.assertion.engine.evaluator.volume_evaluator import (
    VolumeAssertionEvaluator,
)
from datahub_monitors.common.assertion.result.assertion_dry_run_event_handler import (
    AssertionDryRunEventResultHandler,
)
from datahub_monitors.common.assertion.result.assertion_run_event_handler import (
    AssertionRunEventResultHandler,
)
from datahub_monitors.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_monitors.common.graph import DataHubAssertionGraph
from datahub_monitors.common.source.provider import SourceProvider
from datahub_monitors.common.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)
from datahub_monitors.config import DATAHUB_ACCESS_TOKEN, DATAHUB_SERVER


def create_datahub_graph() -> DataHubAssertionGraph:
    """Create a DataHub client based on environment variables."""
    return DataHubAssertionGraph(
        DatahubClientConfig(
            server=DATAHUB_SERVER,
            # When token is not set, the client will automatically try to use
            # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
            token=DATAHUB_ACCESS_TOKEN,
        )
    )


def create_assertion_engine(graph: DataHubAssertionGraph) -> AssertionEngine:
    # Create secret store for resolving recipe credentials
    datahub_secret_store = DataHubSecretStore.create({"graph_client": graph})

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
        SQLAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(graph, [datahub_secret_store]),
            state_provider,
            SourceProvider(),
        ),
        FieldAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(graph, [datahub_secret_store]),
            state_provider,
            SourceProvider(),
        ),
    ]

    # Create result handlers
    result_handlers = [
        AssertionRunEventResultHandler(graph),
        AssertionDryRunEventResultHandler(graph),
    ]

    # Create assertion engine
    return AssertionEngine(evaluators, result_handlers=result_handlers)
