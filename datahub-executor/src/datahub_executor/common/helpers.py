from datahub.ingestion.graph.client import DatahubClientConfig
from datahub.secret.datahub_secret_store import DataHubSecretStore

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.freshness_evaluator import (
    FreshnessAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.sql_evaluator import (
    SQLAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.volume_evaluator import (
    VolumeAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.transformer.assertion_adjustment_transformer import (
    AssertionAdjustmentTransformer,
)
from datahub_executor.common.assertion.engine.transformer.embedded_assertions_transformer import (
    EmbeddedAssertionsTransformer,
)
from datahub_executor.common.assertion.result.assertion_dry_run_event_handler import (
    AssertionDryRunEventResultHandler,
)
from datahub_executor.common.assertion.result.assertion_run_event_handler import (
    AssertionRunEventResultHandler,
)
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.graph import DataHubAssertionGraph
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)
from datahub_executor.config import DATAHUB_GMS_TOKEN, DATAHUB_GMS_URL


def create_datahub_graph() -> DataHubAssertionGraph:
    """Create a DataHub client based on environment variables."""
    return DataHubAssertionGraph(
        DatahubClientConfig(
            server=DATAHUB_GMS_URL,
            # When token is not set, the client will automatically try to use
            # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
            token=DATAHUB_GMS_TOKEN,
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

    assertion_transformers = [
        EmbeddedAssertionsTransformer.create(graph),
        AssertionAdjustmentTransformer.create(graph),
    ]

    # Create result handlers
    result_handlers = [
        AssertionRunEventResultHandler(graph),
        AssertionDryRunEventResultHandler(graph),
    ]

    # Create assertion engine
    return AssertionEngine(
        evaluators, result_handlers=result_handlers, transformers=assertion_transformers
    )
