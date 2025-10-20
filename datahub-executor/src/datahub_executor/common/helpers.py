from typing import Any, Dict, List, Optional

from cachetools import TTLCache, cached
from datahub.ingestion.graph.client import DatahubClientConfig, DataHubGraph
from datahub.secret.datahub_secret_store import DataHubSecretStore
from datahub.secret.environment_secret_store import EnvironmentSecretStore
from datahub.secret.file_secret_store import FileSecretStore

from datahub_executor.common.assertion.engine.engine import AssertionEngine
from datahub_executor.common.assertion.engine.evaluator.field_evaluator import (
    FieldAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.freshness_evaluator import (
    FreshnessAssertionEvaluator,
)
from datahub_executor.common.assertion.engine.evaluator.schema_evaluator import (
    SchemaAssertionEvaluator,
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
from datahub_executor.common.client.config.graphql.query import (
    GRAPHQL_FETCH_CLOUD_LOGGING_CONFIGS,
)
from datahub_executor.common.connection.datahub_ingestion_source_connection_provider import (
    DataHubIngestionSourceConnectionProvider,
)
from datahub_executor.common.metric.client.client import (
    MetricClient,
)
from datahub_executor.common.metric.resolver.resolver import (
    MetricResolver,
)
from datahub_executor.common.monitor.client.client import (
    MonitorClient,
)
from datahub_executor.common.monitor.inference.metric_projection.metric_predictor import (
    MetricPredictor,
)
from datahub_executor.common.monitor.inference.monitor_training_engine import (
    MonitorTrainingEngine,
)
from datahub_executor.common.monitoring.base import METRIC
from datahub_executor.common.source.provider import SourceProvider
from datahub_executor.common.state.datahub_monitor_state_provider import (
    DataHubMonitorStateProvider,
)
from datahub_executor.common.types import CloudLoggingConfig
from datahub_executor.config import (
    DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR,
    DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN,
    DATAHUB_GMS_TOKEN,
    DATAHUB_GMS_URL,
)


def create_datahub_graph() -> DataHubGraph:
    """Create a DataHub client based on environment variables."""
    return DataHubGraph(
        DatahubClientConfig(
            server=DATAHUB_GMS_URL,
            # When token is not set, the client will automatically try to use
            # DATAHUB_SYSTEM_CLIENT_ID and DATAHUB_SYSTEM_CLIENT_SECRET to authenticate.
            token=DATAHUB_GMS_TOKEN,
        )
    )


@cached(
    cache=TTLCache(maxsize=1, ttl=300)
)  # 5 minutes TTL, maxsize=1 since we only cache one config
def fetch_cloud_logging_config(graph: DataHubGraph) -> CloudLoggingConfig:
    result = graph.execute_graphql(GRAPHQL_FETCH_CLOUD_LOGGING_CONFIGS)

    if "error" in result and result["error"] is not None:
        METRIC("WORKER_CLOUD_LOGGING_FETCHER_ERRORS", exception="GmsError").inc()
        raise Exception(
            f"Received error while fetching cloud_logging_configs from GMS! {result.get('error')}"
        )
    if "cloudLoggingConfigsResolver" not in result:
        METRIC(
            "WORKER_CLOUD_LOGGING_FETCHER_ERRORS", exception="IncompleteResults"
        ).inc()
        raise Exception(
            "Found incomplete search results when fetching cloud_logging_configs from GMS!"
        )

    try:
        cloud_logging_config = CloudLoggingConfig.model_validate(
            result["cloudLoggingConfigsResolver"]
        )
    except Exception:
        METRIC("WORKER_CLOUD_LOGGING_FETCHER_ERRORS", exception="ParseError").inc()
        raise Exception(
            f"Failed to convert CloudLoggingConfig object to Python object. {result['cloudLoggingConfigsResolver']}"
        )

    return cloud_logging_config


def paginate_datahub_query_results(
    graph: DataHubGraph,
    query: str,
    query_key: str,
    result_key: str,
    page_size: int,
    user_params: Optional[Dict],
    operation_name: Optional[str] = None,
) -> List[Any]:
    if user_params is None:
        user_params = {}
    results = []
    position = 0

    while True:
        params = dict(user_params)
        params.setdefault("input", {})
        params["input"]["start"] = position
        params["input"]["count"] = page_size

        response = graph.execute_graphql(
            query, variables=params, operation_name=operation_name
        )
        error = response.get("error", None)
        if error is not None:
            raise RuntimeError(f"Received GraphQL error: {error}")

        result = response.get(query_key, {})
        if result_key not in result:
            raise RuntimeError(f"Bad response from GMS: Key {result_key} not found.")

        page_items = result.get(result_key)
        results += page_items
        position += page_size
        if (
            position >= result["total"]
            or (result["start"] + result["count"]) >= result["total"]
        ):
            break

    return results


def create_assertion_engine(graph: DataHubGraph) -> AssertionEngine:
    # Create secret store for resolving recipe credentials
    datahub_secret_store = DataHubSecretStore.create({"graph_client": graph})
    env_secret_store = EnvironmentSecretStore.create({})
    file_secret_store = FileSecretStore.create(
        {
            "basedir": DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR,
            "max_length": DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN,
        }
    )

    # setup state provider
    state_provider = DataHubMonitorStateProvider(graph)

    # Create assertion evaluators
    evaluators = [
        FreshnessAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(
                graph, [env_secret_store, file_secret_store, datahub_secret_store]
            ),
            state_provider,
            SourceProvider(),
            MonitorClient(graph),
        ),
        VolumeAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(
                graph, [env_secret_store, file_secret_store, datahub_secret_store]
            ),
            state_provider,
            SourceProvider(),
            MetricResolver(
                DataHubIngestionSourceConnectionProvider(
                    graph, [env_secret_store, file_secret_store, datahub_secret_store]
                ),
                SourceProvider(),
            ),
            MetricClient(graph=graph),
            MonitorClient(graph=graph),
        ),
        SQLAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(
                graph, [env_secret_store, file_secret_store, datahub_secret_store]
            ),
            state_provider,
            SourceProvider(),
            MonitorClient(graph),
            MetricClient(graph=graph),
        ),
        FieldAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(
                graph, [env_secret_store, file_secret_store, datahub_secret_store]
            ),
            state_provider,
            SourceProvider(),
            MetricResolver(
                DataHubIngestionSourceConnectionProvider(
                    graph, [env_secret_store, file_secret_store, datahub_secret_store]
                ),
                SourceProvider(),
            ),
            MetricClient(graph=graph),
            MonitorClient(graph=graph),
        ),
        SchemaAssertionEvaluator(
            DataHubIngestionSourceConnectionProvider(
                graph, [env_secret_store, file_secret_store, datahub_secret_store]
            ),
            state_provider,
            SourceProvider(),
            MonitorClient(graph),
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


def create_monitor_training_engine(graph: DataHubGraph) -> MonitorTrainingEngine:
    return MonitorTrainingEngine(
        graph,
        MetricClient(graph),
        MetricPredictor(),
        MonitorClient(graph),
    )
