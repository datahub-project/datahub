"""Fixtures and builders shared by the Kafka Connect unit tests.

Fixtures are picked up by pytest; the plain builders are imported with
``from tests.unit.kafka_connect.conftest import ...``.
"""

from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, cast
from unittest.mock import MagicMock

# jpype.imports must be loaded before the kafka_connect modules: transform_plugins
# resolves java.util.regex at import time, and without it RegexRouter support is
# switched off for the whole process.
import jpype
import jpype.imports
import pytest
import requests_mock

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import StructuredLogEntry
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.kafka_connect.common import (
    BaseConnector,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.sql_parsing.schema_resolver import SchemaResolverInterface

CONNECT_URI = "http://connect.example:8083"
CLOUD_CONNECT_URI = (
    "https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1"
)
KAFKA_REST_ENDPOINT = "https://pkc-123.us-east-1.aws.confluent.cloud"
KAFKA_TOPICS_URL = f"{KAFKA_REST_ENDPOINT}/kafka/v3/clusters/lkc-1/topics"


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    """Debezium include/exclude lists and RegexRouter are Java regexes; the
    connectors evaluate them on a JVM through JPype."""
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


@pytest.fixture
def config() -> KafkaConnectSourceConfig:
    return KafkaConnectSourceConfig(
        connect_uri="http://localhost:8083", cluster_name="test"
    )


@pytest.fixture
def report() -> KafkaConnectSourceReport:
    return KafkaConnectSourceReport()


def make_manifest(
    config: Dict[str, Any],
    *,
    name: str = "connector",
    connector_type: str = "source",
    topic_names: Optional[List[str]] = None,
    tasks: Optional[List[Dict[str, Any]]] = None,
    lineages: Optional[List[KafkaConnectLineage]] = None,
) -> ConnectorManifest:
    """A connector as the Connect REST API reports it. ``config`` is typed
    loosely on purpose: the API does not validate value types, and some tests
    plant non-string values to check how the connectors degrade."""
    return ConnectorManifest(
        name=name,
        type=connector_type,
        config=config,
        tasks=tasks or [],
        topic_names=topic_names or [],
        lineages=lineages or [],
    )


def dataset_urn(platform: str, name: str, env: str = "PROD") -> str:
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{name},{env})"


def lineage_map(connector: BaseConnector) -> Dict[str, Optional[str]]:
    """``{topic: source dataset}`` for every lineage the connector extracts."""
    return {
        lineage.target_dataset: lineage.source_dataset
        for lineage in connector.extract_lineages()
    }


def reported(entries: Iterable[StructuredLogEntry], context_fragment: str) -> bool:
    """Whether some report entry carries ``context_fragment`` (usually the
    connector name) in its context. Context is the stable key of a structured
    log entry; titles and messages are wording."""
    return any(
        context_fragment in context for entry in entries for context in entry.context
    )


class StubSchemaResolver(SchemaResolverInterface):
    """The slice of SchemaResolver the connectors read: a platform, the cached
    URN set, per-table column schemas and an optional DataHub graph."""

    platform_instance: Optional[str] = None
    env = "PROD"

    def __init__(
        self,
        platform: str = "kafka",
        urns: Iterable[str] = (),
        error: Optional[Exception] = None,
        graph: Optional[DataHubGraph] = None,
    ) -> None:
        self._platform = platform
        self._urns: Set[str] = set(urns)
        self._error = error
        self._schemas: Dict[str, Dict[str, str]] = {}
        self.graph = graph

    @property
    def platform(self) -> str:
        return self._platform

    def includes_temp_tables(self) -> bool:
        return False

    def get_urns(self) -> Set[str]:
        if self._error is not None:
            raise self._error
        return self._urns

    def resolve_table(self, table: Any) -> Tuple[str, Optional[Dict[str, str]]]:
        return self.get_urn_for_table(table), self._schemas.get(table.table)

    def get_urn_for_table(self, table: Any) -> str:
        return dataset_urn(self._platform, table.table)

    def add_schema(self, table_name: str, schema: Dict[str, str]) -> None:
        self._schemas[table_name] = schema


def attach_resolver(
    connector: BaseConnector, resolver: Optional[SchemaResolverInterface]
) -> None:
    """BaseConnector declares the concrete SchemaResolver; the connectors only
    call the interface, which the stub implements."""
    connector.schema_resolver = cast(Any, resolver)


def mock_graph() -> MagicMock:
    return MagicMock(spec=DataHubGraph)


def self_hosted_source(m: requests_mock.Mocker, **overrides: Any) -> KafkaConnectSource:
    """A source against a self-hosted Connect REST API that lists no connectors
    until a test registers some with ``mock_connector``."""
    m.get(f"{CONNECT_URI}/connectors", json=[])
    config = KafkaConnectSourceConfig(connect_uri=CONNECT_URI, **overrides)
    return KafkaConnectSource(config, PipelineContext(run_id="test"))


def cloud_source(m: requests_mock.Mocker, **overrides: Any) -> KafkaConnectSource:
    """A Confluent Cloud source. Cloud requires a DataHub graph and Connect
    credentials; the schema resolver is off unless a test turns it on."""
    m.get(f"{CLOUD_CONNECT_URI}/connectors", json=[])
    config = KafkaConnectSourceConfig(
        confluent_cloud_environment_id="env-1",
        confluent_cloud_cluster_id="lkc-1",
        username="connect-key",
        password="connect-secret",
        use_schema_resolver=False,
        **overrides,
    )
    ctx = PipelineContext(run_id="test")
    ctx.graph = mock_graph()
    return KafkaConnectSource(config, ctx)


def mock_connector(
    m: requests_mock.Mocker,
    name: str,
    body: Dict[str, Any],
    topics: List[str],
) -> None:
    """Register one connector on the self-hosted Connect REST API: the listing,
    its detail, its runtime topics and its (empty) task list."""
    m.get(f"{CONNECT_URI}/connectors", json=[name])
    m.get(f"{CONNECT_URI}/connectors/{name}", json=body)
    m.get(f"{CONNECT_URI}/connectors/{name}/topics", json={name: {"topics": topics}})
    m.get(f"{CONNECT_URI}/connectors/{name}/tasks", json=[])
