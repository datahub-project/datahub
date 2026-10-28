"""KafkaConnectSource against a mocked Connect REST API and Kafka REST API v3.

Covers the shapes the Connect API hands back for a connector, how ingestion
degrades when parts of the API fail, and how the Confluent Cloud topic list
is located and read.
"""

from typing import Any, Dict, List, cast
from unittest.mock import MagicMock, patch

import jpype
import jpype.imports
import pytest
import requests_mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    ProvidedConfig,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import (
    KafkaConnectSource,
    SinkTopicFilter,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass

CONNECT = "http://connect.example:8083"
CLOUD_CONNECT = (
    "https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1"
)
KAFKA_REST = "https://pkc-123.us-east-1.aws.confluent.cloud"
TOPICS_URL = f"{KAFKA_REST}/kafka/v3/clusters/lkc-1/topics"

S3_SINK_CONFIG: Dict[str, str] = {
    "connector.class": "io.confluent.connect.s3.S3SinkConnector",
    "s3.bucket.name": "my-bucket",
    "topics": "orders",
}
JDBC_SOURCE_CONFIG: Dict[str, str] = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:postgresql://db.example:5432/mydb",
    "table.whitelist": "public.users",
    "topic.prefix": "pg_",
}


@pytest.fixture(scope="session", autouse=True)
def ensure_jvm_started() -> None:
    if not jpype.isJVMStarted():
        jpype.startJVM(jpype.getDefaultJVMPath())


def _self_hosted(m: requests_mock.Mocker, **overrides: Any) -> KafkaConnectSource:
    m.get(f"{CONNECT}/connectors", json=[])
    config = KafkaConnectSourceConfig(connect_uri=CONNECT, **overrides)
    return KafkaConnectSource(config, PipelineContext(run_id="test"))


def _cloud(m: requests_mock.Mocker, **overrides: Any) -> KafkaConnectSource:
    m.get(f"{CLOUD_CONNECT}/connectors", json=[])
    config = KafkaConnectSourceConfig(
        confluent_cloud_environment_id="env-1",
        confluent_cloud_cluster_id="lkc-1",
        username="connect-key",
        password="connect-secret",
        use_schema_resolver=False,
        **overrides,
    )
    ctx = PipelineContext(run_id="test")
    ctx.graph = MagicMock()
    return KafkaConnectSource(config, ctx)


def _mock_connector(
    m: requests_mock.Mocker,
    name: str,
    body: Dict[str, Any],
    topics: List[str],
) -> None:
    m.get(f"{CONNECT}/connectors", json=[name])
    m.get(f"{CONNECT}/connectors/{name}", json=body)
    m.get(f"{CONNECT}/connectors/{name}/topics", json={name: {"topics": topics}})
    m.get(f"{CONNECT}/connectors/{name}/tasks", json=[])


class TestConnectorManifestParsing:
    def test_nested_info_response_is_unwrapped(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            _mock_connector(
                m,
                "s3-sink",
                {
                    "info": {
                        "name": "s3-sink",
                        "type": "sink",
                        "config": S3_SINK_CONFIG,
                        "tasks": [],
                    },
                    "status": {"connector": {"state": "RUNNING"}},
                },
                topics=["orders"],
            )
            manifests = list(source.get_connectors_manifest())

        assert len(manifests) == 1
        assert manifests[0].config == S3_SINK_CONFIG
        assert [lineage.target_dataset for lineage in manifests[0].lineages] == [
            "my-bucket/topics/orders"
        ]

    def test_missing_name_falls_back_to_listing_name(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            _mock_connector(
                m,
                "s3-sink",
                {"type": "sink", "config": S3_SINK_CONFIG, "tasks": []},
                topics=["orders"],
            )
            manifests = list(source.get_connectors_manifest())

        assert [manifest.name for manifest in manifests] == ["s3-sink"]

    def test_missing_type_drops_connector_with_warning(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            _mock_connector(
                m, "broken", {"name": "broken", "config": S3_SINK_CONFIG}, topics=[]
            )
            manifests = list(source.get_connectors_manifest())

        assert manifests == []
        assert any("broken" in str(w.context) for w in source.report.warnings)

    def test_malformed_config_and_tasks_fields_degrade_to_empty(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)

        manifest = source._parse_connector_manifest(
            "odd",
            {"name": "odd", "type": "sink", "config": "not-a-dict", "tasks": "nope"},
        )

        assert manifest is not None
        assert manifest.config == {}
        assert manifest.tasks == []

    def test_connector_detail_fetch_failure_is_reported_and_skipped(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            m.get(f"{CONNECT}/connectors", json=["gone"])
            m.get(f"{CONNECT}/connectors/gone", status_code=500)
            manifests = list(source.get_connectors_manifest())

        assert manifests == []
        assert any("gone" in str(w.context) for w in source.report.warnings)

    def test_tasks_fetch_failure_leaves_tasks_empty(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            _mock_connector(
                m,
                "jdbc",
                {
                    "name": "jdbc",
                    "type": "source",
                    "config": JDBC_SOURCE_CONFIG,
                    "tasks": [],
                },
                topics=["pg_users"],
            )
            m.get(f"{CONNECT}/connectors/jdbc/tasks", status_code=503)
            manifests = list(source.get_connectors_manifest())

        assert len(manifests) == 1
        assert manifests[0].tasks == []
        assert any("jdbc" in str(w.context) for w in source.report.warnings)

    def test_denied_connector_is_reported_as_dropped(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m, connector_patterns={"deny": ["s3-.*"]})
            _mock_connector(
                m,
                "s3-sink",
                {
                    "name": "s3-sink",
                    "type": "sink",
                    "config": S3_SINK_CONFIG,
                    "tasks": [],
                },
                topics=["orders"],
            )
            manifests = list(source.get_connectors_manifest())

        assert manifests == []
        assert "s3-sink" in source.report.filtered

    def test_provided_configs_are_substituted_before_lineage(self) -> None:
        config = dict(JDBC_SOURCE_CONFIG)
        config["connection.url"] = "jdbc:postgresql://${env:DB_HOST}:5432/mydb"
        with requests_mock.Mocker() as m:
            source = _self_hosted(
                m,
                provided_configs=[
                    ProvidedConfig(
                        provider="env", path_key="DB_HOST", value="db.internal"
                    )
                ],
            )
            _mock_connector(
                m,
                "jdbc",
                {"name": "jdbc", "type": "source", "config": config, "tasks": []},
                topics=["pg_users"],
            )
            manifests = list(source.get_connectors_manifest())

        assert (
            manifests[0].config["connection.url"]
            == "jdbc:postgresql://db.internal:5432/mydb"
        )
        assert manifests[0].lineages[0].source_dataset == "mydb.public.users"


class TestJvmUnavailable:
    def test_source_still_constructs_when_no_jvm_can_start(self) -> None:
        jpype_stub = MagicMock()
        jpype_stub.isJVMStarted.return_value = False
        jpype_stub.startJVM.side_effect = RuntimeError("no JDK on this host")
        with (
            requests_mock.Mocker() as m,
            patch(
                "datahub.ingestion.source.kafka_connect.kafka_connect.jpype",
                jpype_stub,
            ),
            patch(
                "datahub.ingestion.source.kafka_connect.kafka_connect.prepare_jvm_settings",
                return_value=(),
            ),
        ):
            source = _self_hosted(m)

        assert source.report.failures == []
        cast(MagicMock, jpype_stub.startJVM).assert_called_once()


class TestConfluentCloudInit:
    def test_cloud_requires_datahub_graph(self) -> None:
        config = KafkaConnectSourceConfig(
            confluent_cloud_environment_id="env-1",
            confluent_cloud_cluster_id="lkc-1",
            username="connect-key",
            password="connect-secret",
        )
        with requests_mock.Mocker() as m:
            m.get(f"{CLOUD_CONNECT}/connectors", json=[])
            with pytest.raises(ValueError):
                KafkaConnectSource(config, PipelineContext(run_id="test"))


class TestKafkaRestTopicList:
    def test_unexpected_response_shape_marks_list_unavailable(self) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m, kafka_rest_endpoint=KAFKA_REST)
            m.get(TOPICS_URL, json={"kind": "Error", "message": "nope"})
            topics = source._get_all_topics_from_kafka_api()

        assert topics is None
        assert any("topics_url" in str(w.context) for w in source.report.warnings)

    def test_runaway_pagination_is_capped(self) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m, kafka_rest_endpoint=KAFKA_REST)
            m.get(
                TOPICS_URL,
                json={
                    "kind": "KafkaTopicList",
                    "data": [{"topic_name": "orders"}],
                    "metadata": {"next": TOPICS_URL},
                },
            )
            with patch(
                "datahub.ingestion.source.kafka_connect.kafka_connect.MAX_KAFKA_TOPIC_PAGES",
                3,
            ):
                topics = source._get_all_topics_from_kafka_api()

        assert topics is None
        assert m.call_count <= 1 + 3

    def test_rest_endpoint_without_cluster_id_in_connect_uri(self) -> None:
        proxy = "https://connect-proxy.example/v1"
        with requests_mock.Mocker() as m:
            m.get(f"{proxy}/connectors", json=[])
            config = KafkaConnectSourceConfig(
                connect_uri=proxy,
                confluent_cloud_environment_id="env-1",
                confluent_cloud_cluster_id="lkc-1",
                username="connect-key",
                password="connect-secret",
                use_schema_resolver=False,
                kafka_rest_endpoint=KAFKA_REST,
            )
            ctx = PipelineContext(run_id="test")
            ctx.graph = MagicMock()
            source = KafkaConnectSource(config, ctx)
            topics = source._get_all_topics_from_kafka_api()

        assert topics is None
        assert not any(call.url.startswith(KAFKA_REST) for call in m.request_history)

    @pytest.mark.parametrize(
        "endpoint",
        [
            "SASL_SSL://pkc-123.us-east-1.aws.confluent.cloud:9092",
            "pkc-123.us-east-1.aws.confluent.cloud:9092",
        ],
    )
    def test_rest_endpoint_derived_from_connector_bootstrap_servers(
        self, endpoint: str
    ) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m)
            m.get(f"{CLOUD_CONNECT}/connectors", json=["local-sink", "cloud-sink"])
            m.get(
                f"{CLOUD_CONNECT}/connectors/local-sink",
                json={"config": {"bootstrap.servers": "localhost:9092"}},
            )
            m.get(
                f"{CLOUD_CONNECT}/connectors/cloud-sink",
                json={"config": {"kafka.endpoint": endpoint}},
            )
            m.get(
                TOPICS_URL,
                json={
                    "kind": "KafkaTopicList",
                    "data": [{"topic_name": "orders"}],
                    "metadata": {"next": None},
                },
            )
            topics = source._get_all_topics_from_kafka_api()

        assert topics == ["orders"]

    def test_no_connector_exposes_endpoint_marks_list_unavailable(self) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m)
            m.get(f"{CLOUD_CONNECT}/connectors", json=["a", "b"])
            m.get(
                f"{CLOUD_CONNECT}/connectors/a",
                json={"config": {"bootstrap.servers": "localhost:9092"}},
            )
            m.get(f"{CLOUD_CONNECT}/connectors/b", status_code=404)
            topics = source._get_all_topics_from_kafka_api()

        assert topics is None
        assert any("connect_uri" in str(w.context) for w in source.report.warnings)

    def test_only_confluent_cloud_brokers_convert_to_a_rest_endpoint(self) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m)

        assert (
            source._convert_broker_to_rest_endpoint("SASL_SSL://broker.internal:9092")
            is None
        )

    def test_self_hosted_without_credentials_sends_no_kafka_auth(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)

        assert source._get_kafka_auth_headers() is None

    def test_empty_connector_list_marks_list_unavailable(self) -> None:
        with requests_mock.Mocker() as m:
            source = _cloud(m)
            topics = source._get_all_topics_from_kafka_api()

        assert topics is None


def _job_io(source: KafkaConnectSource, manifest: ConnectorManifest) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for wu in source.construct_job_workunits(manifest):
        mcp = wu.metadata
        assert isinstance(mcp, MetadataChangeProposalWrapper)
        if isinstance(mcp.aspect, DataJobInputOutputClass):
            out[str(mcp.entityUrn)] = mcp.aspect
    return out


class TestJobIds:
    def _manifest(self, lineages: List[KafkaConnectLineage]) -> ConnectorManifest:
        return ConnectorManifest(
            name="jdbc",
            type="source",
            config=JDBC_SOURCE_CONFIG,
            tasks=[],
            lineages=lineages,
        )

    def test_connect_to_platform_map_prefixes_job_id(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(
                m, connect_to_platform_map={"jdbc": {"postgres": "pg-prod"}}
            )
        manifest = self._manifest(
            [
                KafkaConnectLineage(
                    source_platform="postgres",
                    source_dataset="mydb.public.users",
                    target_platform="kafka",
                    target_dataset="pg_users",
                )
            ]
        )

        jobs = _job_io(source, manifest)

        assert len(jobs) == 1
        job_urn = next(iter(jobs))
        assert job_urn.endswith("pg-prod.mydb.public.users)")
        assert "pg-prod" in jobs[job_urn].inputDatasets[0]

    def test_one_source_feeding_many_topics_gets_distinct_job_ids(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
        manifest = self._manifest(
            [
                KafkaConnectLineage(
                    source_platform="postgres",
                    source_dataset="mydb.public.users",
                    target_platform="kafka",
                    target_dataset=topic,
                )
                for topic in ("users-created", "users-updated")
            ]
        )

        jobs = _job_io(source, manifest)

        assert len(jobs) == 2
        assert any(urn.endswith("users.users-created)") for urn in jobs)
        assert any(urn.endswith("users.users-updated)") for urn in jobs)

    def test_lineage_without_source_uses_unknown_source_job_id(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m, convert_lineage_urns_to_lowercase=True)
        manifest = self._manifest(
            [
                KafkaConnectLineage(
                    source_platform="postgres",
                    source_dataset=None,
                    target_platform="kafka",
                    target_dataset="Query_Results",
                )
            ]
        )

        jobs = _job_io(source, manifest)

        job_urn = next(iter(jobs))
        assert "unknown_source.Query_Results" in job_urn
        assert jobs[job_urn].inputDatasets == []
        assert jobs[job_urn].outputDatasets[0].endswith("query_results,PROD)")


class TestSinkTopicFilter:
    def test_regex_subscription_filters_runtime_topics(self) -> None:
        topics = SinkTopicFilter().filter_stale_topics(
            ["orders", "users", "audit"], {"topics.regex": "(orders|users)"}
        )
        assert topics == ["orders", "users"]

    def test_missing_subscription_keeps_runtime_topics(self) -> None:
        runtime = ["orders", "users"]
        topics = SinkTopicFilter().filter_stale_topics(
            runtime, {"connector.class": "some.Sink"}
        )
        assert topics == runtime


class TestLifecycle:
    def test_close_tolerates_failing_catalog_and_resolver_provider(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
        catalog = MagicMock()
        catalog.close.side_effect = RuntimeError("catalog down")
        provider = MagicMock()
        provider.close.side_effect = RuntimeError("provider down")
        source._catalog = catalog
        source._schema_resolver_provider = provider

        source.close()

        cast(MagicMock, catalog.close).assert_called_once()
        cast(MagicMock, provider.close).assert_called_once()

    def test_partially_read_catalog_warns_once_per_run(self) -> None:
        with requests_mock.Mocker() as m:
            source = _self_hosted(m)
            catalog = MagicMock()
            catalog.is_complete.return_value = False
            source._catalog = catalog
            list(source.get_workunits_internal())

        assert any(
            "confluent_catalog" in str(w.context) for w in source.report.warnings
        )
