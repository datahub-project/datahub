"""Kafka Connect's probe: ingestion's own per-connector steps without its pattern,
and a disclosure allowlist whose absences are the point."""

import base64
from typing import Dict, Iterable, List, Tuple

import pytest
import requests
import requests_mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import _iter_specs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.ingestion.source.kafka_connect.kafka_connect_probe import (
    PROBE_REQUEST_TIMEOUT_SECONDS,
    KafkaConnectMetadataProbe,
)
from datahub.metadata.schema_classes import DataJobInputOutputClass

CONNECT = "http://connect.example:8083"

# Planted credentials. Distinctive so a substring search over serialized output cannot false-pass.
PLANTED_CRED = "mk-7f3a-planted-conn-cred"
PLANTED_URL_CRED = "mk-9c1d-planted-in-jdbc-url"
ROW_VALUE = "4111-1111-1111-1111"
PLANTED_SR_CRED = "mk-5e2b-planted-sr-userinfo"

SINK_CONFIG: Dict[str, str] = {
    "connector.class": "PostgresSink",
    "connection.host": "db.example",
    "connection.port": "5432",
    "db.name": "analytics",
    "topics": "orders,users",
    "table.name.format": "${topic}",
    "connection.user": "svc_writer",
    "connection.password": PLANTED_CRED,
    "connection.url": (
        f"jdbc:postgresql://db.example:5432/analytics?user=svc&password={PLANTED_URL_CRED}"
    ),
    "query": f"SELECT * FROM t WHERE card = '{ROW_VALUE}'",
    "value.converter.basic.auth.user.info": f"sr-key:{PLANTED_SR_CRED}",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "(.*)",
    "transforms.route.replacement": "$1",
}

UNSUPPORTED_SOURCE_CONFIG: Dict[str, str] = {
    "connector.class": "com.example.CustomSource",
    "database.password": PLANTED_CRED,
}

CLUSTER: Dict[str, Tuple[str, Dict[str, str]]] = {
    "orders-sink": ("sink", SINK_CONFIG),
    "legacy-source": ("source", UNSUPPORTED_SOURCE_CONFIG),
}
TOPICS: Dict[str, List[str]] = {"orders-sink": ["orders", "users", "stale_topic"]}


def _recipe(**overrides: object) -> KafkaConnectSourceConfig:
    return KafkaConnectSourceConfig.model_validate(
        {
            "connect_uri": CONNECT,
            "username": "connect-user",
            "password": "test_password",
            **overrides,
        }
    )


def _mock_cluster(
    m: requests_mock.Mocker,
    connectors: Dict[str, Tuple[str, Dict[str, str]]],
    topics: Dict[str, List[str]],
) -> None:
    m.get(f"{CONNECT}/connectors", json=sorted(connectors))
    for name, (connector_type, config) in connectors.items():
        m.get(
            f"{CONNECT}/connectors/{name}",
            json={"name": name, "type": connector_type, "config": config, "tasks": []},
        )
        m.get(
            f"{CONNECT}/connectors/{name}/topics",
            json={name: {"topics": topics.get(name, [])}},
        )
        m.get(f"{CONNECT}/connectors/{name}/tasks", json=[])


def _io_by_job(
    workunits: Iterable[MetadataWorkUnit],
) -> Dict[str, Tuple[List[str], List[str]]]:
    out: Dict[str, Tuple[List[str], List[str]]] = {}
    for wu in workunits:
        mcp = wu.metadata
        if isinstance(mcp, MetadataChangeProposalWrapper) and isinstance(
            mcp.aspect, DataJobInputOutputClass
        ):
            out[str(mcp.entityUrn)] = (
                sorted(mcp.aspect.inputDatasets),
                sorted(mcp.aspect.outputDatasets),
            )
    return out


def _ingested_io(
    config: KafkaConnectSourceConfig,
) -> Dict[str, Tuple[List[str], List[str]]]:
    """What a real ingestion run emits, keyed by DataJob URN -- the parity oracle."""
    source = KafkaConnectSource(config, PipelineContext(run_id="probe-parity"))
    try:
        return _io_by_job(list(source.get_workunits_internal()))
    finally:
        source.close()


def _shim(config: KafkaConnectSourceConfig) -> KafkaConnectSource:
    return KafkaConnectSource.for_probe(
        config,
        session=KafkaConnectSource._create_connect_session(config),
        kafka_session=KafkaConnectSource._create_kafka_session(),
    )


def test_the_probe_shim_enriches_a_manifest_exactly_as_ingestion_does() -> None:
    config = _recipe()
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        expected = _ingested_io(config)

        shim = _shim(config)
        url = f"{CONNECT}/connectors/orders-sink"
        manifest = shim._parse_connector_manifest(
            "orders-sink", shim.session.get(url).json()
        )
        assert isinstance(manifest, ConnectorManifest)
        assert shim._enrich_manifest("orders-sink", manifest, url) is True
        emitted = _io_by_job(list(shim.construct_job_workunits(manifest)))
    assert emitted == expected
    assert emitted, "fixture produced no lineage, so parity proves nothing"


def test_the_shim_drops_an_unsupported_source_connector_as_ingestion_does() -> None:
    config = _recipe()
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        shim = _shim(config)
        url = f"{CONNECT}/connectors/legacy-source"
        manifest = shim._parse_connector_manifest(
            "legacy-source", shim.session.get(url).json()
        )
        assert manifest is not None
        assert shim._enrich_manifest("legacy-source", manifest, url) is False


def test_confluent_cloud_without_credentials_is_refused_by_the_shared_factory() -> None:
    config = KafkaConnectSourceConfig.model_validate(
        {
            "confluent_cloud_environment_id": "env-1",
            "confluent_cloud_cluster_id": "lkc-1",
        }
    )
    with pytest.raises(ValueError):
        KafkaConnectSource._create_connect_session(config)


def test_building_the_provider_opens_no_connection() -> None:
    with requests_mock.Mocker() as m:
        KafkaConnectMetadataProbe.for_config(_recipe()).__exit__(None, None, None)
        assert m.call_count == 0


def test_connectors_lists_every_name_including_denied_ones() -> None:
    config = _recipe(connector_patterns={"deny": ["^orders-sink$"]})
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(config) as probe:
            assert probe.connectors() == ["legacy-source", "orders-sink"]


def test_probe_requests_authenticate_as_ingestion_does_and_time_out() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            probe.connectors()
        expected = "Basic " + base64.b64encode(b"connect-user:test_password").decode()
        assert m.last_request.headers["Authorization"] == expected
        assert all(
            r.timeout == PROBE_REQUEST_TIMEOUT_SECONDS for r in m.request_history
        )


def test_an_auth_failure_raises_instead_of_listing_nothing() -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"{CONNECT}/connectors",
            status_code=401,
            json={"error_code": 401, "message": "Unauthorized"},
        )
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            with pytest.raises(requests.HTTPError):
                probe.connectors()


@pytest.mark.parametrize(
    "name", ["orders-sink/config", "orders-sink?expand=info", "orders-sink#", "a%2Fb"]
)
def test_a_connector_name_carrying_url_syntax_never_reaches_the_source(
    name: str,
) -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            with pytest.raises(ValueError):
                probe._require_listed(name)
        assert m.call_count == 0


def test_an_unlisted_connector_is_a_bad_argument() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            with pytest.raises(ValueError):
                probe._require_listed("no-such-connector")
        assert [r.path for r in m.request_history] == ["/connectors"]


def test_there_is_no_raw_api_command() -> None:
    # A raw passthrough over Connect reaches /connectors/{name}/config, i.e.
    # credentials. See the module docstring before adding one.
    commands = dict(_iter_specs(KafkaConnectMetadataProbe))
    assert "api" not in commands
    assert all(spec.scoped_path_param is None for spec in commands.values())


def test_connector_verdicts_match_ingestions_own_predicate() -> None:
    recipe: Dict[str, object] = {
        "connect_uri": CONNECT,
        "connector_patterns": {"deny": ["^orders-sink$"]},
    }
    names = ["orders-sink", "legacy-source"]
    result = check_filters(
        "kafka-connect", recipe, kind="Connector", parent_path=[], names=names
    )
    # kafka_connect.py get_connectors_manifest: connector_patterns.allowed(name)
    ingestion = KafkaConnectSourceConfig.model_validate(recipe).connector_patterns
    assert [(r.name, r.included) for r in result.results] == [
        (n, ingestion.allowed(n)) for n in names
    ]
    assert result.pattern_field == "connector_patterns"
    assert result.filtering == "by_pattern"
