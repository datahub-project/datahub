"""Kafka Connect's probe: ingestion's own per-connector steps without its pattern,
and a disclosure allowlist whose absences are the point."""

from typing import Dict, Iterable, List, Tuple

import pytest
import requests_mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
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
