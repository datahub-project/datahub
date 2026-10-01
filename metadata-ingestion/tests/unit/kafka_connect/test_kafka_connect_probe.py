"""Kafka Connect's probe: ingestion's own per-connector steps without its pattern,
and a disclosure allowlist whose absences are the point."""

import base64
import json
import logging
import re
from typing import Dict, Iterable, List, Tuple

import pytest
import requests
import requests_mock

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.probe_methods import _iter_specs, run_probe_method
from datahub.ingestion.agent.redact import SENSITIVE_KEY_HINTS
from datahub.ingestion.agent.verdicts import ProbeReadFailed
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka_connect.common import (
    ConnectorManifest,
    KafkaConnectSourceConfig,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.ingestion.source.kafka_connect.kafka_connect_probe import (
    DISCLOSED_CONFIG_KEYS,
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


URI_CRED = "mk-3b8e-planted-uri-userinfo"


@pytest.mark.parametrize(
    "password",
    [
        URI_CRED,
        # requests percent-encodes these in Response.url, so matching the
        # configured value would miss them.
        f"{URI_CRED}|x",
        f"{URI_CRED}^x",
        f"{URI_CRED}{{x}}",
    ],
)
@pytest.mark.parametrize(
    "command,kwargs,failing_path",
    [
        ("connectors", {}, "/connectors"),
        ("connector", {"connector": "orders-sink"}, "/connectors/orders-sink"),
    ],
)
def test_an_http_error_does_not_echo_userinfo_from_connect_uri(
    command: str, kwargs: Dict[str, object], failing_path: str, password: str
) -> None:
    # requests keeps userinfo in Response.url, so raise_for_status() puts a
    # connect_uri's embedded password into the error text, and the CLI masks
    # only recipe values a key hint marks secret -- `connect_uri` is not one.
    authed = f"http://connect-user:{password}@connect.example:8083"
    with requests_mock.Mocker() as m:
        m.get(re.compile(r".*/connectors$"), json=["orders-sink"])
        m.get(re.compile(f".*{re.escape(failing_path)}$"), status_code=500, json={})
        with pytest.raises(requests.HTTPError) as raised:
            run_probe_method("kafka-connect", {"connect_uri": authed}, command, kwargs)
    message = str(raised.value)
    assert URI_CRED not in message
    assert "connect-user" not in message
    assert "connect.example:8083" in message


NO_DB_CRED = "mk-41c7-planted-no-db-url"

# JDBC URLs with no database name: ingestion's parsers raise a ValueError that
# quotes the whole URL, query-string password included.
NO_DB_SOURCE_CONFIG: Dict[str, str] = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "mode": "incrementing",
    "topic.prefix": "nodb-",
    "connection.url": f"jdbc:mysql://db.example:3306?user=u&password={NO_DB_CRED}",
}
NO_DB_SINK_CONFIG: Dict[str, str] = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "orders",
    "connection.url": f"jdbc:postgresql://db.example:5432?user=u&password={NO_DB_CRED}",
}


@pytest.mark.parametrize("command", ["connector", "connector_lineage"])
@pytest.mark.parametrize(
    "connector_type,config",
    [("source", NO_DB_SOURCE_CONFIG), ("sink", NO_DB_SINK_CONFIG)],
)
def test_a_failing_ingestion_step_never_echoes_the_connection_url(
    command: str,
    connector_type: str,
    config: Dict[str, str],
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    with requests_mock.Mocker() as m:
        _mock_cluster(m, {"nodb": (connector_type, config)}, {"nodb": ["orders"]})
        if connector_type == "source":
            # The source parser raises, quoting the URL; the probe must fail
            # without it.
            with pytest.raises(ProbeReadFailed) as raised:
                run_probe_method(
                    "kafka-connect", dict(_RECIPE), command, {"connector": "nodb"}
                )
            assert NO_DB_CRED not in str(raised.value)
            assert "nodb" in str(raised.value)
        else:
            # The sink parser catches its own error and records a warning.
            result = run_probe_method(
                "kafka-connect", dict(_RECIPE), command, {"connector": "nodb"}
            )
            assert NO_DB_CRED not in json.dumps(result.to_dict(), default=str)
    assert NO_DB_CRED not in caplog.text


@pytest.mark.parametrize("scheme", ["http", "https"])
@pytest.mark.parametrize("reserved", ["/", "#", "?", "\\"])
@pytest.mark.parametrize(
    "command,kwargs", [("connectors", {}), ("connector", {"connector": "orders-sink"})]
)
def test_a_connect_uri_whose_password_would_be_misparsed_is_refused(
    scheme: str, reserved: str, command: str, kwargs: Dict[str, object]
) -> None:
    # An unencoded reserved character ends the authority early, so urllib3 sees
    # "user:<password prefix>" as host:port and quotes it in InvalidURL -- text
    # with no "://" or "@" for a structural scrub to find.
    secret = f"{URI_CRED}{reserved}tail"
    uri = f"{scheme}://connect-user:{secret}@connect.example:8083"
    with requests_mock.Mocker() as m:
        with pytest.raises(ValueError) as raised:
            run_probe_method("kafka-connect", {"connect_uri": uri}, command, kwargs)
        assert m.call_count == 0
    assert not isinstance(raised.value, ProbeReadFailed)
    assert URI_CRED not in str(raised.value)
    assert "connect-user" not in str(raised.value)


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


_WITHHELD = (PLANTED_CRED, PLANTED_URL_CRED, ROW_VALUE, PLANTED_SR_CRED, "svc_writer")
_RECIPE: Dict[str, object] = {
    "connect_uri": CONNECT,
    "username": "connect-user",
    "password": "test_password",
}

# A JDBC sink whose URL names no platform DataHub knows: ingestion's handler
# reports it with the raw connection.url as the warning context, and
# report.warning() logs that context to the console.
UNKNOWN_JDBC_SINK_CONFIG: Dict[str, str] = {
    "connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector",
    "topics": "orders",
    "connection.url": f"jdbc:exampledb://db.example:7000/sales?password={PLANTED_URL_CRED}",
}


def test_connector_reports_what_ingestion_resolves_for_it() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            detail = probe.connector("orders-sink")
    assert detail["name"] == "orders-sink"
    assert detail["type"] == "sink"
    assert detail["connector_class"] == "PostgresSink"
    assert detail["handled_by"] == "JdbcSinkConnector"
    assert detail["platform"] == "postgres"
    assert detail["emitted"] is True
    assert detail["lineage_edges"] == 2
    assert detail["flow_urn"] == ("urn:li:dataFlow:(kafka-connect,orders-sink,PROD)")
    # Key names are disclosed so the caller can see what is set...
    config_keys = detail["config_keys"]
    assert isinstance(config_keys, list) and "connection.password" in config_keys
    # ...values only for lineage-relevant keys.
    lineage_config = detail["lineage_config"]
    assert isinstance(lineage_config, dict)
    assert lineage_config["topics"] == "orders,users"
    assert lineage_config["transforms.route.regex"] == "(.*)"
    assert "connection.url" not in lineage_config


def test_an_unsupported_source_connector_is_reported_as_not_emitted() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            detail = probe.connector("legacy-source")
        ingested = _ingested_io(_recipe())
    assert detail["emitted"] is False
    assert detail["handled_by"] is None
    assert not any("legacy-source" in urn for urn in ingested)


@pytest.mark.parametrize(
    "command,kwargs",
    [
        ("connectors", {}),
        ("connector", {"connector": "orders-sink"}),
        ("connector", {"connector": "legacy-source"}),
        ("connector", {"connector": "unknown-jdbc-sink"}),
        ("connector_topics", {"connector": "orders-sink"}),
        ("connector_lineage", {"connector": "unknown-jdbc-sink"}),
        ("connector_lineage", {"connector": "orders-sink"}),
    ],
)
def test_no_command_returns_or_logs_a_credential_from_connector_config(
    command: str, kwargs: Dict[str, object], caplog: pytest.LogCaptureFixture
) -> None:
    # Before the CLI's recipe-secret masking: these values are not in the recipe,
    # so that masking could never have caught them -- not in the result, and not
    # in a log line on stderr either.
    cluster = {**CLUSTER, "unknown-jdbc-sink": ("sink", UNKNOWN_JDBC_SINK_CONFIG)}
    caplog.set_level(logging.DEBUG)
    with requests_mock.Mocker() as m:
        _mock_cluster(m, cluster, TOPICS)
        result = run_probe_method("kafka-connect", dict(_RECIPE), command, kwargs)
    serialized = json.dumps(result.to_dict(), default=str)
    for value in _WITHHELD:
        assert value not in serialized, f"{command} returned {value!r}"
        assert value not in caplog.text, f"{command} logged {value!r}"


def test_the_log_check_above_would_catch_ingestions_logging() -> None:
    # Guards the guard: the same connector through a real ingestion run does
    # log the raw URL, so an empty caplog above is the probe's doing.
    config = _recipe()
    cluster = {"unknown-jdbc-sink": ("sink", UNKNOWN_JDBC_SINK_CONFIG)}
    with requests_mock.Mocker() as m:
        _mock_cluster(m, cluster, {})
        logger = logging.getLogger("datahub")
        records: List[str] = []

        class _Collect(logging.Handler):
            def emit(self, record: logging.LogRecord) -> None:
                records.append(record.getMessage())

        handler = _Collect(level=logging.DEBUG)
        logger.addHandler(handler)
        try:
            _ingested_io(config)
        finally:
            logger.removeHandler(handler)
    assert any(PLANTED_URL_CRED in r for r in records)


def test_no_disclosed_key_names_a_credential() -> None:
    # Pins the allowlist against the two denylists that already exist: the
    # recipe hints and ingestion's own sink property-bag markers.
    ingestion_markers = ("secret", "token", "credential", "password", ".key")
    for key in DISCLOSED_CONFIG_KEYS:
        lowered = key.lower()
        assert not any(h in lowered for h in SENSITIVE_KEY_HINTS), key
        assert not any(mk in lowered for mk in ingestion_markers), key
    # Credential-bearing by value, not by name.
    assert not {"connection.url", "connection.uri", "query"} & DISCLOSED_CONFIG_KEYS


def test_a_connector_deleted_after_listing_is_a_bad_argument() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        m.get(f"{CONNECT}/connectors/orders-sink", status_code=404, json={})
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            with pytest.raises(ValueError):
                probe.connector("orders-sink")


CLOUD_URI = "https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1"
KAFKA_REST = "https://pkc-1.example.confluent.cloud"


def _cloud_recipe() -> KafkaConnectSourceConfig:
    return KafkaConnectSourceConfig.model_validate(
        {
            "confluent_cloud_environment_id": "env-1",
            "confluent_cloud_cluster_id": "lkc-1",
            "username": "cloud-key",
            "password": "test_password",
            "kafka_rest_endpoint": KAFKA_REST,
        }
    )


def test_connector_topics_are_the_runtime_topics_minus_stale_sink_topics() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            # stale_topic is in /topics but not in the sink's `topics`, so
            # ingestion's SinkTopicFilter drops it.
            assert sorted(probe.connector_topics("orders-sink")) == ["orders", "users"]


def test_empty_topics_on_confluent_cloud_say_why() -> None:
    with requests_mock.Mocker() as m:
        m.get(f"{CLOUD_URI}/connectors", json=["orders-sink"])
        with KafkaConnectMetadataProbe.for_config(_cloud_recipe()) as probe:
            assert probe.connector_topics("orders-sink") == []
            assert any("Confluent Cloud" in w for w in probe.warnings)


def test_cluster_topics_lists_non_internal_topics_on_confluent_cloud() -> None:
    with requests_mock.Mocker() as m:
        m.get(
            f"{KAFKA_REST}/kafka/v3/clusters/lkc-1/topics",
            json={
                "kind": "KafkaTopicList",
                "data": [
                    {"topic_name": "orders"},
                    {"topic_name": "_confluent-internal", "is_internal": True},
                ],
                "metadata": {"next": None},
            },
        )
        with KafkaConnectMetadataProbe.for_config(_cloud_recipe()) as probe:
            assert probe.cluster_topics() == ["orders"]


def test_cluster_topics_is_the_wrong_command_on_self_hosted_connect() -> None:
    with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
        with pytest.raises(ValueError):
            probe.cluster_topics()


def test_a_topic_under_a_denied_connector_is_reported_excluded() -> None:
    result = check_filters(
        "kafka-connect",
        {"connect_uri": CONNECT, "connector_patterns": {"deny": ["^orders-sink$"]}},
        kind="Topic",
        parent_path=["orders-sink"],
        names=["orders"],
    )
    assert result.filtering == "unfiltered"
    assert [(r.included, r.excluded_by) for r in result.results] == [
        (False, "connector_patterns")
    ]


def test_connector_lineage_is_exactly_what_ingestion_emits() -> None:
    config = _recipe()
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        expected = _ingested_io(config)
        with KafkaConnectMetadataProbe.for_config(config) as probe:
            edges = probe.connector_lineage("orders-sink")
    got: Dict[str, Tuple[List[str], List[str]]] = {}
    for e in edges:
        job, inputs, outputs = e["job"], e["inputs"], e["outputs"]
        assert isinstance(job, str)
        assert isinstance(inputs, list) and isinstance(outputs, list)
        got[job] = (sorted(inputs), sorted(outputs))
    assert got == {k: v for k, v in expected.items() if "orders-sink" in k}
    assert got


def test_a_denied_connectors_lineage_is_still_shown() -> None:
    config = _recipe(connector_patterns={"deny": ["^orders-sink$"]})
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(config) as probe:
            assert probe.connector_lineage("orders-sink")


def test_an_unemitted_connector_has_no_lineage() -> None:
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(_recipe()) as probe:
            assert probe.connector_lineage("legacy-source") == []


def test_lineage_without_datahub_says_what_it_could_not_reproduce() -> None:
    config = _recipe(use_schema_resolver=True)
    with requests_mock.Mocker() as m:
        _mock_cluster(m, CLUSTER, TOPICS)
        with KafkaConnectMetadataProbe.for_config(config) as probe:
            probe.connector_lineage("orders-sink")
            assert any("use_schema_resolver" in w for w in probe.warnings)
