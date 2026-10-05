"""The Kafka Connect probe against a live Connect cluster: the same connectors
test_kafka_connect.py seeds, read through `datahub recipe probe`'s entry point."""

import json
import logging
from typing import Dict, List, Set

# jpype.imports must precede the kafka_connect modules: transform_plugins
# imports java.util.regex at import time, which only resolves once it is loaded.
import jpype
import jpype.imports  # noqa: F401
import pytest
import requests

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.models import ProbeRunEnvelope
from datahub.ingestion.agent.probe_methods import run_probe_method
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.kafka_connect.common import KafkaConnectSourceConfig
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.metadata.schema_classes import DataFlowInfoClass

# Fixtures imported so pytest resolves them here: the stack and its seeded
# connectors are test_kafka_connect.py's, not restated.
from tests.integration.kafka_connect.test_kafka_connect import (  # noqa: F401
    KAFKA_CONNECT_SERVER,
    kafka_connect_runner,
    loaded_kafka_connect,
    test_resources_dir,
)

pytestmark = pytest.mark.integration_batch_5

# Supported connectors of each shape (JDBC source with and without a router,
# Postgres JDBC source, JDBC sink, a generic_connectors source) plus enough
# denied ones that a verdict mismatch in either direction shows up.
ALLOWED = [
    "^mysql_source1$",
    "^mysql_source2$",
    "^postgres_source$",
    "^mysql_sink$",
    "^generic_source$",
]

RECIPE: Dict[str, object] = {
    "connect_uri": KAFKA_CONNECT_SERVER,
    "connector_patterns": {"allow": ALLOWED},
    # The same substitutions kafka_connect_to_file.yml makes, so lineage
    # resolves the way the golden-file test sees it.
    "provided_configs": [
        {"provider": "env", "path_key": "MYSQL_PORT", "value": "3306"},
        {"provider": "env", "path_key": "MYSQL_DB", "value": "librarydb"},
        {
            "provider": "env",
            "path_key": "MYSQL_CONNECTION_URL",
            "value": "jdbc:mysql://test_mysql:3306/librarydb",
        },
        {
            "provider": "env",
            "path_key": "POSTGRES_CONNECTION_URL",
            "value": "jdbc:postgresql://test_postgres:5432/postgres",
        },
    ],
    "generic_connectors": [
        {
            "connector_name": "generic_source",
            "source_dataset": "generic-dataset",
            "source_platform": "generic-platform",
        }
    ],
}

SEEDED = {
    "mysql_source1",
    "mysql_source2",
    "mysql_source3",
    "mysql_source4",
    "mysql_source5",
    "mysql_sink",
    "debezium-mysql-connector",
    "postgres_source",
    "debezium-postgres-connector",
    "debezium-sqlserver-connector",
    "generic_source",
    "source_mongodb_connector",
    "confluent_s3_sink_connector",
    "bigquery-sink-connector",
}

# Key fragments whose values the probe must never return: credentials, and
# values that carry them (connection URLs/URIs) or carry row values (query).
_WITHHELD_KEY_FRAGMENTS = ("password", "secret", ".url", ".uri", "query", "key.id")


def _withheld_values(provided: List[Dict[str, str]]) -> Set[str]:
    """Every credential-bearing value in the seeded connector configs, read off
    the cluster rather than restated here, plus what provided_configs
    substitutes into them."""
    values: Set[str] = set()
    for name in sorted(SEEDED):
        response = requests.get(f"{KAFKA_CONNECT_SERVER}/connectors/{name}/config")
        response.raise_for_status()
        for key, value in response.json().items():
            if any(f in key.lower() for f in _WITHHELD_KEY_FRAGMENTS):
                # Too short to search for without matching unrelated text
                # (the S3 sink's dummy AWS keys are "x").
                if len(str(value)) >= 4:
                    values.add(str(value))
    values |= {p["value"] for p in provided if "URL" in p["path_key"]}
    return values


def _run(command: str, **kwargs: object) -> ProbeRunEnvelope:
    result = run_probe_method("kafka-connect", dict(RECIPE), command, dict(kwargs))
    assert result.failures == [], f"{command} {kwargs}: {result.failures}"
    return result.to_dict()


def _ingested_flows() -> Set[str]:
    """The DataFlow names a real ingestion run with RECIPE emits."""
    config = KafkaConnectSourceConfig.model_validate(dict(RECIPE))
    source = KafkaConnectSource(config, PipelineContext(run_id="probe-parity"))
    names: Set[str] = set()
    try:
        for wu in source.get_workunits_internal():
            mcp = wu.metadata
            if isinstance(mcp, MetadataChangeProposalWrapper) and isinstance(
                mcp.aspect, DataFlowInfoClass
            ):
                names.add(mcp.aspect.name)
    finally:
        source.close()
    return names


@pytest.mark.usefixtures("loaded_kafka_connect")
def test_probe_commands_answer_from_the_live_cluster_without_disclosing_secrets(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.DEBUG)
    provided = RECIPE["provided_configs"]
    assert isinstance(provided, list)
    withheld = _withheld_values(provided)
    assert withheld, (
        "fixture seeds no credential-bearing config, so this proves nothing"
    )

    outputs: List[ProbeRunEnvelope] = []

    listing = _run("connectors")
    outputs.append(listing)
    names = listing["result"]
    assert isinstance(names, list)
    assert set(names) >= SEEDED

    detail = _run("connector", connector="mysql_source1")
    outputs.append(detail)
    record = detail["result"]
    assert isinstance(record, dict)
    assert record["name"] == "mysql_source1"
    assert record["type"] == "source"
    assert record["emitted"] is True
    config_keys = record["config_keys"]
    assert isinstance(config_keys, list) and "connection.url" in config_keys
    lineage_config = record["lineage_config"]
    assert isinstance(lineage_config, dict)
    assert lineage_config["topic.prefix"] == "test-mysql-jdbc-"
    assert "connection.url" not in lineage_config

    # Credential-heavy configs: the Debezium connectors carry database.password,
    # mysql_source4 a password in its URL and a query.
    for name in (
        "debezium-mysql-connector",
        "debezium-postgres-connector",
        "debezium-sqlserver-connector",
        "mysql_source4",
        "source_mongodb_connector",
        "confluent_s3_sink_connector",
    ):
        outputs.append(_run("connector", connector=name))

    topics = _run("connector_topics", connector="mysql_source1")
    outputs.append(topics)
    topic_names = topics["result"]
    assert isinstance(topic_names, list)
    assert topic_names and all(t.startswith("test-mysql-jdbc-") for t in topic_names)

    lineage = _run("connector_lineage", connector="mysql_source1")
    outputs.append(lineage)
    edges = lineage["result"]
    assert isinstance(edges, list) and edges
    for edge in edges:
        assert isinstance(edge, dict)
        edge_outputs = edge["outputs"]
        assert isinstance(edge_outputs, list)
        assert all("urn:li:dataPlatform:kafka" in urn for urn in edge_outputs)

    for name in ("mysql_source4", "debezium-mysql-connector", "mysql_sink"):
        outputs.append(_run("connector_lineage", connector=name))

    rendered = "\n".join(repr(o) for o in outputs) + json.dumps(outputs, default=str)
    # Messages, not caplog.text: the formatted text carries logger names, and
    # the Postgres fixture's password is the product's own lowercase name, which
    # every "datahub.*" logger name contains.
    logged = "\n".join(record.getMessage() for record in caplog.records)
    for value in withheld:
        assert value not in rendered, f"probe result disclosed {value!r}"
        if value != "datahub":
            assert value not in logged, f"probe logged {value!r}"


@pytest.mark.usefixtures("loaded_kafka_connect")
def test_probe_filter_verdicts_match_the_dataflows_ingestion_emits() -> None:
    listed = _run("connectors")["result"]
    assert isinstance(listed, list)

    verdicts = check_filters(
        "kafka-connect", dict(RECIPE), kind="Connector", parent_path=[], names=listed
    )
    included = {r.name for r in verdicts.results if r.included}
    emitted = _ingested_flows()

    # Every allowed connector here is one ingestion supports, so pattern
    # verdicts and emitted DataFlows must agree exactly.
    assert included == emitted
    assert included, "the allow list matched nothing, so parity proves nothing"
    assert set(listed) - included, "nothing was denied, so parity proves nothing"

    # And `connector` says so per connector: the `emitted` flag is the half of
    # the answer a pattern cannot give.
    for name in sorted(included):
        record = _run("connector", connector=name)["result"]
        assert isinstance(record, dict)
        assert record["emitted"] is True, name
