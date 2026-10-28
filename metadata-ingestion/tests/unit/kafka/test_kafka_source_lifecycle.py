import concurrent.futures
from typing import Dict, Iterator, List, Tuple, cast
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.admin import ConfigEntry, ConfigResource
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.kafka.kafka import (
    KafkaSource,
    KafkaSourceConfig,
    get_kafka_admin_client,
    get_kafka_consumer,
)
from datahub.ingestion.source.kafka.kafka_constants import (
    OAUTH_CALLBACK_POLL_TIMEOUT_SECONDS,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass

KAFKA_MODULE = "datahub.ingestion.source.kafka.kafka"
REGISTRY_MODULE = "datahub.ingestion.source.confluent_schema_registry"
OAUTH_CALLBACK = "tests.integration.kafka.oauth:create_token"


@pytest.fixture
def clients() -> Iterator[Tuple[MagicMock, MagicMock, MagicMock]]:
    """Patch every network client the source constructs: consumer, admin, registry."""
    with (
        patch(f"{KAFKA_MODULE}.confluent_kafka.Consumer", autospec=True) as consumer,
        patch(f"{KAFKA_MODULE}.AdminClient", autospec=True) as admin,
        patch(f"{REGISTRY_MODULE}.SchemaRegistryClient", autospec=True) as registry,
    ):
        registry.return_value.get_subjects.return_value = []
        yield consumer, admin, registry


def _topics(consumer: MagicMock, names: List[str]) -> None:
    metadata = MagicMock()
    metadata.topics = dict.fromkeys(names)
    consumer.return_value.list_topics.return_value = metadata


def _dataset_properties(source: KafkaSource) -> List[DatasetPropertiesClass]:
    return [
        wu.metadata.aspect
        for wu in source.get_workunits()
        if isinstance(wu.metadata, MetadataChangeProposalWrapper)
        and isinstance(wu.metadata.aspect, DatasetPropertiesClass)
    ]


def test_oauth_callback_config_forces_an_initial_poll(clients):
    # librdkafka only runs the OAuth callback on poll(), so a client built with
    # oauth_cb must poll once up front or the first real request fails auth.
    consumer, admin, _ = clients
    connection = KafkaConsumerConnectionConfig(
        bootstrap="localhost:9092", consumer_config={"oauth_cb": OAUTH_CALLBACK}
    )

    get_kafka_consumer(connection)
    get_kafka_admin_client(connection)

    consumer.return_value.poll.assert_called_once_with(
        timeout=OAUTH_CALLBACK_POLL_TIMEOUT_SECONDS
    )
    admin.return_value.poll.assert_called_once_with(
        timeout=OAUTH_CALLBACK_POLL_TIMEOUT_SECONDS
    )


def test_plain_config_does_not_poll_on_construction(clients):
    consumer, admin, _ = clients
    connection = KafkaConsumerConnectionConfig(bootstrap="localhost:9092")

    get_kafka_consumer(connection)
    get_kafka_admin_client(connection)

    consumer.return_value.poll.assert_not_called()
    admin.return_value.poll.assert_not_called()


def test_test_connection_reports_broker_and_registry_capabilities(clients):
    consumer, _, registry = clients
    registry.return_value.get_subjects.return_value = ["t-value"]

    report = KafkaSource.test_connection(
        {"connection": {"bootstrap": "localhost:9092"}}
    )

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    consumer.return_value.close.assert_called_once()


def test_test_connection_reports_failures_without_raising(clients):
    consumer, _, registry = clients
    consumer.return_value.list_topics.side_effect = RuntimeError("broker down")
    registry.return_value.get_subjects.side_effect = RuntimeError("registry down")

    report = KafkaSource.test_connection(
        {"connection": {"bootstrap": "localhost:9092"}}
    )

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert report.capability_report is not None
    schema_capability = report.capability_report[SourceCapability.SCHEMA_METADATA]
    assert not schema_capability.capable
    assert schema_capability.failure_reason
    # The consumer must be released even when the checks fail.
    consumer.return_value.close.assert_called_once()


def test_bad_schema_registry_class_fails_init_and_releases_consumer(clients):
    consumer, _, _ = clients
    config = KafkaSourceConfig.model_validate(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "schema_registry_class": "datahub.does.not.exist:Registry",
        }
    )

    with pytest.raises(ImportError):
        KafkaSource(config, PipelineContext(run_id="test"))

    # A failed init must not leak the broker connection / consumer-group slot.
    consumer.return_value.close.assert_called_once()


def test_close_tolerates_client_close_failures(clients):
    consumer, _, _ = clients
    consumer.return_value.close.side_effect = RuntimeError("already closed")
    source = KafkaSource.create(
        {"connection": {"bootstrap": "localhost:9092"}}, PipelineContext(run_id="t")
    )
    catalog = MagicMock()
    catalog.close.side_effect = RuntimeError("catalog gone")
    source.topic_catalog = catalog

    source.close()  # must not raise

    catalog.close.assert_called_once()


def test_topic_configs_from_admin_client_become_custom_properties(clients):
    consumer, admin, _ = clients
    _topics(consumer, ["orders", "audit"])

    orders_future: concurrent.futures.Future = concurrent.futures.Future()
    orders_future.set_result(
        {
            "retention.ms": ConfigEntry("retention.ms", "86400000"),
            "cleanup.policy": ConfigEntry("cleanup.policy", "compact"),
            # Entries the broker reports as unset are skipped, not rendered as "None".
            "min.insync.replicas": None,
            "segment.ms": ConfigEntry("segment.ms", "1000"),  # not a tracked key
        }
    )
    audit_future: concurrent.futures.Future = concurrent.futures.Future()
    audit_future.set_exception(RuntimeError("not authorized"))
    admin.return_value.describe_configs.return_value = {
        ConfigResource(ConfigResource.Type.TOPIC, "orders"): orders_future,
        ConfigResource(ConfigResource.Type.TOPIC, "audit"): audit_future,
    }

    source = KafkaSource.create(
        {"connection": {"bootstrap": "localhost:9092"}}, PipelineContext(run_id="t")
    )
    by_topic: Dict[str, Dict[str, str]] = {
        cast(str, props.name): props.customProperties
        for props in _dataset_properties(source)
    }

    assert by_topic["orders"] == {
        "retention.ms": "86400000",
        "cleanup.policy": "compact",
    }
    # One topic's failed describe must not block the others or abort the run.
    assert by_topic["audit"] == {}


def test_external_url_base_links_each_topic(clients):
    consumer, _, _ = clients
    _topics(consumer, ["orders"])

    source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "external_url_base": "https://console.example.com/topics/",
        },
        PipelineContext(run_id="t"),
    )
    [props] = _dataset_properties(source)

    assert props.externalUrl == "https://console.example.com/topics/orders"


def test_one_failing_topic_does_not_abort_the_run(clients):
    consumer, _, _ = clients
    _topics(consumer, ["good", "bad"])
    source = KafkaSource.create(
        {"connection": {"bootstrap": "localhost:9092"}}, PipelineContext(run_id="t")
    )
    original = source._extract_record_with_schemas

    def flaky(topic: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if topic == "bad":
            raise RuntimeError("boom")
        return original(topic, *args, **kwargs)

    with patch.object(source, "_extract_record_with_schemas", side_effect=flaky):
        names = {props.name for props in _dataset_properties(source)}

    assert names == {"good"}
    assert any("bad" in ctx for w in source.report.warnings for ctx in w.context)


def test_subject_entities_skip_subjects_without_schema_and_report_failures(clients):
    consumer, _, registry = clients
    _topics(consumer, [])
    registry.return_value.get_subjects.return_value = [
        "no-schema-value",
        "broken-value",
        "ok-value",
    ]
    schemas = {
        "ok-value": '{"type":"record","name":"Ok","fields":[{"name":"a","type":"string"}]}',
        # Malformed Avro registered under a subject: parsing raises past the registry.
        "broken-value": "{not avro",
    }

    def latest(subject_name: str) -> RegisteredSchema:
        if subject_name not in schemas:
            raise KeyError(subject_name)
        return RegisteredSchema(
            schema_id=1,
            guid=None,
            schema=Schema(schema_str=schemas[subject_name], schema_type="AVRO"),
            subject=subject_name,
            version=1,
        )

    registry.return_value.get_latest_version.side_effect = latest
    source = KafkaSource.create(
        {
            "connection": {"bootstrap": "localhost:9092"},
            "ingest_schemas_as_entities": True,
        },
        PipelineContext(run_id="t"),
    )

    names = {props.name for props in _dataset_properties(source)}

    # Only the subject with a parseable schema becomes a SCHEMA dataset; the
    # malformed one is reported rather than aborting the run.
    assert names == {"ok-value"}
    assert any(
        "broken-value" in ctx for w in source.report.warnings for ctx in w.context
    )
