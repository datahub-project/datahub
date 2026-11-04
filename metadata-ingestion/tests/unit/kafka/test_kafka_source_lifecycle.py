import concurrent.futures
from typing import Dict, cast
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka.admin import ConfigEntry, ConfigResource
from confluent_kafka.schema_registry.schema_registry_client import (
    RegisteredSchema,
    Schema,
)

from datahub.configuration.kafka import KafkaConsumerConnectionConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import SourceCapability
from datahub.ingestion.source.kafka.confluent_catalog import KafkaTopicCatalog
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
from tests.unit.kafka.conftest import (
    BOOTSTRAP,
    KafkaClientMocks,
    SourceFactory,
    emitted_aspects,
    warned_about,
)

OAUTH_CALLBACK = "tests.integration.kafka.oauth:create_token"


def test_oauth_callback_config_forces_an_initial_poll(
    kafka_clients: KafkaClientMocks,
) -> None:
    """librdkafka only runs the OAuth callback on poll(), so a client built with
    oauth_cb must poll once up front or the first real request fails auth."""
    connection = KafkaConsumerConnectionConfig(
        bootstrap=BOOTSTRAP, consumer_config={"oauth_cb": OAUTH_CALLBACK}
    )

    get_kafka_consumer(connection)
    get_kafka_admin_client(connection)

    kafka_clients.consumer.return_value.poll.assert_called_once_with(
        timeout=OAUTH_CALLBACK_POLL_TIMEOUT_SECONDS
    )
    kafka_clients.admin.return_value.poll.assert_called_once_with(
        timeout=OAUTH_CALLBACK_POLL_TIMEOUT_SECONDS
    )


def test_plain_config_does_not_poll_on_construction(
    kafka_clients: KafkaClientMocks,
) -> None:
    connection = KafkaConsumerConnectionConfig(bootstrap=BOOTSTRAP)

    get_kafka_consumer(connection)
    get_kafka_admin_client(connection)

    kafka_clients.consumer.return_value.poll.assert_not_called()
    kafka_clients.admin.return_value.poll.assert_not_called()


def test_test_connection_reports_broker_and_registry_capabilities(
    kafka_clients: KafkaClientMocks,
) -> None:
    kafka_clients.registry.return_value.get_subjects.return_value = ["t-value"]

    report = KafkaSource.test_connection({"connection": {"bootstrap": BOOTSTRAP}})

    assert report.basic_connectivity is not None
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report[SourceCapability.SCHEMA_METADATA].capable
    kafka_clients.consumer.return_value.close.assert_called_once()


def test_test_connection_reports_failures_without_raising(
    kafka_clients: KafkaClientMocks,
) -> None:
    """Broker and registry failures land in the report (with reasons) instead of
    propagating, and the probe consumer is still released."""
    kafka_clients.consumer.return_value.list_topics.side_effect = RuntimeError(
        "broker down"
    )
    kafka_clients.registry.return_value.get_subjects.side_effect = RuntimeError(
        "registry down"
    )

    report = KafkaSource.test_connection({"connection": {"bootstrap": BOOTSTRAP}})

    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert report.capability_report is not None
    schema_capability = report.capability_report[SourceCapability.SCHEMA_METADATA]
    assert not schema_capability.capable
    assert schema_capability.failure_reason
    kafka_clients.consumer.return_value.close.assert_called_once()


def test_bad_schema_registry_class_fails_init_and_releases_consumer(
    kafka_clients: KafkaClientMocks,
) -> None:
    """A failed __init__ must not leak the broker connection / consumer-group slot."""
    config = KafkaSourceConfig.model_validate(
        {
            "connection": {"bootstrap": BOOTSTRAP},
            "schema_registry_class": "datahub.does.not.exist:Registry",
        }
    )

    with pytest.raises(ImportError):
        KafkaSource(config, PipelineContext(run_id="test"))

    kafka_clients.consumer.return_value.close.assert_called_once()


def test_close_tolerates_client_close_failures(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    kafka_clients.consumer.return_value.close.side_effect = RuntimeError(
        "already closed"
    )
    source = make_kafka_source()
    catalog = MagicMock(spec=KafkaTopicCatalog)
    catalog.close.side_effect = RuntimeError("catalog gone")
    source.topic_catalog = catalog

    source.close()  # must not raise

    catalog.close.assert_called_once()


def test_topic_configs_from_admin_client_become_custom_properties(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    """Only tracked, set config keys are rendered; one topic's failed
    describe_configs future must not block the others or abort the run."""
    kafka_clients.set_topics(["orders", "audit"])

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
    kafka_clients.admin.return_value.describe_configs.return_value = {
        ConfigResource(ConfigResource.Type.TOPIC, "orders"): orders_future,
        ConfigResource(ConfigResource.Type.TOPIC, "audit"): audit_future,
    }

    source = make_kafka_source()
    by_topic: Dict[str, Dict[str, str]] = {
        cast(str, props.name): props.customProperties
        for props in emitted_aspects(source, DatasetPropertiesClass)
    }

    assert by_topic["orders"] == {
        "retention.ms": "86400000",
        "cleanup.policy": "compact",
    }
    assert by_topic["audit"] == {}


def test_external_url_base_links_each_topic(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    kafka_clients.set_topics(["orders"])

    source = make_kafka_source(
        {"external_url_base": "https://console.example.com/topics/"}
    )
    [props] = emitted_aspects(source, DatasetPropertiesClass)

    assert props.externalUrl == "https://console.example.com/topics/orders"


def test_one_failing_topic_does_not_abort_the_run(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    """An exception while extracting one topic is reported against that topic
    and the remaining topics are still emitted."""
    kafka_clients.set_topics(["good", "bad"])
    source = make_kafka_source()
    original = source._extract_record_with_schemas

    def flaky(topic: str, *args, **kwargs):  # type: ignore[no-untyped-def]
        if topic == "bad":
            raise RuntimeError("boom")
        return original(topic, *args, **kwargs)

    with patch.object(source, "_extract_record_with_schemas", side_effect=flaky):
        names = {
            props.name for props in emitted_aspects(source, DatasetPropertiesClass)
        }

    assert names == {"good"}
    assert warned_about(source.report, "bad")


def test_subject_entities_skip_subjects_without_schema_and_report_failures(
    kafka_clients: KafkaClientMocks, make_kafka_source: SourceFactory
) -> None:
    """With ingest_schemas_as_entities, only a subject with a parseable schema
    becomes a SCHEMA dataset; a malformed one is reported, not fatal."""
    kafka_clients.set_topics([])
    kafka_clients.registry.return_value.get_subjects.return_value = [
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

    kafka_clients.registry.return_value.get_latest_version.side_effect = latest
    source = make_kafka_source({"ingest_schemas_as_entities": True})

    names = {props.name for props in emitted_aspects(source, DatasetPropertiesClass)}

    assert names == {"ok-value"}
    assert warned_about(source.report, "broken-value")
