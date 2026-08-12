from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.kafka.kafka import (
    KafkaSource,
    KafkaSourceConfig,
    is_confluent_cloud_bootstrap,
)
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    KafkaSchemaClass,
    SchemaMetadataClass,
)
from datahub.sdk.dataset import Dataset

NO_LINKS_WARNING = "No Confluent Cloud links emitted"
TOPIC = "orders"
ENVIRONMENT_ID = "env-xxxxx"
CLUSTER_ID = "lkc-xxxxx"
CONFLUENT_CLOUD_BOOTSTRAP = "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092"
SELF_HOSTED_BOOTSTRAP = "broker.example.com:9092"
# What an open-source broker reports for the same generic KIP-78 field.
OPAQUE_CLUSTER_ID = "MkU3OEVBNTcwNTJENDM2Qk"
CONSOLE_URL = (
    f"https://confluent.cloud/environments/{ENVIRONMENT_ID}"
    f"/clusters/{CLUSTER_ID}/topics/{TOPIC}"
)
EXTERNAL_URL_BASE = "https://console.aiven.io/project/p/kafka/topics"


@pytest.mark.parametrize(
    "bootstrap,expected",
    [
        (CONFLUENT_CLOUD_BOOTSTRAP, True),
        ("PKC-XXXXX.US-EAST-1.AWS.CONFLUENT.CLOUD:9092", True),
        (f"{CONFLUENT_CLOUD_BOOTSTRAP},pkc-yyyyy.confluent.cloud:9092", True),
        (SELF_HOSTED_BOOTSTRAP, False),
        # One broker outside Confluent Cloud means the run is not addressing a
        # Confluent Cloud cluster, whichever order the list is given in.
        (f"{CONFLUENT_CLOUD_BOOTSTRAP},{SELF_HOSTED_BOOTSTRAP}", False),
        (f"{SELF_HOSTED_BOOTSTRAP},{CONFLUENT_CLOUD_BOOTSTRAP}", False),
        # A suffix match on the whole string would accept this.
        ("evil-confluent.cloud.example.com:9092", False),
        ("", False),
        (",", False),
    ],
)
def test_is_confluent_cloud_bootstrap(bootstrap: str, expected: bool) -> None:
    assert is_confluent_cloud_bootstrap(bootstrap) is expected


@pytest.fixture
def mock_admin_client():
    with patch(
        "datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True
    ) as mock:
        yield mock


def build_source(
    mock_kafka: Mock,
    bootstrap: str = CONFLUENT_CLOUD_BOOTSTRAP,
    cluster_id: Optional[str] = CLUSTER_ID,
    **config_overrides: Any,
) -> KafkaSource:
    cluster_metadata = MagicMock()
    cluster_metadata.topics = {TOPIC: None}
    cluster_metadata.cluster_id = cluster_id
    mock_kafka.return_value.list_topics.return_value = cluster_metadata

    config: Dict[str, Any] = {"connection": {"bootstrap": bootstrap}}
    config.update(config_overrides)
    return KafkaSource(
        KafkaSourceConfig.model_validate(config),
        PipelineContext(run_id="test"),
    )


def no_links_warnings(source: KafkaSource) -> List[str]:
    return [
        warning.title
        for warning in source.report.warnings
        if warning.title == NO_LINKS_WARNING
    ]


def external_urls_of(workunits: List[MetadataWorkUnit]) -> List[Optional[str]]:
    return [
        properties.externalUrl
        for properties in (
            workunit.get_aspect_of_type(DatasetPropertiesClass)
            for workunit in workunits
        )
        if properties is not None
    ]


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
class TestConfluentCloudExternalUrl:
    def test_topic_links_to_the_confluent_cloud_console(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = build_source(mock_kafka, confluent_cloud_environment_id=ENVIRONMENT_ID)

        external_urls = external_urls_of(list(source.get_workunits()))

        assert CONSOLE_URL in external_urls

    def test_no_link_when_the_cluster_is_not_confluent_cloud(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = build_source(
            mock_kafka,
            bootstrap=SELF_HOSTED_BOOTSTRAP,
            confluent_cloud_environment_id=ENVIRONMENT_ID,
        )

        external_urls = external_urls_of(list(source.get_workunits()))

        assert external_urls
        assert all(url is None for url in external_urls)
        assert no_links_warnings(source)

    def test_no_link_when_the_cluster_id_is_not_a_confluent_cloud_id(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        # Every broker since Kafka 0.10.1 reports a cluster id, so a
        # confluent.cloud endpoint is not on its own enough to trust its shape.
        source = build_source(
            mock_kafka,
            cluster_id=OPAQUE_CLUSTER_ID,
            confluent_cloud_environment_id=ENVIRONMENT_ID,
        )

        external_urls = external_urls_of(list(source.get_workunits()))

        assert external_urls
        assert all(url is None for url in external_urls)
        assert no_links_warnings(source)

    def test_no_link_without_an_environment_id(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = build_source(mock_kafka)

        external_urls = external_urls_of(list(source.get_workunits()))

        assert external_urls
        assert all(url is None for url in external_urls)
        assert not no_links_warnings(source)

    def test_an_explicit_external_url_base_wins(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = build_source(
            mock_kafka,
            confluent_cloud_environment_id=ENVIRONMENT_ID,
            external_url_base=EXTERNAL_URL_BASE,
        )

        external_urls = external_urls_of(list(source.get_workunits()))

        assert f"{EXTERNAL_URL_BASE}/{TOPIC}" in external_urls
        assert CONSOLE_URL not in external_urls

    def test_no_warning_when_an_explicit_base_covers_a_failed_guard(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        # Setting both on a cluster that fails the Confluent guards still yields
        # links, from the base. Warning that none were emitted would contradict
        # what the run actually produced.
        source = build_source(
            mock_kafka,
            bootstrap=SELF_HOSTED_BOOTSTRAP,
            confluent_cloud_environment_id=ENVIRONMENT_ID,
            external_url_base=EXTERNAL_URL_BASE,
        )

        external_urls = external_urls_of(list(source.get_workunits()))

        assert f"{EXTERNAL_URL_BASE}/{TOPIC}" in external_urls
        assert not no_links_warnings(source)

    def test_external_url_base_still_works_on_its_own(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = build_source(
            mock_kafka,
            bootstrap=SELF_HOSTED_BOOTSTRAP,
            cluster_id=None,
            external_url_base=f"{EXTERNAL_URL_BASE}/",
        )

        external_urls = external_urls_of(list(source.get_workunits()))

        assert f"{EXTERNAL_URL_BASE}/{TOPIC}" in external_urls

    def test_subjects_get_no_external_url(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        # A subject is not addressable under a topic path, so neither link applies.
        source = build_source(
            mock_kafka,
            confluent_cloud_environment_id=ENVIRONMENT_ID,
            external_url_base=EXTERNAL_URL_BASE,
            ingest_schemas_as_entities=True,
        )

        subjects = list(
            source._emit_dataset(
                topic=f"{TOPIC}-value",
                is_subject=True,
                topic_detail=None,
                extra_topic_config=None,
                schema_metadata=SchemaMetadataClass(
                    schemaName=f"{TOPIC}-value",
                    platform=make_data_platform_urn("kafka"),
                    version=0,
                    hash="",
                    platformSchema=KafkaSchemaClass(documentSchema=""),
                    fields=[],
                ),
            )
        )

        assert subjects
        assert all(
            isinstance(subject, Dataset) and subject.external_url is None
            for subject in subjects
        )
