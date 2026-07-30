from typing import Dict, List, Optional
from unittest.mock import MagicMock, Mock, patch

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.kafka.confluent_catalog import (
    CatalogKafkaTopic,
    KafkaTopicCatalog,
)
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.metadata.schema_classes import DatasetPropertiesClass, GlobalTagsClass

CONFLUENT_SCHEMA_REGISTRY_URL = "https://psrc-abc123.us-east-1.aws.confluent.cloud"
# Kept local in tests that build a KafkaSource, so no lookup leaves the machine.
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC = "orders"


@pytest.fixture
def mock_admin_client():
    with patch(
        "datahub.ingestion.source.kafka.kafka.AdminClient", autospec=True
    ) as mock:
        yield mock


def make_source_config(
    catalog: Optional[Dict[str, object]] = None,
    schema_registry_config: Optional[Dict[str, str]] = None,
    schema_registry_url: str = CONFLUENT_SCHEMA_REGISTRY_URL,
) -> KafkaSourceConfig:
    return KafkaSourceConfig.model_validate(
        {
            "connection": {
                "bootstrap": "localhost:9092",
                "schema_registry_url": schema_registry_url,
                "schema_registry_config": schema_registry_config or {},
            },
            "confluent_catalog": catalog or {},
        }
    )


def enabled_catalog_config(**overrides: object) -> Dict[str, object]:
    config: Dict[str, object] = {
        "enabled": True,
        "schema_registry_url": CONFLUENT_SCHEMA_REGISTRY_URL,
        "api_key": "sr-key",
        "api_secret": "sr-secret",
    }
    config.update(overrides)
    return config


def attach_catalog(
    source: KafkaSource, topics: List[CatalogKafkaTopic]
) -> KafkaTopicCatalog:
    """Give the source a catalog that serves `topics` without any network calls."""
    client = Mock(spec=ConfluentStreamCatalogClient)
    client.fetch_entities.return_value = topics
    catalog = KafkaTopicCatalog(
        source.source_config.confluent_catalog, source.report, client=client
    )
    source.topic_catalog = catalog
    return catalog


def make_catalog(
    topics: List[CatalogKafkaTopic],
    report: KafkaSourceReport,
    **catalog_overrides: object,
) -> KafkaTopicCatalog:
    config = make_source_config(
        enabled_catalog_config(**catalog_overrides)
    ).confluent_catalog

    client = Mock(spec=ConfluentStreamCatalogClient)
    client.fetch_entities.return_value = topics
    return KafkaTopicCatalog(config, report, client=client)


def aspects_of(workunits: List[MetadataWorkUnit], aspect_name: str) -> List[object]:
    return [
        wu.metadata.aspect
        for wu in workunits
        if hasattr(wu.metadata, "aspectName")
        and wu.metadata.aspectName == aspect_name
        and hasattr(wu.metadata, "aspect")
    ]


class TestCatalogConnectionInheritance:
    def test_catalog_is_disabled_by_default(self) -> None:
        assert not make_source_config().confluent_catalog.enabled

    def test_credentials_are_inherited_from_the_schema_registry_connection(
        self,
    ) -> None:
        config = make_source_config(
            catalog={"enabled": True},
            schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
        )

        catalog = config.confluent_catalog
        assert catalog.schema_registry_url == CONFLUENT_SCHEMA_REGISTRY_URL
        assert catalog.get_credentials() == ("sr-key", "sr-secret")

    def test_explicit_catalog_credentials_win_over_inherited_ones(self) -> None:
        config = make_source_config(
            catalog={
                "enabled": True,
                "schema_registry_url": "https://psrc-other.aws.confluent.cloud",
                "api_key": "explicit-key",
                "api_secret": "explicit-secret",
            },
            schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
        )

        catalog = config.confluent_catalog
        assert catalog.schema_registry_url == "https://psrc-other.aws.confluent.cloud"
        assert catalog.get_credentials() == ("explicit-key", "explicit-secret")

    def test_enabling_without_any_credentials_is_rejected(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            make_source_config(catalog={"enabled": True})

        message = str(exc_info.value)
        assert "confluent_catalog.api_key" in message
        assert "confluent_catalog.api_secret" in message


class TestTopicLookup:
    def test_topic_metadata_is_fetched_once_and_reused(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog([CatalogKafkaTopic(name=TOPIC)], report)

        assert catalog.get_topic(TOPIC) is not None
        assert catalog.get_topic(TOPIC) is not None
        client = catalog.client
        assert isinstance(client, Mock)
        assert client.fetch_entities.call_count == 1
        assert report.catalog_topics_fetched == 1

    def test_unknown_topic_returns_nothing(self) -> None:
        catalog = make_catalog([CatalogKafkaTopic(name=TOPIC)], KafkaSourceReport())

        assert catalog.get_topic("payments") is None

    def test_topic_name_in_two_clusters_is_skipped_and_reported(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [
                CatalogKafkaTopic(name=TOPIC, clusterId="lkc-111"),
                CatalogKafkaTopic(name=TOPIC, clusterId="lkc-222"),
            ],
            report,
        )

        assert catalog.get_topic(TOPIC) is None
        assert len(report.warnings) == 1

    def test_cluster_id_disambiguates_repeated_topic_names(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [
                CatalogKafkaTopic(name=TOPIC, clusterId="lkc-111", tags=["pii"]),
                CatalogKafkaTopic(name=TOPIC, clusterId="lkc-222", tags=["public"]),
            ],
            report,
            cluster_id="lkc-222",
        )

        topic = catalog.get_topic(TOPIC)
        assert topic is not None
        assert topic.tags == ["public"]
        assert not report.warnings


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
class TestCatalogMetadataOnTopics:
    def build_source(
        self,
        mock_kafka: Mock,
        topics: List[CatalogKafkaTopic],
        **catalog_overrides: object,
    ) -> KafkaSource:
        cluster_metadata = MagicMock()
        cluster_metadata.topics = {TOPIC: None}
        mock_kafka.return_value.list_topics.return_value = cluster_metadata

        source = KafkaSource(
            make_source_config(
                catalog=enabled_catalog_config(**catalog_overrides),
                schema_registry_url=SCHEMA_REGISTRY_URL,
            ),
            PipelineContext(run_id="test"),
        )
        attach_catalog(source, topics)
        return source

    def test_tags_and_business_metadata_land_on_the_topic(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = self.build_source(
            mock_kafka,
            [
                CatalogKafkaTopic(
                    name=TOPIC,
                    tags=["PII"],
                    business_metadata=[{"name": "owning_team", "value": "payments"}],  # type: ignore[list-item]
                )
            ],
        )

        workunits = list(source.get_workunits())

        tags = aspects_of(workunits, "globalTags")
        assert any(
            isinstance(aspect, GlobalTagsClass)
            and [tag.tag for tag in aspect.tags] == ["urn:li:tag:PII"]
            for aspect in tags
        )
        properties = aspects_of(workunits, "datasetProperties")
        assert any(
            isinstance(aspect, DatasetPropertiesClass)
            and aspect.customProperties.get("owning_team") == "payments"
            for aspect in properties
        )
        assert source.report.catalog_tagged_topics == 1
        assert source.report.catalog_topics_with_business_metadata == 1

    def test_toggles_suppress_each_kind_of_metadata(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = self.build_source(
            mock_kafka,
            [
                CatalogKafkaTopic(
                    name=TOPIC,
                    tags=["PII"],
                    business_metadata=[{"name": "owning_team", "value": "payments"}],  # type: ignore[list-item]
                )
            ],
            include_tags=False,
            include_business_metadata=False,
        )

        workunits = list(source.get_workunits())

        assert not aspects_of(workunits, "globalTags")
        properties = aspects_of(workunits, "datasetProperties")
        assert all(
            isinstance(aspect, DatasetPropertiesClass)
            and "owning_team" not in aspect.customProperties
            for aspect in properties
        )
        assert source.report.catalog_tagged_topics == 0

    def test_topics_absent_from_the_catalog_are_left_alone(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = self.build_source(
            mock_kafka, [CatalogKafkaTopic(name="some_other_topic", tags=["PII"])]
        )

        workunits = list(source.get_workunits())

        assert not aspects_of(workunits, "globalTags")
        assert source.report.catalog_tagged_topics == 0
