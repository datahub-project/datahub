from typing import Dict, List, Optional, Tuple
from unittest.mock import MagicMock, Mock, patch

import pytest
from pydantic import ValidationError

from datahub.configuration.common import ConfigurationWarning
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluent.client import (
    CatalogFetchResult,
    ConfluentStreamCatalogClient,
)
from datahub.ingestion.source.kafka.confluent_catalog import (
    CatalogKafkaTopic,
    KafkaTopicCatalog,
)
from datahub.ingestion.source.kafka.kafka import KafkaSource, KafkaSourceConfig
from datahub.ingestion.source.kafka.kafka_report import KafkaSourceReport
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    GlobalTagsClass,
    UpstreamLineageClass,
)

CONFLUENT_SCHEMA_REGISTRY_URL = "https://psrc-abc123.us-east-1.aws.confluent.cloud"
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
    platform_instance: Optional[str] = None,
) -> KafkaSourceConfig:
    return KafkaSourceConfig.model_validate(
        {
            "platform_instance": platform_instance,
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
    source: KafkaSource, topics: List[CatalogKafkaTopic], complete: bool = True
) -> KafkaTopicCatalog:
    client = Mock(spec=ConfluentStreamCatalogClient)
    client.fetch_entities.return_value = CatalogFetchResult(topics, complete=complete)
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
    client.fetch_entities.return_value = CatalogFetchResult(topics, complete=True)
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

    def test_partial_catalog_credentials_inherit_the_missing_side(self) -> None:
        config = make_source_config(
            catalog={
                "enabled": True,
                "api_key": "explicit-key",
            },
            schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
        )

        assert config.confluent_catalog.get_credentials() == (
            "explicit-key",
            "sr-secret",
        )

        config = make_source_config(
            catalog={
                "enabled": True,
                "api_secret": "explicit-secret",
            },
            schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
        )

        assert config.confluent_catalog.get_credentials() == (
            "sr-key",
            "explicit-secret",
        )

    def test_enabling_without_any_credentials_is_rejected(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            make_source_config(catalog={"enabled": True})

        message = str(exc_info.value)
        assert "confluent_catalog.api_key" in message
        assert "confluent_catalog.api_secret" in message

    def test_inherited_schema_registry_url_must_be_http(self) -> None:
        with pytest.raises(ValueError, match="schema_registry_url"):
            make_source_config(
                catalog={"enabled": True},
                schema_registry_url="psrc-missing-scheme.confluent.cloud",
                schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
            )


class TestCatalogCredentialMasking:
    def setup_method(self) -> None:
        from datahub.masking.bootstrap import initialize_secret_masking
        from datahub.masking.secret_registry import SecretRegistry

        SecretRegistry.reset_instance()
        initialize_secret_masking()

    def teardown_method(self) -> None:
        from datahub.masking.bootstrap import shutdown_secret_masking
        from datahub.masking.secret_registry import SecretRegistry

        shutdown_secret_masking()
        SecretRegistry.reset_instance()

    def test_inherited_basic_auth_credentials_are_registered_for_masking(self) -> None:
        from datahub.masking.secret_registry import SecretRegistry

        make_source_config(
            catalog={"enabled": True},
            schema_registry_config={"basic.auth.user.info": "sr-key:sr-secret"},
        )

        # basic.auth.user.info lives in a plain Dict the ConfigModel secret walker
        # never sees, so the after-validator must register the inherited halves by hand.
        registry = SecretRegistry.get_instance()
        assert registry.get_secret_value("confluent_catalog.api_secret") == "sr-secret"


class TestTopicLookup:
    def test_topic_metadata_is_fetched_once_and_reused(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog([CatalogKafkaTopic(name=TOPIC)], report)

        assert catalog.get_topic(TOPIC) is not None
        assert catalog.get_topic(TOPIC) is not None
        client = catalog.client
        assert isinstance(client, Mock)
        assert client.fetch_entities.call_count == 1
        assert report.catalog_topics_indexed == 1

    def test_unknown_topic_returns_nothing(self) -> None:
        catalog = make_catalog([CatalogKafkaTopic(name=TOPIC)], KafkaSourceReport())

        assert catalog.get_topic("payments") is None

    def test_topic_name_in_two_clusters_is_skipped_and_reported(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [
                CatalogKafkaTopic(name=TOPIC, logical_cluster_id="lkc-111"),
                CatalogKafkaTopic(name=TOPIC, logical_cluster_id="lkc-222"),
            ],
            report,
        )

        assert catalog.get_topic(TOPIC) is None
        assert len(report.warnings) == 1

    def test_cluster_id_disambiguates_repeated_topic_names(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [
                CatalogKafkaTopic(
                    name=TOPIC, logical_cluster_id="lkc-111", tags=["pii"]
                ),
                CatalogKafkaTopic(
                    name=TOPIC, logical_cluster_id="lkc-222", tags=["public"]
                ),
            ],
            report,
            cluster_id="lkc-222",
        )

        topic = catalog.get_topic(TOPIC)
        assert topic is not None
        assert topic.tags == ["public"]
        assert not report.warnings

    def test_cluster_id_is_read_when_the_catalog_is_first_fetched(self) -> None:
        catalog = make_catalog(
            [
                CatalogKafkaTopic(name=TOPIC, logical_cluster_id="lkc-111"),
                CatalogKafkaTopic(name=TOPIC, logical_cluster_id="lkc-222"),
            ],
            KafkaSourceReport(),
        )
        catalog.config.cluster_id = "lkc-222"

        topic = catalog.get_topic(TOPIC)
        assert topic is not None and topic.cluster_id == "lkc-222"

    def test_cluster_id_matching_nothing_is_reported(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [CatalogKafkaTopic(name=TOPIC, logical_cluster_id="lkc-111")],
            report,
            cluster_id="lkc-999",
        )

        assert catalog.get_topic(TOPIC) is None
        assert report.catalog_topics_indexed == 0
        assert len(report.warnings) == 1

    def test_case_variant_topic_names_are_indexed_separately(self) -> None:
        report = KafkaSourceReport()
        catalog = make_catalog(
            [
                CatalogKafkaTopic(name="Orders", logical_cluster_id="lkc-111"),
                CatalogKafkaTopic(name="orders", logical_cluster_id="lkc-222"),
            ],
            report,
        )

        assert catalog.get_topic("Orders") is not None
        assert catalog.get_topic("orders") is not None
        assert catalog.get_topic("ORDERS") is None
        assert report.catalog_topics_indexed == 2
        assert not report.warnings


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
class TestCatalogMetadataOnTopics:
    def build_source(
        self,
        mock_kafka: Mock,
        topics: List[CatalogKafkaTopic],
        topic_detail: Optional[object] = None,
        catalog_complete: bool = True,
        **catalog_overrides: object,
    ) -> KafkaSource:
        cluster_metadata = MagicMock()
        cluster_metadata.topics = {TOPIC: topic_detail}
        mock_kafka.return_value.list_topics.return_value = cluster_metadata

        source = KafkaSource(
            make_source_config(
                catalog=enabled_catalog_config(**catalog_overrides),
                schema_registry_url=SCHEMA_REGISTRY_URL,
            ),
            PipelineContext(run_id="test"),
        )
        attach_catalog(source, topics, complete=catalog_complete)
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

    def test_a_repeated_tag_is_emitted_once(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = self.build_source(
            mock_kafka, [CatalogKafkaTopic(name=TOPIC, tags=["PII", "PII", "Tier1"])]
        )

        workunits = list(source.get_workunits())

        tags = aspects_of(workunits, "globalTags")
        assert any(
            isinstance(aspect, GlobalTagsClass)
            and [tag.tag for tag in aspect.tags]
            == ["urn:li:tag:PII", "urn:li:tag:Tier1"]
            for aspect in tags
        )

    def test_business_metadata_never_overwrites_a_broker_property(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        partition = MagicMock()
        partition.replicas = [0, 1]
        topic_detail = MagicMock()
        topic_detail.partitions = {0: partition}

        source = self.build_source(
            mock_kafka,
            [
                CatalogKafkaTopic(
                    name=TOPIC,
                    business_metadata=[  # type: ignore[list-item]
                        {"name": "Partitions", "value": "999"},
                        {"name": "Governance.owning_team", "value": "payments"},
                    ],
                )
            ],
            topic_detail=topic_detail,
        )

        workunits = list(source.get_workunits())

        properties = [
            aspect
            for aspect in aspects_of(workunits, "datasetProperties")
            if isinstance(aspect, DatasetPropertiesClass)
        ]
        assert any(
            aspect.customProperties.get("Partitions") == "1"
            and aspect.customProperties.get("Governance.owning_team") == "payments"
            for aspect in properties
        )
        assert any(
            "Partitions" in warning.context[0] for warning in source.report.warnings
        )

    def test_partial_catalog_warns_once_but_still_applies_found_tags(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source = self.build_source(
            mock_kafka,
            [CatalogKafkaTopic(name=TOPIC, tags=["PII"])],
            catalog_complete=False,
        )

        workunits = list(source.get_workunits())

        tags = aspects_of(workunits, "globalTags")
        assert any(
            isinstance(aspect, GlobalTagsClass)
            and [tag.tag for tag in aspect.tags] == ["urn:li:tag:PII"]
            for aspect in tags
        )
        partial_warnings = [
            warning
            for warning in source.report.warnings
            if "only partially read" in warning.message
        ]
        assert len(partial_warnings) == 1

    def test_non_confluent_cloud_endpoint_skips_the_catalog(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        cluster_metadata = MagicMock()
        cluster_metadata.topics = {TOPIC: None}
        mock_kafka.return_value.list_topics.return_value = cluster_metadata

        source = KafkaSource(
            make_source_config(
                catalog=enabled_catalog_config(
                    schema_registry_url="https://schema-registry.internal.example.com"
                ),
                schema_registry_url=SCHEMA_REGISTRY_URL,
            ),
            PipelineContext(run_id="test"),
        )

        assert source.topic_catalog is None
        assert any(
            "Confluent Cloud only" in warning.message
            for warning in source.report.warnings
        )


def mirror_of(source_name: str, source_cluster: Optional[str]) -> CatalogKafkaTopic:
    return CatalogKafkaTopic.model_validate(
        {
            "name": TOPIC,
            "logical_cluster_id": "lkc-dest",
            "source_topic": {
                "name": source_name,
                "logical_cluster_id": source_cluster,
            },
        }
    )

    @pytest.mark.parametrize(
        "source_topic, expected_name",
        [
            (None, None),
            ([], None),
            ({"name": "orders_origin"}, "orders_origin"),
            ([{"name": "orders_origin"}], "orders_origin"),
            ([{"name": "orders_origin"}, {"name": "ignored_second"}], "orders_origin"),
            (["orders_origin"], None),
        ],
    )
    def test_source_topic_accepts_object_list_and_null_shapes(
        self, source_topic: object, expected_name: Optional[str]
    ) -> None:
        # The live shape is unconfirmed: a validation failure here would drop the
        # topic and its tags, so every plausible shape must parse.
        topic = CatalogKafkaTopic.model_validate(
            {"name": TOPIC, "source_topic": source_topic}
        )

        if expected_name is None:
            assert topic.source_topic is None
        else:
            assert topic.source_topic is not None
            assert topic.source_topic.name == expected_name


class TestTopicCatalogQuery:
    def sent_query(self, **catalog_overrides: object) -> str:
        catalog = make_catalog([], KafkaSourceReport(), **catalog_overrides)
        catalog.get_topic(TOPIC)
        assert isinstance(catalog.client, Mock)
        return catalog.client.fetch_entities.call_args.args[0]

    def test_mirror_fields_are_only_requested_when_lineage_is_enabled(self) -> None:
        without_lineage = self.sent_query()
        with_lineage = self.sent_query(include_lineage=True, cluster_id="lkc-1")

        for query in (without_lineage, with_lineage):
            assert "tags" in query and "business_metadata" in query
            assert "{mirror_fields}" not in query
        assert "source_topic" not in without_lineage
        assert "source_topic" in with_lineage


@patch("datahub.ingestion.source.kafka.kafka.confluent_kafka.Consumer", autospec=True)
class TestCatalogLineage:
    def run(
        self,
        mock_kafka: Mock,
        topics: List[CatalogKafkaTopic],
        platform_instance: Optional[str] = "dest",
        complete: bool = True,
        **catalog_overrides: object,
    ) -> Tuple[KafkaSource, List[UpstreamLineageClass]]:
        cluster_metadata = MagicMock()
        cluster_metadata.topics = {TOPIC: None}
        mock_kafka.return_value.list_topics.return_value = cluster_metadata

        source = KafkaSource(
            make_source_config(
                catalog=enabled_catalog_config(**catalog_overrides),
                schema_registry_url=SCHEMA_REGISTRY_URL,
                platform_instance=platform_instance,
            ),
            PipelineContext(run_id="test"),
        )
        attach_catalog(source, topics, complete=complete)
        lineage = [
            aspect
            for aspect in aspects_of(list(source.get_workunits()), "upstreamLineage")
            if isinstance(aspect, UpstreamLineageClass)
        ]
        return source, lineage

    def test_mapped_source_cluster_gets_an_edge_in_its_platform_instance(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, "lkc-src")],
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-src": "src"},
        )

        assert [u.dataset for u in lineage[0].upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:kafka,src.orders,PROD)"
        ]
        assert source.report.catalog_mirror_lineage_edges == 1

    def test_unmapped_source_cluster_is_skipped_and_reported(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, "lkc-src")],
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-other": "other"},
        )

        assert not lineage
        assert list(source.report.catalog_mirror_lineage_skipped) == [TOPIC]
        assert source.report.warnings

    def test_source_without_a_cluster_id_is_skipped_and_reported(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, None)],
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-src": "src"},
        )

        assert not lineage
        assert list(source.report.catalog_mirror_lineage_skipped) == [TOPIC]

    def test_source_in_this_recipes_cluster_uses_its_platform_instance(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        topic = CatalogKafkaTopic.model_validate(
            {
                "name": TOPIC,
                "logical_cluster_id": "lkc-dest",
                "source_topic": {
                    "name": "src.orders",
                    "logical_cluster_id": "lkc-dest",
                },
            }
        )

        _, lineage = self.run(
            mock_kafka, [topic], include_lineage=True, cluster_id="lkc-dest"
        )

        assert [u.dataset for u in lineage[0].upstreams] == [
            "urn:li:dataset:(urn:li:dataPlatform:kafka,dest.src.orders,PROD)"
        ]

    def test_partial_catalog_without_cluster_id_emits_no_mirror_lineage(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, "lkc-src")],
            complete=False,
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-src": "src"},
        )

        assert not lineage
        assert list(source.report.catalog_mirror_lineage_skipped) == [TOPIC]

    def test_edge_back_to_the_topic_itself_is_skipped(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        # Both clusters ingested without a platform instance: the mirror keeps the
        # source name, so the "upstream" URN is the topic's own URN.
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, "lkc-src")],
            platform_instance=None,
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-src": ""},
        )

        assert not lineage
        assert list(source.report.catalog_mirror_lineage_skipped) == [TOPIC]

    def test_lineage_is_off_by_default(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [mirror_of(TOPIC, "lkc-src")],
            cluster_id_to_platform_instance={"lkc-src": "src"},
        )

        assert not lineage
        assert source.report.catalog_mirror_lineage_edges == 0

    def test_topic_without_a_mirror_source_emits_no_lineage(
        self, mock_kafka: Mock, mock_admin_client: Mock
    ) -> None:
        source, lineage = self.run(
            mock_kafka,
            [CatalogKafkaTopic(name=TOPIC)],
            include_lineage=True,
            cluster_id_to_platform_instance={"lkc-src": "src"},
        )

        assert not lineage
        assert not source.report.catalog_mirror_lineage_skipped


def test_mirror_lineage_without_any_cluster_mapping_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_source_config(catalog=enabled_catalog_config(include_lineage=True))


def test_mirror_lineage_with_the_catalog_disabled_warns() -> None:
    with pytest.warns(ConfigurationWarning):
        make_source_config(catalog={"enabled": False, "include_lineage": True})


def test_catalog_close_releases_the_client() -> None:
    catalog = make_catalog([], KafkaSourceReport())

    catalog.close()

    assert isinstance(catalog.client, Mock)
    catalog.client.close.assert_called_once()
