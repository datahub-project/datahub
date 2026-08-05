from typing import Dict, Mapping, Optional, Sequence, Set, Type
from unittest.mock import Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.kafka.kafka_config import KafkaConfluentCatalogConfig
from datahub.ingestion.source.kafka_connect.common import (
    KAFKA,
    ConfluentCatalogConfig,
    ConnectorManifest,
    KafkaConnectLineage,
    KafkaConnectSourceConfig,
    KafkaConnectSourceReport,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog import (
    CatalogConnector,
    ConnectorCatalog,
)
from datahub.ingestion.source.kafka_connect.confluent_catalog_constants import (
    LINEAGE_SOURCE_CATALOG,
    LINEAGE_SOURCE_PROPERTY,
)
from datahub.ingestion.source.kafka_connect.kafka_connect import KafkaConnectSource
from datahub.metadata.schema_classes import GlobalTagsClass

REGISTRY_LOOKUP = (
    "datahub.ingestion.source.kafka_connect.connector_registry."
    "ConnectorRegistry.get_connector_for_manifest"
)

SOURCE_CONNECTOR_CONFIG = {
    "connector.class": "PostgresCdcSource",
    "database.dbname": "ecommerce",
    "database.server.name": "pg_cdc",
    "table.include.list": "public.orders",
}

SINK_CONNECTOR_CONFIG = {
    "connector.class": "PostgresSink",
    "topics": "orders",
}


def make_catalog_config(**overrides: object) -> ConfluentCatalogConfig:
    defaults: Dict[str, object] = {
        "enabled": True,
        "schema_registry_url": "https://psrc-abc123.us-east-1.aws.confluent.cloud",
        "api_key": "sr-key",
        "api_secret": "sr-secret",
        "include_lineage": True,
    }
    defaults.update(overrides)
    return ConfluentCatalogConfig(**defaults)  # type: ignore[arg-type]


def make_catalog(
    connectors: Sequence[Mapping[str, object]], **config_overrides: object
) -> ConnectorCatalog:
    client = Mock(spec=ConfluentStreamCatalogClient)
    client.fetch_entities.return_value = [
        CatalogConnector.model_validate(payload) for payload in connectors
    ]
    return ConnectorCatalog(
        make_catalog_config(**config_overrides),
        KafkaConnectSourceReport(),
        client=client,
    )


def as_wrapper(workunit: MetadataWorkUnit) -> MetadataChangeProposalWrapper:
    metadata = workunit.metadata
    assert isinstance(metadata, MetadataChangeProposalWrapper)
    return metadata


def make_cloud_source(
    catalog_connectors: Sequence[Mapping[str, object]] = (),
    **catalog_overrides: object,
) -> KafkaConnectSource:
    with patch("requests.Session.get") as mock_get:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = []
        mock_get.return_value = response

        config = KafkaConnectSourceConfig(
            confluent_cloud_environment_id="env-abc123",
            confluent_cloud_cluster_id="lkc-abc123",
            username="connect-key",
            password="connect-secret",
            use_schema_resolver=False,
            confluent_catalog=make_catalog_config(**catalog_overrides),
        )
        source = KafkaConnectSource(config, Mock())

    catalog = make_catalog(catalog_connectors, **catalog_overrides)
    catalog.report = source.report
    source._catalog = catalog
    return source


def make_manifest(
    name: str = "source_postgres_cdc_01",
    connector_type: str = "source",
    config: Optional[Dict[str, str]] = None,
) -> ConnectorManifest:
    return ConnectorManifest(
        name=name,
        type=connector_type,
        config=dict(config if config is not None else SOURCE_CONNECTOR_CONFIG),
        tasks=[],
    )


class TestConfluentCatalogConfig:
    def test_disabled_config_needs_no_credentials(self) -> None:
        config = ConfluentCatalogConfig()
        assert not config.enabled

    def test_catalog_lineage_is_opt_in(self) -> None:
        config = ConfluentCatalogConfig(
            enabled=True,
            schema_registry_url="https://psrc-abc123.aws.confluent.cloud",
            api_key="sr-key",
            api_secret="sr-secret",
        )
        assert not config.include_lineage
        assert config.include_tags
        assert config.include_business_metadata

    def test_enabled_without_credentials_is_rejected(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            ConfluentCatalogConfig(enabled=True)

        message = str(exc_info.value)
        assert "confluent_catalog.schema_registry_url" in message
        assert "confluent_catalog.api_key" in message
        assert "confluent_catalog.api_secret" in message

    def test_enabled_with_partial_credentials_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            ConfluentCatalogConfig(
                enabled=True,
                schema_registry_url="https://psrc-abc123.aws.confluent.cloud",
                api_key="sr-key",
            )

    def test_source_config_defaults_to_disabled_catalog(self) -> None:
        config = KafkaConnectSourceConfig(connect_uri="http://localhost:8083")
        assert not config.confluent_catalog.enabled

    def test_known_catalog_config_subclasses_have_not_changed(self) -> None:
        # Subclasses that skip validate_connection fail later in the client —
        # adding one has to be deliberate.
        known: Set[Type[ConfluentStreamCatalogConfig]] = {
            ConfluentCatalogConfig,
            KafkaConfluentCatalogConfig,
        }
        assert set(ConfluentStreamCatalogConfig.__subclasses__()) == known


class TestConnectorCatalog:
    def test_parses_connector_with_null_collections(self) -> None:
        catalog = make_catalog(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "qualifiedName": "lcc-111",
                    "tags": None,
                    "business_metadata": None,
                    "topics": [
                        {
                            "name": "orders",
                            "qualifiedName": "lkc-abc123:orders",
                            "tags": ["pii"],
                            "business_metadata": [{"name": "team", "value": "core"}],
                        }
                    ],
                }
            ]
        )

        connector = catalog.get_connector("source_postgres_cdc_01")

        assert connector is not None
        assert connector.tags == []
        assert connector.business_metadata == []
        assert connector.get_topic_names() == ["orders"]
        assert connector.topics[0].tags == ["pii"]

    def test_null_topics_are_tolerated(self) -> None:
        catalog = make_catalog([{"name": "c1", "topics": None}])

        connector = catalog.get_connector("c1")
        assert connector is not None
        assert connector.get_topic_names() == []

    def test_duplicate_topics_are_collapsed(self) -> None:
        catalog = make_catalog(
            [
                {
                    "name": "c1",
                    "topics": [
                        {"name": "orders"},
                        {"name": "orders"},
                        {"name": "payments"},
                    ],
                }
            ]
        )

        connector = catalog.get_connector("c1")
        assert connector is not None
        assert connector.get_topic_names() == ["orders", "payments"]

    def test_lookup_tolerates_case_differences(self) -> None:
        catalog = make_catalog([{"name": "Source_Postgres_01"}])

        assert catalog.get_connector("source_postgres_01") is not None

    def test_repeated_connector_name_is_skipped_and_reported(self) -> None:
        catalog = make_catalog(
            [
                {"name": "source_postgres_01", "topics": [{"name": "orders"}]},
                {"name": "source_postgres_01", "topics": [{"name": "payments"}]},
            ]
        )

        assert catalog.get_connector("source_postgres_01") is None
        assert catalog.report.catalog_connectors_fetched == 0
        assert len(catalog.report.warnings) == 1

    def test_case_variant_connector_names_block_insensitive_lookup(self) -> None:
        catalog = make_catalog(
            [
                {"name": "Source_Postgres_01", "topics": [{"name": "orders"}]},
                {"name": "source_postgres_01", "topics": [{"name": "payments"}]},
            ]
        )

        assert catalog.get_connector("Source_Postgres_01") is not None
        assert catalog.get_connector("source_postgres_01") is not None
        assert catalog.get_connector("SOURCE_POSTGRES_01") is None
        assert catalog.report.catalog_connectors_fetched == 2
        assert any(
            "Case-insensitive Stream Catalog connector lookup is disabled"
            in warning.message
            for warning in catalog.report.warnings
        )

    def test_connectors_are_fetched_once_per_run(self) -> None:
        catalog = make_catalog([{"name": "c1"}])

        catalog.get_connector("c1")
        catalog.get_connector("c1")
        catalog.get_connectors()

        client = catalog.client
        assert isinstance(client, Mock)
        assert client.fetch_entities.call_count == 1
        assert catalog.report.catalog_connectors_fetched == 1


class TestCatalogLineage:
    def test_source_connector_uses_catalog_topics(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "type": "SOURCE",
                    "topics": [{"name": "orders"}, {"name": "payments"}],
                }
            ]
        )
        manifest = make_manifest()

        assert source.extract_connector_lineages(manifest)

        assert [lineage.target_dataset for lineage in manifest.lineages] == [
            "orders",
            "payments",
        ]
        assert all(lineage.source_dataset is None for lineage in manifest.lineages)
        assert all(lineage.target_platform == "kafka" for lineage in manifest.lineages)
        assert manifest.lineages[0].job_property_bag == {
            LINEAGE_SOURCE_PROPERTY: LINEAGE_SOURCE_CATALOG
        }
        assert source.report.catalog_lineage_connectors == 1

    def test_source_connector_without_registry_handler_still_gets_lineage(self) -> None:
        source = make_cloud_source(
            [{"name": "exotic_source", "topics": [{"name": "orders"}]}]
        )
        manifest = make_manifest(
            name="exotic_source", config={"connector.class": "SomeUnknownSource"}
        )

        assert source.extract_connector_lineages(manifest)
        assert [lineage.target_dataset for lineage in manifest.lineages] == ["orders"]

    def test_unknown_source_connector_without_catalog_topics_is_still_skipped(
        self,
    ) -> None:
        source = make_cloud_source()
        manifest = make_manifest(
            name="exotic_source", config={"connector.class": "SomeUnknownSource"}
        )

        assert not source.extract_connector_lineages(manifest)

    def test_missing_catalog_topics_falls_back_and_is_reported(self) -> None:
        source = make_cloud_source([{"name": "source_postgres_cdc_01", "topics": []}])
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = []
        registry_connector.extract_flow_property_bag.return_value = {}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            assert source.extract_connector_lineages(manifest)

        registry_connector.extract_lineages.assert_called_once()
        assert list(source.report.catalog_lineage_fallbacks) == [manifest.name]
        assert len(source.report.warnings) == 1

    def test_include_lineage_disabled_leaves_existing_path_alone(self) -> None:
        source = make_cloud_source(
            [{"name": "source_postgres_cdc_01", "topics": [{"name": "orders"}]}],
            include_lineage=False,
        )
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = []
        registry_connector.extract_flow_property_bag.return_value = {}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            source.extract_connector_lineages(manifest)

        registry_connector.extract_lineages.assert_called_once()
        assert source.report.catalog_lineage_connectors == 0

    def test_sink_connector_keeps_the_cluster_wide_candidate_topics(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "sink_postgres_01",
                    "type": "SINK",
                    "topics": [{"name": "orders"}],
                }
            ]
        )
        manifest = make_manifest(
            name="sink_postgres_01",
            connector_type="sink",
            config=SINK_CONNECTOR_CONFIG,
        )

        sink_lineage = KafkaConnectLineage(
            source_dataset="orders",
            source_platform=KAFKA,
            target_dataset="public.orders",
            target_platform="postgres",
        )
        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = [sink_lineage]
        registry_connector.extract_flow_property_bag.return_value = {}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(
                source,
                "_get_all_topics_from_kafka_api",
                return_value=["orders", "payments"],
            ),
        ):
            source.extract_connector_lineages(manifest)

        assert registry_connector.all_cluster_topics == ["orders", "payments"]
        assert manifest.lineages == [sink_lineage]
        registry_connector.extract_lineages.assert_called_once()

    def test_business_metadata_is_merged_into_flow_properties(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "topics": [{"name": "orders"}],
                    "business_metadata": [
                        {"name": "team", "value": "core"},
                        {"name": "critical", "value": True},
                        {"name": "unset", "value": None},
                    ],
                }
            ]
        )
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_flow_property_bag.return_value = {"tasks.max": "1"}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            source.extract_connector_lineages(manifest)

        assert manifest.flow_property_bag == {
            "tasks.max": "1",
            "team": "core",
            "critical": "True",
        }

    def test_catalog_business_metadata_does_not_overwrite_connector_config(
        self,
    ) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "business_metadata": [
                        {"name": "tasks.max", "value": "99"},
                        {"name": "team", "value": "core"},
                    ],
                }
            ]
        )
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = []
        registry_connector.extract_flow_property_bag.return_value = {"tasks.max": "1"}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            source.extract_connector_lineages(manifest)

        assert manifest.flow_property_bag == {
            "tasks.max": "1",
            "team": "core",
        }
        assert any(
            "collide with connector config" in warning.message
            for warning in source.report.warnings
        )

    def test_unsupported_sink_still_gets_catalog_business_metadata(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "exotic_sink",
                    "business_metadata": [{"name": "team", "value": "core"}],
                }
            ]
        )
        manifest = make_manifest(
            name="exotic_sink",
            connector_type="sink",
            config={"connector.class": "SomeUnknownSink"},
        )

        with (
            patch(REGISTRY_LOOKUP, return_value=None),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            assert source.extract_connector_lineages(manifest)

        assert manifest.lineages == []
        assert manifest.flow_property_bag == {"team": "core"}

    def test_catalog_lineage_drops_topics_absent_from_the_live_cluster(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "topics": [{"name": "orders"}, {"name": "stale_topic"}],
                }
            ]
        )
        manifest = make_manifest()

        with (
            patch(REGISTRY_LOOKUP, return_value=Mock()),
            patch.object(
                source,
                "_get_all_topics_from_kafka_api",
                return_value=["orders", "payments"],
            ),
        ):
            assert source.extract_connector_lineages(manifest)

        assert [lineage.target_dataset for lineage in manifest.lineages] == ["orders"]
        assert any(
            "not present on the live Kafka cluster" in warning.message
            for warning in source.report.warnings
        )


class TestCatalogTags:
    def test_connector_tags_are_emitted_on_the_data_flow(self) -> None:
        source = make_cloud_source(
            [{"name": "source_postgres_cdc_01", "tags": ["gold", "pii"]}]
        )

        workunits = list(source.construct_catalog_metadata_workunits(make_manifest()))

        assert len(workunits) == 1
        aspect = as_wrapper(workunits[0]).aspect
        assert isinstance(aspect, GlobalTagsClass)
        assert [tag.tag for tag in aspect.tags] == [
            "urn:li:tag:gold",
            "urn:li:tag:pii",
        ]
        assert source.report.catalog_tagged_flows == 1

    def test_topic_tags_are_left_to_the_kafka_source(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "topics": [{"name": "orders", "tags": ["pii"]}],
                }
            ]
        )

        workunits = list(source.construct_catalog_metadata_workunits(make_manifest()))

        assert workunits == []

    def test_tags_can_be_disabled(self) -> None:
        source = make_cloud_source(
            [{"name": "source_postgres_cdc_01", "tags": ["gold"]}], include_tags=False
        )

        assert list(source.construct_catalog_metadata_workunits(make_manifest())) == []

    def test_no_workunits_when_the_connector_is_absent_from_the_catalog(self) -> None:
        source = make_cloud_source()

        assert list(source.construct_catalog_metadata_workunits(make_manifest())) == []


class TestCatalogCreation:
    def test_catalog_is_skipped_for_self_hosted_deployments(self) -> None:
        with patch("requests.Session.get") as mock_get:
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = []
            mock_get.return_value = response

            config = KafkaConnectSourceConfig(
                connect_uri="http://localhost:8083",
                confluent_catalog=make_catalog_config(),
            )
            source = KafkaConnectSource(config, Mock())

        assert source._catalog is None
        assert len(source.report.warnings) == 1
