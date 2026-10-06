from typing import Dict, List, Mapping, Optional, Sequence, Set, Type
from unittest.mock import Mock, patch

import pytest
import requests
from requests.adapters import HTTPAdapter

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.confluent.client import (
    CatalogFetchResult,
    ConfluentStreamCatalogClient,
)
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
from datahub.ingestion.source.kafka_connect.sink_connectors import (
    BigQuerySinkConnector,
    ConfluentS3SinkConnector,
)
from datahub.ingestion.source.kafka_connect.source_connectors import (
    DebeziumSourceConnector,
)
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
    client.fetch_entities.return_value = CatalogFetchResult(
        [CatalogConnector.model_validate(payload) for payload in connectors],
        complete=True,
    )
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

    def test_lookup_is_case_sensitive(self) -> None:
        catalog = make_catalog([{"name": "Source_Postgres_01"}])

        assert catalog.get_connector("Source_Postgres_01") is not None
        assert catalog.get_connector("source_postgres_01") is None

    def test_repeated_connector_name_is_skipped_and_reported(self) -> None:
        catalog = make_catalog(
            [
                {"name": "source_postgres_01", "topics": [{"name": "orders"}]},
                {"name": "source_postgres_01", "topics": [{"name": "payments"}]},
            ]
        )

        assert catalog.get_connector("source_postgres_01") is None
        assert catalog.report.catalog_connectors_indexed == 0
        assert len(catalog.report.warnings) == 1

    def test_case_variant_connector_names_are_indexed_separately(self) -> None:
        catalog = make_catalog(
            [
                {"name": "Source_Postgres_01", "topics": [{"name": "orders"}]},
                {"name": "source_postgres_01", "topics": [{"name": "payments"}]},
            ]
        )

        assert catalog.get_connector("Source_Postgres_01") is not None
        assert catalog.get_connector("source_postgres_01") is not None
        assert catalog.get_connector("SOURCE_POSTGRES_01") is None
        assert catalog.report.catalog_connectors_indexed == 2
        assert not catalog.report.warnings

    def test_connectors_are_fetched_once_per_run(self) -> None:
        catalog = make_catalog([{"name": "c1"}])

        catalog.get_connector("c1")
        catalog.get_connector("c1")
        catalog.get_connectors()

        client = catalog.client
        assert isinstance(client, Mock)
        assert client.fetch_entities.call_count == 1
        assert catalog.report.catalog_connectors_indexed == 1


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

    def test_unknown_source_with_all_catalog_topics_filtered_is_still_skipped(
        self,
    ) -> None:
        # Catalog listed topics, but none are on the live cluster — no registry
        # handler either, so drop the connector rather than emit an empty flow.
        source = make_cloud_source(
            [{"name": "exotic_source", "topics": [{"name": "stale_topic"}]}]
        )
        manifest = make_manifest(
            name="exotic_source", config={"connector.class": "SomeUnknownSource"}
        )

        with patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]):
            assert not source.extract_connector_lineages(manifest)

        assert "exotic_source" in list(source.report.filtered)
        assert any(
            "Lineage for Source Connector not supported" in warning.message
            and any("exotic_source" in ctx for ctx in (warning.context or []))
            for warning in source.report.warnings
        )
        # A dropped connector has no fallback to suppress, so no drop warning.
        assert not any(
            "Dropping all lineage for a connector" in warning.message
            for warning in source.report.warnings
        )

    def test_missing_catalog_topics_falls_back_and_is_reported(self) -> None:
        source = make_cloud_source([{"name": "source_postgres_cdc_01", "topics": []}])
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = []
        registry_connector.extract_flow_property_bag.return_value = {}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
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
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
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
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
        ):
            source.extract_connector_lineages(manifest)

        assert manifest.flow_property_bag == {
            "tasks.max": "1",
            "team": "core",
            "critical": "True",
        }
        assert source.report.catalog_connectors_with_business_metadata == 1

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
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
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
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
        ):
            assert source.extract_connector_lineages(manifest)

        assert manifest.lineages == []
        assert manifest.flow_property_bag == {"team": "core"}
        assert source.report.catalog_connectors_with_business_metadata == 1

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

    def test_unavailable_live_topic_list_keeps_catalog_lineage(self) -> None:
        source = make_cloud_source(
            [
                {
                    "name": "source_postgres_cdc_01",
                    "topics": [{"name": "orders"}, {"name": "payments"}],
                }
            ]
        )
        manifest = make_manifest()

        with (
            patch(REGISTRY_LOOKUP, return_value=Mock()),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
        ):
            assert source.extract_connector_lineages(manifest)

        assert [lineage.target_dataset for lineage in manifest.lineages] == [
            "orders",
            "payments",
        ]

    def test_empty_live_topic_list_drops_all_catalog_lineage(self) -> None:
        source = make_cloud_source(
            [{"name": "source_postgres_cdc_01", "topics": [{"name": "orders"}]}]
        )
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = [
            KafkaConnectLineage(
                source_dataset="should-not-be-used",
                source_platform="postgres",
                target_dataset="should-not-be-used",
                target_platform=KAFKA,
            )
        ]
        registry_connector.extract_flow_property_bag.return_value = {}
        registry_connector.requires_cluster_topics.return_value = False

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
        ):
            assert source.extract_connector_lineages(manifest)

        registry_connector.extract_lineages.assert_not_called()
        assert manifest.lineages == []
        assert any(
            "not present on the live Kafka cluster" in warning.message
            for warning in source.report.warnings
        )
        assert any(
            "Dropping all lineage for a connector" in warning.message
            for warning in source.report.warnings
        )
        assert list(source.report.catalog_lineage_fallbacks) == []

    def test_empty_live_topic_list_does_not_fall_back_to_config_derivation(
        self,
    ) -> None:
        source = make_cloud_source()
        manifest = make_manifest()

        with (
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=[]),
            patch.object(
                source,
                "_get_topics_from_connector_config",
                return_value=["derived-from-config"],
            ) as derive,
        ):
            assert source._get_topics_confluent_cloud_from_manifest(manifest) == []

        derive.assert_not_called()

    def test_kafka_rest_failure_warns_without_catalog_wording(self) -> None:
        source = make_cloud_source()
        source._catalog = None
        source._all_kafka_topics_resolved = False
        source._all_kafka_topics_cache = None

        with patch.object(
            source, "_parse_confluent_cloud_info", return_value=(None, None)
        ):
            assert source._get_all_topics_from_kafka_api() is None

        assert any(
            warning.message
            == "Could not resolve the Kafka REST endpoint for the live-cluster topic list"
            for warning in source.report.warnings
        )
        assert not any(
            "Stream Catalog" in warning.message for warning in source.report.warnings
        )

    def test_kafka_rest_failure_warns_with_catalog_cross_check_note(self) -> None:
        source = make_cloud_source()
        source._all_kafka_topics_resolved = False
        source._all_kafka_topics_cache = None

        with patch.object(
            source, "_parse_confluent_cloud_info", return_value=(None, None)
        ):
            assert source._get_all_topics_from_kafka_api() is None

        assert any(
            "Stream Catalog lineage will not be cross-checked" in warning.message
            for warning in source.report.warnings
        )

    def test_kafka_rest_topic_list_is_fetched_once_per_run(self) -> None:
        source = make_cloud_source()
        source._catalog = None
        source._all_kafka_topics_resolved = False
        source._all_kafka_topics_cache = None

        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "kind": "KafkaTopicList",
            "data": [
                {"topic_name": "orders", "is_internal": False},
                {"topic_name": "__consumer_offsets", "is_internal": True},
            ],
        }

        with (
            patch.object(
                source,
                "_parse_confluent_cloud_info",
                return_value=("https://pkc.confluent.cloud", "lkc-abc123"),
            ),
            patch.object(
                source,
                "_get_kafka_auth_headers",
                return_value={"Authorization": "Basic abc"},
            ),
            patch.object(source.kafka_session, "get", return_value=response) as get,
        ):
            assert source._get_all_topics_from_kafka_api() == ["orders"]
            assert source._get_all_topics_from_kafka_api() == ["orders"]

        assert get.call_count == 1

    def test_connector_absent_from_catalog_does_not_claim_empty_topics(self) -> None:
        source = make_cloud_source()
        manifest = make_manifest()

        registry_connector = Mock()
        registry_connector.extract_lineages.return_value = []
        registry_connector.extract_flow_property_bag.return_value = {}

        with (
            patch(REGISTRY_LOOKUP, return_value=registry_connector),
            patch.object(source, "_get_all_topics_from_kafka_api", return_value=None),
        ):
            assert source.extract_connector_lineages(manifest)

        assert list(source.report.catalog_lineage_fallbacks) == []
        assert not any(
            "listed no topics" in warning.message for warning in source.report.warnings
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
    def test_catalog_is_skipped_for_non_confluent_cloud_registry(self) -> None:
        with patch("requests.Session.get") as mock_get:
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = []
            mock_get.return_value = response

            config = KafkaConnectSourceConfig(
                connect_uri="https://api.confluent.cloud/connect/v1/environments/env-1/clusters/lkc-1",
                confluent_cloud_environment_id="env-1",
                confluent_cloud_cluster_id="lkc-1",
                username="connect-key",
                password="connect-secret",
                use_schema_resolver=False,
                confluent_catalog=make_catalog_config(
                    schema_registry_url="https://schema-registry.internal.example.com",
                ),
            )
            source = KafkaConnectSource(config, Mock())

        assert source._catalog is None
        assert len(source.report.warnings) == 1
        assert "Schema Registry endpoint" in source.report.warnings[0].message


class TestAvailableTopicsNoneVsEmpty:
    def test_empty_cluster_list_does_not_fall_back_to_topic_names(self) -> None:
        manifest = make_manifest(
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            }
        )
        manifest.topic_names = ["should-not-be-used"]
        connector = DebeziumSourceConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = []

        assert connector.available_topics() == []

    def test_unavailable_cluster_list_falls_back_to_topic_names(self) -> None:
        manifest = make_manifest(
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.users",
            }
        )
        manifest.topic_names = ["myserver.public.users"]
        connector = DebeziumSourceConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = None

        assert connector.available_topics() == ["myserver.public.users"]


class TestDebeziumEventRouterTopicFiltering:
    def _make_event_router_connector(
        self,
        *,
        all_cluster_topics: Optional[List[str]],
        topic_names: Optional[List[str]] = None,
    ) -> DebeziumSourceConnector:
        manifest = make_manifest(
            name="outbox-cdc",
            config={
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "database.server.name": "myserver",
                "database.dbname": "mydb",
                "table.include.list": "public.outbox",
                "transforms": "outbox,route",
                "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": ".*",
                "transforms.route.replacement": "events.$1",
            },
        )
        if topic_names is not None:
            manifest.topic_names = topic_names
        connector = DebeziumSourceConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = all_cluster_topics
        return connector

    def test_has_event_router_transform(self) -> None:
        connector = self._make_event_router_connector(all_cluster_topics=None)
        assert connector._has_event_router_transform()

    def test_filter_topics_uses_empty_cluster_list_not_topic_names(self) -> None:
        connector = self._make_event_router_connector(
            all_cluster_topics=[],
            topic_names=["events.OrderCreated", "unrelated"],
        )

        assert connector._filter_topics_for_event_router() == []

    def test_filter_topics_keeps_regex_router_prefix_matches(self) -> None:
        connector = self._make_event_router_connector(
            all_cluster_topics=[
                "events.OrderCreated",
                "events.PaymentCaptured",
                "other.topic",
            ]
        )

        assert connector._filter_topics_for_event_router() == [
            "events.OrderCreated",
            "events.PaymentCaptured",
        ]

    def test_extract_lineages_for_event_router_uses_filtered_topics(self) -> None:
        connector = self._make_event_router_connector(
            all_cluster_topics=["events.OrderCreated", "other.topic"]
        )

        lineages = connector._extract_lineages_for_event_router("postgres", "mydb")

        assert [lineage.target_dataset for lineage in lineages] == [
            "events.OrderCreated"
        ]
        assert lineages[0].source_dataset == "mydb.public.outbox"


class TestParseConfluentCloudInfo:
    def test_explicit_endpoint_and_cluster_id(self) -> None:
        source = make_cloud_source()
        source.config.kafka_rest_endpoint = (
            "https://pkc-xyz.us-east-1.aws.confluent.cloud"
        )

        endpoint, cluster_id = source._parse_confluent_cloud_info()

        assert endpoint == "https://pkc-xyz.us-east-1.aws.confluent.cloud"
        assert cluster_id == "lkc-abc123"

    def test_derived_from_connector_configs(self) -> None:
        source = make_cloud_source()
        source.config.kafka_rest_endpoint = None

        with patch.object(
            source,
            "_derive_kafka_rest_endpoint_from_connectors",
            return_value="https://pkc-derived.confluent.cloud",
        ):
            endpoint, cluster_id = source._parse_confluent_cloud_info()

        assert endpoint == "https://pkc-derived.confluent.cloud"
        assert cluster_id == "lkc-abc123"

    def test_cluster_id_only_fallback(self) -> None:
        source = make_cloud_source()
        source.config.kafka_rest_endpoint = None

        with patch.object(
            source, "_derive_kafka_rest_endpoint_from_connectors", return_value=None
        ):
            endpoint, cluster_id = source._parse_confluent_cloud_info()

        assert endpoint is None
        assert cluster_id == "lkc-abc123"

    def test_total_failure_returns_none_pair(self) -> None:
        with patch("requests.Session.get") as mock_get:
            response = Mock()
            response.raise_for_status.return_value = None
            response.json.return_value = []
            mock_get.return_value = response

            config = KafkaConnectSourceConfig(
                connect_uri="https://connect.example.com",
                username="connect-key",
                password="connect-secret",
                use_schema_resolver=False,
                confluent_catalog=make_catalog_config(enabled=False),
            )
            source = KafkaConnectSource(config, Mock())

        source.config.kafka_rest_endpoint = None
        with patch.object(
            source, "_derive_kafka_rest_endpoint_from_connectors", return_value=None
        ):
            endpoint, cluster_id = source._parse_confluent_cloud_info()

        assert endpoint is None
        assert cluster_id is None

    def test_extract_cluster_id_from_connect_uri(self) -> None:
        source = make_cloud_source()
        assert source._extract_cluster_id_from_connect_uri() == "lkc-abc123"

    def test_convert_broker_to_rest_endpoint(self) -> None:
        source = make_cloud_source()
        assert (
            source._convert_broker_to_rest_endpoint(
                "SASL_SSL://pkc-abc.us-east-1.aws.confluent.cloud:9092"
            )
            == "https://pkc-abc.us-east-1.aws.confluent.cloud"
        )


class TestKafkaSessionRetryAdapter:
    def test_kafka_session_mounts_retry_adapter(self) -> None:
        source = make_cloud_source()
        https_adapter = source.kafka_session.get_adapter("https://example.com")
        assert isinstance(https_adapter, HTTPAdapter)
        assert https_adapter.max_retries.total == 2
        assert 429 in https_adapter.max_retries.status_forcelist
        assert 500 in https_adapter.max_retries.status_forcelist

    def test_fetch_failure_warns_and_returns_none(self) -> None:
        source = make_cloud_source()
        source._catalog = None
        source._all_kafka_topics_resolved = False
        source._all_kafka_topics_cache = None

        with (
            patch.object(
                source,
                "_parse_confluent_cloud_info",
                return_value=("https://pkc.confluent.cloud", "lkc-abc123"),
            ),
            patch.object(
                source,
                "_get_kafka_auth_headers",
                return_value={"Authorization": "Basic abc"},
            ),
            patch.object(
                source.kafka_session,
                "get",
                side_effect=requests.exceptions.ConnectionError("boom"),
            ),
        ):
            assert source._get_all_topics_from_kafka_api() is None

        assert any(
            warning.message == "Failed to get topics from the Kafka REST API"
            for warning in source.report.warnings
        )


class TestSinkAvailableTopicsMigration:
    def test_s3_regex_expands_against_cluster_topics_on_cloud(self) -> None:
        manifest = make_manifest(
            name="s3-sink",
            connector_type="sink",
            config={
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "s3.bucket.name": "my-bucket",
                "topics.regex": "orders.*",
            },
        )
        manifest.topic_names = []
        connector = ConfluentS3SinkConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = ["orders", "orders_dlq", "payments"]

        assert sorted(connector.get_topics_from_config()) == ["orders", "orders_dlq"]

    def test_empty_regex_match_does_not_fall_back_to_all_cluster_topics(self) -> None:
        manifest = make_manifest(
            name="s3-sink",
            connector_type="sink",
            config={
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "s3.bucket.name": "my-bucket",
                "topics.regex": "no_such_prefix.*",
            },
        )
        manifest.topic_names = []
        connector = ConfluentS3SinkConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = ["orders", "payments", "unrelated"]

        assert connector.get_topics_from_config() == []
        assert (
            connector._resolve_subscribed_topics(
                manifest, connector.get_topics_from_config()
            )
            == []
        )

    def test_authoritative_empty_cluster_resolves_to_no_topics(self) -> None:
        manifest = make_manifest(
            name="s3-sink",
            connector_type="sink",
            config={
                "connector.class": "io.confluent.connect.s3.S3SinkConnector",
                "s3.bucket.name": "my-bucket",
                "topics": "orders,payments",
            },
        )
        manifest.topic_names = []
        connector = ConfluentS3SinkConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        # [] (not None) is an authoritative empty cluster: the subscribed topics no
        # longer exist, so they must not fall through to the configured subscription.
        connector.all_cluster_topics = []

        assert (
            connector._resolve_subscribed_topics(manifest, ["orders", "payments"]) == []
        )

    def test_bigquery_lineage_uses_available_topics_not_empty_topic_names(
        self,
    ) -> None:
        manifest = make_manifest(
            name="bq-sink",
            connector_type="sink",
            config={
                "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
                "project": "my-project",
                "defaultDataset": "analytics",
                "topics": "orders,payments",
            },
        )
        manifest.topic_names = []
        connector = BigQuerySinkConnector(
            manifest, make_cloud_source().config, KafkaConnectSourceReport()
        )
        connector.all_cluster_topics = ["orders", "payments", "unrelated"]

        lineages = connector.extract_lineages()
        source_datasets: List[str] = []
        for lineage in lineages:
            assert lineage.source_dataset is not None
            source_datasets.append(lineage.source_dataset)
        assert sorted(source_datasets) == ["orders", "payments"]
