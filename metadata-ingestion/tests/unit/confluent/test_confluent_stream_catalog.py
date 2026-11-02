from typing import Dict, List, Optional, Sequence
from unittest.mock import Mock, patch

import pytest
import requests
from pydantic import Field

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.confluent.models import CatalogEntity, index_by_name

ROOT_KEY = "kafka_topic"
QUERY = "{ kafka_topic(limit: {limit}, offset: {offset}) { name } }"


class SampleEntity(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="logical_cluster_id")


def make_config(**overrides: object) -> ConfluentStreamCatalogConfig:
    defaults: Dict[str, object] = {
        "enabled": True,
        "schema_registry_url": "https://psrc-abc123.us-east-1.aws.confluent.cloud",
        "api_key": "sr-key",
        "api_secret": "sr-secret",
    }
    defaults.update(overrides)
    return ConfluentStreamCatalogConfig(**defaults)  # type: ignore[arg-type]


def make_response(entities: Optional[Sequence[object]]) -> Mock:
    response = Mock()
    response.raise_for_status.return_value = None
    response.json.return_value = {"data": {ROOT_KEY: entities}}
    return response


def make_client(
    responses: List[Mock], **config_overrides: object
) -> ConfluentStreamCatalogClient:
    session = Mock()
    session.post.side_effect = responses
    return ConfluentStreamCatalogClient(
        make_config(**config_overrides), SourceReport(), session=session
    )


def fetch(client: ConfluentStreamCatalogClient) -> List[SampleEntity]:
    return client.fetch_entities(QUERY, ROOT_KEY, SampleEntity).entities


class TestConfluentStreamCatalogConfig:
    def test_catalog_is_disabled_by_default(self) -> None:
        assert not ConfluentStreamCatalogConfig().enabled

    def test_disabled_config_skips_validation(self) -> None:
        config = ConfluentStreamCatalogConfig(enabled=False, page_size=0)
        assert config.page_size == 0

    def test_non_http_schema_registry_url_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must use HTTPS"):
            make_config(schema_registry_url="psrc-abc123.aws.confluent.cloud")

    def test_http_confluent_cloud_schema_registry_url_is_rejected(self) -> None:
        with pytest.raises(ValueError, match="must use HTTPS"):
            make_config(
                schema_registry_url="http://psrc-abc123.us-east-1.aws.confluent.cloud"
            )

    def test_http_schema_registry_url_is_rejected_for_all_endpoints(self) -> None:
        with pytest.raises(ValueError, match="must use HTTPS to protect credentials"):
            make_config(schema_registry_url="http://localhost:8081")

    @pytest.mark.parametrize("page_size", [0, 5000])
    def test_out_of_range_page_size_is_rejected(self, page_size: int) -> None:
        with pytest.raises(ValueError):
            make_config(page_size=page_size)

    def test_non_positive_timeout_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            make_config(timeout_seconds=0)

    def test_graphql_endpoint_is_derived_from_schema_registry_url(self) -> None:
        config = make_config(
            schema_registry_url="https://psrc-abc123.aws.confluent.cloud/"
        )

        assert (
            config.get_graphql_endpoint()
            == "https://psrc-abc123.aws.confluent.cloud/catalog/graphql"
        )

    def test_validate_connection_names_the_missing_fields(self) -> None:
        config = ConfluentStreamCatalogConfig(
            enabled=True, schema_registry_url="https://psrc-abc123.aws.confluent.cloud"
        )

        with pytest.raises(ValueError) as exc_info:
            config.validate_connection()

        message = str(exc_info.value)
        assert "confluent_catalog.api_key" in message
        assert "confluent_catalog.api_secret" in message
        assert "confluent_catalog.schema_registry_url" not in message

    def test_validate_connection_passes_when_complete(self) -> None:
        make_config().validate_connection()

    @pytest.mark.parametrize(
        "schema_registry_url,expected",
        [
            ("https://psrc-abc123.us-east-1.aws.confluent.cloud", True),
            ("https://schema-registry.internal.example.com", False),
        ],
    )
    def test_confluent_cloud_endpoints_are_recognised(
        self, schema_registry_url: str, expected: bool
    ) -> None:
        config = make_config(schema_registry_url=schema_registry_url)

        assert config.is_confluent_cloud_endpoint() is expected

    def test_localhost_http_is_not_confluent_cloud(self) -> None:
        config = ConfluentStreamCatalogConfig(
            enabled=False,
            schema_registry_url="http://localhost:8081",
        )
        assert config.is_confluent_cloud_endpoint() is False


class TestConfluentStreamCatalogClient:
    def test_entities_are_parsed_with_aliases_and_null_collections(self) -> None:
        client = make_client(
            [
                make_response(
                    [
                        {
                            "name": "orders",
                            "qualifiedName": "lkc-123:orders",
                            "logical_cluster_id": "lkc-123",
                            "tags": None,
                            "business_metadata": None,
                        }
                    ]
                )
            ]
        )

        entities = fetch(client)

        assert len(entities) == 1
        assert entities[0].qualified_name == "lkc-123:orders"
        assert entities[0].cluster_id == "lkc-123"
        assert entities[0].tags == []
        assert entities[0].business_metadata == []

    def test_pages_until_a_short_page_is_returned(self) -> None:
        client = make_client(
            [
                make_response([{"name": "topic_0"}, {"name": "topic_1"}]),
                make_response([{"name": "topic_2"}, {"name": "topic_3"}]),
                make_response([{"name": "topic_4"}]),
            ],
            page_size=2,
        )

        entities = fetch(client)

        assert [entity.name for entity in entities] == [
            "topic_0",
            "topic_1",
            "topic_2",
            "topic_3",
            "topic_4",
        ]
        session = client.session
        assert isinstance(session, Mock)
        assert [call.kwargs["json"] for call in session.post.call_args_list] == [
            {"query": "{ kafka_topic(limit: 2, offset: 0) { name } }"},
            {"query": "{ kafka_topic(limit: 2, offset: 2) { name } }"},
            {"query": "{ kafka_topic(limit: 2, offset: 4) { name } }"},
        ]

    def test_query_without_pagination_placeholders_is_a_failure(self) -> None:
        client = make_client([])

        result = client.fetch_entities(
            "{ kafka_topic(limit: 10) { name } }", ROOT_KEY, SampleEntity
        )
        assert result.entities == []
        assert result.complete is False
        session = client.session
        assert isinstance(session, Mock)
        assert session.post.call_count == 0
        assert len(client.report.failures) == 1
        assert not client.report.warnings

    def test_rejected_request_reports_the_server_response(self) -> None:
        response = Mock()
        response.text = "Validation error: Field 'clusterId' is undefined"
        response.raise_for_status.side_effect = requests.HTTPError(
            "400 Client Error", response=response
        )
        client = make_client([response])

        assert fetch(client) == []
        contexts = [
            context for warning in client.report.warnings for context in warning.context
        ]
        assert any("Field 'clusterId' is undefined" in context for context in contexts)

    @pytest.mark.parametrize("status", [401, 403])
    def test_rejected_credentials_are_a_failure_not_a_warning(
        self, status: int
    ) -> None:
        response = Mock()
        response.status_code = status
        response.text = "Forbidden"
        response.raise_for_status.side_effect = requests.HTTPError(
            f"{status} Client Error", response=response
        )
        client = make_client([response])

        result = client.fetch_entities(QUERY, ROOT_KEY, SampleEntity)

        assert result.entities == []
        assert result.complete is False
        assert not client.report.warnings
        assert len(client.report.failures) == 1
        assert any(
            "rejected the credentials" in failure.message
            for failure in client.report.failures
        )

    def test_a_fully_read_catalog_reports_complete(self) -> None:
        client = make_client(
            [make_response([{"name": "topic_0"}, {"name": "topic_1"}])],
            page_size=5,
        )

        result = client.fetch_entities(QUERY, ROOT_KEY, SampleEntity)

        assert [entity.name for entity in result.entities] == ["topic_0", "topic_1"]
        assert result.complete is True

    def test_a_partial_catalog_reports_incomplete(self) -> None:
        broken = Mock()
        broken.raise_for_status.side_effect = requests.ConnectionError("reset by peer")
        client = make_client(
            [make_response([{"name": "topic_0"}, {"name": "topic_1"}]), broken],
            page_size=2,
        )

        result = client.fetch_entities(QUERY, ROOT_KEY, SampleEntity)

        assert [entity.name for entity in result.entities] == ["topic_0", "topic_1"]
        assert result.complete is False

    def test_graphql_errors_are_reported_and_yield_nothing(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "errors": [{"message": "Stream Governance is not enabled"}]
        }
        client = make_client([response])

        assert fetch(client) == []
        assert len(client.report.warnings) == 1

    def test_http_failure_is_reported_and_yields_nothing(self) -> None:
        session = Mock()
        session.post.side_effect = requests.ConnectionError("connection refused")
        client = ConfluentStreamCatalogClient(
            make_config(), SourceReport(), session=session
        )

        assert client.fetch_entities(QUERY, ROOT_KEY, SampleEntity).entities == []
        assert len(client.report.warnings) == 1

    def test_unparseable_entity_is_skipped_without_losing_the_rest(self) -> None:
        client = make_client(
            [make_response([{"qualifiedName": "no-name"}, {"name": "orders"}])]
        )

        result = client.fetch_entities(QUERY, ROOT_KEY, SampleEntity)

        assert [entity.name for entity in result.entities] == ["orders"]
        assert len(client.report.warnings) == 1
        # A dropped entity leaves a gap, so the result must not look authoritative
        # or a caller could delete metadata for the missing entity.
        assert result.complete is False

    def test_missing_data_key_is_a_warning(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}
        client = make_client([response])

        assert fetch(client) == []
        assert len(client.report.warnings) == 1
        assert not client.report.failures

    def test_non_list_entity_field_is_a_warning(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": {"kafka_topic": {"name": "orders"}}}
        client = make_client([response])

        assert fetch(client) == []
        assert len(client.report.warnings) == 1
        assert not client.report.failures

    def test_missing_queried_field_is_a_warning(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"data": {"some_other_entity": []}}
        client = make_client([response])

        assert fetch(client) == []
        assert len(client.report.warnings) == 1
        assert not client.report.failures

    def test_failure_part_way_through_pagination_is_reported_as_partial(self) -> None:
        broken = Mock()
        broken.raise_for_status.side_effect = requests.ConnectionError("reset by peer")
        client = make_client(
            [make_response([{"name": "topic_0"}, {"name": "topic_1"}]), broken],
            page_size=2,
        )

        assert [entity.name for entity in fetch(client)] == ["topic_0", "topic_1"]
        messages = [warning.message for warning in client.report.warnings]
        assert any("partial" in message for message in messages)

    def test_failure_on_the_first_page_is_not_reported_as_partial(self) -> None:
        broken = Mock()
        broken.raise_for_status.side_effect = requests.ConnectionError("reset by peer")

        client = make_client([broken])

        assert fetch(client) == []
        assert len(client.report.warnings) == 1

    def test_non_object_page_items_do_not_end_pagination_early(self) -> None:
        client = make_client(
            [
                make_response([{"name": "topic_0"}, "not-an-object"]),
                make_response([{"name": "topic_1"}]),
            ],
            page_size=2,
        )

        assert [entity.name for entity in fetch(client)] == ["topic_0", "topic_1"]
        assert any(
            "non-object" in warning.message for warning in client.report.warnings
        )

    def test_pagination_stops_at_the_page_safety_limit(self) -> None:
        full_page = make_response([{"name": f"topic_{i}"} for i in range(2)])
        client = make_client([full_page] * 3, page_size=2)

        with patch(
            "datahub.ingestion.source.confluent.client.MAX_CATALOG_PAGES",
            2,
        ):
            entities = fetch(client)

        assert len(entities) == 4
        assert len(client.report.warnings) == 1
        assert not client.report.failures


class TestCatalogEntityHelpers:
    def test_business_metadata_values_are_stringified_and_blanks_dropped(self) -> None:
        entity = SampleEntity.model_validate(
            {
                "name": "orders",
                "business_metadata": [
                    {"name": "Governance.owner_team", "value": "core"},
                    {"name": "critical", "value": True},
                    {"name": "tier", "value": 1},
                    {"name": "unset", "value": None},
                ],
            }
        )

        assert entity.properties_from_business_metadata() == {
            "Governance.owner_team": "core",
            "critical": "True",
            "tier": "1",
        }

    def test_duplicate_business_metadata_names_are_detected(self) -> None:
        entity = SampleEntity.model_validate(
            {
                "name": "orders",
                "business_metadata": [
                    {"name": "team", "value": "core"},
                    {"name": "team", "value": "payments"},
                ],
            }
        )

        assert entity.duplicate_business_metadata_names() == ["team"]
        assert entity.properties_from_business_metadata() == {"team": "payments"}

    def test_null_only_duplicate_business_metadata_names_are_ignored(self) -> None:
        entity = SampleEntity.model_validate(
            {
                "name": "orders",
                "business_metadata": [
                    {"name": "team", "value": None},
                    {"name": "team", "value": None},
                    {"name": "owner", "value": "alice"},
                ],
            }
        )

        assert entity.duplicate_business_metadata_names() == []
        assert entity.properties_from_business_metadata() == {"owner": "alice"}

    def test_exact_lookup_does_not_fall_back_to_case_insensitive(self) -> None:
        index = index_by_name([SampleEntity(name="Orders")])

        assert index.get("Orders") is not None
        assert index.get("orders") is None
        assert index.get("payments") is None

    def test_repeated_names_are_held_back_as_ambiguous(self) -> None:
        index = index_by_name(
            [
                SampleEntity(name="orders", logical_cluster_id="lkc-1"),
                SampleEntity(name="orders", logical_cluster_id="lkc-2"),
                SampleEntity(name="payments", logical_cluster_id="lkc-1"),
            ]
        )

        assert index.get("orders") is None
        assert sorted(index.ambiguous) == ["orders"]
        assert index.get("payments") is not None

    def test_empty_names_are_counted(self) -> None:
        index = index_by_name(
            [SampleEntity(name=""), SampleEntity(name="orders"), SampleEntity(name="")]
        )

        assert index.empty_name_count == 2
        assert index.get("orders") is not None

    def test_index_keeps_subclass_fields(self) -> None:
        index = index_by_name([SampleEntity(name="orders", logical_cluster_id="lkc-1")])

        entity = index.get("orders")
        assert entity is not None and entity.cluster_id == "lkc-1"
