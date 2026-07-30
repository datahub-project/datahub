from typing import Dict, List, Mapping, Optional, Sequence
from unittest.mock import Mock

import pytest
from pydantic import Field

from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.source.confluent.client import ConfluentStreamCatalogClient
from datahub.ingestion.source.confluent.config import ConfluentStreamCatalogConfig
from datahub.ingestion.source.confluent.models import (
    CatalogEntity,
    index_by_name,
    lookup_by_name,
)

ROOT_KEY = "kafka_topic"
QUERY = "query Example($limit: Int, $offset: Int) { kafka_topic { name } }"


class SampleEntity(CatalogEntity):
    cluster_id: Optional[str] = Field(default=None, alias="clusterId")


def make_config(**overrides: object) -> ConfluentStreamCatalogConfig:
    defaults: Dict[str, object] = {
        "enabled": True,
        "schema_registry_url": "https://psrc-abc123.us-east-1.aws.confluent.cloud",
        "api_key": "sr-key",
        "api_secret": "sr-secret",
    }
    defaults.update(overrides)
    return ConfluentStreamCatalogConfig(**defaults)  # type: ignore[arg-type]


def make_response(entities: Optional[Sequence[Mapping[str, object]]]) -> Mock:
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
    return client.fetch_entities(QUERY, ROOT_KEY, SampleEntity)


class TestConfluentStreamCatalogConfig:
    def test_catalog_is_disabled_by_default(self) -> None:
        assert not ConfluentStreamCatalogConfig().enabled

    def test_disabled_config_skips_validation(self) -> None:
        config = ConfluentStreamCatalogConfig(enabled=False, page_size=0)
        assert config.page_size == 0

    def test_non_http_schema_registry_url_is_rejected(self) -> None:
        with pytest.raises(ValueError):
            make_config(schema_registry_url="psrc-abc123.aws.confluent.cloud")

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
            config.validate_connection("confluent_catalog")

        message = str(exc_info.value)
        assert "confluent_catalog.api_key" in message
        assert "confluent_catalog.api_secret" in message
        assert "confluent_catalog.schema_registry_url" not in message

    def test_validate_connection_passes_when_complete(self) -> None:
        make_config().validate_connection("confluent_catalog")


class TestConfluentStreamCatalogClient:
    def test_entities_are_parsed_with_aliases_and_null_collections(self) -> None:
        client = make_client(
            [
                make_response(
                    [
                        {
                            "name": "orders",
                            "qualifiedName": "lkc-123:orders",
                            "clusterId": "lkc-123",
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
                make_response([{"name": "topic_2"}]),
            ],
            page_size=2,
        )

        entities = fetch(client)

        assert [entity.name for entity in entities] == ["topic_0", "topic_1", "topic_2"]
        session = client.session
        assert isinstance(session, Mock)
        assert session.post.call_count == 2
        assert session.post.call_args_list[1].kwargs["json"]["variables"] == {
            "limit": 2,
            "offset": 2,
        }

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
        session.post.side_effect = Exception("connection refused")
        client = ConfluentStreamCatalogClient(
            make_config(), SourceReport(), session=session
        )

        assert client.fetch_entities(QUERY, ROOT_KEY, SampleEntity) == []
        assert len(client.report.warnings) == 1

    def test_unparseable_entity_is_skipped_without_losing_the_rest(self) -> None:
        client = make_client(
            [make_response([{"qualifiedName": "no-name"}, {"name": "orders"}])]
        )

        entities = fetch(client)

        assert [entity.name for entity in entities] == ["orders"]
        assert len(client.report.warnings) == 1

    def test_missing_data_key_yields_nothing(self) -> None:
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {}

        assert fetch(make_client([response])) == []


class TestCatalogEntityHelpers:
    def test_business_metadata_values_are_stringified_and_blanks_dropped(self) -> None:
        entity = SampleEntity.model_validate(
            {
                "name": "orders",
                "business_metadata": [
                    {"name": "team", "value": "core"},
                    {"name": "critical", "value": True},
                    {"name": "tier", "value": 1},
                    {"name": "unset", "value": None},
                ],
            }
        )

        assert entity.properties_from_business_metadata() == {
            "team": "core",
            "critical": "True",
            "tier": "1",
        }

    def test_lookup_tolerates_case_differences(self) -> None:
        index = index_by_name([SampleEntity(name="Orders")])

        assert lookup_by_name(index, "orders") is not None
        assert lookup_by_name(index, "payments") is None
