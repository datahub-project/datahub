from datetime import datetime
from unittest.mock import Mock

import pytest
from freezegun import freeze_time

from datahub.errors import ItemNotFoundError, MultipleItemsFoundError, SdkUsageError
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.metadata.schema_classes import AuditStampClass
from datahub.metadata.urns import CorpGroupUrn, CorpUserUrn
from datahub.sdk.main_client import DEFAULT_ACTOR_URN, DataHubClient


@pytest.fixture
def mock_graph() -> Mock:
    graph = Mock(spec=DataHubGraph)
    graph.exists.return_value = False
    return graph


def test_client_creation(mock_graph: Mock) -> None:
    assert DataHubClient(graph=mock_graph)
    assert DataHubClient(server="https://example.com", token="token")


def test_client_init_errors(mock_graph: Mock) -> None:
    config = DatahubClientConfig(server="https://example.com", token="token")

    with pytest.raises(SdkUsageError):
        DataHubClient(server="https://example.com", graph=mock_graph)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient(server="https://example.com", config=config)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient(config=config, graph=mock_graph)  # type: ignore
    with pytest.raises(SdkUsageError):
        DataHubClient()  # type: ignore


def test_resolve_user(mock_graph: Mock) -> None:
    client = DataHubClient(graph=mock_graph)

    # This test doesn't really validate the graphql query or vars.
    # It probably makes more sense to test via smoke-tests.

    mock_graph.get_urns_by_filter.return_value = []
    with pytest.raises(ItemNotFoundError):
        client.resolve.user(name="User")

    mock_graph.get_urns_by_filter.return_value = ["urn:li:corpuser:user"]
    assert client.resolve.user(name="User") == CorpUserUrn("urn:li:corpuser:user")

    mock_graph.get_urns_by_filter.return_value = [
        "urn:li:corpuser:user",
        "urn:li:corpuser:user2",
    ]
    with pytest.raises(MultipleItemsFoundError):
        client.resolve.user(name="User")


def create_jwt_token(payload: dict) -> str:
    """Helper function to create a JWT token for testing."""
    import jwt

    return jwt.encode(payload, "secret", algorithm="HS256")


@pytest.mark.parametrize(
    "token,fallback_actor,expected_result",
    [
        pytest.param(
            create_jwt_token({"actorId": "test_user", "type": "PERSONAL"}),
            None,
            CorpUserUrn.create_from_id("test_user"),
            id="valid_jwt_personal_type",
        ),
        pytest.param(
            None,
            None,
            DEFAULT_ACTOR_URN,
            id="no_token_default_actor",
        ),
        pytest.param(
            None,
            CorpUserUrn.create_from_id("fallback_user"),
            CorpUserUrn.create_from_id("fallback_user"),
            id="no_token_with_fallback",
        ),
        pytest.param(
            create_jwt_token({"type": "PERSONAL"}),
            None,
            DEFAULT_ACTOR_URN,
            id="missing_actor_id",
        ),
        pytest.param(
            create_jwt_token({"actorId": "", "type": "PERSONAL"}),
            CorpGroupUrn.create_from_id("fallback_group"),
            CorpGroupUrn.create_from_id("fallback_group"),
            id="empty_actor_id_with_fallback",
        ),
        pytest.param(
            create_jwt_token({"actorId": "test_user", "type": "SERVICE"}),
            None,
            DEFAULT_ACTOR_URN,
            id="non_personal_type",
        ),
        pytest.param(
            create_jwt_token({"actorId": "test_user"}),
            CorpUserUrn.create_from_id("custom_fallback"),
            CorpUserUrn.create_from_id("custom_fallback"),
            id="missing_type_field",
        ),
        pytest.param(
            "invalid.jwt.token",
            None,
            DEFAULT_ACTOR_URN,
            id="invalid_jwt_format",
        ),
        pytest.param(
            "malformed_jwt",
            CorpUserUrn.create_from_id("error_fallback"),
            CorpUserUrn.create_from_id("error_fallback"),
            id="malformed_jwt_with_fallback",
        ),
    ],
)
def test_audit_actor(token, fallback_actor, expected_result):
    """Test audit_actor method with various JWT token scenarios."""
    mock_graph = Mock(spec=DataHubGraph)
    mock_config = Mock()
    mock_config.token = token
    mock_graph.config = mock_config

    client = DataHubClient(graph=mock_graph)

    result = client.audit_actor(fallback_actor=fallback_actor)

    assert result == expected_result


FROZEN_TIME = "2024-01-15 12:30:45"


@freeze_time(FROZEN_TIME)
@pytest.mark.parametrize(
    "token,fallback_actor,fallback_timestamp,expected_actor,expected_time_ms",
    [
        pytest.param(
            create_jwt_token({"actorId": "stamp_user", "type": "PERSONAL"}),
            None,
            None,
            CorpUserUrn.create_from_id("stamp_user"),
            1705321845000,  # FROZEN_TIME in milliseconds (UTC)
            id="valid_jwt_current_time",
        ),
        pytest.param(
            None,
            CorpUserUrn.create_from_id("fallback_user"),
            None,
            CorpUserUrn.create_from_id("fallback_user"),
            1705321845000,  # FROZEN_TIME in milliseconds (UTC)
            id="fallback_actor_current_time",
        ),
        pytest.param(
            None,
            None,
            datetime(2023, 6, 15, 10, 30, 0),
            DEFAULT_ACTOR_URN,
            1686817800000,  # Custom timestamp in milliseconds
            id="custom_timestamp",
        ),
        pytest.param(
            create_jwt_token({"actorId": "jwt_user", "type": "PERSONAL"}),
            CorpGroupUrn.create_from_id("group_fallback"),
            datetime(2023, 12, 25, 18, 45, 30),
            CorpUserUrn.create_from_id("jwt_user"),
            1703526330000,  # Custom timestamp in milliseconds
            id="jwt_overrides_fallback_with_custom_time",
        ),
    ],
)
def test_audit_stamp(
    token, fallback_actor, fallback_timestamp, expected_actor, expected_time_ms
):
    """Test audit_stamp method with various scenarios."""
    mock_graph = Mock(spec=DataHubGraph)
    mock_config = Mock()
    mock_config.token = token
    mock_graph.config = mock_config

    client = DataHubClient(graph=mock_graph)

    result = client.audit_stamp(
        fallback_actor=fallback_actor, fallback_timestamp=fallback_timestamp
    )

    # Verify the result is an AuditStampClass
    assert isinstance(result, AuditStampClass)

    # Verify the actor is correct
    assert result.actor == str(expected_actor)

    # Verify the timestamp is exactly what we expect
    assert result.time == expected_time_ms
