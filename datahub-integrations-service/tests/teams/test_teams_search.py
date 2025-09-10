"""Tests for Teams search functionality."""

from unittest.mock import AsyncMock, patch

import pytest

from datahub_integrations.teams.config import TeamsAppDetails, TeamsConnection
from datahub_integrations.teams.search import SearchResponse, TeamsSearchService


@pytest.fixture
def mock_teams_config() -> TeamsConnection:
    """Mock Teams configuration."""
    return TeamsConnection(
        app_details=TeamsAppDetails(
            app_id="test-app-id",
            app_password="test-app-password",
            tenant_id="test-tenant-id",
        )
    )


@pytest.fixture
def search_service(mock_teams_config: TeamsConnection) -> TeamsSearchService:
    """Create TeamsSearchService with mock config."""
    return TeamsSearchService(mock_teams_config)


@pytest.mark.asyncio
async def test_search_channels_empty_query(search_service: TeamsSearchService) -> None:
    """Test channel search with empty query."""
    result = await search_service.search_channels("")

    assert isinstance(result, SearchResponse)
    assert result.results == []
    assert result.hasMore is False
    assert result.totalCount == 0


@pytest.mark.asyncio
async def test_search_channels_with_results(search_service: TeamsSearchService) -> None:
    """Test channel search with mocked Graph API results."""
    mock_graph_results = [
        {
            "id": "channel-1",
            "displayName": "General",
            "teamId": "team-1",
            "teamName": "Engineering",
            "description": "General discussion",
            "membershipType": "standard",
        },
        {
            "id": "channel-2",
            "displayName": "Random",
            "teamId": "team-1",
            "teamName": "Engineering",
            "description": "Random chatter",
            "membershipType": "standard",
        },
    ]

    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_channels = AsyncMock(return_value=mock_graph_results)

        result = await search_service.search_channels("general", limit=10)

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        assert result.results[0].id == "channel-1"
        assert result.results[0].type == "channel"
        assert result.results[0].displayName == "General"
        assert result.results[0].teamName == "Engineering"
        assert result.hasMore is False
        assert result.totalCount == 2

        # Verify Graph API client was called correctly
        mock_client.search_channels.assert_called_once_with(
            query="general",
            limit=11,  # +1 to check hasMore
            team_ids=None,
        )


@pytest.mark.asyncio
async def test_search_channels_has_more(search_service: TeamsSearchService) -> None:
    """Test channel search with hasMore=True."""
    # Create more results than the limit
    mock_graph_results = [
        {
            "id": f"channel-{i}",
            "displayName": f"Channel {i}",
            "teamId": "team-1",
            "teamName": "Engineering",
            "membershipType": "standard",
        }
        for i in range(6)  # 6 results, but limit is 5
    ]

    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_channels = AsyncMock(return_value=mock_graph_results)

        result = await search_service.search_channels("channel", limit=5)

        assert len(result.results) == 5  # Limited to 5
        assert result.hasMore is True  # Should be True since we had 6 results
        assert result.totalCount == 5


@pytest.mark.asyncio
async def test_search_channels_with_team_ids(
    search_service: TeamsSearchService,
) -> None:
    """Test channel search with specific team IDs."""
    mock_graph_results = [
        {
            "id": "channel-1",
            "displayName": "General",
            "teamId": "team-1",
            "teamName": "Engineering",
            "membershipType": "standard",
        }
    ]

    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_channels = AsyncMock(return_value=mock_graph_results)

        result = await search_service.search_channels(
            "general", limit=10, team_ids=["team-1", "team-2"]
        )

        assert len(result.results) == 1

        # Verify team_ids were passed correctly
        mock_client.search_channels.assert_called_once_with(
            query="general", limit=11, team_ids=["team-1", "team-2"]
        )


@pytest.mark.asyncio
async def test_search_channels_error_handling(
    search_service: TeamsSearchService,
) -> None:
    """Test channel search error handling."""
    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_channels = AsyncMock(side_effect=Exception("API Error"))

        result = await search_service.search_channels("test")

        # Should return empty results on error
        assert isinstance(result, SearchResponse)
        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0


@pytest.mark.asyncio
async def test_search_users_with_results(search_service: TeamsSearchService) -> None:
    """Test user search with mocked Graph API results."""
    mock_graph_results = [
        {
            "id": "user-1",
            "displayName": "John Doe",
            "email": "john.doe@company.com",
            "userPrincipalName": "john.doe@company.com",
        },
        {
            "id": "user-2",
            "displayName": "Jane Smith",
            "email": "jane.smith@company.com",
            "userPrincipalName": "jane.smith@company.com",
        },
    ]

    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_users = AsyncMock(return_value=mock_graph_results)

        result = await search_service.search_users("john", limit=10)

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        assert result.results[0].id == "user-1"
        assert result.results[0].type == "user"
        assert result.results[0].displayName == "John Doe"
        assert result.results[0].email == "john.doe@company.com"
        assert result.hasMore is False
        assert result.totalCount == 2


@pytest.mark.asyncio
async def test_search_all_combined(search_service: TeamsSearchService) -> None:
    """Test combined search for users and channels."""
    mock_user_results = [
        {"id": "user-1", "displayName": "John Doe", "email": "john.doe@company.com"}
    ]

    mock_channel_results = [
        {
            "id": "channel-1",
            "displayName": "General",
            "teamId": "team-1",
            "teamName": "Engineering",
            "membershipType": "standard",
        }
    ]

    with patch.object(search_service, "graph_client") as mock_client:
        mock_client.search_users = AsyncMock(return_value=mock_user_results)
        mock_client.search_channels = AsyncMock(return_value=mock_channel_results)

        result = await search_service.search_all(
            "test", limit=25, search_users=True, search_channels=True
        )

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        # Users should come first in results
        assert result.results[0].type == "user"
        assert result.results[1].type == "channel"


def test_search_service_singleton() -> None:
    """Test that get_search_service returns the same instance."""
    from datahub_integrations.teams.search import get_search_service

    service1 = get_search_service()
    service2 = get_search_service()

    assert service1 is service2
