"""Comprehensive tests for Teams search functionality to increase coverage."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datahub_integrations.teams.config import TeamsAppDetails, TeamsConnection
from datahub_integrations.teams.search import (
    SearchResponse,
    SearchResult,
    TeamsSearchService,
    get_search_service,
)


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


class TestTeamsSearchServiceInit:
    """Test TeamsSearchService initialization and configuration."""

    def test_init_with_config(self, mock_teams_config: TeamsConnection) -> None:
        """Test service initialization with provided config."""
        service = TeamsSearchService(mock_teams_config)
        assert service.config == mock_teams_config
        assert service._graph_client is None

    @patch("datahub_integrations.teams.search.teams_config")
    def test_init_without_config(self, mock_teams_config_module: MagicMock) -> None:
        """Test service initialization without provided config."""
        mock_config = MagicMock()
        mock_teams_config_module.get_config.return_value = mock_config

        service = TeamsSearchService()
        assert service.config == mock_config
        mock_teams_config_module.get_config.assert_called_once()

    @patch("datahub_integrations.teams.search.GraphApiClient")
    def test_graph_client_property_lazy_init(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test graph_client property creates client on first access."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        # First access should create client
        client = search_service.graph_client
        assert client == mock_client
        assert search_service._graph_client == mock_client
        mock_graph_client_class.assert_called_once_with(search_service.config)

        # Second access should return same client
        client2 = search_service.graph_client
        assert client2 == mock_client
        assert mock_graph_client_class.call_count == 1


class TestSearchChannels:
    """Test search_channels method with comprehensive scenarios."""

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_channels_success(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test successful channel search."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_channels = AsyncMock(
            return_value=[
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
                    "membershipType": "private",
                },
            ]
        )

        result = await search_service.search_channels("general", limit=10)

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        assert result.hasMore is False
        assert result.totalCount == 2

        # Check first result
        assert result.results[0].id == "channel-1"
        assert result.results[0].type == "channel"
        assert result.results[0].displayName == "General"
        assert result.results[0].teamId == "team-1"
        assert result.results[0].teamName == "Engineering"
        assert result.results[0].description == "General discussion"
        assert result.results[0].membershipType == "standard"

        # Verify API call
        mock_client.search_channels.assert_called_once_with(
            query="general", limit=11, team_ids=None
        )

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_channels_with_team_ids(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test channel search with specific team IDs."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_channels = AsyncMock(
            return_value=[
                {"id": "channel-1", "displayName": "General", "teamId": "team-1"}
            ]
        )

        team_ids = ["team-1", "team-2"]
        result = await search_service.search_channels(
            "test", limit=25, team_ids=team_ids
        )

        assert len(result.results) == 1
        mock_client.search_channels.assert_called_once_with(
            query="test", limit=26, team_ids=team_ids
        )

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_channels_has_more_true(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test channel search with hasMore=True when results exceed limit."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        # Return 6 results for limit of 5
        mock_client.search_channels = AsyncMock(
            return_value=[
                {"id": f"channel-{i}", "displayName": f"Channel {i}"} for i in range(6)
            ]
        )

        result = await search_service.search_channels("test", limit=5)

        assert len(result.results) == 5  # Limited to requested amount
        assert result.hasMore is True  # Should be True since we got 6 results
        assert result.totalCount == 5

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_channels_missing_optional_fields(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test channel search with missing optional fields."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_channels = AsyncMock(
            return_value=[
                {
                    "id": "channel-1",
                    "displayName": "Minimal Channel",
                    # Missing teamId, teamName, description, membershipType
                }
            ]
        )

        result = await search_service.search_channels("test")

        assert len(result.results) == 1
        channel = result.results[0]
        assert channel.teamId is None
        assert channel.teamName is None
        assert channel.description is None
        assert channel.membershipType == "standard"  # Default value

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_channels_empty_query(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test channel search with empty/whitespace query."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        # Empty string
        result = await search_service.search_channels("")
        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0

        # Whitespace only
        result = await search_service.search_channels("   ")
        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0

        # Verify API was not called
        mock_client.search_channels.assert_not_called()

    @patch("datahub_integrations.teams.search.GraphApiClient")
    @patch("datahub_integrations.teams.search.logger")
    async def test_search_channels_api_exception(
        self,
        mock_logger: MagicMock,
        mock_graph_client_class: MagicMock,
        search_service: TeamsSearchService,
    ) -> None:
        """Test channel search with API exception."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_channels = AsyncMock(
            side_effect=Exception("Graph API error")
        )

        result = await search_service.search_channels("test")

        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0
        mock_logger.error.assert_called_once()


class TestSearchUsers:
    """Test search_users method with comprehensive scenarios."""

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_users_success(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test successful user search."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_users = AsyncMock(
            return_value=[
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
        )

        result = await search_service.search_users("john", limit=10)

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        assert result.hasMore is False
        assert result.totalCount == 2

        # Check first result
        user = result.results[0]
        assert user.id == "user-1"
        assert user.type == "user"
        assert user.displayName == "John Doe"
        assert user.email == "john.doe@company.com"
        assert user.userPrincipalName == "john.doe@company.com"

        # Verify API call
        mock_client.search_users.assert_called_once_with(query="john", limit=11)

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_users_has_more_true(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test user search with hasMore=True when results exceed limit."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        # Return 4 results for limit of 3
        mock_client.search_users = AsyncMock(
            return_value=[
                {"id": f"user-{i}", "displayName": f"User {i}"} for i in range(4)
            ]
        )

        result = await search_service.search_users("test", limit=3)

        assert len(result.results) == 3  # Limited to requested amount
        assert result.hasMore is True  # Should be True since we got 4 results
        assert result.totalCount == 3

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_users_missing_optional_fields(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test user search with missing optional fields."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_users = AsyncMock(
            return_value=[
                {
                    "id": "user-1",
                    "displayName": "Minimal User",
                    # Missing email, userPrincipalName
                }
            ]
        )

        result = await search_service.search_users("test")

        assert len(result.results) == 1
        user = result.results[0]
        assert user.email is None
        assert user.userPrincipalName is None

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_users_empty_query(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test user search with empty/whitespace query."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        # Empty string
        result = await search_service.search_users("")
        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0

        # Whitespace only
        result = await search_service.search_users("   ")
        assert result.results == []

        # Verify API was not called
        mock_client.search_users.assert_not_called()

    @patch("datahub_integrations.teams.search.GraphApiClient")
    @patch("datahub_integrations.teams.search.logger")
    async def test_search_users_api_exception(
        self,
        mock_logger: MagicMock,
        mock_graph_client_class: MagicMock,
        search_service: TeamsSearchService,
    ) -> None:
        """Test user search with API exception."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.search_users = AsyncMock(side_effect=Exception("Graph API error"))

        result = await search_service.search_users("test")

        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0
        mock_logger.error.assert_called_once()


class TestSearchAll:
    """Test search_all method with comprehensive scenarios."""

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_users_and_channels(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test combined search for both users and channels."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        # Mock search_users and search_channels methods
        with (
            patch.object(search_service, "search_users") as mock_search_users,
            patch.object(search_service, "search_channels") as mock_search_channels,
        ):
            mock_search_users.return_value = SearchResponse(
                results=[
                    SearchResult(id="user-1", type="user", displayName="John Doe")
                ],
                hasMore=False,
                totalCount=1,
            )
            mock_search_channels.return_value = SearchResponse(
                results=[
                    SearchResult(id="channel-1", type="channel", displayName="General")
                ],
                hasMore=False,
                totalCount=1,
            )

            result = await search_service.search_all(
                "test", limit=25, search_users=True, search_channels=True
            )

            assert len(result.results) == 2
            # Users should come first in sorted results
            assert result.results[0].type == "user"
            assert result.results[1].type == "channel"
            assert result.hasMore is False
            assert result.totalCount == 2

            # Verify method calls - users get half, channels get remaining based on actual results
            mock_search_users.assert_called_once_with(
                "test", 12
            )  # Half of limit (25//2)
            mock_search_channels.assert_called_once_with(
                "test", 24, None
            )  # limit - len(user_results) = 25-1

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_users_only(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test search for users only."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        with (
            patch.object(search_service, "search_users") as mock_search_users,
            patch.object(search_service, "search_channels") as mock_search_channels,
        ):
            mock_search_users.return_value = SearchResponse(
                results=[
                    SearchResult(id="user-1", type="user", displayName="John Doe")
                ],
                hasMore=False,
                totalCount=1,
            )

            result = await search_service.search_all(
                "test", limit=25, search_users=True, search_channels=False
            )

            assert len(result.results) == 1
            assert result.results[0].type == "user"

            mock_search_users.assert_called_once_with("test", 25)  # Full limit
            mock_search_channels.assert_not_called()

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_channels_only(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test search for channels only."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        with (
            patch.object(search_service, "search_users") as mock_search_users,
            patch.object(search_service, "search_channels") as mock_search_channels,
        ):
            mock_search_channels.return_value = SearchResponse(
                results=[
                    SearchResult(id="channel-1", type="channel", displayName="General")
                ],
                hasMore=False,
                totalCount=1,
            )

            result = await search_service.search_all(
                "test",
                limit=25,
                search_users=False,
                search_channels=True,
                team_ids=["team-1"],
            )

            assert len(result.results) == 1
            assert result.results[0].type == "channel"

            mock_search_users.assert_not_called()
            mock_search_channels.assert_called_once_with("test", 25, ["team-1"])

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_has_more_limit_exceeded(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test search_all with results exceeding limit."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        with (
            patch.object(search_service, "search_users") as mock_search_users,
            patch.object(search_service, "search_channels") as mock_search_channels,
        ):
            # Return more results than limit
            mock_search_users.return_value = SearchResponse(
                results=[
                    SearchResult(id=f"user-{i}", type="user", displayName=f"User {i}")
                    for i in range(8)
                ],
                hasMore=False,
                totalCount=8,
            )
            mock_search_channels.return_value = SearchResponse(
                results=[
                    SearchResult(
                        id=f"channel-{i}", type="channel", displayName=f"Channel {i}"
                    )
                    for i in range(5)
                ],
                hasMore=False,
                totalCount=5,
            )

            result = await search_service.search_all("test", limit=10)

            assert len(result.results) == 10  # Limited to requested amount
            assert (
                result.hasMore is True
            )  # Should be True since we had 13 total results
            assert result.totalCount == 10

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_alphabetical_sorting(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test search_all alphabetical sorting within types."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        with (
            patch.object(search_service, "search_users") as mock_search_users,
            patch.object(search_service, "search_channels") as mock_search_channels,
        ):
            # Return unsorted results
            mock_search_users.return_value = SearchResponse(
                results=[
                    SearchResult(id="user-1", type="user", displayName="Zoe User"),
                    SearchResult(id="user-2", type="user", displayName="Alice User"),
                ],
                hasMore=False,
                totalCount=2,
            )
            mock_search_channels.return_value = SearchResponse(
                results=[
                    SearchResult(
                        id="channel-1", type="channel", displayName="Zebra Channel"
                    ),
                    SearchResult(
                        id="channel-2", type="channel", displayName="Alpha Channel"
                    ),
                ],
                hasMore=False,
                totalCount=2,
            )

            result = await search_service.search_all("test", limit=25)

            assert len(result.results) == 4
            # Users first, then channels, sorted alphabetically within each type
            assert result.results[0].displayName == "Alice User"
            assert result.results[1].displayName == "Zoe User"
            assert result.results[2].displayName == "Alpha Channel"
            assert result.results[3].displayName == "Zebra Channel"

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_search_all_empty_query(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test search_all with empty/whitespace query."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        result = await search_service.search_all("")
        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0

    @patch("datahub_integrations.teams.search.GraphApiClient")
    @patch("datahub_integrations.teams.search.logger")
    async def test_search_all_exception(
        self,
        mock_logger: MagicMock,
        mock_graph_client_class: MagicMock,
        search_service: TeamsSearchService,
    ) -> None:
        """Test search_all with exception during search."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client

        with patch.object(search_service, "search_users") as mock_search_users:
            mock_search_users.side_effect = Exception("Search error")

            result = await search_service.search_all("test")

            assert result.results == []
            assert result.hasMore is False
            assert result.totalCount == 0
            mock_logger.error.assert_called_once()


class TestListAllChannels:
    """Test list_all_channels method with comprehensive scenarios."""

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_list_all_channels_success(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test successful listing of all channels."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.list_all_channels = AsyncMock(
            return_value=[
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
                    "teamId": "team-2",
                    "teamName": "Marketing",
                    "description": "Random chatter",
                    "membershipType": "private",
                },
            ]
        )

        result = await search_service.list_all_channels(limit=50)

        assert isinstance(result, SearchResponse)
        assert len(result.results) == 2
        assert result.hasMore is False
        assert result.totalCount == 2

        # Check first result
        channel = result.results[0]
        assert channel.id == "channel-1"
        assert channel.type == "channel"
        assert channel.displayName == "General"
        assert channel.teamId == "team-1"
        assert channel.teamName == "Engineering"
        assert channel.description == "General discussion"
        assert channel.membershipType == "standard"

        # Verify API call
        mock_client.list_all_channels.assert_called_once_with(limit=51)

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_list_all_channels_has_more_true(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test list_all_channels with hasMore=True when results exceed limit."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        # Return 6 results for limit of 5
        mock_client.list_all_channels = AsyncMock(
            return_value=[
                {"id": f"channel-{i}", "displayName": f"Channel {i}"} for i in range(6)
            ]
        )

        result = await search_service.list_all_channels(limit=5)

        assert len(result.results) == 5  # Limited to requested amount
        assert result.hasMore is True  # Should be True since we got 6 results
        assert result.totalCount == 5

    @patch("datahub_integrations.teams.search.GraphApiClient")
    async def test_list_all_channels_missing_optional_fields(
        self, mock_graph_client_class: MagicMock, search_service: TeamsSearchService
    ) -> None:
        """Test list_all_channels with missing optional fields."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.list_all_channels = AsyncMock(
            return_value=[
                {
                    "id": "channel-1",
                    "displayName": "Minimal Channel",
                    # Missing teamId, teamName, description, membershipType
                }
            ]
        )

        result = await search_service.list_all_channels()

        assert len(result.results) == 1
        channel = result.results[0]
        assert channel.teamId is None
        assert channel.teamName is None
        assert channel.description is None
        assert channel.membershipType == "standard"  # Default value

    @patch("datahub_integrations.teams.search.GraphApiClient")
    @patch("datahub_integrations.teams.search.logger")
    async def test_list_all_channels_api_exception(
        self,
        mock_logger: MagicMock,
        mock_graph_client_class: MagicMock,
        search_service: TeamsSearchService,
    ) -> None:
        """Test list_all_channels with API exception."""
        mock_client = MagicMock()
        mock_graph_client_class.return_value = mock_client
        mock_client.list_all_channels = AsyncMock(
            side_effect=Exception("Graph API error")
        )

        result = await search_service.list_all_channels()

        assert result.results == []
        assert result.hasMore is False
        assert result.totalCount == 0
        mock_logger.error.assert_called_once()


class TestGlobalSearchService:
    """Test global search service singleton."""

    @patch("datahub_integrations.teams.search._search_service", None)
    def test_get_search_service_creates_singleton(self) -> None:
        """Test that get_search_service creates and returns singleton."""
        # Reset global variable
        import datahub_integrations.teams.search

        datahub_integrations.teams.search._search_service = None

        service1 = get_search_service()
        service2 = get_search_service()

        assert service1 is service2
        assert isinstance(service1, TeamsSearchService)

    @patch("datahub_integrations.teams.search._search_service", None)
    @patch("datahub_integrations.teams.search.teams_config")
    def test_get_search_service_uses_config(
        self, mock_teams_config_module: MagicMock
    ) -> None:
        """Test that get_search_service uses teams_config."""
        # Reset global variable
        import datahub_integrations.teams.search

        datahub_integrations.teams.search._search_service = None

        mock_config = MagicMock()
        mock_teams_config_module.get_config.return_value = mock_config

        service = get_search_service()

        assert service.config == mock_config
        mock_teams_config_module.get_config.assert_called_once()


class TestSearchModels:
    """Test SearchResult and SearchResponse models."""

    def test_search_result_model_full(self) -> None:
        """Test SearchResult model with all fields."""
        result = SearchResult(
            id="test-id",
            type="user",
            displayName="Test User",
            email="test@example.com",
            userPrincipalName="test@example.com",
            teamId="team-1",
            teamName="Test Team",
            description="Test description",
            membershipType="standard",
        )

        assert result.id == "test-id"
        assert result.type == "user"
        assert result.displayName == "Test User"
        assert result.email == "test@example.com"
        assert result.userPrincipalName == "test@example.com"
        assert result.teamId == "team-1"
        assert result.teamName == "Test Team"
        assert result.description == "Test description"
        assert result.membershipType == "standard"

    def test_search_result_model_minimal(self) -> None:
        """Test SearchResult model with minimal required fields."""
        result = SearchResult(id="test-id", type="channel", displayName="Test Channel")

        assert result.id == "test-id"
        assert result.type == "channel"
        assert result.displayName == "Test Channel"
        assert result.email is None
        assert result.userPrincipalName is None
        assert result.teamId is None
        assert result.teamName is None
        assert result.description is None
        assert result.membershipType is None

    def test_search_response_model(self) -> None:
        """Test SearchResponse model."""
        results = [
            SearchResult(id="1", type="user", displayName="User 1"),
            SearchResult(id="2", type="channel", displayName="Channel 1"),
        ]

        response = SearchResponse(results=results, hasMore=True, totalCount=2)

        assert len(response.results) == 2
        assert response.hasMore is True
        assert response.totalCount == 2
        assert response.results[0].id == "1"
        assert response.results[1].id == "2"
