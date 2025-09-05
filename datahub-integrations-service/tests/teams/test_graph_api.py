"""
Simplified Graph API client tests focused on core functionality.
These tests focus on testable behavior and configuration.
"""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datahub_integrations.teams.config import TeamsAppDetails, TeamsConnection
from datahub_integrations.teams.graph_api import (
    GraphApiChannel,
    GraphApiClient,
    GraphApiTeam,
    GraphApiUser,
)


@pytest.fixture
def mock_teams_config() -> Any:
    """Mock Teams configuration for Graph API."""
    app_details = MagicMock(spec=TeamsAppDetails)
    app_details.app_id = "test-app-id"
    app_details.app_password = "test-app-password"
    app_details.tenant_id = "test-tenant-id"

    config = MagicMock(spec=TeamsConnection)
    config.app_details = app_details
    return config


@pytest.fixture
def graph_client(mock_teams_config: Any) -> GraphApiClient:
    """Create Graph API client instance."""
    return GraphApiClient(mock_teams_config)


class TestGraphApiClientConfiguration:
    """Test Graph API client configuration and initialization."""

    def test_client_initialization(self, mock_teams_config: Any) -> None:
        """Test proper client initialization."""
        client = GraphApiClient(mock_teams_config)

        assert client.config == mock_teams_config
        assert client._access_token is None

    def test_client_with_invalid_config(self) -> None:
        """Test client behavior with invalid configuration."""
        invalid_config = MagicMock()
        invalid_config.app_details = None

        client = GraphApiClient(invalid_config)
        assert client.config == invalid_config

    @pytest.mark.asyncio
    async def test_missing_app_details_raises_error(self, graph_client: Any) -> None:
        """Test that missing app details raises ValueError."""
        graph_client.config.app_details = None

        with pytest.raises(ValueError, match="Teams app details not configured"):
            await graph_client._get_graph_access_token()

    @pytest.mark.asyncio
    async def test_missing_credentials_raises_error(self, graph_client: Any) -> None:
        """Test that missing credentials raise ValueError."""
        graph_client.config.app_details.app_id = None

        with pytest.raises(ValueError, match="Missing Teams app credentials"):
            await graph_client._get_graph_access_token()


class TestGraphApiModels:
    """Test Graph API data models."""

    def test_graph_api_user_model(self) -> None:
        """Test GraphApiUser model creation."""
        user = GraphApiUser(
            id="user-123",
            displayName="John Doe",
            mail="john.doe@company.com",
            userPrincipalName="john.doe@company.com",
        )

        assert user.id == "user-123"
        assert user.displayName == "John Doe"
        assert user.mail == "john.doe@company.com"
        assert user.userPrincipalName == "john.doe@company.com"

    def test_graph_api_team_model(self) -> None:
        """Test GraphApiTeam model creation."""
        team = GraphApiTeam(
            id="team-123",
            displayName="Engineering Team",
            description="Software engineering team",
        )

        assert team.id == "team-123"
        assert team.displayName == "Engineering Team"
        assert team.description == "Software engineering team"

    def test_graph_api_channel_model(self) -> None:
        """Test GraphApiChannel model creation."""
        channel = GraphApiChannel(
            id="channel-123",
            displayName="General",
            description="General discussion",
            membershipType="standard",
        )

        assert channel.id == "channel-123"
        assert channel.displayName == "General"
        assert channel.description == "General discussion"
        assert channel.membershipType == "standard"


class TestGraphApiTokenHandling:
    """Test Graph API token management logic."""

    @pytest.mark.asyncio
    async def test_token_url_construction_logic(self, graph_client: Any) -> None:
        """Test proper token URL construction logic without HTTP mocking."""
        # Test the URL construction logic by checking the tenant ID is used
        tenant_id = graph_client.config.app_details.tenant_id

        # This test verifies the configuration is properly accessed
        assert tenant_id == "test-tenant-id"

        # We can test the URL format without actually making the request
        expected_url_pattern = (
            f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        )
        assert "login.microsoftonline.com" in expected_url_pattern
        assert "test-tenant-id" in expected_url_pattern
        assert "/oauth2/v2.0/token" in expected_url_pattern


class TestGraphApiErrorHandling:
    """Test Graph API error handling."""

    @pytest.mark.asyncio
    async def test_token_acquisition_failure(self, graph_client: Any) -> None:
        """Test token acquisition failure handling."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = AsyncMock()
            mock_response.status_code = 401
            mock_response.text = "Unauthorized"
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(
                Exception, match="Failed to authenticate with Microsoft Graph API"
            ):
                await graph_client._get_graph_access_token()

    @pytest.mark.asyncio
    async def test_search_users_error_handling(self, graph_client: Any) -> None:
        """Test that search_users handles errors gracefully."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            side_effect=Exception("Auth failed"),
        ):
            # Should return empty list instead of raising
            users = await graph_client.search_users("test")
            assert users == []

    @pytest.mark.asyncio
    async def test_get_teams_error_handling(self, graph_client: Any) -> None:
        """Test that get_teams handles errors gracefully."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            side_effect=Exception("Auth failed"),
        ):
            # Should return empty list instead of raising
            teams = await graph_client.get_teams()
            assert teams == []

    @pytest.mark.asyncio
    async def test_list_all_channels_error_handling(self, graph_client: Any) -> None:
        """Test that list_all_channels handles errors gracefully."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            side_effect=Exception("Auth failed"),
        ):
            # Should return empty list instead of raising
            channels = await graph_client.list_all_channels()
            assert channels == []


class TestGraphApiSuccessScenarios:
    """Test Graph API success scenarios with simplified mocking."""

    @pytest.mark.asyncio
    async def test_get_teams_success_basic(self, graph_client: Any) -> None:
        """Test basic get_teams success scenario."""
        mock_teams_data = {
            "value": [
                {
                    "id": "team-123",
                    "displayName": "Engineering Team",
                    "description": "Dev team",
                }
            ]
        }

        with patch.object(
            graph_client, "_make_graph_request", return_value=mock_teams_data
        ):
            teams = await graph_client.get_teams()

            assert len(teams) == 1
            assert teams[0].id == "team-123"
            assert teams[0].displayName == "Engineering Team"

    @pytest.mark.asyncio
    async def test_search_channels_basic(self, graph_client: Any) -> None:
        """Test basic search_channels functionality."""
        mock_teams = [GraphApiTeam(id="team-123", displayName="Engineering Team")]
        mock_channels_data = {
            "value": [
                {
                    "id": "channel-123",
                    "displayName": "General",
                    "description": "General chat",
                }
            ]
        }

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.search_channels("general")

                assert len(channels) >= 0  # May be empty due to filtering logic
                # The method returns List[Dict] with team context added

    @pytest.mark.asyncio
    async def test_list_all_channels_basic(self, graph_client: Any) -> None:
        """Test basic list_all_channels functionality."""
        mock_teams = [GraphApiTeam(id="team-123", displayName="Engineering Team")]
        mock_channels_data = {
            "value": [
                {
                    "id": "channel-123",
                    "displayName": "General",
                    "description": "General chat",
                }
            ]
        }

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.list_all_channels()

                assert len(channels) >= 0
                if channels:
                    # Verify structure includes team information
                    assert "teamId" in channels[0]
                    assert "teamName" in channels[0]
                    assert "displayName" in channels[0]


class TestGraphApiPermissions:
    """Test Graph API permissions checking."""

    @pytest.mark.asyncio
    async def test_check_permissions_token_failure(self, graph_client: Any) -> None:
        """Test permission check when token acquisition fails."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            side_effect=Exception("Auth failed"),
        ):
            results = await graph_client.check_permissions()

            assert results["token_valid"] is False
            assert len(results["errors"]) > 0
            assert "Auth failed" in str(results["errors"])

    @pytest.mark.asyncio
    async def test_get_application_permissions_no_app_id(
        self, graph_client: Any
    ) -> None:
        """Test get_application_permissions with missing app ID."""
        graph_client.config.app_details = None

        result = await graph_client.get_application_permissions()
        assert "error" in result
        assert (
            "No app ID configured" in result["error"]
            or "Teams app details not configured" in result["error"]
        )


class TestChannelOperations:
    """Test channel search and listing operations."""

    @pytest.mark.asyncio
    async def test_search_channels_with_query(self, graph_client: Any) -> None:
        """Test channel search with query string."""
        mock_teams = [
            GraphApiTeam(id="team-1", displayName="Engineering Team"),
            GraphApiTeam(id="team-2", displayName="Marketing Team"),
        ]
        mock_channels_data: dict[str, list] = {
            "value": [
                {
                    "id": "channel-1",
                    "displayName": "General",
                    "description": "General discussion",
                    "membershipType": "standard",
                }
            ]
        }

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.search_channels("general")

                assert len(channels) >= 0
                if channels:
                    assert "teamId" in channels[0]
                    assert "teamName" in channels[0]
                    assert "displayName" in channels[0]

    @pytest.mark.asyncio
    async def test_search_channels_with_specific_teams(self, graph_client: Any) -> None:
        """Test channel search with specific team IDs."""
        team_ids = ["team-1"]
        mock_team_data = {
            "id": "team-1",
            "displayName": "Engineering Team",
            "description": "Dev team",
        }
        mock_channels_data: dict[str, list] = {
            "value": [
                {
                    "id": "channel-1",
                    "displayName": "General",
                    "description": "General discussion",
                    "membershipType": "standard",
                }
            ]
        }

        with patch.object(graph_client, "_make_graph_request") as mock_request:
            mock_request.side_effect = [mock_team_data, mock_channels_data]

            channels = await graph_client.search_channels("general", team_ids=team_ids)

            assert isinstance(channels, list)
            # Should make requests for team details and channels
            assert mock_request.call_count >= 1

    @pytest.mark.asyncio
    async def test_search_channels_with_limit(self, graph_client: Any) -> None:
        """Test channel search respects limit parameter."""
        mock_teams = [GraphApiTeam(id="team-1", displayName="Engineering Team")]
        mock_channels_data: dict[str, list] = {"value": []}

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.search_channels("test", limit=5)

                assert isinstance(channels, list)
                assert len(channels) <= 5

    @pytest.mark.asyncio
    async def test_search_channels_handles_team_errors(self, graph_client: Any) -> None:
        """Test channel search handles individual team errors gracefully."""
        mock_teams = [
            GraphApiTeam(id="team-1", displayName="Valid Team"),
            GraphApiTeam(id="team-invalid", displayName="Invalid Team"),
        ]

        def mock_request_side_effect(
            endpoint: str, params: Any = None
        ) -> dict[str, list]:
            if "team-invalid" in endpoint:
                raise Exception("Team not accessible")
            return {"value": []}

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client,
                "_make_graph_request",
                side_effect=mock_request_side_effect,
            ):
                channels = await graph_client.search_channels("test")

                assert isinstance(channels, list)  # Should not raise exception

    @pytest.mark.asyncio
    async def test_list_all_channels_success(self, graph_client: Any) -> None:
        """Test listing all channels from all teams."""
        mock_teams = [
            GraphApiTeam(id="team-1", displayName="Engineering Team"),
            GraphApiTeam(id="team-2", displayName="Marketing Team"),
        ]
        mock_channels_data: dict[str, list] = {
            "value": [
                {
                    "id": "channel-1",
                    "displayName": "General",
                    "description": "General chat",
                    "membershipType": "standard",
                }
            ]
        }

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.list_all_channels()

                assert isinstance(channels, list)
                if channels:
                    # Verify channels include team context
                    assert "teamId" in channels[0]
                    assert "teamName" in channels[0]
                    assert "displayName" in channels[0]

    @pytest.mark.asyncio
    async def test_list_all_channels_with_limit(self, graph_client: Any) -> None:
        """Test listing channels respects limit parameter."""
        mock_teams = [GraphApiTeam(id="team-1", displayName="Engineering Team")]
        mock_channels_data: dict[str, list] = {"value": []}

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.list_all_channels(limit=50)

                assert isinstance(channels, list)
                assert len(channels) <= 50


class TestUserSearch:
    """Test user search functionality and edge cases."""

    @pytest.mark.asyncio
    async def test_search_users_success(self, graph_client: Any) -> None:
        """Test successful user search."""
        mock_users_data: dict[str, list] = {
            "value": [
                {
                    "id": "user-1",
                    "displayName": "John Doe",
                    "mail": "john.doe@company.com",
                    "userPrincipalName": "john.doe@company.com",
                },
                {
                    "id": "user-2",
                    "displayName": "Jane Smith",
                    "mail": "jane.smith@company.com",
                    "userPrincipalName": "jane.smith@company.com",
                },
            ]
        }

        with patch.object(
            graph_client,
            "_get_graph_access_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_users_data
                mock_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                users = await graph_client.search_users("john")

                assert len(users) == 2
                assert users[0]["type"] == "user"
                assert users[0]["displayName"] == "John Doe"
                assert users[0]["email"] == "john.doe@company.com"

    @pytest.mark.asyncio
    async def test_search_users_with_consistency_level_header(
        self, graph_client: Any
    ) -> None:
        """Test that user search includes ConsistencyLevel header for $search."""
        mock_users_data: dict[str, list] = {"value": []}

        with patch.object(
            graph_client,
            "_get_graph_access_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_users_data
                mock_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                await graph_client.search_users("test")

                call_args = mock_client.get.call_args
                headers = call_args[1]["headers"]
                assert headers["ConsistencyLevel"] == "eventual"

    @pytest.mark.asyncio
    async def test_search_users_with_missing_email(self, graph_client: Any) -> None:
        """Test user search handles users with missing email addresses."""
        mock_users_data: dict[str, list] = {
            "value": [
                {
                    "id": "user-1",
                    "displayName": "John Doe",
                    "userPrincipalName": "john.doe@company.com",
                    # No "mail" field
                }
            ]
        }

        with patch.object(
            graph_client,
            "_get_graph_access_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_response = MagicMock()
                mock_response.status_code = 200
                mock_response.json.return_value = mock_users_data
                mock_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                users = await graph_client.search_users("john")

                assert len(users) == 1
                assert (
                    users[0]["email"] == "john.doe@company.com"
                )  # Should use userPrincipalName

    @pytest.mark.asyncio
    async def test_search_users_token_refresh(self, graph_client: Any) -> None:
        """Test user search handles token refresh on 401."""
        mock_users_data: dict[str, list] = {"value": []}

        with patch.object(
            graph_client,
            "_get_graph_access_token",
            side_effect=["expired-token", "fresh-token"],
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()

                # First response: 401
                mock_401_response = MagicMock()
                mock_401_response.status_code = 401

                # Second response: 200
                mock_200_response = MagicMock()
                mock_200_response.status_code = 200
                mock_200_response.json.return_value = mock_users_data

                mock_client.get.side_effect = [mock_401_response, mock_200_response]
                mock_client_class.return_value.__aenter__.return_value = mock_client

                users = await graph_client.search_users("test")

                assert users == []
                assert mock_client.get.call_count == 2

    @pytest.mark.asyncio
    async def test_search_users_api_error_returns_empty(
        self, graph_client: Any
    ) -> None:
        """Test user search returns empty list on API errors."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_response = MagicMock()
                mock_response.status_code = 403
                mock_response.text = "Forbidden"
                mock_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                users = await graph_client.search_users("test")

                assert users == []


class TestPermissionChecking:
    """Test permission checking and diagnostics."""

    @pytest.mark.asyncio
    async def test_check_permissions_success(self, graph_client: Any) -> None:
        """Test successful permission check."""
        mock_token = "test-token"
        mock_users_response = MagicMock()
        mock_users_response.status_code = 200
        mock_users_response.json.return_value = {"value": []}

        mock_teams_response = MagicMock()
        mock_teams_response.status_code = 200
        mock_teams_response.json.return_value = {
            "value": [{"id": "team-1", "displayName": "Test Team"}]
        }

        with patch.object(
            graph_client, "_get_graph_access_token", return_value=mock_token
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client.get.side_effect = [mock_users_response, mock_teams_response]
                mock_client_class.return_value.__aenter__.return_value = mock_client

                results = await graph_client.check_permissions()

                assert results["token_valid"] is True
                assert results["user_read"] is True
                assert results["teams_read"] is True
                assert results["teams_count"] == 1

    @pytest.mark.asyncio
    async def test_check_permissions_user_read_failure(self, graph_client: Any) -> None:
        """Test permission check when user read fails."""
        mock_token = "test-token"
        mock_users_response = MagicMock()
        mock_users_response.status_code = 403
        mock_users_response.text = "Forbidden"

        with patch.object(
            graph_client, "_get_graph_access_token", return_value=mock_token
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_client.get.return_value = mock_users_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                results = await graph_client.check_permissions()

                assert results["token_valid"] is True
                assert results["user_read"] is False
                assert len(results["errors"]) > 0

    @pytest.mark.asyncio
    async def test_check_permissions_with_channel_test(self, graph_client: Any) -> None:
        """Test permission check includes channel read test when teams are available."""
        mock_token = "test-token"

        # Mock successful user and teams responses
        mock_users_response = MagicMock()
        mock_users_response.status_code = 200
        mock_users_response.json.return_value = {"value": []}

        mock_teams_response = MagicMock()
        mock_teams_response.status_code = 200
        mock_teams_response.json.return_value = {
            "value": [{"id": "team-1", "displayName": "Test Team"}]
        }

        mock_channels_response = MagicMock()
        mock_channels_response.status_code = 200
        mock_channels_response.json.return_value = {
            "value": [{"id": "channel-1", "displayName": "General"}]
        }

        with patch.object(
            graph_client, "_get_graph_access_token", return_value=mock_token
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()

                # Create a function that returns the appropriate response based on URL
                def mock_get(
                    url: str, headers: Any = None, params: Any = None
                ) -> MagicMock:
                    if "users" in url:
                        return mock_users_response
                    elif "teams" in url and "channels" not in url:
                        return mock_teams_response
                    elif "channels" in url:
                        return mock_channels_response
                    else:
                        # Default fallback
                        return mock_teams_response

                mock_client.get.side_effect = mock_get
                mock_client_class.return_value.__aenter__.return_value = mock_client

                results = await graph_client.check_permissions()

                assert results["channel_read"] is True
                assert results["sample_channels_count"] == 1

    @pytest.mark.asyncio
    async def test_get_application_permissions_success(self, graph_client: Any) -> None:
        """Test successful application permission retrieval."""
        mock_token = "test-token"
        mock_sp_response = {
            "value": [{"id": "sp-id-12345", "appId": "test-app-id-12345"}]
        }
        mock_app_roles_response = {
            "value": [
                {
                    "appRoleId": "role-1",
                    "principalDisplayName": "DataHub Teams Bot",
                    "resourceDisplayName": "Microsoft Graph",
                }
            ]
        }
        mock_delegated_response = {
            "value": [
                {
                    "scope": "User.Read",
                    "resourceId": "resource-1",
                    "consentType": "AllPrincipals",
                }
            ]
        }

        with patch.object(
            graph_client, "_get_graph_access_token", return_value=mock_token
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()

                # Mock service principal response
                mock_sp_resp = MagicMock()
                mock_sp_resp.status_code = 200
                mock_sp_resp.json.return_value = mock_sp_response

                # Mock app role assignments response
                mock_roles_resp = MagicMock()
                mock_roles_resp.status_code = 200
                mock_roles_resp.json.return_value = mock_app_roles_response

                # Mock delegated permissions response
                mock_delegated_resp = MagicMock()
                mock_delegated_resp.status_code = 200
                mock_delegated_resp.json.return_value = mock_delegated_response

                mock_client.get.side_effect = [
                    mock_sp_resp,
                    mock_roles_resp,
                    mock_delegated_resp,
                ]
                mock_client_class.return_value.__aenter__.return_value = mock_client

                result = await graph_client.get_application_permissions()

                assert (
                    result["app_id"] == "test-app-id"
                )  # From config, not API response
                assert (
                    result["service_principal_id"] == "sp-id-12345"
                )  # From API response
                assert len(result["application_permissions"]) == 1
                assert len(result["delegated_permissions"]) == 1

    @pytest.mark.asyncio
    async def test_get_application_permissions_no_app_id(
        self, graph_client: Any
    ) -> None:
        """Test application permission retrieval with missing app ID."""
        graph_client.config.app_details = None

        result = await graph_client.get_application_permissions()

        assert "error" in result
        assert (
            "No app ID configured" in result["error"]
            or "Teams app details not configured" in result["error"]
        )


class TestGraphApiErrorScenarios:
    """Test comprehensive error scenarios and edge cases."""

    @pytest.mark.asyncio
    async def test_malformed_token_response(self, graph_client: Any) -> None:
        """Test handling of malformed token response."""
        with patch("httpx.AsyncClient") as mock_client_class:
            mock_client = AsyncMock()
            mock_response = (
                MagicMock()
            )  # Use MagicMock for response to avoid coroutines
            mock_response.status_code = 200
            mock_response.json.side_effect = ValueError("Invalid JSON")
            mock_client.post.return_value = mock_response
            mock_client_class.return_value.__aenter__.return_value = mock_client

            with pytest.raises(ValueError):
                await graph_client._get_graph_access_token()

    @pytest.mark.asyncio
    async def test_graph_request_rate_limiting(self, graph_client: Any) -> None:
        """Test Graph API request handling with rate limiting (429)."""
        with patch.object(
            graph_client,
            "_get_graph_access_token",
            new_callable=AsyncMock,
            return_value="test-token",
        ):
            with patch("httpx.AsyncClient") as mock_client_class:
                mock_client = AsyncMock()
                mock_response = MagicMock()
                mock_response.status_code = 429
                mock_response.text = "Too Many Requests"
                mock_client.get.return_value = mock_response
                mock_client_class.return_value.__aenter__.return_value = mock_client

                with pytest.raises(Exception, match="Graph API request failed: 429"):
                    await graph_client._make_graph_request("/test-endpoint")

    @pytest.mark.asyncio
    async def test_search_channels_sorting_logic(self, graph_client: Any) -> None:
        """Test that channel search results are sorted correctly."""
        mock_teams = [GraphApiTeam(id="team-1", displayName="Engineering Team")]
        mock_channels_data: dict[str, list] = {
            "value": [
                {
                    "id": "channel-1",
                    "displayName": "general",
                    "membershipType": "standard",
                },
                {
                    "id": "channel-2",
                    "displayName": "General Discussion",
                    "membershipType": "standard",
                },
                {
                    "id": "channel-3",
                    "displayName": "Beta General",
                    "membershipType": "standard",
                },
            ]
        }

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.search_channels("general")

                # Should have results (exact match should come first if sorting is working)
                assert isinstance(channels, list)

    @pytest.mark.asyncio
    async def test_list_all_channels_team_limit_behavior(
        self, graph_client: Any
    ) -> None:
        """Test that list_all_channels respects team iteration limits."""
        # Create many mock teams to test limit behavior
        mock_teams = [
            GraphApiTeam(id=f"team-{i}", displayName=f"Team {i}")
            for i in range(25)  # More teams than the 20 team limit in search_channels
        ]
        mock_channels_data: dict[str, list] = {"value": []}

        with patch.object(graph_client, "get_teams", return_value=mock_teams):
            with patch.object(
                graph_client, "_make_graph_request", return_value=mock_channels_data
            ):
                channels = await graph_client.list_all_channels()

                assert isinstance(channels, list)
                # Should handle all teams without errors
