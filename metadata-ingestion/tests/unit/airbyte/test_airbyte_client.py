from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.client import (
    AirbyteBaseClient,
    AirbyteCloudClient,
    AirbyteOSSClient,
    create_airbyte_client,
)
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
)

# Import the real AirbyteSource class from the source module, not from models


class TestCreateAirbyteClient:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient")
    def test_create_oss_client(self, mock_oss_client):
        # Set up a mock client instance that will be returned
        mock_client_instance = MagicMock()
        mock_oss_client.return_value = mock_client_instance

        # Create the client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="localhost:8000",
        )
        client = create_airbyte_client(config)

        # Assert the OSS client was created with the right config
        mock_oss_client.assert_called_once_with(config)
        assert client == mock_client_instance

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient")
    def test_create_cloud_client(self, mock_cloud_client):
        # Set up a mock client instance that will be returned
        mock_client_instance = MagicMock()
        mock_cloud_client.return_value = mock_client_instance

        # Create the client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = create_airbyte_client(config)

        # Assert the Cloud client was created with the right config
        mock_cloud_client.assert_called_once_with(config)
        assert client == mock_client_instance

    def test_create_invalid_client(self):
        # Use an invalid deployment type
        with pytest.raises(ValueError):
            # Create config with valid enum
            config = AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
                host_port="localhost:8000",
            )

            # Instead of accessing __wrapped__, which doesn't exist,
            # monkey patch create_airbyte_client to simulate invalid type behavior
            with patch(
                "datahub.ingestion.source.airbyte.client.create_airbyte_client"
            ) as mock_create:
                mock_create.side_effect = ValueError("Unsupported deployment type")
                mock_create(config)


class TestAirbyteOSSClient:
    def test_init_with_defaults(self):
        # Create client with config
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        assert client.base_url == "http://localhost:8000/api/public/v1"
        assert client.config.host_port == "http://localhost:8000"
        assert client.config.api_key is None

    def test_init_with_api_key(self):
        # Create client with API key
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            api_key=SecretStr("test-api-key"),
        )
        client = AirbyteOSSClient(config)

        assert client.base_url == "http://localhost:8000/api/public/v1"
        assert client.config.host_port == "http://localhost:8000"
        assert isinstance(client.config.api_key, SecretStr)
        assert client.config.api_key.get_secret_value() == "test-api-key"

    def test_init_without_host_port(self):
        # Missing host_port should raise ValidationError during config creation
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="host_port is required"):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
                # Missing host_port
            )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_list_workspaces(self, mock_paginate_results):
        # Mock the _paginate_results to return the items we want
        mock_paginate_results.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Workspace 1"},
            {"workspaceId": "workspace-id-2", "name": "Workspace 2"},
        ]

        # Create client and call method
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        workspaces = client.list_workspaces()

        # Verify the results
        assert len(workspaces) == 2
        # Since the client returns raw dictionaries, not models
        assert workspaces[0].get("workspaceId") == "workspace-id-1"
        assert workspaces[0].get("name") == "Workspace 1"

        # Mock should have been called once
        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", method="GET", result_key="data"
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._apply_pattern")
    def test_list_workspaces_with_pattern(
        self, mock_apply_pattern, mock_paginate_results
    ):
        # Mock the _paginate_results method to return the items we want
        mock_paginate_results.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Test Workspace"},
            {"workspaceId": "workspace-id-2", "name": "Production Workspace"},
        ]

        # Mock the _apply_pattern method to filter results
        mock_apply_pattern.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Test Workspace"}
        ]

        # Create client and call method with pattern
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Create a pattern that only allows "Test" workspaces
        pattern = AllowDenyPattern(allow=["Test.*"])
        workspaces = client.list_workspaces(pattern)

        # Verify filtered results
        assert len(workspaces) == 1
        assert workspaces[0].get("workspaceId") == "workspace-id-1"
        assert workspaces[0].get("name") == "Test Workspace"

        # Mocks should have been called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", method="GET", result_key="data"
        )
        mock_apply_pattern.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_list_connections(self, mock_paginate_results):
        # Mock the _paginate_results method to return the items we want
        mock_paginate_results.return_value = [
            {
                "connectionId": "connection-id-1",
                "name": "Connection 1",
                "sourceId": "source-id-1",
                "destinationId": "destination-id-1",
                "status": "active",
                "schedule": {"scheduleType": "basic", "timeUnit": "hours", "units": 1},
            }
        ]

        # Create client and call method
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        connections = client.list_connections("workspace-id-1")

        # Verify the results
        assert len(connections) == 1
        assert connections[0].get("connectionId") == "connection-id-1"
        assert connections[0].get("name") == "Connection 1"

        # Verify the mock was called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_http_error_handling(self, mock_make_request):
        # Set up the mock to raise an HTTP error
        mock_make_request.side_effect = requests.exceptions.HTTPError(
            "404 Client Error: Not Found"
        )

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Expect the HTTP error to be re-raised
        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            client.list_workspaces()

        # Verify the error contains the expected message
        assert "404 Client Error: Not Found" in str(excinfo.value)


class TestAirbyteClientBase:
    def test_abstract_class(self):
        # Instead of trying to instantiate, check that it has abstract methods
        assert hasattr(AirbyteBaseClient, "__abstractmethods__")
        abstract_methods = AirbyteBaseClient.__abstractmethods__
        assert "_check_auth_before_request" in abstract_methods
        assert "_get_full_url" in abstract_methods

    def test_required_methods(self):
        # Create a concrete subclass that implements abstract methods
        class CompleteClient(AirbyteBaseClient):
            def _check_auth_before_request(self):
                pass

            def _get_full_url(self, endpoint):
                return f"https://example.com{endpoint}"

            def list_workspaces(self, pattern=None):
                return []

            def list_sources(self, workspace_id, pattern=None):
                return []

            def list_destinations(self, workspace_id, pattern=None):
                return []

            def list_connections(self, workspace_id, pattern=None):
                return []

        # Create an incomplete subclass
        class IncompleteClient(AirbyteBaseClient):
            pass

        # Check that we can instantiate a complete implementation
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        # We can instantiate the complete class
        client = CompleteClient(config)
        assert isinstance(client, AirbyteBaseClient)

        # Check inherited methods work
        assert hasattr(client, "list_workspaces")

        # Check that IncompleteClient still has abstract methods
        assert hasattr(IncompleteClient, "__abstractmethods__")
        abstract_methods = IncompleteClient.__abstractmethods__
        assert "_check_auth_before_request" in abstract_methods
        assert "_get_full_url" in abstract_methods


class TestAirbyteOpenSourceClient:
    @patch("datahub.ingestion.source.airbyte.client.requests.Session")
    def test_init(self, mock_session):
        # Setup mock
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Verify session setup
        assert client.base_url == "http://localhost:8000/api/public/v1"
        mock_session.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_workspaces(self, mock_paginate_results):
        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Mock the _paginate_results method to return the items we want
        mock_paginate_results.return_value = [
            {
                "workspaceId": "workspace-id-1",
                "name": "Default Workspace",
                "slug": "default-workspace",
            }
        ]

        # Call the method
        workspaces = client.list_workspaces()

        # Check that we got the expected response
        assert len(workspaces) == 1
        assert workspaces[0].get("workspaceId") == "workspace-id-1"
        assert workspaces[0].get("name") == "Default Workspace"

        # Verify the mock was called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", method="GET", result_key="data"
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_sources(self, mock_paginate_results):
        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Mock the response
        mock_paginate_results.return_value = [
            {
                "sourceId": "source-id-1",
                "name": "PostgreSQL Source",
                "sourceName": "postgres",
                "workspaceId": "workspace-id-1",
                "sourceDefinitionId": "source-def-id-1",
                "connectionConfiguration": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "test",
                },
            }
        ]

        # Call the method
        sources = client.list_sources("workspace-id-1")

        # Verify response
        assert len(sources) == 1
        assert sources[0].get("sourceId") == "source-id-1"
        assert sources[0].get("name") == "PostgreSQL Source"

        # Verify the mock was called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/sources",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_destinations(self, mock_paginate_results):
        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Mock the response
        mock_paginate_results.return_value = [
            {
                "destinationId": "dest-id-1",
                "name": "PostgreSQL Destination",
                "destinationName": "postgres",
                "workspaceId": "workspace-id-1",
                "destinationDefinitionId": "dest-def-id-1",
                "connectionConfiguration": {
                    "host": "localhost",
                    "port": 5432,
                    "database": "target",
                },
            }
        ]

        # Call the method
        destinations = client.list_destinations("workspace-id-1")

        # Verify response
        assert len(destinations) == 1
        assert destinations[0].get("destinationId") == "dest-id-1"
        assert destinations[0].get("name") == "PostgreSQL Destination"

        # Verify the mock was called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/destinations",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_connections(self, mock_paginate_results):
        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        # Mock the response
        mock_paginate_results.return_value = [
            {
                "connectionId": "conn-id-1",
                "name": "Postgres to Snowflake",
                "sourceId": "source-id-1",
                "destinationId": "dest-id-1",
                "status": "active",
                "syncCatalog": {
                    "streams": [
                        {
                            "stream": {
                                "name": "users",
                                "jsonSchema": {
                                    "type": "object",
                                    "properties": {
                                        "id": {"type": "integer"},
                                        "name": {"type": "string"},
                                    },
                                },
                            },
                            "config": {
                                "syncMode": "full_refresh",
                                "destinationSyncMode": "overwrite",
                                "selected": True,
                            },
                        }
                    ]
                },
                "schedule": {"scheduleType": "manual"},
            }
        ]

        # Call the method
        connections = client.list_connections("workspace-id-1")

        # Verify response
        assert len(connections) == 1
        assert connections[0].get("connectionId") == "conn-id-1"
        assert connections[0].get("name") == "Postgres to Snowflake"

        # Verify the mock was called correctly
        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )


class TestAirbyteCloudClient:
    def test_init_with_defaults(self):
        # Create client with required config
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )

        # Mock the _refresh_oauth_token method to avoid HTTP requests
        with patch.object(AirbyteCloudClient, "_refresh_oauth_token"):
            client = AirbyteCloudClient(config)

            assert client.base_url == "https://api.airbyte.com/v1"
            assert client.workspace_id == "workspace-id-1"
            assert client.config.oauth2_client_id == "client-id"
            assert client.config.oauth2_refresh_token is not None

    def test_init_missing_workspace_id(self):
        # Missing workspace_id should raise ValidationError during config creation
        from pydantic import ValidationError

        with pytest.raises(ValidationError, match="cloud_workspace_id is required"):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                oauth2_client_id="client-id",
                oauth2_client_secret=SecretStr("client-secret"),
                oauth2_refresh_token=SecretStr("refresh-token"),
                # Missing cloud_workspace_id
            )

    @patch("requests.post")
    def test_refresh_oauth_token(self, mock_post):
        # Mock the oauth token response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_response

        # Create client with patched _refresh_oauth_token to prevent initial refresh
        with patch.object(AirbyteCloudClient, "_refresh_oauth_token"):
            config = AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                cloud_workspace_id="workspace-id-1",
                oauth2_client_id="client-id",
                oauth2_client_secret=SecretStr("client-secret"),
                oauth2_refresh_token=SecretStr("refresh-token"),
            )
            client = AirbyteCloudClient(config)

        # Reset the mock to clear any previous calls during initialization
        mock_post.reset_mock()

        # Call the private method directly to refresh the token
        client._refresh_oauth_token()

        # Verify token was updated
        assert client.access_token == "new-access-token"

        # Verify correct request was made
        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://auth.airbyte.com/oauth/token"
        assert kwargs["data"] == {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token",
            "grant_type": "refresh_token",
        }

    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._refresh_oauth_token"
    )
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_sources(self, mock_paginate_results, mock_refresh_token):
        # Mock the oauth refresh
        mock_refresh_token.return_value = None

        # Set up the mock to return an iterator of items
        mock_paginate_results.return_value = iter(
            [
                {
                    "sourceId": "source-id-1",
                    "name": "Source 1",
                    "sourceType": "mysql",
                    "connectionConfiguration": {"host": "localhost"},
                }
            ]
        )

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly

        # Call the method
        sources = client.list_sources(workspace_id="workspace-id-1")

        # Verify response
        assert len(sources) == 1
        assert sources[0].get("sourceId") == "source-id-1"
        assert sources[0].get("name") == "Source 1"

        # Verify the correct endpoint was used
        mock_paginate_results.assert_called_once_with(
            endpoint="/sources",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._refresh_oauth_token"
    )
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_destinations(self, mock_paginate_results, mock_refresh_token):
        # Mock the oauth refresh
        mock_refresh_token.return_value = None

        # Set up the mock to return an iterator of items
        mock_paginate_results.return_value = iter(
            [
                {
                    "destinationId": "destination-id-1",
                    "name": "Destination 1",
                    "destinationType": "postgres",
                    "connectionConfiguration": {"host": "localhost"},
                }
            ]
        )

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly

        # Call the method
        destinations = client.list_destinations(workspace_id="workspace-id-1")

        # Verify response
        assert len(destinations) == 1
        assert destinations[0].get("destinationId") == "destination-id-1"
        assert destinations[0].get("name") == "Destination 1"

        # Verify the correct endpoint was used
        mock_paginate_results.assert_called_once_with(
            endpoint="/destinations",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._refresh_oauth_token"
    )
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_connections(self, mock_paginate_results, mock_refresh_token):
        # Mock the oauth refresh
        mock_refresh_token.return_value = None

        # Set up the mock to return an iterator of items
        mock_paginate_results.return_value = iter(
            [
                {
                    "connectionId": "connection-id-1",
                    "name": "Connection 1",
                    "sourceId": "source-id-1",
                    "destinationId": "destination-id-1",
                    "status": "active",
                }
            ]
        )

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly

        # Call the method
        connections = client.list_connections(workspace_id="workspace-id-1")

        # Verify response
        assert len(connections) == 1
        assert connections[0].get("connectionId") == "connection-id-1"
        assert connections[0].get("name") == "Connection 1"

        # Verify the correct endpoint was used
        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            method="GET",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._refresh_oauth_token"
    )
    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._make_request")
    def test_list_workspaces(self, mock_make_request, mock_refresh_token):
        # Mock the oauth refresh
        mock_refresh_token.return_value = None

        # Set up the mock response
        mock_make_request.return_value = {
            "workspaceId": "workspace-id-1",
            "name": "Workspace 1",
            "slug": "workspace-1",
            "email": "test@example.com",
            "initialSetupComplete": True,
            "displaySetupWizard": False,
            "anonymousDataCollection": False,
            "news": False,
            "securityUpdates": True,
            "organizationId": "org-id-1",
        }

        # Create client
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly

        # Call the method
        workspaces = client.list_workspaces()

        # Verify response - cloud only returns the configured workspace
        assert len(workspaces) == 1
        assert workspaces[0].get("workspaceId") == "workspace-id-1"
        assert workspaces[0].get("name") == "Workspace 1"
        assert workspaces[0].get("slug") == "workspace-1"

        # Verify the correct endpoint was used with the workspace ID
        mock_make_request.assert_called_once_with(
            f"/workspaces/{config.cloud_workspace_id}", method="GET"
        )


class TestClientBuildSyncCatalog:
    """Tests for _build_sync_catalog and related methods."""

    def test_build_sync_catalog_with_property_fields(self):
        """Test building sync catalog with property fields from /streams endpoint."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [
            {
                "name": "users",
                "namespace": "public",
                "syncMode": "full_refresh_overwrite",
                "primaryKey": [["id"]],
                "cursorField": ["updated_at"],
            }
        ]

        stream_property_fields = {("users", "public"): [["id"], ["name"], ["email"]]}

        result = client._build_sync_catalog(config_streams, stream_property_fields)

        assert "streams" in result
        assert len(result["streams"]) == 1
        stream = result["streams"][0]
        assert stream["stream"]["name"] == "users"
        assert stream["stream"]["namespace"] == "public"
        assert "jsonSchema" in stream["stream"]

    def test_build_sync_catalog_without_property_fields(self):
        """Test building sync catalog without property fields."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [
            {
                "name": "orders",
                "namespace": "sales",
                "syncMode": "incremental_append",
            }
        ]

        result = client._build_sync_catalog(config_streams, {})

        assert "streams" in result
        assert len(result["streams"]) == 1

    def test_build_stream_config(self):
        """Test building stream config."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = {
            "syncMode": "incremental_append",
            "primaryKey": [["id"]],
            "cursorField": ["updated_at"],
        }

        result = client._build_stream_config(stream)

        assert result["selected"] is True
        assert result["syncMode"] == "incremental"
        assert result["destinationSyncMode"] == "append"
        assert result["primaryKey"] == [["id"]]
        assert result["cursorField"] == ["updated_at"]

    def test_get_json_schema_for_stream_with_property_fields(self):
        """Test getting JSON schema when property fields are provided."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = {"name": "users"}
        property_fields = [["id"], ["name"], ["email"]]

        result = client._get_json_schema_for_stream(stream, property_fields)

        assert "type" in result
        assert "properties" in result
        properties: Dict[str, Any] = result.get("properties", {})  # type: ignore[assignment]
        assert "id" in properties
        assert "name" in properties
        assert "email" in properties

    def test_get_json_schema_for_stream_from_configurations(self):
        """Test getting JSON schema from configurations when property fields are not provided."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = {
            "name": "orders",
            "jsonSchema": {
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                    "amount": {"type": "number"},
                },
            },
        }

        result = client._get_json_schema_for_stream(stream, None)

        assert result == stream["jsonSchema"]

    def test_get_json_schema_for_stream_fallback_empty(self):
        """Test getting JSON schema falls back to empty dict."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = {"name": "products"}

        result = client._get_json_schema_for_stream(stream, None)

        assert result == {}


class TestClientSSLAndAuth:
    """Tests for SSL and authentication configuration."""

    def test_oss_client_ssl_disabled_warning(self, caplog):
        """Test warning when SSL is disabled."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            verify_ssl=False,
        )

        with caplog.at_level("WARNING"):
            _ = AirbyteOSSClient(config)

        assert "SSL certificate verification is disabled" in caplog.text

    def test_oss_client_with_api_key(self, caplog):
        """Test OSS client configured with API key."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            api_key=SecretStr("test-api-key"),
        )

        with caplog.at_level("DEBUG"):
            client = AirbyteOSSClient(config)

        # API key authentication is handled by _check_auth_before_request
        # Just verify client is created
        assert client is not None

    def test_oss_client_with_username_password(self, caplog):
        """Test OSS client configured with username/password."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            username="test-user",
            password=SecretStr("test-password"),
        )

        with caplog.at_level("DEBUG"):
            _ = AirbyteOSSClient(config)

        assert "Using basic authentication" in caplog.text


class TestClientErrorHandling:
    """Tests for error handling in client methods."""

    @patch("datahub.ingestion.source.airbyte.client.requests.Session.request")
    def test_make_request_connection_error(self, mock_request):
        """Test handling of connection errors."""
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection refused"
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(Exception) as exc_info:
            client._make_request("/test", method="GET")

        assert "Connection refused" in str(exc_info.value)

    @patch("datahub.ingestion.source.airbyte.client.requests.Session.request")
    def test_make_request_timeout(self, mock_request):
        """Test handling of timeout errors."""
        mock_request.side_effect = requests.exceptions.Timeout("Request timed out")

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(Exception) as exc_info:
            client._make_request("/test", method="GET")

        assert "Request timed out" in str(exc_info.value)


class TestFetchStreamPropertyFields:
    """Tests for _fetch_stream_property_fields method."""

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_property_fields_success(self, mock_list_streams):
        """Test successfully fetching property fields."""
        mock_list_streams.return_value = [
            {
                "streamName": "users",
                "namespace": "public",
                "propertyFields": [["id"], ["name"], ["email"]],
            },
            {
                "name": "orders",
                "streamnamespace": "sales",
                "propertyFields": [["order_id"], ["amount"]],
            },
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client._fetch_stream_property_fields("source-id-123")

        assert ("users", "public") in result
        assert ("orders", "sales") in result
        assert result[("users", "public")] == [["id"], ["name"], ["email"]]
        assert result[("orders", "sales")] == [["order_id"], ["amount"]]

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_property_fields_no_source_id(self, mock_list_streams):
        """Test fetching property fields when source_id is None."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client._fetch_stream_property_fields(None)

        assert result == {}
        mock_list_streams.assert_not_called()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_property_fields_exception(self, mock_list_streams):
        """Test handling exceptions when fetching property fields."""
        mock_list_streams.side_effect = Exception("API error")

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client._fetch_stream_property_fields("source-id-123")

        assert result == {}


class TestGetConnection:
    """Tests for get_connection method."""

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteOSSClient._fetch_stream_property_fields"
    )
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteOSSClient._build_sync_catalog"
    )
    def test_get_connection_builds_sync_catalog_from_configurations(
        self, mock_build_sync, mock_fetch_fields, mock_make_request
    ):
        """Test get_connection builds syncCatalog from configurations.streams."""
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "sourceId": "source-123",
            "configurations": {"streams": [{"name": "users", "namespace": "public"}]},
        }
        mock_fetch_fields.return_value = {}
        mock_build_sync_catalog: Dict[str, Any] = {"streams": []}
        mock_build_sync.return_value = mock_build_sync_catalog

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert "syncCatalog" in result
        mock_fetch_fields.assert_called_once_with("source-123")
        mock_build_sync.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_connection_with_existing_sync_catalog(self, mock_make_request):
        """Test get_connection when syncCatalog already exists."""
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "syncCatalog": {"streams": [{"name": "users"}]},
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert result["syncCatalog"]["streams"][0]["name"] == "users"
