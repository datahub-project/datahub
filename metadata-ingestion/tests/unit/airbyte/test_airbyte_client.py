from typing import Any, Dict
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import SecretStr, ValidationError
from requests.auth import HTTPBasicAuth

from datahub.configuration.common import AllowDenyPattern
from datahub.ingestion.source.airbyte.client import (
    AirbyteApiError,
    AirbyteAuthenticationError,
    AirbyteBaseClient,
    AirbyteCloudClient,
    AirbyteOSSClient,
    create_airbyte_client,
)
from datahub.ingestion.source.airbyte.config import (
    AirbyteClientConfig,
    AirbyteDeploymentType,
    OAuth2GrantType,
)
from datahub.ingestion.source.airbyte.models import (
    AirbyteConfigStreamRef,
    AirbyteStreamApiMetadata,
    AirbyteSyncCatalog,
    PropertyFieldPath,
    StreamIdentifier,
    SyncCatalogBuildResult,
)


class TestCreateAirbyteClient:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient")
    def test_create_oss_client(self, mock_oss_client):
        mock_client_instance = MagicMock()
        mock_oss_client.return_value = mock_client_instance

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="localhost:8000",
        )
        client = create_airbyte_client(config)

        mock_oss_client.assert_called_once_with(config)
        assert client == mock_client_instance

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient")
    def test_create_cloud_client(self, mock_cloud_client):
        mock_client_instance = MagicMock()
        mock_cloud_client.return_value = mock_client_instance

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = create_airbyte_client(config)

        mock_cloud_client.assert_called_once_with(config)
        assert client == mock_client_instance

    def test_create_invalid_client(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="localhost:8000",
        )
        # Force an invalid deployment type by bypassing pydantic validation
        object.__setattr__(config, "deployment_type", "invalid_type")
        with pytest.raises(ValueError, match="Invalid deployment type"):
            create_airbyte_client(config)


class TestAirbyteOSSClient:
    def test_init_with_defaults(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        assert client.base_url == "http://localhost:8000/api/public/v1"
        assert client.config.host_port == "http://localhost:8000"
        assert client.config.api_key is None

    def test_init_with_api_key(self):
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
        with pytest.raises(ValidationError, match="host_port is required"):
            AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_list_workspaces(self, mock_paginate_results):
        mock_paginate_results.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Workspace 1"},
            {"workspaceId": "workspace-id-2", "name": "Workspace 2"},
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        workspaces = client.list_workspaces()

        assert len(workspaces) == 2
        assert workspaces[0].workspace_id == "workspace-id-1"
        assert workspaces[0].name == "Workspace 1"

        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", result_key="data"
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    @patch("datahub.ingestion.source.airbyte.client.apply_pattern")
    def test_list_workspaces_with_pattern(
        self, mock_apply_pattern, mock_paginate_results
    ):
        mock_paginate_results.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Test Workspace"},
            {"workspaceId": "workspace-id-2", "name": "Production Workspace"},
        ]

        mock_apply_pattern.return_value = [
            {"workspaceId": "workspace-id-1", "name": "Test Workspace"}
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        pattern = AllowDenyPattern(allow=["Test.*"])
        workspaces = client.list_workspaces(pattern)

        assert len(workspaces) == 1
        assert workspaces[0].workspace_id == "workspace-id-1"
        assert workspaces[0].name == "Test Workspace"

        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", result_key="data"
        )
        mock_apply_pattern.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_list_connections(self, mock_paginate_results):
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
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        connections = client.list_connections("workspace-id-1")
        assert len(connections) == 1
        assert connections[0].connection_id == "connection-id-1"
        assert connections[0].name == "Connection 1"
        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_list_connections_skips_inactive_by_default(self, mock_paginate_results):
        mock_paginate_results.return_value = [
            {
                "connectionId": "active-id",
                "name": "Active Connection",
                "sourceId": "source-id-1",
                "destinationId": "destination-id-1",
                "status": "active",
            },
            {
                "connectionId": "inactive-id",
                "name": "Inactive Connection",
                "sourceId": "source-id-2",
                "destinationId": "destination-id-2",
                "status": "inactive",
            },
        ]
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        connections = client.list_connections("workspace-id-1")
        assert [c.connection_id for c in connections] == ["active-id"]

        connections = client.list_connections("workspace-id-1", include_inactive=True)
        assert [c.connection_id for c in connections] == ["active-id", "inactive-id"]

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_http_error_handling(self, mock_make_request):
        mock_make_request.side_effect = requests.exceptions.HTTPError(
            "404 Client Error: Not Found"
        )
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(requests.exceptions.HTTPError) as excinfo:
            client.list_workspaces()

        assert "404 Client Error: Not Found" in str(excinfo.value)


class TestAirbyteClientBase:
    def test_abstract_class(self):
        assert hasattr(AirbyteBaseClient, "__abstractmethods__")
        abstract_methods = AirbyteBaseClient.__abstractmethods__
        assert "_check_auth_before_request" in abstract_methods
        assert "_get_full_url" in abstract_methods

    def test_required_methods(self):
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

            def list_connections(
                self, workspace_id, pattern=None, include_inactive=False
            ):
                return []

        class IncompleteClient(AirbyteBaseClient):
            pass

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )

        client = CompleteClient(config)
        assert isinstance(client, AirbyteBaseClient)
        assert hasattr(client, "list_workspaces")

        assert hasattr(IncompleteClient, "__abstractmethods__")
        abstract_methods = IncompleteClient.__abstractmethods__
        assert "_check_auth_before_request" in abstract_methods
        assert "_get_full_url" in abstract_methods


class TestAirbyteOpenSourceClient:
    @patch("datahub.ingestion.source.airbyte.client.requests.Session")
    def test_init(self, mock_session):
        mock_session_instance = MagicMock()
        mock_session.return_value = mock_session_instance
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        assert client.base_url == "http://localhost:8000/api/public/v1"
        mock_session.assert_called_once()

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_workspaces(self, mock_paginate_results):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        mock_paginate_results.return_value = [
            {
                "workspaceId": "workspace-id-1",
                "name": "Default Workspace",
                "slug": "default-workspace",
            }
        ]
        workspaces = client.list_workspaces()
        assert len(workspaces) == 1
        assert workspaces[0].workspace_id == "workspace-id-1"
        assert workspaces[0].name == "Default Workspace"
        mock_paginate_results.assert_called_once_with(
            endpoint="/workspaces", result_key="data"
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_sources(self, mock_paginate_results):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
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
        sources = client.list_sources("workspace-id-1")
        assert len(sources) == 1
        assert sources[0].source_id == "source-id-1"
        assert sources[0].name == "PostgreSQL Source"
        mock_paginate_results.assert_called_once_with(
            endpoint="/sources",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_destinations(self, mock_paginate_results):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
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
        destinations = client.list_destinations("workspace-id-1")
        assert len(destinations) == 1
        assert destinations[0].destination_id == "dest-id-1"
        assert destinations[0].name == "PostgreSQL Destination"
        mock_paginate_results.assert_called_once_with(
            endpoint="/destinations",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._paginate_results")
    def test_get_connections(self, mock_paginate_results):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
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
        connections = client.list_connections("workspace-id-1")
        assert len(connections) == 1
        assert connections[0].connection_id == "conn-id-1"
        assert connections[0].name == "Postgres to Snowflake"
        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )


class TestAirbyteCloudClient:
    def test_init_with_defaults(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )

        # Mock the _acquire_token method to avoid HTTP requests
        with patch.object(AirbyteCloudClient, "_acquire_token"):
            client = AirbyteCloudClient(config)

            assert client.base_url == "https://api.airbyte.com/v1"
            assert client.workspace_id == "workspace-id-1"
            assert client.config.oauth2_client_id == "client-id"
            assert client.config.oauth2_refresh_token is not None

    def test_init_missing_workspace_id(self):
        # Missing workspace_id should raise ValidationError during config creation
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
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "new-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_response

        # Patch out the auto token fetch in the constructor so we can drive
        # the refresh path explicitly below.
        with patch.object(AirbyteCloudClient, "_acquire_token"):
            config = AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                cloud_workspace_id="workspace-id-1",
                oauth2_client_id="client-id",
                oauth2_client_secret=SecretStr("client-secret"),
                oauth2_refresh_token=SecretStr("refresh-token"),
            )
            client = AirbyteCloudClient(config)

        mock_post.reset_mock()

        client._refresh_oauth_token()

        assert client.access_token == "new-access-token"

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://auth.airbyte.com/oauth/token"
        assert kwargs["data"] == {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "refresh_token": "refresh-token",
            "grant_type": "refresh_token",
        }

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._acquire_token")
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_sources(self, mock_paginate_results, mock_acquire_token):
        mock_acquire_token.return_value = None

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
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly
        sources = client.list_sources(workspace_id="workspace-id-1")
        assert len(sources) == 1
        assert sources[0].source_id == "source-id-1"
        assert sources[0].name == "Source 1"

        mock_paginate_results.assert_called_once_with(
            endpoint="/sources",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._acquire_token")
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_destinations(self, mock_paginate_results, mock_acquire_token):
        mock_acquire_token.return_value = None

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
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly
        destinations = client.list_destinations(workspace_id="workspace-id-1")
        assert len(destinations) == 1
        assert destinations[0].destination_id == "destination-id-1"
        assert destinations[0].name == "Destination 1"

        mock_paginate_results.assert_called_once_with(
            endpoint="/destinations",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._acquire_token")
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteCloudClient._paginate_results"
    )
    def test_get_connections(self, mock_paginate_results, mock_acquire_token):
        mock_acquire_token.return_value = None

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
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"  # Set the access token directly
        connections = client.list_connections(workspace_id="workspace-id-1")
        assert len(connections) == 1
        assert connections[0].connection_id == "connection-id-1"
        assert connections[0].name == "Connection 1"

        mock_paginate_results.assert_called_once_with(
            endpoint="/connections",
            params={"workspaceId": "workspace-id-1"},
            result_key="data",
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._acquire_token")
    @patch("datahub.ingestion.source.airbyte.client.AirbyteCloudClient._make_request")
    def test_list_workspaces(self, mock_make_request, mock_acquire_token):
        mock_acquire_token.return_value = None

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
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            oauth2_refresh_token=SecretStr("refresh-token"),
        )
        client = AirbyteCloudClient(config)
        client.access_token = "test-token"
        workspaces = client.list_workspaces()

        # Cloud only ever exposes the single configured workspace.
        assert len(workspaces) == 1
        assert workspaces[0].workspace_id == "workspace-id-1"
        assert workspaces[0].name == "Workspace 1"

        mock_make_request.assert_called_once_with(
            f"/workspaces/{config.cloud_workspace_id}"
        )

    @patch("requests.post")
    def test_client_credentials_token(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "client-creds-access-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_response

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
        )
        client = AirbyteCloudClient(config)

        assert client.access_token == "client-creds-access-token"

        mock_post.assert_called_once()
        args, kwargs = mock_post.call_args
        assert args[0] == "https://auth.airbyte.com/oauth/token"
        assert kwargs["data"] == {
            "client_id": "client-id",
            "client_secret": "client-secret",
            "grant_type": "client_credentials",
        }

    def test_client_credentials_missing_refresh_token(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
            # No refresh token -> grant type should fall through to client_credentials.
        )

        with patch.object(AirbyteCloudClient, "_acquire_token"):
            client = AirbyteCloudClient(config)
            assert client.config.oauth2_grant_type == OAuth2GrantType.CLIENT_CREDENTIALS
            assert client.config.oauth2_refresh_token is None

    @patch("time.time")
    @patch("requests.post")
    def test_token_auto_refresh_client_credentials(self, mock_post, mock_time):
        mock_time.return_value = 1000

        initial_response = MagicMock()
        initial_response.status_code = 200
        initial_response.json.return_value = {
            "access_token": "initial-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        refreshed_response = MagicMock()
        refreshed_response.status_code = 200
        refreshed_response.json.return_value = {
            "access_token": "refreshed-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }

        mock_post.side_effect = [initial_response, refreshed_response]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.CLOUD,
            cloud_workspace_id="workspace-id-1",
            oauth2_client_id="client-id",
            oauth2_client_secret=SecretStr("client-secret"),
        )
        client = AirbyteCloudClient(config)

        assert client.access_token == "initial-token"

        # Jump past the token's 1-hour expiry so the next check triggers a refresh.
        mock_time.return_value = 4000

        client._check_token_expiry()

        assert client.access_token == "refreshed-token"
        assert mock_post.call_count == 2

    @patch("requests.post")
    @patch("requests.Session.get")
    def test_retry_on_401_with_client_credentials(self, mock_get, mock_post):
        mock_token_response = MagicMock()
        mock_token_response.status_code = 200
        mock_token_response.json.return_value = {
            "access_token": "new-token",
            "expires_in": 3600,
            "token_type": "Bearer",
        }
        mock_post.return_value = mock_token_response

        mock_401_response = MagicMock()
        mock_401_response.status_code = 401
        mock_401_response.raise_for_status.side_effect = requests.HTTPError(
            response=mock_401_response
        )

        mock_success_response = MagicMock()
        mock_success_response.status_code = 200
        mock_success_response.json.return_value = {"workspaceId": "workspace-id-1"}

        mock_get.side_effect = [mock_401_response, mock_success_response]

        with patch.object(AirbyteCloudClient, "_acquire_token"):
            config = AirbyteClientConfig(
                deployment_type=AirbyteDeploymentType.CLOUD,
                cloud_workspace_id="workspace-id-1",
                oauth2_client_id="client-id",
                oauth2_client_secret=SecretStr("client-secret"),
            )
            client = AirbyteCloudClient(config)

        result = client._make_request("/workspaces/workspace-id-1")

        assert result == {"workspaceId": "workspace-id-1"}
        assert mock_get.call_count == 2
        mock_post.assert_called_once()


class TestClientBuildSyncCatalog:
    def test_build_sync_catalog_with_property_fields(self):
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

        stream_api_metadata = AirbyteStreamApiMetadata(
            property_fields_by_stream={
                StreamIdentifier(stream_name="users", namespace="public"): [
                    PropertyFieldPath(path=["id"]),
                    PropertyFieldPath(path=["name"]),
                    PropertyFieldPath(path=["email"]),
                ]
            }
        )

        build_result = client._build_sync_catalog(config_streams, stream_api_metadata)

        assert len(build_result.catalog.streams) == 1
        stream = build_result.catalog.streams[0]
        assert stream.stream.name == "users"
        assert stream.stream.namespace == "public"
        assert stream.stream.json_schema

    def test_build_sync_catalog_without_property_fields(self):
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

        build_result = client._build_sync_catalog(
            config_streams, AirbyteStreamApiMetadata()
        )

        assert len(build_result.catalog.streams) == 1

    def test_build_sync_catalog_backfills_namespace_from_streams_api(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [
            {
                "name": "events",
                "syncMode": "full_refresh_overwrite",
            }
        ]
        stream_api_metadata = AirbyteStreamApiMetadata(
            property_fields_by_stream={
                StreamIdentifier(stream_name="events", namespace="my_schema"): [
                    PropertyFieldPath(path=["id"]),
                ]
            },
            namespaces_by_name={"events": ["my_schema"]},
        )

        build_result = client._build_sync_catalog(config_streams, stream_api_metadata)

        stream = build_result.catalog.streams[0]
        assert stream.stream.name == "events"
        assert stream.stream.namespace == "my_schema"
        properties: Dict[str, Any] = stream.stream.json_schema.get("properties", {})
        assert "id" in properties

    def test_build_sync_catalog_refuses_to_pair_same_named_streams_by_order(self):
        """Config order and /streams discovery order are unrelated, so two
        same-named streams must stay unnamespaced rather than be guessed."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [
            {"name": "users", "syncMode": "full_refresh_overwrite"},
            {"name": "users", "syncMode": "full_refresh_overwrite"},
        ]
        stream_api_metadata = AirbyteStreamApiMetadata(
            namespaces_by_name={"users": ["public", "analytics"]},
        )

        build_result = client._build_sync_catalog(config_streams, stream_api_metadata)

        assert [s.stream.namespace for s in build_result.catalog.streams] == [
            None,
            None,
        ]
        assert build_result.ambiguous == {"users": ["public", "analytics"]}

    def test_build_sync_catalog_deduces_namespace_claimed_by_no_sibling(self):
        """An explicitly-namespaced sibling accounts for one candidate, leaving
        exactly one for the unnamed stream — deducible, so not ambiguous."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [{"name": "users", "namespace": "public"}, {"name": "users"}],
            AirbyteStreamApiMetadata(
                namespaces_by_name={"users": ["public", "analytics"]}
            ),
        )

        assert [s.stream.namespace for s in build_result.catalog.streams] == [
            "public",
            "analytics",
        ]
        assert build_result.ambiguous == {}

    def test_build_sync_catalog_leaves_unnamed_stream_when_siblings_claim_all(self):
        """Reusing the only discovered namespace would collide with the sibling
        that already declares it, so the unnamed stream keeps none."""
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [{"name": "users", "namespace": "public"}, {"name": "users"}],
            AirbyteStreamApiMetadata(namespaces_by_name={"users": ["public"]}),
        )

        assert [s.stream.namespace for s in build_result.catalog.streams] == [
            "public",
            None,
        ]
        assert build_result.ambiguous == {}

    def test_build_sync_catalog_skips_partial_multi_schema_backfill(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [{"name": "users", "syncMode": "full_refresh_overwrite"}]
        stream_api_metadata = AirbyteStreamApiMetadata(
            namespaces_by_name={"users": ["public", "analytics"]},
        )

        build_result = client._build_sync_catalog(config_streams, stream_api_metadata)

        assert build_result.catalog.streams[0].stream.namespace is None
        assert build_result.ambiguous == {"users": ["public", "analytics"]}

    def test_build_sync_catalog_broadcasts_single_namespace_to_many_unnamed(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [{"name": "events"}, {"name": "events"}],
            AirbyteStreamApiMetadata(namespaces_by_name={"events": ["my_schema"]}),
        )

        assert [s.stream.namespace for s in build_result.catalog.streams] == [
            "my_schema",
            "my_schema",
        ]
        assert build_result.ambiguous == {}

    def test_build_sync_catalog_leaves_unknown_stream_name_unnamespaced(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [{"name": "events"}],
            AirbyteStreamApiMetadata(namespaces_by_name={"orders": ["sales"]}),
        )

        assert build_result.catalog.streams[0].stream.namespace is None
        assert build_result.ambiguous == {}

    def test_build_sync_catalog_prefers_config_namespace_over_backfill(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        config_streams = [{"name": "users", "namespace": "explicit_schema"}]
        build_result = client._build_sync_catalog(
            config_streams,
            AirbyteStreamApiMetadata(
                namespaces_by_name={"users": ["streams_api_schema"]}
            ),
        )

        assert build_result.catalog.streams[0].stream.namespace == "explicit_schema"
        assert build_result.ambiguous == {}

    def test_build_sync_catalog_coerces_non_string_stream_name(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [{"name": 123, "namespace": None}], AirbyteStreamApiMetadata()
        )

        assert build_result.catalog.streams[0].stream.name == "123"
        assert build_result.catalog.streams[0].stream.namespace is None

    def test_build_sync_catalog_normalizes_off_spec_stream_shapes(self):
        """Off-spec scalars/flat lists degrade to a best-effort stream.

        Airbyte declares primaryKey as string[][] and cursorField as string[],
        but payloads in the wild flatten them. Pydantic will not widen a scalar
        into a list on its own, and rejecting the entry would cost the whole
        connection.
        """
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [
                {
                    "name": "events",
                    "primaryKey": ["id"],
                    "cursorField": "updated_at",
                    "jsonSchema": "not-a-schema",
                    "selectedFields": {"fieldPath": ["id"]},
                },
                {
                    "name": "orders",
                    "primaryKey": [None, ["id"]],
                    "cursorField": None,
                },
                {"name": "refunds", "primaryKey": 7, "cursorField": 7},
            ],
            AirbyteStreamApiMetadata(),
        )

        assert build_result.skipped_stream_payloads == []
        events, orders, refunds = build_result.catalog.streams
        assert events.stream.name == "events"
        assert events.stream.json_schema == {}
        assert events.config.primary_key == [["id"]]
        assert events.config.cursor_field == ["updated_at"]
        assert orders.config.primary_key == [["id"]]
        assert orders.config.cursor_field == []
        assert refunds.config.primary_key == [["7"]]
        assert refunds.config.cursor_field == ["7"]

    def test_build_sync_catalog_skips_unreadable_stream_without_losing_others(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        build_result = client._build_sync_catalog(
            [
                {"name": "events"},
                {"name": "orders", "fieldSelectionEnabled": "not-a-bool"},
            ],
            AirbyteStreamApiMetadata(),
        )

        assert [s.stream.name for s in build_result.catalog.streams] == ["events"]
        assert len(build_result.skipped_stream_payloads) == 1
        assert "orders" in build_result.skipped_stream_payloads[0]

    def test_build_stream_config(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = AirbyteConfigStreamRef.model_validate(
            {
                "syncMode": "incremental_append",
                "primaryKey": [["id"]],
                "cursorField": ["updated_at"],
            }
        )

        result = client._build_stream_config(stream)

        assert result.selected is True
        assert result.sync_mode == "incremental"
        assert result.destination_sync_mode == "append"
        assert result.primary_key == [["id"]]
        assert result.cursor_field == ["updated_at"]

    @pytest.mark.parametrize(
        "sync_mode,expected_source,expected_destination",
        [
            # The source mode is `full_refresh`, not `full`: splitting on the
            # first "_" would mis-read every full_refresh_* value.
            ("full_refresh_overwrite", "full_refresh", "overwrite"),
            ("full_refresh_append", "full_refresh", "append"),
            ("incremental_append", "incremental", "append"),
            ("incremental_deduped_history", "incremental", "deduped_history"),
            (None, "full_refresh", "overwrite"),
            # Airbyte's marker for a disabled stream must survive intact so
            # `is_enabled()` still sees it.
            ("null", "null", "overwrite"),
        ],
    )
    def test_build_stream_config_splits_sync_mode(
        self, sync_mode, expected_source, expected_destination
    ):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client._build_stream_config(
            AirbyteConfigStreamRef.model_validate({"syncMode": sync_mode})
        )

        assert result.sync_mode == expected_source
        assert result.destination_sync_mode == expected_destination

    def test_get_json_schema_for_stream_with_property_fields(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = AirbyteConfigStreamRef.model_validate({"name": "users"})
        property_fields = [
            PropertyFieldPath(path=["id"]),
            PropertyFieldPath(path=["name"]),
            PropertyFieldPath(path=["email"]),
        ]

        result = client._get_json_schema_for_stream(stream, property_fields)

        assert "type" in result
        assert "properties" in result
        properties: Dict[str, Any] = result.get("properties", {})  # type: ignore[assignment]
        assert "id" in properties
        assert "name" in properties
        assert "email" in properties

    def test_get_json_schema_for_stream_from_configurations(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        json_schema = {
            "type": "object",
            "properties": {
                "order_id": {"type": "integer"},
                "amount": {"type": "number"},
            },
        }
        stream = AirbyteConfigStreamRef.model_validate(
            {"name": "orders", "jsonSchema": json_schema}
        )

        result = client._get_json_schema_for_stream(stream, None)

        assert result == json_schema

    def test_get_json_schema_for_stream_fallback_empty(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        stream = AirbyteConfigStreamRef.model_validate({"name": "products"})

        result = client._get_json_schema_for_stream(stream, None)

        assert result == {}


class TestClientSSLAndAuth:
    def test_oss_client_ssl_disabled_warning(self, caplog):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            verify_ssl=False,
        )

        with caplog.at_level("WARNING"):
            _ = AirbyteOSSClient(config)

        assert "SSL certificate verification is disabled" in caplog.text

    def test_oss_client_with_api_key(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            api_key=SecretStr("test-api-key"),
        )

        # API key auth is applied lazily via _check_auth_before_request,
        # so just check the client constructs cleanly with the secret set.
        client = AirbyteOSSClient(config)
        assert client is not None

    def test_oss_client_with_username_password(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            username="test-user",
            password=SecretStr("test-password"),
        )

        client = AirbyteOSSClient(config)

        assert isinstance(client.session.auth, HTTPBasicAuth)
        assert client.session.auth.username == "test-user"

    @patch("datahub.ingestion.source.airbyte.client.requests.post")
    def test_oss_client_with_oauth2(self, mock_post):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "access_token": "test-access-token-12345",
            "token_type": "Bearer",
            "expires_in": 3600,
        }
        mock_post.return_value = mock_response

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            oauth2_client_id="test-client-id",
            oauth2_client_secret=SecretStr("test-client-secret"),
        )

        client = AirbyteOSSClient(config)

        assert "Authorization" in client.session.headers
        assert (
            client.session.headers["Authorization"] == "Bearer test-access-token-12345"
        )

        mock_post.assert_called_once()
        call_args = mock_post.call_args
        assert "applications/token" in call_args[0][0]
        assert call_args[1]["data"]["grant_type"] == "client_credentials"
        assert call_args[1]["data"]["client_id"] == "test-client-id"
        assert call_args[1]["data"]["client_secret"] == "test-client-secret"


class TestClientErrorHandling:
    @patch("datahub.ingestion.source.airbyte.client.requests.Session.request")
    def test_make_request_connection_error(self, mock_request):
        mock_request.side_effect = requests.exceptions.ConnectionError(
            "Connection refused"
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(Exception) as exc_info:
            client._make_request("/test")

        assert "Connection refused" in str(exc_info.value)

    @patch("datahub.ingestion.source.airbyte.client.requests.Session.request")
    def test_make_request_timeout(self, mock_request):
        mock_request.side_effect = requests.exceptions.Timeout("Request timed out")

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(Exception) as exc_info:
            client._make_request("/test")

        assert "Request timed out" in str(exc_info.value)

    @pytest.mark.parametrize("status_code", [401, 403])
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._do_get")
    def test_make_request_raises_authentication_error_on_auth_status(
        self, mock_do_get, status_code
    ):
        response = MagicMock()
        response.status_code = status_code
        response.text = "Unauthorized"
        response.json.side_effect = ValueError("not json")
        mock_do_get.side_effect = requests.HTTPError(
            f"{status_code} Client Error", response=response
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(AirbyteAuthenticationError) as exc_info:
            client._make_request("/streams")

        assert exc_info.value.status_code == status_code


class TestFetchStreamApiMetadata:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_property_fields(self, mock_list_streams):
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

        result = client._fetch_stream_api_metadata(
            "source-id-123"
        ).property_fields_by_stream

        users_stream = StreamIdentifier(stream_name="users", namespace="public")
        orders_stream = StreamIdentifier(stream_name="orders", namespace="sales")

        assert users_stream in result
        assert orders_stream in result
        assert result[users_stream] == [
            PropertyFieldPath(path=["id"]),
            PropertyFieldPath(path=["name"]),
            PropertyFieldPath(path=["email"]),
        ]
        assert result[orders_stream] == [
            PropertyFieldPath(path=["order_id"]),
            PropertyFieldPath(path=["amount"]),
        ]

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_unique_namespaces(self, mock_list_streams):
        # `streamName` + `streamnamespace` (all lowercase) is what the Public API
        # StreamProperties schema actually emits.
        mock_list_streams.return_value = [
            {
                "streamName": "events",
                "streamnamespace": "my_schema",
                "propertyFields": [["id"]],
            },
            {
                "streamName": "orders",
                "streamnamespace": "my_schema",
            },
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert metadata.namespaces_by_name == {
            "events": ["my_schema"],
            "orders": ["my_schema"],
        }
        assert (
            StreamIdentifier(stream_name="events", namespace="my_schema")
            in metadata.property_fields_by_stream
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_preserves_multi_schema_order(
        self, mock_list_streams
    ):
        mock_list_streams.return_value = [
            {
                "streamName": "users",
                "streamnamespace": "public",
                "propertyFields": [["id"]],
            },
            {
                "streamName": "users",
                "streamnamespace": "analytics",
                "propertyFields": [["id"]],
            },
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert metadata.namespaces_by_name == {"users": ["public", "analytics"]}

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_empty_alias_falls_through(
        self, mock_list_streams
    ):
        # Some versions emit the preferred key as an empty string; the row must
        # fall through to the alternate keys instead of being dropped.
        mock_list_streams.return_value = [
            {
                "streamName": "",
                "name": "users",
                "namespace": "",
                "streamNamespace": "public",
                "propertyFields": [["id"]],
            }
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert metadata.namespaces_by_name == {"users": ["public"]}
        assert (
            StreamIdentifier(stream_name="users", namespace="public")
            in metadata.property_fields_by_stream
        )

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_no_source_id(self, mock_list_streams):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata(None)

        assert metadata.property_fields_by_stream == {}
        assert metadata.namespaces_by_name == {}
        mock_list_streams.assert_not_called()

    @pytest.mark.parametrize(
        "error,expected_status",
        [
            (
                AirbyteApiError(
                    "Airbyte API request failed: 404 - /streams endpoint missing",
                    status_code=404,
                ),
                404,
            ),
            (
                # Message mentions 404 but status_code is unset (retry exhaustion /
                # connection error). Must still mark unavailable; status stays None.
                AirbyteApiError(
                    "Airbyte API request failed: 500 - upstream returned code=404 in body"
                ),
                None,
            ),
            (
                AirbyteApiError("500 Internal Server Error", status_code=500),
                500,
            ),
            (
                AirbyteApiError(
                    "Error connecting to Airbyte API: Max retries exceeded with url: "
                    "/api/public/v1/streams (Caused by ResponseError('too many 500 "
                    "error responses'))"
                ),
                None,
            ),
        ],
    )
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_api_error_returns_unavailable(
        self, mock_list_streams, error, expected_status
    ):
        mock_list_streams.side_effect = error

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-id-123")

        assert metadata.unavailable is True
        assert metadata.unavailable_status_code == expected_status
        assert metadata.unavailable_message == str(error)
        assert metadata.property_fields_by_stream == {}
        assert metadata.namespaces_by_name == {}
        assert metadata.namespaces_absent is False

    @pytest.mark.parametrize("status_code", [400, 422])
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_client_error_reraises(
        self, mock_list_streams, status_code
    ):
        mock_list_streams.side_effect = AirbyteApiError(
            f"Airbyte API request failed: {status_code} - bad request",
            status_code=status_code,
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(AirbyteApiError) as exc_info:
            client._fetch_stream_api_metadata("source-id-123")

        assert exc_info.value.status_code == status_code
        assert client._stream_api_metadata_cache.get("source-id-123") is None

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_invalid_json_reraises(self, mock_list_streams):
        # 2xx with a non-JSON body keeps the HTTP status, so it must not degrade
        # to unavailable the way a connection error (status_code=None) does.
        mock_list_streams.side_effect = AirbyteApiError(
            "Airbyte API returned invalid JSON (HTTP 200)", status_code=200
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(AirbyteApiError) as exc_info:
            client._fetch_stream_api_metadata("source-id-123")

        assert exc_info.value.status_code == 200
        assert not AirbyteOSSClient._is_streams_api_unavailable_error(exc_info.value)

    @patch("datahub.ingestion.source.airbyte.client.requests.Session.get")
    def test_do_get_invalid_json_preserves_http_status(self, mock_get):
        response = MagicMock()
        response.status_code = 200
        response.raise_for_status.return_value = None
        response.json.side_effect = ValueError("Expecting value")
        mock_get.return_value = response

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(AirbyteApiError) as exc_info:
            client._do_get("http://localhost:8000/api/public/v1/streams")

        assert exc_info.value.status_code == 200
        assert "invalid JSON" in str(exc_info.value)

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_flags_namespaceless_response(
        self, mock_list_streams
    ):
        # Airbyte below 1.7.0 describes the streams but has no namespace field,
        # which the source can only warn about if the client flags it.
        mock_list_streams.return_value = [
            {"streamName": "users", "propertyFields": [["id"]]}
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-id-123")

        assert metadata.namespaces_by_name == {}
        assert metadata.namespaces_absent is True
        assert metadata.unavailable is False

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_caches_per_source(self, mock_list_streams):
        mock_list_streams.return_value = [
            {"streamName": "users", "streamnamespace": "public"}
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        first = client._fetch_stream_api_metadata("source-1")
        second = client._fetch_stream_api_metadata("source-1")
        client._fetch_stream_api_metadata("source-2")

        assert first == second
        assert mock_list_streams.call_count == 2

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_normalizes_scalar_property_fields(
        self, mock_list_streams
    ):
        mock_list_streams.return_value = [
            {
                "streamName": "users",
                "streamnamespace": "public",
                "propertyFields": "id",
            },
            {"streamName": "orders", "streamnamespace": "sales", "propertyFields": 42},
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert metadata.skipped_rows == []
        assert metadata.namespaces_by_name == {"users": ["public"], "orders": ["sales"]}
        assert metadata.property_fields_by_stream[
            StreamIdentifier(stream_name="users", namespace="public")
        ] == [PropertyFieldPath(path=["id"])]

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_skips_unreadable_row(self, mock_list_streams):
        mock_list_streams.return_value = [
            "not-an-object",
            {"streamName": "users", "streamnamespace": "public"},
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert len(metadata.skipped_rows) == 1
        assert metadata.skipped_rows[0].startswith("/streams[0]")
        assert metadata.namespaces_by_name == {"users": ["public"]}

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_does_not_flag_unreadable_response(
        self, mock_list_streams
    ):
        # No namespace survives, but the cause is the payloads rather than the
        # Airbyte version, so the version warning must not claim this one.
        mock_list_streams.return_value = ["not-an-object", 7]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        metadata = client._fetch_stream_api_metadata("source-1")

        assert len(metadata.skipped_rows) == 2
        assert metadata.namespaces_by_name == {}
        assert metadata.namespaces_absent is False

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_fetch_stream_api_metadata_reraises_authentication_error(
        self, mock_list_streams
    ):
        mock_list_streams.side_effect = AirbyteAuthenticationError(
            "Failed to get OAuth2 token: HTTP 401 - contains 404 in noise",
            status_code=401,
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        with pytest.raises(AirbyteAuthenticationError, match="401"):
            client._fetch_stream_api_metadata("source-id-123")


class TestGetConnection:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteOSSClient._fetch_stream_api_metadata"
    )
    @patch(
        "datahub.ingestion.source.airbyte.client.AirbyteOSSClient._build_sync_catalog"
    )
    def test_get_connection_builds_sync_catalog_from_configurations(
        self, mock_build_sync, mock_fetch_metadata, mock_make_request
    ):
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "sourceId": "source-123",
            "destinationId": "dest-123",
            "configurations": {"streams": [{"name": "users", "namespace": "public"}]},
        }
        mock_fetch_metadata.return_value = AirbyteStreamApiMetadata()
        mock_build_sync.return_value = SyncCatalogBuildResult(
            catalog=AirbyteSyncCatalog(streams=[])
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert result.sync_catalog is not None
        assert result.ambiguous_stream_namespaces == {}
        mock_fetch_metadata.assert_called_once_with("source-123")
        mock_build_sync.assert_called_once()
        build_args = mock_build_sync.call_args[0]
        assert build_args[0] == [{"name": "users", "namespace": "public"}]
        assert isinstance(build_args[1], AirbyteStreamApiMetadata)

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    def test_get_connection_backfills_namespace_into_sync_catalog(
        self, mock_list_streams, mock_make_request
    ):
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "sourceId": "source-123",
            "destinationId": "dest-123",
            "configurations": {
                "streams": [{"name": "events", "syncMode": "full_refresh_overwrite"}]
            },
        }
        mock_list_streams.return_value = [
            {
                "streamName": "events",
                "namespace": "my_schema",
                "propertyFields": [["id"]],
            }
        ]

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert result.sync_catalog is not None
        assert result.sync_catalog.streams is not None
        stream = result.sync_catalog.streams[0].stream
        assert stream is not None
        assert stream.name == "events"
        assert stream.namespace == "my_schema"

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient.list_streams")
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_connection_survives_streams_500(
        self, mock_make_request, mock_list_streams
    ):
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "sourceId": "source-123",
            "destinationId": "dest-123",
            "configurations": {"streams": [{"name": "users", "namespace": "public"}]},
        }
        mock_list_streams.side_effect = AirbyteApiError(
            "500 Internal Server Error", status_code=500
        )

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert result.connection_id == "conn-123"
        assert result.streams_api_unavailable is True
        assert result.sync_catalog is not None
        assert result.sync_catalog.streams[0].stream.name == "users"

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_connection_with_existing_sync_catalog(self, mock_make_request):
        # When the connection payload already has a syncCatalog we should use it
        # as-is rather than re-building one from `configurations.streams`.
        mock_make_request.return_value = {
            "connectionId": "conn-123",
            "sourceId": "source-123",
            "destinationId": "dest-123",
            "syncCatalog": {"streams": [{"stream": {"name": "users"}, "config": {}}]},
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)

        result = client.get_connection("conn-123")

        assert result.sync_catalog is not None
        assert result.sync_catalog.streams[0].stream.name == "users"


class TestClientSSLAndHeaders:
    def test_client_with_extra_headers(self):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            extra_headers={"X-Custom-Header": "test-value"},
        )
        client = AirbyteOSSClient(config)

        assert "X-Custom-Header" in client.session.headers
        assert client.session.headers["X-Custom-Header"] == "test-value"

    @patch("os.path.isfile", return_value=True)
    def test_client_with_valid_ssl_ca_cert(self, mock_isfile):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            verify_ssl=True,
            ssl_ca_cert="/path/to/ca-cert.pem",
        )
        client = AirbyteOSSClient(config)

        assert client.session.verify == "/path/to/ca-cert.pem"

    @patch("os.path.isfile", return_value=False)
    def test_client_with_invalid_ssl_ca_cert(self, mock_isfile, caplog):
        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
            verify_ssl=True,
            ssl_ca_cert="/invalid/path/ca-cert.pem",
        )

        with caplog.at_level("WARNING"):
            _ = AirbyteOSSClient(config)

        assert "CA certificate file not found" in caplog.text


class TestClientListJobs:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_jobs_basic(self, mock_make_request):
        mock_make_request.return_value = {
            "data": [
                {"jobId": "job-1", "status": "succeeded"},
                {"jobId": "job-2", "status": "failed"},
            ]
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        jobs = client.list_jobs("connection-123")

        assert len(jobs) == 2
        assert jobs[0]["jobId"] == "job-1"

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_jobs_with_date_filters(self, mock_make_request):
        mock_make_request.return_value = {"data": [{"jobId": "job-1"}]}

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        client.list_jobs(
            "connection-123",
            workspace_id="workspace-1",
            start_date="2024-01-01T00:00:00Z",
            end_date="2024-12-31T23:59:59Z",
        )

        mock_make_request.assert_called_once()
        call_args = mock_make_request.call_args
        params = call_args[1]["params"]
        assert params["workspaceId"] == "workspace-1"
        assert params["updatedAtStart"] == "2024-01-01T00:00:00Z"
        assert params["updatedAtEnd"] == "2024-12-31T23:59:59Z"

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_jobs_legacy_format(self, mock_make_request):
        # Older Airbyte versions returned `{"jobs": [...]}` instead of `{"data": [...]}`.
        mock_make_request.return_value = {
            "jobs": [
                {"jobId": "job-1", "status": "succeeded"},
                {"jobId": "job-2", "status": "failed"},
            ]
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        jobs = client.list_jobs("connection-123")

        assert len(jobs) == 2
        assert jobs[0]["jobId"] == "job-1"


class TestClientGetMethods:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_source(self, mock_make_request):
        mock_make_request.return_value = {
            "sourceId": "source-123",
            "name": "Test Source",
            "sourceType": "postgres",
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        result = client.get_source("source-123")

        assert result.source_id == "source-123"
        mock_make_request.assert_called_once_with("/sources/source-123")

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_destination(self, mock_make_request):
        mock_make_request.return_value = {
            "destinationId": "dest-123",
            "name": "Test Destination",
            "destinationType": "snowflake",
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        result = client.get_destination("dest-123")

        assert result.destination_id == "dest-123"
        mock_make_request.assert_called_once_with("/destinations/dest-123")

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_get_job(self, mock_make_request):
        mock_make_request.return_value = {
            "jobId": "job-123",
            "status": "succeeded",
            "jobType": "sync",
            "bytesCommitted": 1024000,
            "recordsCommitted": 5000,
            "streamStatuses": [
                {"streamName": "users", "recordsCommitted": 3000},
                {"streamName": "orders", "recordsCommitted": 2000},
            ],
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        result = client.get_job("job-123")

        assert result["jobId"] == "job-123"
        assert result["status"] == "succeeded"
        assert result["bytesCommitted"] == 1024000
        assert result["recordsCommitted"] == 5000
        assert len(result["streamStatuses"]) == 2


class TestClientListStreams:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_streams(self, mock_make_request):
        mock_make_request.return_value = {
            "streams": [
                {"streamName": "users", "namespace": "public"},
                {"streamName": "orders", "namespace": "public"},
            ]
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        streams = client.list_streams("source-123")

        assert len(streams) == 2
        assert streams[0]["streamName"] == "users"

    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_streams_empty(self, mock_make_request):
        mock_make_request.return_value = {"streams": []}

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        streams = client.list_streams("source-123")

        assert streams == []


class TestClientListTags:
    @patch("datahub.ingestion.source.airbyte.client.AirbyteOSSClient._make_request")
    def test_list_tags(self, mock_make_request):
        mock_make_request.return_value = {
            "tags": [
                {"id": "tag-1", "name": "production"},
                {"id": "tag-2", "name": "critical"},
            ]
        }

        config = AirbyteClientConfig(
            deployment_type=AirbyteDeploymentType.OPEN_SOURCE,
            host_port="http://localhost:8000",
        )
        client = AirbyteOSSClient(config)
        tags = client.list_tags("workspace-123")

        assert len(tags) == 2
        assert tags[0]["name"] == "production"
