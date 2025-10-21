from unittest.mock import Mock

import pytest

from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection


class TestSnowflakeConnection:
    def test_authentication_type_determines_credential_usage(self) -> None:
        """Test that authentication type correctly determines which credentials are used."""
        # Test password authentication excludes private key fields
        password_connection = SnowflakeConnection(
            account="test_account",
            warehouse="test_warehouse",
            user="test_user",
            password="test_password",
            role="test_role",
            authentication_type="DEFAULT_AUTHENTICATOR",
        )

        assert password_connection.authentication_type == "DEFAULT_AUTHENTICATOR"
        assert password_connection.password is not None
        assert password_connection.private_key is None
        assert password_connection.private_key_password is None

        # Test private key authentication excludes password field
        test_private_key = (
            "-----BEGIN PRIVATE KEY-----\ntest_key_content\n-----END PRIVATE KEY-----"
        )
        key_connection = SnowflakeConnection(
            account="test_account",
            warehouse="test_warehouse",
            user="test_user",
            authentication_type="KEY_PAIR_AUTHENTICATOR",
            private_key=test_private_key,  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
            private_key_password="key_password",  # type: ignore[arg-type]  # Pydantic coerces str to SecretStr
            role="test_role",
        )

        assert key_connection.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        assert key_connection.password is None
        assert key_connection.private_key is not None
        assert key_connection.private_key.get_secret_value() == test_private_key
        assert key_connection.private_key_password is not None
        assert key_connection.private_key_password.get_secret_value() == "key_password"

    def test_datahub_integration_parses_json_blob_correctly(self) -> None:
        """Test that from_datahub correctly parses nested JSON configuration from GraphQL response."""
        mock_graph = Mock()
        # Mock the GraphQL response structure that get_connection_json expects.
        # This tests the actual parsing logic, not just property assignment.
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:datahubConnection:snowflake",
                "details": {
                    "type": "JSON",  # Connection type must be JSON
                    "name": "snowflake",
                    "json": {
                        # The blob contains JSON-encoded connection parameters
                        "blob": '{"account": "test_account", "warehouse": "test_warehouse", "user": "test_user", "password": "test_password", "role": "test_role", "authentication_type": "DEFAULT_AUTHENTICATOR"}'
                    },
                },
            }
        }

        connection = SnowflakeConnection.from_datahub(mock_graph)

        # Verify the JSON parsing worked correctly
        assert connection.authentication_type == "DEFAULT_AUTHENTICATOR"
        assert connection.password == "test_password"
        assert connection.private_key is None

    def test_datahub_integration_handles_escaped_newlines_in_private_keys(self) -> None:
        """Test that from_datahub correctly unescapes newlines in PEM private keys from JSON."""
        mock_graph = Mock()
        test_private_key = (
            "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----"
        )

        # Mock GraphQL response with escaped newlines in JSON blob
        mock_graph.execute_graphql.return_value = {
            "connection": {
                "urn": "urn:li:datahubConnection:snowflake",
                "details": {
                    "type": "JSON",
                    "name": "snowflake",
                    "json": {
                        # Test that escaped newlines in JSON are properly unescaped
                        "blob": '{"account": "test_account", "warehouse": "test_warehouse", "user": "test_user", "authentication_type": "KEY_PAIR_AUTHENTICATOR", "private_key": "-----BEGIN PRIVATE KEY-----\\ntest_key\\n-----END PRIVATE KEY-----", "private_key_password": "key_password", "role": "test_role"}'
                    },
                },
            }
        }

        connection = SnowflakeConnection.from_datahub(mock_graph)

        # Verify that JSON parsing correctly unescaped the newlines
        assert connection.private_key is not None
        assert connection.private_key.get_secret_value() == test_private_key
        assert connection.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        assert connection.password is None

    def test_datahub_integration_raises_clear_error_when_connection_not_found(
        self,
    ) -> None:
        """Test that from_datahub raises a clear error when connection URN doesn't exist."""
        mock_graph = Mock()
        # Mock GraphQL response with no connection found (connection field is None)
        # This simulates the case where the connection URN doesn't exist in DataHub
        mock_graph.execute_graphql.return_value = {"connection": None}

        # Verify that a clear error is raised when no config is found
        with pytest.raises(Exception, match="No snowflake config found"):
            SnowflakeConnection.from_datahub(mock_graph)
