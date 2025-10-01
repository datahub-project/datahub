from unittest.mock import Mock, patch

import pytest

from datahub_integrations.analytics.snowflake.connection import SnowflakeConnection


class TestSnowflakeConnection:
    def test_connection_with_password_auth(self):
        """Test connection creation with password authentication."""
        connection = SnowflakeConnection(
            account="test_account",
            warehouse="test_warehouse",
            user="test_user",
            password="test_password",
            role="test_role",
            authentication_type="DEFAULT_AUTHENTICATOR",
        )

        assert connection.account == "test_account"
        assert connection.warehouse == "test_warehouse"
        assert connection.user == "test_user"
        assert connection.password == "test_password"
        assert connection.role == "test_role"
        assert connection.authentication_type == "DEFAULT_AUTHENTICATOR"
        assert connection.private_key is None
        assert connection.private_key_password is None

    def test_connection_with_private_key_auth(self):
        """Test connection creation with private key authentication."""
        test_private_key = "-----BEGIN PRIVATE KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC...\n-----END PRIVATE KEY-----"

        connection = SnowflakeConnection(
            account="test_account",
            warehouse="test_warehouse",
            user="test_user",
            authentication_type="KEY_PAIR_AUTHENTICATOR",
            private_key=test_private_key,
            private_key_password="key_password",
            role="test_role",
        )

        assert connection.account == "test_account"
        assert connection.warehouse == "test_warehouse"
        assert connection.user == "test_user"
        assert connection.password is None
        assert connection.role == "test_role"
        assert connection.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        assert connection.private_key == test_private_key
        assert connection.private_key_password == "key_password"

    @patch("datahub_integrations.graphql.connection.get_connection_json")
    def test_from_datahub_password_auth(self, mock_get_connection):
        """Test loading connection from DataHub with password auth."""
        mock_graph = Mock()
        mock_get_connection.return_value = {
            "account": "test_account",
            "warehouse": "test_warehouse",
            "user": "test_user",
            "password": "test_password",
            "role": "test_role",
            "authentication_type": "DEFAULT_AUTHENTICATOR",
        }

        connection = SnowflakeConnection.from_datahub(mock_graph)

        assert connection.authentication_type == "DEFAULT_AUTHENTICATOR"
        assert connection.password == "test_password"
        assert connection.private_key is None

    @patch("datahub_integrations.graphql.connection.get_connection_json")
    def test_from_datahub_private_key_auth(self, mock_get_connection):
        """Test loading connection from DataHub with private key auth."""
        mock_graph = Mock()
        test_private_key = (
            "-----BEGIN PRIVATE KEY-----\ntest_key\n-----END PRIVATE KEY-----"
        )

        mock_get_connection.return_value = {
            "account": "test_account",
            "warehouse": "test_warehouse",
            "user": "test_user",
            "authentication_type": "KEY_PAIR_AUTHENTICATOR",
            "private_key": test_private_key,
            "private_key_password": "key_password",
            "role": "test_role",
        }

        connection = SnowflakeConnection.from_datahub(mock_graph)

        assert connection.authentication_type == "KEY_PAIR_AUTHENTICATOR"
        assert connection.private_key == test_private_key
        assert connection.private_key_password == "key_password"
        assert connection.password is None

    @patch("datahub_integrations.graphql.connection.get_connection_json")
    def test_from_datahub_missing_config(self, mock_get_connection):
        """Test error handling when no config is found."""
        mock_graph = Mock()
        mock_get_connection.return_value = None

        with pytest.raises(Exception, match="No snowflake config found"):
            SnowflakeConnection.from_datahub(mock_graph)
