"""Unit tests for SnowflakeConnectionConfigPermissive."""

from unittest.mock import Mock, mock_open, patch

from datahub_integrations.propagation.snowflake.config import (
    SnowflakeAuthenticationType,
    SnowflakeConnectionConfigPermissive,
)


class TestSnowflakeConnectionConfigPermissive:
    """Test the SnowflakeConnectionConfigPermissive class."""

    def test_get_connect_args_password_auth(self) -> None:
        """Test get_connect_args with password authentication."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.DEFAULT_AUTHENTICATOR.value,
        )

        connect_args = config.get_connect_args()

        # Should not include private_key for password auth
        assert "private_key" not in connect_args
        # Should include performance settings
        assert "CLIENT_PREFETCH_THREADS" in connect_args
        assert "CLIENT_SESSION_KEEP_ALIVE" in connect_args

    def test_get_connect_args_key_pair_auth_with_private_key_string(self) -> None:
        """Test get_connect_args with key pair authentication using private_key string."""
        # Use a real but minimal RSA private key for testing
        test_private_key = """-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQC7VJTUt9Us8cKj
MzEfYyjiWA4R4/M2bS1+fWIcPm15j9TKp4kE7M2D0zW1F8CkQCcG5tCzCQCrqyTy
dxbLu6CnWvI0oVR7zQRxB3xfhTq0V/7qGz5E5JGXVH5p8I4kF0m7kbhRkAJ8YGv4
-----END PRIVATE KEY-----"""

        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key=test_private_key,
        )

        # Mock the private key processing to avoid cryptography errors with test key
        with patch.object(config, "_process_private_key") as mock_process_private_key:
            mock_process_private_key.return_value = b"processed_key_bytes"

            connect_args = config.get_connect_args()

            # Should include private_key for key pair auth
            assert "private_key" in connect_args
            assert connect_args["private_key"] == b"processed_key_bytes"
            mock_process_private_key.assert_called_once()

    def test_get_connect_args_caches_result(self) -> None:
        """Test that get_connect_args caches the result."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.DEFAULT_AUTHENTICATOR.value,
        )

        # First call
        connect_args1 = config.get_connect_args()
        # Second call should return cached result
        connect_args2 = config.get_connect_args()

        assert connect_args1 is connect_args2

    def test_load_private_key_bytes_from_string(self) -> None:
        """Test _load_private_key_bytes loads from private_key string."""
        test_key = "-----BEGIN PRIVATE KEY-----\\ntest\\n-----END PRIVATE KEY-----"

        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key=test_key,
        )

        result = config._load_private_key_bytes()

        # Should unescape \n to actual newlines
        expected = b"-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
        assert result == expected

    def test_load_private_key_bytes_from_file(self) -> None:
        """Test _load_private_key_bytes loads from private_key_path file."""
        test_key_content = (
            b"-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
        )

        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key_path="/path/to/key.pem",
        )

        with patch("builtins.open", mock_open(read_data=test_key_content)):
            result = config._load_private_key_bytes()

        assert result == test_key_content

    def test_load_private_key_bytes_unescapes_forward_slashes(self) -> None:
        """Test that _load_private_key_bytes unescapes forward slashes."""
        test_key = "-----BEGIN PRIVATE KEY-----\\/test\\/-----END PRIVATE KEY-----"

        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key=test_key,
        )

        result = config._load_private_key_bytes()

        # Forward slashes should be unescaped
        expected = b"-----BEGIN PRIVATE KEY-----/test/-----END PRIVATE KEY-----"
        assert result == expected

    def test_get_password_bytes_returns_none_when_no_password(self) -> None:
        """Test _get_password_bytes returns None when private_key_password is None."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key="test_key",
        )

        result = config._get_password_bytes()

        assert result is None

    def test_get_password_bytes_handles_string_password(self) -> None:
        """Test _get_password_bytes handles regular string password (from automation config)."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key="test_key",
            private_key_password="my_password",
        )

        result = config._get_password_bytes()

        assert result == b"my_password"

    def test_get_password_bytes_handles_secret_str_password(self) -> None:
        """Test _get_password_bytes handles pydantic SecretStr password."""
        import pydantic

        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key="test_key",
            private_key_password=pydantic.SecretStr("secret_password"),
        )

        result = config._get_password_bytes()

        # Should extract the secret value and encode it
        assert result == b"secret_password"

    def test_create_native_connection_password_auth(self) -> None:
        """Test create_native_connection with password authentication."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            password="test_password",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.DEFAULT_AUTHENTICATOR.value,
        )

        with patch(
            "datahub_integrations.propagation.snowflake.config.snowflake.connector.connect"
        ) as mock_connect:
            mock_connection = Mock()
            mock_connect.return_value = mock_connection

            result = config.create_native_connection(application="test_app")

            assert result == mock_connection
            # Verify password auth was used
            call_kwargs = mock_connect.call_args[1]
            assert call_kwargs["user"] == "test_user"
            assert call_kwargs["password"] == "test_password"
            assert call_kwargs["application"] == "test_app"

    def test_create_native_connection_key_pair_auth(self) -> None:
        """Test create_native_connection with key pair authentication."""
        config = SnowflakeConnectionConfigPermissive(
            account_id="test_account",
            warehouse="test_warehouse",
            username="test_user",
            role="test_role",
            authentication_type=SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR.value,
            private_key="test_key",
        )

        # Mock get_connect_args to return processed key
        with patch.object(config, "get_connect_args") as mock_get_connect_args:
            mock_get_connect_args.return_value = {"private_key": b"processed_key"}

            with patch(
                "datahub_integrations.propagation.snowflake.config.snowflake.connector.connect"
            ) as mock_connect:
                mock_connection = Mock()
                mock_connect.return_value = mock_connection

                result = config.create_native_connection(application="test_app")

                assert result == mock_connection
                # Verify key pair auth was used
                call_kwargs = mock_connect.call_args[1]
                assert call_kwargs["user"] == "test_user"
                assert (
                    call_kwargs["authenticator"]
                    == SnowflakeAuthenticationType.KEY_PAIR_AUTHENTICATOR
                )
                assert call_kwargs["private_key"] == b"processed_key"
                assert call_kwargs["application"] == "test_app"
