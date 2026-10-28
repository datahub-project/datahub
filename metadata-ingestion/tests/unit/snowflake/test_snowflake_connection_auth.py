"""Credential handling in SnowflakeConnectionConfig.get_connection().

Each authentication type hands a different credential to the Snowflake driver.
These tests check what reaches snowflake.connector.connect for key-pair, OAuth
(client secret and certificate), externally supplied OAuth tokens and workload
identity, plus how connection failures are classified.
"""

import pathlib
from typing import Dict
from unittest.mock import MagicMock, patch

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnection,
    SnowflakeConnectionConfig,
    SnowflakePermissionError,
)

BASE: Dict[str, object] = {"account_id": "acct", "username": "user"}


def _pem(password: bytes | None = None) -> bytes:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    encryption: serialization.KeySerializationEncryption = (
        serialization.BestAvailableEncryption(password)
        if password
        else serialization.NoEncryption()
    )
    return key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=encryption,
    )


def _connect_kwargs(config: SnowflakeConnectionConfig) -> dict:
    with patch("snowflake.connector.connect", return_value=MagicMock()) as connect:
        config.get_connection()
    return dict(connect.call_args.kwargs)


def test_inline_private_key_with_escaped_newlines_is_sent_as_der_bytes() -> None:
    pem = _pem().decode().replace("\n", "\\n")
    config = SnowflakeConnectionConfig.model_validate(
        {**BASE, "authentication_type": "KEY_PAIR_AUTHENTICATOR", "private_key": pem}
    )

    kwargs = _connect_kwargs(config)

    assert isinstance(kwargs["private_key"], bytes)
    assert kwargs["private_key"][:1] == b"\x30"  # DER sequence, not PEM text
    assert "password" not in kwargs
    assert kwargs["authenticator"] == "SNOWFLAKE_JWT"


def test_encrypted_private_key_file_is_decrypted_with_the_configured_password(
    tmp_path: pathlib.Path,
) -> None:
    key_path = tmp_path / "rsa_key.p8"
    key_path.write_bytes(_pem(password=b"s3cret"))
    config = SnowflakeConnectionConfig.model_validate(
        {
            **BASE,
            "authentication_type": "KEY_PAIR_AUTHENTICATOR",
            "private_key_path": str(key_path),
            "private_key_password": "s3cret",
        }
    )

    kwargs = _connect_kwargs(config)

    assert isinstance(kwargs["private_key"], bytes)


def test_oauth_token_from_recipe_is_passed_through_verbatim() -> None:
    config = SnowflakeConnectionConfig.model_validate(
        {**BASE, "authentication_type": "OAUTH_AUTHENTICATOR_TOKEN", "token": "abc"}
    )

    kwargs = _connect_kwargs(config)

    assert kwargs["authenticator"] == "oauth"
    assert kwargs["token"] == "abc"


def _oauth_config(**overrides: object) -> Dict[str, object]:
    return {
        **BASE,
        "authentication_type": "OAUTH_AUTHENTICATOR",
        "oauth_config": {
            "provider": "microsoft",
            "client_id": "client",
            "client_secret": "secret",
            "authority_url": "https://login.example.com/tenant",
            "scopes": ["scope"],
            **overrides,
        },
    }


def test_oauth_client_secret_flow_exchanges_a_token_before_connecting() -> None:
    config = SnowflakeConnectionConfig.model_validate(_oauth_config())

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_connection.OAuthTokenGenerator"
    ) as generator:
        generator.return_value.get_token_with_secret.return_value = {
            "access_token": "exchanged"
        }
        kwargs = _connect_kwargs(config)

    generator.return_value.get_token_with_secret.assert_called_once_with(
        secret="secret", scopes=["scope"]
    )
    assert kwargs["token"] == "exchanged"
    assert kwargs["authenticator"].lower() == "oauth"


def test_oauth_certificate_flow_uses_the_certificate_grant() -> None:
    config = SnowflakeConnectionConfig.model_validate(
        _oauth_config(
            use_certificate=True,
            encoded_oauth_public_key="cHVi",
            encoded_oauth_private_key="cHJpdg==",
        )
    )

    with patch(
        "datahub.ingestion.source.snowflake.snowflake_connection.OAuthTokenGenerator"
    ) as generator:
        generator.return_value.get_token_with_certificate.return_value = {
            "access_token": "cert-token"
        }
        kwargs = _connect_kwargs(config)

    generator.return_value.get_token_with_certificate.assert_called_once()
    assert kwargs["token"] == "cert-token"


def test_oauth_response_without_access_token_is_a_configuration_error() -> None:
    config = SnowflakeConnectionConfig.model_validate(_oauth_config())

    with (
        patch(
            "datahub.ingestion.source.snowflake.snowflake_connection.OAuthTokenGenerator"
        ) as generator,
        pytest.raises(ConfigurationError, match="access_token"),
    ):
        generator.return_value.get_token_with_secret.return_value = {"error": "x"}
        config.get_connection()


def test_workload_identity_sends_provider_but_no_secret() -> None:
    config = SnowflakeConnectionConfig.model_validate(
        {
            **BASE,
            "authentication_type": "WORKLOAD_IDENTITY_AUTHENTICATOR",
            "workload_identity_provider": "aws",
        }
    )

    kwargs = _connect_kwargs(config)

    assert kwargs["authenticator"] == "WORKLOAD_IDENTITY"
    assert kwargs["workload_identity_provider"] == "AWS"
    assert "password" not in kwargs and "token" not in kwargs


def test_workload_identity_provider_must_match_authentication_type() -> None:
    with pytest.raises(ValueError, match="workload_identity_provider"):
        SnowflakeConnectionConfig.model_validate(
            {**BASE, "authentication_type": "WORKLOAD_IDENTITY_AUTHENTICATOR"}
        )
    with pytest.raises(ValueError, match="workload_identity_provider"):
        SnowflakeConnectionConfig.model_validate(
            {**BASE, "password": "p", "workload_identity_provider": "aws"}
        )


def test_connect_failures_are_classified_as_permission_or_configuration() -> None:
    config = SnowflakeConnectionConfig.model_validate({**BASE, "password": "p"})

    with (
        patch(
            "snowflake.connector.connect",
            side_effect=RuntimeError("Role 'X' is not granted to this user"),
        ),
        pytest.raises(SnowflakePermissionError),
    ):
        config.get_connection()

    with (
        patch("snowflake.connector.connect", side_effect=RuntimeError("timeout")),
        pytest.raises(ConfigurationError),
    ):
        config.get_connection()


def test_query_wraps_authorization_errors_and_exposes_close_state() -> None:
    native = MagicMock()
    native.cursor.return_value.execute.side_effect = RuntimeError(
        "SQL compilation error: Database 'X' does not exist or not authorized."
    )
    native.is_closed.return_value = False
    connection = SnowflakeConnection(native)

    with pytest.raises(SnowflakePermissionError):
        connection.query("select 1")
    assert not connection.is_closed()

    connection.close()
    native.close.assert_called_once()
