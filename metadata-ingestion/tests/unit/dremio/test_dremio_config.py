import pytest
from pydantic import ValidationError

from datahub.ingestion.source.dremio.dremio_config import (
    DremioConnectionConfig,
    DremioSourceConfig,
)


class TestDremioConfigValidators:
    def test_invalid_auth_method_raises(self):
        with pytest.raises(ValidationError) as exc_info:
            DremioConnectionConfig(
                hostname="localhost",
                tls=False,
                authentication_method="oauth2",
                password="token",
            )
        assert (
            "oauth2" in str(exc_info.value)
            or "authentication_method" in str(exc_info.value).lower()
        )

    def test_pat_with_explicit_none_password_raises(self):
        # Pydantic v2 field_validator only runs when the field is explicitly provided.
        # Passing password=None explicitly triggers the PAT validation check.
        with pytest.raises(ValidationError) as exc_info:
            DremioConnectionConfig(
                hostname="localhost",
                tls=False,
                authentication_method="PAT",
                password=None,
            )
        error_text = str(exc_info.value).lower()
        assert "pat" in error_text or "token" in error_text or "password" in error_text

    def test_password_auth_without_password_allowed(self):
        """The 'password' method doesn't require the password field — it may come from env."""
        config = DremioConnectionConfig(
            hostname="localhost",
            tls=False,
            authentication_method="password",
        )
        assert config.password is None

    def test_transparent_secret_str_serializes_plaintext(self):
        """TransparentSecretStr must serialize as plain text (used for cross-process pass-through)."""
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="supersecret",
        )
        data = config.model_dump()
        assert data["password"] == "supersecret"

    def test_transparent_secret_str_masked_in_repr(self):
        config = DremioSourceConfig(
            hostname="localhost",
            tls=False,
            authentication_method="PAT",
            password="supersecret",
        )
        assert "supersecret" not in repr(config.password)
