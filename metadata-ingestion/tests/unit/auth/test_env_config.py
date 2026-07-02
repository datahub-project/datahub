"""Tests for env-based auth config (DATAHUB_AUTH_TYPE) and its wiring into
load_client_config, which is what ingestion default sinks and the CLI use."""

import pytest

from datahub.cli.config_utils import load_client_config
from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.env import build_auth_config_from_env

_AUTH_ENV_VARS = [
    "DATAHUB_AUTH_TYPE",
    "DATAHUB_AUTH_TOKEN_FILE",
    "DATAHUB_AUTH_AUDIENCE",
    "DATAHUB_AUTH_SCOPE",
    "DATAHUB_AUTH_TOKEN_ENDPOINT",
    "DATAHUB_AUTH_CLIENT_ID",
    "DATAHUB_AUTH_CLIENT_SECRET",
    "DATAHUB_AUTH_AZURE_TENANT_ID",
    "DATAHUB_AUTH_AZURE_CLIENT_ID",
    "DATAHUB_AUTH_AZURE_SCOPE",
    "DATAHUB_AUTH_AZURE_CLIENT_SECRET",
    "DATAHUB_GMS_URL",
    "DATAHUB_GMS_HOST",
    "DATAHUB_GMS_PORT",
    "DATAHUB_GMS_TOKEN",
]


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for var in _AUTH_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def test_returns_none_when_auth_type_unset():
    assert build_auth_config_from_env() is None


def test_oidc_client_credentials(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "https://idp/token")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "s3cret-value")
    monkeypatch.setenv("DATAHUB_AUTH_SCOPE", "openid")

    auth = build_auth_config_from_env()
    assert auth is not None and auth.type == "oidc_client_credentials"
    assert auth.config["token_endpoint"] == "https://idp/token"
    assert auth.config["client_id"] == "cid"
    assert auth.config["scope"] == "openid"
    # Secrets are SecretStr so dumps/reprs mask them.
    assert auth.config["client_secret"].get_secret_value() == "s3cret-value"
    assert "s3cret-value" not in repr(auth)


def test_azure_entra_secret_optional(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "azure_entra")
    monkeypatch.setenv("DATAHUB_AUTH_AZURE_TENANT_ID", "tid")
    monkeypatch.setenv("DATAHUB_AUTH_AZURE_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_AZURE_SCOPE", "api://datahub-gms/.default")

    auth = build_auth_config_from_env()
    assert auth is not None and auth.type == "azure_entra"
    assert auth.config == {
        "tenant_id": "tid",
        "client_id": "cid",
        "scope": "api://datahub-gms/.default",
    }


def test_k8s_oidc_all_vars_optional(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")

    auth = build_auth_config_from_env()
    assert auth is not None and auth.type == "k8s_oidc"
    assert auth.config == {}


def test_missing_required_vars_named_in_error(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "https://idp/token")

    with pytest.raises(ConfigurationError) as exc:
        build_auth_config_from_env()
    assert "DATAHUB_AUTH_CLIENT_ID" in str(exc.value)
    assert "DATAHUB_AUTH_CLIENT_SECRET" in str(exc.value)


def test_unknown_auth_type_raises(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "bogus")
    with pytest.raises(ConfigurationError):
        build_auth_config_from_env()


def test_load_client_config_uses_env_auth(monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")

    config = load_client_config()
    assert config.server == "http://gms:8080"
    assert config.auth is not None and config.auth.type == "k8s_oidc"
    assert config.token is None


def test_load_client_config_env_auth_wins_over_static_token(monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "static-pat")
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "k8s_oidc")

    config = load_client_config()
    assert config.auth is not None
    assert config.token is None


def test_load_client_config_static_token_without_auth_type(monkeypatch):
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://gms:8080")
    monkeypatch.setenv("DATAHUB_GMS_TOKEN", "static-pat")

    config = load_client_config()
    assert config.token == "static-pat"
    assert config.auth is None
