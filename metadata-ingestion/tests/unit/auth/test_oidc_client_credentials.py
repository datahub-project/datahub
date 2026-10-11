import time

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.auth.oidc_client_credentials import (
    OidcClientCredentialsTokenProvider,
)


def test_fetches_access_token(requests_mock):
    requests_mock.post(
        "http://idp/token",
        json={"access_token": "at-123", "expires_in": 3600},
    )
    provider = OidcClientCredentialsTokenProvider.create(
        {
            "token_endpoint": "http://idp/token",
            "client_id": "cid",
            "client_secret": "secret",
            "scope": "datahub",
        }
    )
    result = provider.get_token()
    assert result.token == "at-123"
    # expires_at derived from `expires_in`, not from decoding the token.
    assert result.expires_at is not None
    assert time.time() + 3000 < result.expires_at <= time.time() + 3600
    body = requests_mock.last_request.text
    assert "grant_type=client_credentials" in body
    assert "scope=datahub" in body


def test_missing_expires_in_yields_no_expiry(requests_mock):
    requests_mock.post("http://idp/token", json={"access_token": "at-123"})
    provider = OidcClientCredentialsTokenProvider.create(
        {"token_endpoint": "http://idp/token", "client_id": "c", "client_secret": "s"}
    )
    result = provider.get_token()
    assert result.token == "at-123"
    assert result.expires_at is None


def test_error_response_raises(requests_mock):
    requests_mock.post("http://idp/token", status_code=401, text="bad client")
    provider = OidcClientCredentialsTokenProvider.create(
        {"token_endpoint": "http://idp/token", "client_id": "c", "client_secret": "s"}
    )
    with pytest.raises(ConfigurationError):
        provider.get_token()


def test_missing_access_token_raises(requests_mock):
    requests_mock.post("http://idp/token", json={})
    provider = OidcClientCredentialsTokenProvider.create(
        {"token_endpoint": "http://idp/token", "client_id": "c", "client_secret": "s"}
    )
    with pytest.raises(ConfigurationError):
        provider.get_token()
