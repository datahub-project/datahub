import pytest

# gql lives in the `circuit-breaker` extra, not the base `dev` deps the fast
# `testQuick` unit gate installs — skip this whole module there rather than fail
# at collection. It still runs in the integration-tests jobs, which pull the extra.
pytest.importorskip("gql")

from datahub.api.circuit_breaker.circuit_breaker import AbstractCircuitBreaker
from datahub.api.gql_transport import build_gql_transport
from datahub.api.graphql.base import BaseApi
from datahub.emitter.token_provider import TokenProviderAuth
from datahub.ingestion.auth.registry import AuthConfig


def _auth_of(transport):
    # gql's RequestsHTTPTransport stores the requests AuthBase on `.auth`.
    return getattr(transport, "auth", None)


def test_static_token_bakes_header_no_auth():
    t = build_gql_transport("http://gms", token="tok")
    assert t.headers == {"Authorization": "Bearer tok"}
    assert _auth_of(t) is None


def test_declarative_auth_installs_token_provider():
    auth = AuthConfig(
        type="oidc_client_credentials",
        config={
            "token_endpoint": "http://idp/token",
            "client_id": "cid",
            "client_secret": "csecret",
        },
    )
    t = build_gql_transport("http://gms", auth=auth)
    assert isinstance(_auth_of(t), TokenProviderAuth)
    assert t.headers is None


def test_env_oauth_installs_token_provider(monkeypatch):
    monkeypatch.setenv("DATAHUB_AUTH_TYPE", "oidc_client_credentials")
    monkeypatch.setenv("DATAHUB_AUTH_TOKEN_ENDPOINT", "http://idp/token")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_ID", "cid")
    monkeypatch.setenv("DATAHUB_AUTH_CLIENT_SECRET", "csecret")
    t = build_gql_transport("http://gms")
    assert isinstance(_auth_of(t), TokenProviderAuth)


def test_no_creds_no_env_is_unauthenticated(monkeypatch):
    monkeypatch.delenv("DATAHUB_AUTH_TYPE", raising=False)
    t = build_gql_transport("http://gms")
    assert _auth_of(t) is None
    assert t.headers is None


def test_base_api_declarative_auth_routes_through_helper():
    api = BaseApi(
        datahub_host="http://gms",
        datahub_auth=AuthConfig(
            type="oidc_client_credentials",
            config={
                "token_endpoint": "http://idp/token",
                "client_id": "cid",
                "client_secret": "csecret",
            },
        ),
    )
    assert isinstance(api.transport.auth, TokenProviderAuth)


def test_base_api_static_token_still_bakes_header():
    api = BaseApi(datahub_host="http://gms", datahub_token="tok")
    assert api.transport.headers == {"Authorization": "Bearer tok"}
    assert getattr(api.transport, "auth", None) is None


def test_circuit_breaker_declarative_auth_routes_through_helper():
    class _CB(AbstractCircuitBreaker):
        def is_circuit_breaker_active(self, urn: str) -> bool:
            return False

    cb = _CB(
        datahub_host="http://gms",
        datahub_auth=AuthConfig(
            type="oidc_client_credentials",
            config={
                "token_endpoint": "http://idp/token",
                "client_id": "cid",
                "client_secret": "csecret",
            },
        ),
    )
    assert isinstance(cb.transport.auth, TokenProviderAuth)
