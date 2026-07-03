"""The version check must honor OAuth (auth=) client configs, not just static
tokens — it resolves a fresh token from the provider for its single request."""

from datahub.ingestion.graph.config import DatahubClientConfig
from datahub.upgrade.upgrade import _resolve_request_token


def test_resolves_static_token():
    config = DatahubClientConfig(server="http://gms:8080", token="pat")
    assert _resolve_request_token(config) == "pat"


def test_resolves_none_when_unauthenticated():
    config = DatahubClientConfig(server="http://gms:8080")
    assert _resolve_request_token(config) is None


def test_resolves_token_from_auth_provider():
    config = DatahubClientConfig(
        server="http://gms:8080",
        auth={"type": "static", "config": {"token": "provider-tok"}},
    )
    assert _resolve_request_token(config) == "provider-tok"
