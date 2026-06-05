import time

import requests

from datahub.emitter.token_provider import (
    CachingTokenProvider,
    StaticTokenProvider,
    TokenProvider,
    TokenProviderAuth,
)


def _jwt_with_exp(exp_epoch: int) -> str:
    # Unsigned-looking JWT: header.payload.sig — only the payload is parsed.
    import base64
    import json

    def b64(d: dict) -> str:
        raw = json.dumps(d).encode()
        return base64.urlsafe_b64encode(raw).decode().rstrip("=")

    return f"{b64({'alg': 'RS256'})}.{b64({'exp': exp_epoch})}.sig"


def test_static_token_provider_returns_token():
    assert StaticTokenProvider("abc").get_token() == "abc"


def test_caching_provider_caches_until_refresh_buffer():
    calls = []

    def fetch() -> str:
        calls.append(1)
        return _jwt_with_exp(int(time.time()) + 3600)

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 1  # second call served from cache


def test_caching_provider_refetches_when_within_buffer():
    def fetch() -> str:
        return _jwt_with_exp(int(time.time()) + 100)  # expires inside 300s buffer

    calls = []

    def counting_fetch() -> str:
        calls.append(1)
        return fetch()

    provider = CachingTokenProvider(counting_fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 2  # always stale -> re-fetch


def test_caching_provider_refetches_when_exp_unparseable():
    calls = []

    def fetch() -> str:
        calls.append(1)
        return "not-a-jwt"

    provider = CachingTokenProvider(fetch)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 2  # unknown expiry -> never cache by expiry


def test_caching_provider_invalidate_forces_refetch():
    calls = []

    def fetch() -> str:
        calls.append(1)
        return _jwt_with_exp(int(time.time()) + 3600)

    provider = CachingTokenProvider(fetch)
    provider.get_token()
    provider.invalidate()
    provider.get_token()
    assert len(calls) == 2


class _SeqProvider(TokenProvider):
    def __init__(self, tokens):
        self._tokens = list(tokens)
        self.invalidated = 0

    def get_token(self) -> str:
        return self._tokens[min(self.invalidated, len(self._tokens) - 1)]

    def invalidate(self) -> None:
        self.invalidated += 1


def test_auth_sets_fresh_header_each_call():
    provider = _SeqProvider(["t1"])
    auth = TokenProviderAuth(provider, retry_on_401=False)
    req = requests.Request("GET", "http://x/").prepare()
    auth(req)
    assert req.headers["Authorization"] == "Bearer t1"


def test_auth_retries_once_on_401(requests_mock):
    provider = _SeqProvider(["stale", "fresh"])
    auth = TokenProviderAuth(provider)
    requests_mock.get(
        "http://gms/api",
        [{"status_code": 401}, {"status_code": 200, "text": "ok"}],
    )
    resp = requests.get("http://gms/api", auth=auth)
    assert resp.status_code == 200
    assert provider.invalidated == 1
    assert resp.request.headers["Authorization"] == "Bearer fresh"
