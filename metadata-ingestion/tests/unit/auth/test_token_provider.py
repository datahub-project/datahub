import time

import requests

from datahub.emitter.token_provider import (
    CachingTokenProvider,
    StaticTokenProvider,
    TokenProvider,
    TokenProviderAuth,
    TokenResult,
)


def test_static_token_provider_returns_token():
    result = StaticTokenProvider("abc").get_token()
    assert result.token == "abc"
    assert result.expires_at is None


def test_caching_provider_caches_until_refresh_buffer():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() + 3600)

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 1  # second call served from cache


def test_caching_provider_refetches_when_within_buffer():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() + 100)  # expires inside 300s buffer

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 2  # always stale -> re-fetch


def test_caching_provider_refetches_when_expiry_unknown():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t")  # no expiry reported

    provider = CachingTokenProvider(fetch)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 2  # unknown expiry -> never cache by expiry


def test_caching_provider_invalidate_forces_refetch():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() + 3600)

    provider = CachingTokenProvider(fetch)
    provider.get_token()
    provider.invalidate()
    provider.get_token()
    assert len(calls) == 2


class _SeqProvider(TokenProvider):
    def __init__(self, tokens):
        self._tokens = list(tokens)
        self.invalidated = 0

    def get_token(self) -> TokenResult:
        return TokenResult(self._tokens[min(self.invalidated, len(self._tokens) - 1)])

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


def test_auth_passes_non_401_through_untouched(requests_mock):
    # The retry hook fires on every response; a success must pass straight
    # through without invalidating the cached token.
    provider = _SeqProvider(["t1"])
    auth = TokenProviderAuth(provider)
    requests_mock.get("http://gms/api", status_code=200, text="ok")
    resp = requests.get("http://gms/api", auth=auth)
    assert resp.status_code == 200
    assert provider.invalidated == 0


def test_auth_retries_at_most_once_on_repeated_401(requests_mock):
    # A persistent 401 must not loop: invalidate + retry once, then surface
    # the failure rather than refreshing forever.
    provider = _SeqProvider(["stale", "still-stale"])
    auth = TokenProviderAuth(provider)
    requests_mock.get(
        "http://gms/api",
        [{"status_code": 401}, {"status_code": 401}],
    )
    resp = requests.get("http://gms/api", auth=auth)
    assert resp.status_code == 401
    assert provider.invalidated == 1  # exactly one refresh attempt, no loop


def test_auth_does_not_retry_when_provider_cannot_invalidate(requests_mock):
    # StaticTokenProvider has no invalidate(); a 401 has no fresh token to fall
    # back to, so the response passes through instead of retrying.
    auth = TokenProviderAuth(StaticTokenProvider("tok"))
    requests_mock.get("http://gms/api", status_code=401)
    resp = requests.get("http://gms/api", auth=auth)
    assert resp.status_code == 401
