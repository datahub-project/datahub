import io
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


def test_caching_provider_clamps_buffer_for_short_lived_tokens():
    # A token whose lifetime <= the refresh buffer (e.g. Keycloak's default
    # 300s tokens vs the default 300s buffer) must still get useful caching:
    # the buffer clamps to half the lifetime instead of marking every token
    # permanently stale (one IdP round trip per client request).
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() + 100)

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 1  # cached: effective buffer is 50s, not 300s


def test_caching_provider_refetches_within_clamped_buffer():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        # 100s lifetime -> clamped 50s buffer; pretend it was fetched 60s ago
        # by reporting an expiry only 40s away.
        return TokenResult("t", time.time() + 40)

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    # Simulate the passage of time: shift the recorded fetch time back so the
    # cached token sits inside its clamped refresh window.
    assert provider._cached is not None
    result, fetched_at = provider._cached
    provider._cached = (result, fetched_at - 60)
    provider.get_token()
    assert len(calls) == 2


def test_caching_provider_refetches_expired_token():
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() - 1)  # already expired

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    provider.get_token()
    provider.get_token()
    assert len(calls) == 2


def test_caching_provider_warns_once_on_nonpositive_lifetime(caplog):
    # An IdP reporting expires_in <= 0 (or a client clock ahead of the token
    # response) degrades to one synchronous IdP fetch per request — that must
    # be loud, not silent, and warned only once.
    calls = []

    def fetch() -> TokenResult:
        calls.append(1)
        return TokenResult("t", time.time() - 10)

    provider = CachingTokenProvider(fetch, refresh_buffer_seconds=300)
    with caplog.at_level("WARNING"):
        provider.get_token()
        provider.get_token()
    assert len(calls) == 2  # already-expired tokens are re-fetched per request
    warnings = [r for r in caplog.records if "lifetime" in r.message]
    assert len(warnings) == 1


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


def test_auth_warns_once_when_replacing_existing_authorization_header(caplog):
    # A pre-existing Authorization header (e.g. proxy auth injected via
    # extra_headers) is replaced by the provider token — loudly, once.
    provider = _SeqProvider(["t1"])
    auth = TokenProviderAuth(provider, retry_on_401=False)
    with caplog.at_level("WARNING"):
        for _ in range(2):
            req = requests.Request(
                "GET", "http://x/", headers={"Authorization": "Basic proxy-cred"}
            ).prepare()
            auth(req)
    assert req.headers["Authorization"] == "Bearer t1"
    warnings = [r for r in caplog.records if "Authorization" in r.message]
    assert len(warnings) == 1


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


def test_auth_does_not_retry_401_across_hosts():
    # requests strips Authorization on cross-host redirects; the 401-retry hook
    # must not re-attach the GMS credential to a response coming from another
    # host, or a redirecting server could capture the token.
    provider = CachingTokenProvider(lambda: TokenResult("tok", time.time() + 3600))
    auth = TokenProviderAuth(provider)

    original = requests.PreparedRequest()
    original.prepare(method="GET", url="http://gms:8080/config")
    auth(original)

    redirected = requests.PreparedRequest()
    redirected.prepare(method="GET", url="http://evil.example.com/config", headers={})
    response = requests.Response()
    response.status_code = 401
    response.request = redirected
    response._content = b""

    result = auth._handle_401(response)
    assert result is response  # returned untouched, no cross-host retry


def test_auth_does_not_retry_401_after_scheme_or_port_change():
    # requests strips Authorization not only across hosts but also on same-host
    # scheme downgrades and port changes (should_strip_auth); the retry hook
    # must not re-attach the token in those cases either — e.g. an https->http
    # redirect would otherwise get the fresh bearer token over plaintext.
    provider = CachingTokenProvider(lambda: TokenResult("tok", time.time() + 3600))
    auth = TokenProviderAuth(provider)

    original = requests.PreparedRequest()
    original.prepare(method="GET", url="https://gms:8080/config")
    auth(original)

    for redirected_url in ["http://gms:8080/config", "https://gms:9999/config"]:
        redirected = requests.PreparedRequest()
        redirected.prepare(method="GET", url=redirected_url, headers={})
        response = requests.Response()
        response.status_code = 401
        response.request = redirected
        response._content = b""

        result = auth._handle_401(response)
        assert result is response  # returned untouched, no retry


def test_auth_does_not_retry_401_with_streamed_body():
    # A file/generator body was consumed by the first send and cannot be
    # replayed — a retry would transmit an empty body under the original
    # Content-Length. The hook must surface the 401 instead.
    provider = CachingTokenProvider(lambda: TokenResult("tok", time.time() + 3600))
    auth = TokenProviderAuth(provider)

    original = requests.PreparedRequest()
    original.prepare(
        method="POST", url="http://gms:8080/aspects", data=io.BytesIO(b"payload")
    )
    auth(original)

    response = requests.Response()
    response.status_code = 401
    response.request = original
    response._content = b""

    assert auth._handle_401(response) is response


def test_auth_retries_401_on_same_host_via_hook():
    provider = CachingTokenProvider(lambda: TokenResult("tok", time.time() + 3600))
    auth = TokenProviderAuth(provider)

    original = requests.PreparedRequest()
    original.prepare(method="GET", url="http://gms:8080/config")
    auth(original)

    class _FakeConnection:
        def __init__(self):
            self.sent = []

        def send(self, prepared, **kwargs):
            self.sent.append(prepared)
            ok = requests.Response()
            ok.status_code = 200
            ok.request = prepared
            ok._content = b""
            return ok

    response = requests.Response()
    response.status_code = 401
    response.request = original
    response._content = b""
    connection = _FakeConnection()
    response.connection = connection  # type: ignore[attr-defined]

    result = auth._handle_401(response)
    assert result.status_code == 200
    assert len(connection.sent) == 1  # same-host 401 still retried once
