"""The shared `api` command: inherited, gated by the mixing class, and careful
about the four things every connector was getting slightly differently.
"""

from typing import Dict, List, Sequence

import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.probe_methods import (
    ProbeMethodSpec,
    _enforce_gates,
    _iter_specs,
)
from datahub.ingestion.agent.rest_passthrough import (
    DEFAULT_API_TIMEOUT_SECONDS,
    RestApiPassthrough,
)


class _Response:
    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.parsed = False

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise RuntimeError(f"HTTP {self.status}")

    def json(self) -> object:
        self.parsed = True
        return {"ok": True}


class _Session:
    def __init__(self, status: int = 200) -> None:
        self.status = status
        self.calls: List[Dict[str, object]] = []
        self.last = _Response(status)

    def get(self, url: str, headers: Dict[str, str], timeout: int) -> _Response:
        self.calls.append({"url": url, "headers": headers, "timeout": timeout})
        self.last = _Response(self.status)
        return self.last


class _Provider(RestApiPassthrough):
    api_base_url = "https://api.example.com/v1"
    api_allowlist: Sequence[str] = ("GET /projects", "GET /projects/{id}/runs")

    def __init__(self, status: int = 200) -> None:
        # `fake` is the typed handle for assertions; api_session is what the mixin
        # reads, and is declared Optional[requests.Session] there.
        self.fake = _Session(status)
        self.api_session = self.fake  # type: ignore[assignment]

    def api_headers(self) -> Dict[str, str]:
        return {"Authorization": "Bearer tok"}


def _api_spec(owner: type) -> ProbeMethodSpec:
    spec = getattr(getattr(owner, "api", None), "__probe_command__", None)
    assert isinstance(spec, ProbeMethodSpec)
    return spec


def test_the_inherited_command_is_discovered_like_any_other():
    # Discovery walks dir(), so a mixin can supply a command -- which is what lets
    # this be shared at all rather than copied per connector.
    assert "api" in dict(_iter_specs(_Provider))


def test_the_gate_uses_the_mixing_class_allowlist_not_the_mixins():
    provider = _Provider()
    _enforce_gates(_api_spec(_Provider), provider, {"path": "/projects"})
    with pytest.raises(ApiScopeError):
        _enforce_gates(_api_spec(_Provider), provider, {"path": "/cells"})


def test_a_provider_that_forgets_its_allowlist_is_reported_as_the_provider_bug():
    # The mixin annotates api_allowlist without assigning it, so an unset list
    # reads as None. Defaulting to () would refuse every path with "not in this
    # connector's allowlist" -- blaming the caller for a list nobody wrote.
    class Forgetful(RestApiPassthrough):
        api_base_url = "https://api.example.com"

    with pytest.raises(ValueError, match="no api_allowlist") as caught:
        _enforce_gates(_api_spec(Forgetful), Forgetful(), {"path": "/projects"})
    assert not isinstance(caught.value, ApiScopeError)


def test_the_request_carries_the_connectors_headers_and_an_explicit_timeout():
    provider = _Provider()
    provider.api("/projects")
    call = provider.fake.calls[0]
    assert call["url"] == "https://api.example.com/v1/projects"
    assert call["headers"] == {"Authorization": "Bearer tok"}
    assert call["timeout"] == DEFAULT_API_TIMEOUT_SECONDS


def test_an_error_body_raises_instead_of_being_returned_as_metadata():
    # A 403 page parsed and handed back looks like an answer, which is worse than
    # a failure: the agent would report an empty listing as fact.
    provider = _Provider(status=403)
    with pytest.raises(RuntimeError, match="HTTP 403"):
        provider.api("/projects")
    assert provider.fake.last.parsed is False


def test_a_connector_can_route_through_its_own_fetcher():
    # Mode's does curl-equivalent logging and rate-limit accounting; a probe that
    # bypassed it would behave differently from ingestion on the same call.
    class OwnFetcher(_Provider):
        def __init__(self) -> None:
            super().__init__()
            self.fetched: List[str] = []

        def api_fetch_json(self, url: str) -> object:
            self.fetched.append(url)
            return {"via": "own fetcher"}

    provider = OwnFetcher()
    assert provider.api("/projects") == {"via": "own fetcher"}
    assert provider.fetched == ["https://api.example.com/v1/projects"]
    # The session was never touched, so nothing silently went around it.
    assert provider.fake.calls == []


def test_a_provider_with_no_session_and_no_override_fails_loudly():
    class NoTransport(RestApiPassthrough):
        api_base_url = "https://api.example.com"
        api_allowlist: Sequence[str] = ("GET /projects",)

    with pytest.raises(AssertionError, match="api_session"):
        NoTransport().api("/projects")
