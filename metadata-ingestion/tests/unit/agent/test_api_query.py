from typing import List, Sequence

import pytest

from datahub.ingestion.agent.api_gate import ApiScopeError
from datahub.ingestion.agent.api_query import execute_scoped_api
from datahub.ingestion.source.mode_probe import ModeProbeSource


class FakeApiProvider:
    api_allowlist: Sequence[str] = ("GET /spaces", "GET /spaces/{token}/reports")

    def __init__(self) -> None:
        self.fetched: List[str] = []

    def get_json(self, path: str) -> object:
        self.fetched.append(path)
        return {"_embedded": {"spaces": []}}


class NotApiCapable:
    pass


def test_a_refused_request_never_reaches_the_connector():
    provider = FakeApiProvider()
    with pytest.raises(ApiScopeError):
        execute_scoped_api(provider, "mode", "GET", "/spaces/sp1/members")
    assert provider.fetched == []


def test_a_write_never_reaches_the_connector():
    provider = FakeApiProvider()
    with pytest.raises(ApiScopeError):
        execute_scoped_api(provider, "mode", "DELETE", "/spaces")
    assert provider.fetched == []


def test_a_permitted_request_is_fetched_and_reported():
    provider = FakeApiProvider()
    result = execute_scoped_api(provider, "mode", "GET", "/spaces/sp1/reports")
    assert provider.fetched == ["/spaces/sp1/reports"]
    assert result.method == "GET"
    assert result.path == "/spaces/sp1/reports"


def test_a_provider_without_an_api_surface_is_a_clear_error():
    with pytest.raises(ValueError, match="does not expose an API probe surface"):
        execute_scoped_api(NotApiCapable(), "postgres", "GET", "/spaces")


def test_mode_declares_an_allowlist_and_does_not_expose_get_json_as_a_method():
    assert ModeProbeSource.api_allowlist
    # Annotating get_json would put an arbitrary path on `probe run`, reaching
    # the API without the allowlist check.
    assert getattr(ModeProbeSource.get_json, "__probe_command__", None) is None
