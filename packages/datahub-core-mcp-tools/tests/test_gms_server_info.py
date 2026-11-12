from typing import Any, Mapping

import pytest

from datahub_core_mcp_tools.gms_server_info import GmsServerInfo, GmsServerInfoCache


def _config(version: str | None, server_env: str | None = "core") -> dict[str, Any]:
    return {
        "versions": {"acryldata/datahub": {"version": version}},
        "datahub": {"serverEnv": server_env},
    }


@pytest.mark.parametrize(
    ("version", "server_env", "parsed", "is_cloud"),
    [
        ("v1.7.0", "core", (1, 7, 0, 0), False),
        ("v1.8.0rc2", "core", (1, 8, 0, 0), False),
        ("v0.3.16.1", "cloud", (0, 3, 16, 1), True),
        # Local and unreleased builds report a placeholder instead of a release.
        ("v0.0.0-dev", "core", None, False),
        (None, None, None, False),
        ("not-a-version", "core", None, False),
    ],
)
def test_from_config(
    version: str | None,
    server_env: str | None,
    parsed: tuple[int, int, int, int] | None,
    is_cloud: bool,
) -> None:
    info = GmsServerInfo.from_config(_config(version, server_env))

    assert info.parsed_version == parsed
    assert info.is_cloud == is_cloud


def test_from_empty_config() -> None:
    info = GmsServerInfo.from_config({})

    assert info == GmsServerInfo(version=None, server_env=None)


class _FakeGms:
    def __init__(self) -> None:
        self.now = 0.0
        self.calls: list[str] = []
        self.version = "v1.7.0"
        self.error: Exception | None = None

    def fetch(self, url: str) -> Mapping[str, Any]:
        self.calls.append(url)
        if self.error:
            raise self.error
        return _config(self.version)


@pytest.fixture
def gms() -> _FakeGms:
    return _FakeGms()


@pytest.fixture
def cache(gms: _FakeGms) -> GmsServerInfoCache:
    return GmsServerInfoCache(
        ttl_seconds=60, fetch_config=gms.fetch, clock=lambda: gms.now
    )


def test_cached_per_url_for_the_ttl(gms: _FakeGms, cache: GmsServerInfoCache) -> None:
    assert cache.get("http://gms:8080/").version == "v1.7.0"
    gms.version = "v1.8.0"
    gms.now = 59

    assert cache.get("http://gms:8080").version == "v1.7.0"
    assert cache.get("http://other:8080").version == "v1.8.0"
    assert gms.calls == ["http://gms:8080", "http://other:8080"]

    gms.now = 61
    assert cache.get("http://gms:8080").version == "v1.8.0"


def test_failures_propagate_and_are_not_cached(
    gms: _FakeGms, cache: GmsServerInfoCache
) -> None:
    gms.error = ConnectionError("GMS is down")
    with pytest.raises(ConnectionError):
        cache.get("http://gms:8080")

    gms.error = None
    assert cache.get("http://gms:8080").version == "v1.7.0"
    assert len(gms.calls) == 2


def test_caches_are_independent(gms: _FakeGms) -> None:
    first = GmsServerInfoCache(fetch_config=gms.fetch, clock=lambda: gms.now)
    second = GmsServerInfoCache(fetch_config=gms.fetch, clock=lambda: gms.now)

    first.get("http://gms:8080")
    second.get("http://gms:8080")

    assert len(gms.calls) == 2
