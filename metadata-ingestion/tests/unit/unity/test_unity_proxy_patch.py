"""The databricks-sql proxy-auth monkeypatch must never break SQL connectivity:
every failure inside it has to fall back to the original THttpClient.open, and
applying it must fail soft when the connector internals are not as expected."""

import logging
from typing import Any, Optional
from unittest.mock import MagicMock, patch

import pytest
from urllib3 import HTTPSConnectionPool

from datahub.ingestion.source.unity import proxy_patch
from datahub.ingestion.source.unity.proxy_patch import (
    _create_patched_open_method,
    _log_proxy_environment,
    apply_databricks_proxy_fix,
    mask_proxy_credentials,
)

PROXY_URL = "http://proxy.example.com:3128"
PROXY_URL_WITH_AUTH = "http://alice:s3cret@proxy.example.com:3128"


class _FakeThriftClient:
    """The attributes of databricks-sql's THttpClient that the patch reads."""

    def __init__(
        self,
        scheme: str = "https",
        proxy: bool = False,
        proxy_uri: Optional[str] = None,
        with_real_host: bool = True,
    ) -> None:
        self.scheme = scheme
        self.host = "proxy.example.com" if proxy else "dbc.example.com"
        self.port = 8080 if proxy else 443
        self.max_connections = 2
        self._proxy = proxy
        self.proxy_uri = proxy_uri
        if proxy and with_real_host:
            self.realhost = "dbc.example.com"
            self.realport = 443

    def using_proxy(self) -> bool:
        return self._proxy


def _pool(client: Any) -> Any:
    return getattr(client, "_THttpClient__pool", None)


@pytest.fixture
def proxy_env(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """No proxy variables at all, so each test sets exactly the ones it means."""
    for var in proxy_patch.PROXY_VARS:
        monkeypatch.delenv(var, raising=False)
    return monkeypatch


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        (None, "None"),
        ("http://user:secret@proxy:3128", "http://user:***@proxy:3128"),
        ("http://user@proxy:3128", "http://user:***@proxy:3128"),
        ("http://proxy:3128", "http://proxy:3128"),
    ],
)
def test_mask_proxy_credentials_never_leaks_a_password(
    url: Optional[str], expected: str
) -> None:
    assert mask_proxy_credentials(url) == expected


def test_direct_connection_builds_a_pool_without_calling_the_original_open() -> None:
    original = MagicMock(name="original_open")
    client = _FakeThriftClient()

    _create_patched_open_method(original)(client)

    pool = _pool(client)
    assert isinstance(pool, HTTPSConnectionPool)
    assert (pool.host, pool.port) == (client.host, client.port)
    original.assert_not_called()


def test_unknown_scheme_falls_back_to_the_original_open() -> None:
    original = MagicMock(name="original_open")
    client = _FakeThriftClient(scheme="ftp")

    _create_patched_open_method(original)(client)

    original.assert_called_once_with(client)
    assert _pool(client) is None


def test_proxy_client_without_real_host_falls_back_before_building_a_proxy_manager(
    proxy_env: pytest.MonkeyPatch,
) -> None:
    """realhost/realport are what the proxied pool connects to; without them
    there is nothing to build, so the fallback happens before ProxyManager."""
    proxy_env.setenv("HTTPS_PROXY", PROXY_URL)
    original = MagicMock(name="original_open")
    client = _FakeThriftClient(proxy=True, proxy_uri=PROXY_URL, with_real_host=False)

    with patch("urllib3.poolmanager.ProxyManager") as proxy_manager:
        _create_patched_open_method(original)(client)

    original.assert_called_once_with(client)
    proxy_manager.assert_not_called()


def test_proxy_credentials_from_the_environment_become_proxy_auth_headers(
    proxy_env: pytest.MonkeyPatch,
) -> None:
    """The fix itself: databricks-sql < 3 dropped the Proxy-Authorization header,
    so the patched pool is built through a ProxyManager carrying it."""
    proxy_env.setenv("HTTPS_PROXY", PROXY_URL_WITH_AUTH)
    original = MagicMock(name="original_open")
    client = _FakeThriftClient(proxy=True, proxy_uri=PROXY_URL_WITH_AUTH)

    with patch("urllib3.poolmanager.ProxyManager") as proxy_manager:
        _create_patched_open_method(original)(client)

    assert proxy_manager.call_args.args == (PROXY_URL_WITH_AUTH,)
    assert "proxy-authorization" in proxy_manager.call_args.kwargs["proxy_headers"]
    connection = proxy_manager.return_value.connection_from_host
    connection.assert_called_once()
    assert connection.call_args.kwargs["host"] == client.realhost
    assert connection.call_args.kwargs["port"] == client.realport
    assert _pool(client) is connection.return_value
    original.assert_not_called()


def test_proxy_without_credentials_builds_a_pool_without_auth_headers(
    proxy_env: pytest.MonkeyPatch,
) -> None:
    proxy_env.setenv("HTTPS_PROXY", PROXY_URL)
    original = MagicMock(name="original_open")
    client = _FakeThriftClient(proxy=True, proxy_uri=PROXY_URL)

    with patch("urllib3.poolmanager.ProxyManager") as proxy_manager:
        _create_patched_open_method(original)(client)

    assert proxy_manager.call_args.kwargs["proxy_headers"] is None
    original.assert_not_called()
    assert _pool(client) is not None


def test_failure_inside_the_patch_falls_back_to_the_original_open() -> None:
    original = MagicMock(name="original_open")
    client = _FakeThriftClient(proxy=True, proxy_uri=PROXY_URL)
    client.using_proxy = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]

    _create_patched_open_method(original)(client)

    original.assert_called_once_with(client)


def test_failure_in_the_patch_and_in_the_fallback_raises_the_fallback_error() -> None:
    original = MagicMock(side_effect=ConnectionError("fallback failed"))
    client = _FakeThriftClient(proxy=True, proxy_uri=PROXY_URL)

    with (
        patch(
            "urllib3.poolmanager.ProxyManager", side_effect=RuntimeError("patch broke")
        ),
        pytest.raises(ConnectionError),
    ):
        _create_patched_open_method(original)(client)


def test_apply_fix_replaces_thrift_open_and_reports_success() -> None:
    import databricks.sql.auth.thrift_http_client as thrift_http

    before = thrift_http.THttpClient.open
    try:
        assert apply_databricks_proxy_fix()
        assert thrift_http.THttpClient.open is not before
    finally:
        thrift_http.THttpClient.open = before  # type: ignore[method-assign]


@pytest.mark.parametrize(
    "error",
    [
        ImportError("no databricks.sql"),
        AttributeError("module 'thrift.transport' has no attribute 'THttpClient'"),
        AttributeError("something else"),
        RuntimeError("unexpected"),
    ],
)
def test_apply_fix_fails_soft_when_connector_internals_are_unavailable(
    error: Exception,
) -> None:
    """Only the known thrift incompatibility is surfaced to the operator as a
    global warning; any other import problem just leaves the fix unapplied."""
    with (
        patch.object(proxy_patch, "_ensure_thrift_imports", side_effect=error),
        patch(
            "datahub.utilities.global_warning_util.add_global_warning"
        ) as global_warning,
    ):
        assert not apply_databricks_proxy_fix()

    is_thrift_incompat = isinstance(error, AttributeError) and "thrift" in str(error)
    assert global_warning.called == is_thrift_incompat


def test_proxy_environment_is_logged_masked(
    proxy_env: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    proxy_env.setenv("HTTPS_PROXY", "http://user:secret@proxy.example.com:3128")

    with caplog.at_level(logging.INFO, logger=proxy_patch.__name__):
        _log_proxy_environment()

    assert "secret" not in caplog.text
    assert "user:***@proxy.example.com" in caplog.text
