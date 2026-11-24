from typing import Optional

import pytest

from datahub_ai_service.config import AiServiceConfig
from datahub_ai_service.mcp.host_guard import HostGuard

DEFAULTS = AiServiceConfig(gms_url="http://gms")
FRONTEND_IP = "172.18.0.5"


def scope(
    host: Optional[str] = "localhost:9002",
    origin: Optional[str] = None,
    forwarded_host: Optional[str] = None,
    peer: str = "127.0.0.1",
    extra: tuple[tuple[bytes, bytes], ...] = (),
) -> dict:
    headers = list(extra)
    for name, value in (
        (b"host", host),
        (b"origin", origin),
        (b"x-forwarded-host", forwarded_host),
    ):
        if value is not None:
            headers.append((name, value.encode()))
    return {"type": "http", "headers": headers, "client": (peer, 50000)}


def guard(trusted: tuple[str, ...] = ()) -> HostGuard:
    return HostGuard(
        DEFAULTS.allowed_hosts,
        DEFAULTS.allowed_origins,
        trusted,
        resolve=lambda name: {FRONTEND_IP} if name == "datahub-frontend" else set(),
    )


@pytest.mark.parametrize(
    "request_scope",
    [
        scope(),
        scope(host="LOCALHOST:9005"),
        scope(host="127.0.0.1"),
        scope(host="[::1]:9002"),
        scope(origin="http://localhost:9002"),
        scope(origin="https://127.0.0.1"),
    ],
)
def test_local_requests_pass(request_scope: dict) -> None:
    assert guard().rejection(request_scope) is None


@pytest.mark.parametrize(
    "request_scope",
    [
        # A rebound page: the browser still names the attacker's host.
        scope(host="attacker.example:9002"),
        scope(origin="http://attacker.example:9002"),
        scope(origin="null"),
        scope(origin="ftp://localhost"),
        scope(host="localhost.attacker.example"),
        scope(host=None),
        scope(host="localhost:http"),
        # Two Host headers: ambiguous, refused.
        scope(extra=((b"host", b"attacker.example"),)),
    ],
)
def test_foreign_requests_are_refused(request_scope: dict) -> None:
    assert guard().rejection(request_scope) is not None


def test_forwarded_host_is_ignored_from_untrusted_peers() -> None:
    # A direct caller can't vouch for itself with X-Forwarded-Host.
    spoofed = scope(host="attacker.example", forwarded_host="localhost:9002")
    harmless = scope(host="localhost:9005", forwarded_host="attacker.example")

    assert guard().rejection(spoofed) is not None
    assert guard().rejection(harmless) is None


@pytest.mark.parametrize(
    "trusted", [(FRONTEND_IP,), ("172.18.0.0/16",), ("datahub-frontend",)]
)
def test_forwarded_host_counts_from_the_trusted_frontend(
    trusted: tuple[str, ...],
) -> None:
    proxied = scope(
        host="datahub-ai-service:9005",
        forwarded_host="localhost:9002",
        peer=FRONTEND_IP,
    )
    rebound = scope(
        host="datahub-ai-service:9005",
        forwarded_host="attacker.example:9002",
        origin="http://attacker.example:9002",
        peer=FRONTEND_IP,
    )

    assert guard(trusted).rejection(proxied) is None
    assert guard(trusted).rejection(rebound) is not None


def test_configured_hosts_and_ports() -> None:
    custom = HostGuard(
        ["datahub.example.com", "ops.example.com:8443"], ["https://datahub.example.com"]
    )

    assert custom.rejection(scope(host="datahub.example.com")) is None
    assert custom.rejection(scope(host="ops.example.com:8443")) is None
    assert custom.rejection(scope(host="ops.example.com:9000")) is not None
    assert (
        custom.rejection(
            scope(host="datahub.example.com", origin="https://datahub.example.com")
        )
        is None
    )
    assert (
        custom.rejection(
            scope(host="datahub.example.com", origin="http://datahub.example.com")
        )
        is not None
    )
    assert custom.rejection(scope(host="localhost")) is not None


def test_invalid_configuration_fails_at_startup() -> None:
    with pytest.raises(ValueError):
        HostGuard(["localhost"], ["localhost"])
