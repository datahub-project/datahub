"""DNS-rebinding protection for /mcp while GMS auth is off.

With auth off, any request that reaches /mcp acts as DataHub's system user. A web
page can rebind its own hostname to this machine and then call /mcp from the
browser as a same-origin request. Such a request still carries the attacker's
hostname in ``Host`` (and ``Origin``), so /mcp only answers requests addressed to
an allowed host, from an allowed origin when there is one.

The allowlists are configured, never derived from the request: ``Host`` is
exactly what the attacker controls. Behind the frontend, the host the client used
arrives in ``X-Forwarded-Host``, which is believed only from a configured trusted
proxy; from anyone else it is ignored.
"""

import dataclasses
import ipaddress
import socket
import threading
import time
from typing import Callable, Optional, Sequence
from urllib.parse import urlsplit

from starlette.types import Scope

TRUSTED_PROXY_RESOLVE_TTL_SECONDS = 60.0


@dataclasses.dataclass(frozen=True)
class _HostRule:
    host: str
    port: Optional[int]  # None: any port

    def matches(self, host: str, port: Optional[int]) -> bool:
        return host == self.host and (self.port is None or port == self.port)


@dataclasses.dataclass(frozen=True)
class _OriginRule:
    scheme: str
    host: _HostRule


def _split_host_port(value: str) -> tuple[str, Optional[int]]:
    """``host``, ``host:port``, ``[v6]`` or ``[v6]:port``, lowercased."""
    value = value.strip().lower()
    if value.startswith("["):
        host, _, rest = value[1:].partition("]")
        port = rest[1:] if rest.startswith(":") else ""
    else:
        host, _, port = value.partition(":")
    if port and not port.isdigit():
        raise ValueError(f"Invalid port in {value!r}")
    return host.rstrip("."), int(port) if port else None


def parse_host_rule(entry: str) -> _HostRule:
    host, port = _split_host_port(entry)
    if not host:
        raise ValueError(f"Invalid host {entry!r}")
    return _HostRule(host, port)


def parse_origin_rule(entry: str) -> _OriginRule:
    parts = urlsplit(entry.strip().lower())
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ValueError(f"Invalid origin {entry!r}: expected scheme://host[:port]")
    return _OriginRule(parts.scheme, parse_host_rule(parts.netloc))


class _TrustedProxies:
    """Peers allowed to say which host a client used: IPs, networks or hostnames.

    Hostnames (e.g. the frontend's compose service name) are resolved at most once
    a minute, since a container's address can change when it restarts.
    """

    def __init__(
        self,
        entries: Sequence[str],
        resolve: Callable[[str], set[str]],
        clock: Callable[[], float],
    ) -> None:
        self._networks: list[ipaddress.IPv4Network | ipaddress.IPv6Network] = []
        self._hostnames: list[str] = []
        for entry in entries:
            try:
                self._networks.append(ipaddress.ip_network(entry.strip(), strict=False))
            except ValueError:
                self._hostnames.append(entry.strip().lower())
        self._resolve = resolve
        self._clock = clock
        self._resolved: tuple[float, frozenset[str]] = (float("-inf"), frozenset())
        self._lock = threading.Lock()

    def __contains__(self, peer: str) -> bool:
        try:
            address = ipaddress.ip_address(peer)
        except ValueError:
            return False
        if any(address in network for network in self._networks):
            return True
        return bool(self._hostnames) and peer in self._hostname_addresses()

    def _hostname_addresses(self) -> frozenset[str]:
        with self._lock:
            expires, addresses = self._resolved
            if self._clock() < expires:
                return addresses
            resolved: set[str] = set()
            for name in self._hostnames:
                try:
                    resolved |= self._resolve(name)
                except OSError:
                    pass  # not up yet: trust nobody by that name for now
            self._resolved = (
                self._clock() + TRUSTED_PROXY_RESOLVE_TTL_SECONDS,
                frozenset(resolved),
            )
            return self._resolved[1]


def _resolve(name: str) -> set[str]:
    return {str(info[4][0]) for info in socket.getaddrinfo(name, None)}


def _header(scope: Scope, name: bytes) -> Optional[str]:
    values = [v for k, v in scope.get("headers", []) if k.lower() == name]
    # More than one is ambiguous: refuse rather than pick.
    return values[0].decode("latin-1") if len(values) == 1 else None


class HostGuard:
    def __init__(
        self,
        allowed_hosts: Sequence[str],
        allowed_origins: Sequence[str],
        trusted_proxies: Sequence[str] = (),
        *,
        resolve: Callable[[str], set[str]] = _resolve,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._hosts = [parse_host_rule(h) for h in allowed_hosts if h.strip()]
        self._origins = [parse_origin_rule(o) for o in allowed_origins if o.strip()]
        self._trusted = _TrustedProxies(
            [p for p in trusted_proxies if p.strip()], resolve, clock
        )

    def rejection(self, scope: Scope) -> Optional[str]:
        """Why this request may not use /mcp, or None if it may."""
        peer = (scope.get("client") or ("", 0))[0]
        host_header = (
            _header(scope, b"x-forwarded-host") if peer in self._trusted else None
        ) or _header(scope, b"host")
        if host_header is None:
            return "The request has no single Host header."
        try:
            host, port = _split_host_port(host_header.split(",")[0])
        except ValueError:
            return "The request's Host header is malformed."
        if not any(rule.matches(host, port) for rule in self._hosts):
            return f"Host {host!r} is not in DATAHUB_AI_ALLOWED_HOSTS."

        origin = _header(scope, b"origin")
        if origin is not None and not self._origin_allowed(origin):
            return f"Origin {origin!r} is not in DATAHUB_AI_ALLOWED_ORIGINS."
        return None

    def _origin_allowed(self, origin: str) -> bool:
        try:
            parts = urlsplit(origin.strip().lower())
            host, port = _split_host_port(parts.netloc)
        except ValueError:
            return False
        if port is None:
            port = {"http": 80, "https": 443}.get(parts.scheme)
        return any(
            rule.scheme == parts.scheme and rule.host.matches(host, port)
            for rule in self._origins
        )
