from __future__ import annotations

import logging
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, Optional, Tuple
from urllib.parse import urlparse

import requests

logger = logging.getLogger(__name__)

# Tokens are refreshed this many seconds before they expire to avoid presenting a
# credential that expires mid-flight on a long-running ingestion task.
DEFAULT_REFRESH_BUFFER_SECONDS = 300


@dataclass(frozen=True)
class TokenResult:
    """A bearer token plus the expiry reported by the issuing provider.

    `expires_at` is absolute epoch seconds, taken from the OAuth token response
    (e.g. `expires_in`, or azure-identity's `expires_on`) — never decoded from
    the token itself, which is opaque to the client (RFC 6749). `None` means the
    provider does not report an expiry, so the token is re-fetched on each call.
    """

    token: str
    expires_at: Optional[float] = None


class TokenProvider(ABC):
    """Produces a bearer token for authenticating to DataHub GMS.

    Implementations return a *fresh* token each call; callers invoke
    get_token() per request, so short-lived/rotating credentials work
    transparently. Concrete, configurable providers also implement create().
    """

    @abstractmethod
    def get_token(self) -> TokenResult: ...

    @classmethod
    def create(cls, config: Optional[dict]) -> "TokenProvider":
        raise NotImplementedError(
            f"{cls.__name__} is not constructable from declarative config"
        )


class StaticTokenProvider(TokenProvider):
    """Wraps a fixed token string. Back-compat for the existing PAT path."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self) -> TokenResult:
        return TokenResult(self._token)

    @classmethod
    def create(cls, config: Optional[dict]) -> "StaticTokenProvider":
        cfg = config or {}
        token = cfg.get("token")
        if not token:
            raise ValueError("static token provider requires config.token")
        return cls(token)


class CachingTokenProvider(TokenProvider):
    """Caches a token from a raw fetch callable and refreshes before expiry.

    All refresh logic lives here so concrete providers only implement raw
    acquisition. Thread-safe: a single DataHubGraph/emitter may be shared by
    concurrent ingestion tasks in the executor.
    """

    def __init__(
        self,
        fetch: Callable[[], TokenResult],
        *,
        refresh_buffer_seconds: int = DEFAULT_REFRESH_BUFFER_SECONDS,
    ) -> None:
        self._fetch = fetch
        self._refresh_buffer_seconds = refresh_buffer_seconds
        self._lock = threading.Lock()
        self._cached: Optional[Tuple[TokenResult, float]] = None

    def get_token(self) -> TokenResult:
        with self._lock:
            if self._cached is not None and not self._is_stale(*self._cached):
                return self._cached[0]
            result = self._fetch()
            self._cached = (result, time.time())
            return result

    def invalidate(self) -> None:
        with self._lock:
            self._cached = None

    def _is_stale(self, cached: TokenResult, fetched_at: float) -> bool:
        # No reported expiry -> always re-fetch. The underlying acquisition is
        # cheap (a projected-token file read, or azure-identity's own caching).
        if cached.expires_at is None:
            return True
        # Clamp the buffer to half the token's observed lifetime: with an IdP
        # issuing tokens whose lifetime <= the buffer (e.g. Keycloak's default
        # 300s access tokens vs the default 300s buffer), a fixed buffer would
        # mark every token permanently stale and turn each client request into
        # a synchronous IdP round trip.
        lifetime = cached.expires_at - fetched_at
        buffer = min(self._refresh_buffer_seconds, lifetime / 2)
        return time.time() >= (cached.expires_at - buffer)


def _origin(url: Optional[str]) -> Optional[Tuple[str, Optional[str], Optional[int]]]:
    if not url:
        return None
    parsed = urlparse(url)
    return (parsed.scheme, parsed.hostname, parsed.port)


class TokenProviderAuth(requests.auth.AuthBase):
    """Installs a fresh bearer token on each request from a TokenProvider.

    On a 401 it invalidates the provider's cache (if supported) and retries
    once — covering early server-side revocation. Bounded via thread-local state.
    """

    def __init__(self, provider: TokenProvider, *, retry_on_401: bool = True) -> None:
        self._provider = provider
        self._retry_on_401 = retry_on_401
        self._thread_local = threading.local()

    def __call__(self, request: requests.PreparedRequest) -> requests.PreparedRequest:
        self._thread_local.retried = False
        self._thread_local.original_origin = _origin(request.url)
        request.headers["Authorization"] = f"Bearer {self._provider.get_token().token}"
        if self._retry_on_401:
            request.register_hook("response", self._handle_401)
        return request

    def _handle_401(
        self, response: requests.Response, **kwargs: object
    ) -> requests.Response:
        if response.status_code != 401 or getattr(self._thread_local, "retried", False):
            return response
        # Never re-attach the token after a redirect off the original origin:
        # requests strips Authorization not just on cross-host redirects but also
        # on same-host scheme downgrades and port changes (should_strip_auth), and
        # retrying here would hand the GMS credential to that other endpoint —
        # e.g. over plaintext HTTP after an https->http redirect. Strict
        # (scheme, host, port) equality is slightly stricter than requests' rule
        # (it also refuses the benign http->https upgrade); the cost is only a
        # surfaced 401, never a leaked token.
        if _origin(response.request.url) != getattr(
            self._thread_local, "original_origin", None
        ):
            return response
        # A streamed body (file object / generator) was consumed by the first
        # send and cannot be replayed — a retry would transmit an empty body
        # under the original Content-Length. Surface the 401 instead.
        body = response.request.body
        if body is not None and not isinstance(body, (str, bytes)):
            return response
        invalidate = getattr(self._provider, "invalidate", None)
        if invalidate is None:
            return response
        self._thread_local.retried = True
        invalidate()
        # Drain and release the connection before retrying.
        _ = response.content
        response.close()
        prepared = response.request.copy()
        prepared.headers["Authorization"] = f"Bearer {self._provider.get_token().token}"
        new_response = response.connection.send(prepared, **kwargs)  # type: ignore[attr-defined]
        new_response.history.append(response)
        new_response.request = prepared
        return new_response
