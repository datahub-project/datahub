from __future__ import annotations

import base64
import json
import logging
import threading
import time
from abc import ABC, abstractmethod
from typing import Callable, Optional

import requests

logger = logging.getLogger(__name__)

# JWTs are refreshed this many seconds before their 'exp' to avoid presenting a
# token that expires mid-flight on a long-running ingestion task.
DEFAULT_REFRESH_BUFFER_SECONDS = 300


class TokenProvider(ABC):
    """Produces a bearer token for authenticating to DataHub GMS.

    Implementations return a *fresh* token each call; callers invoke
    get_token() per request, so short-lived/rotating credentials work
    transparently. Concrete, configurable providers also implement create().
    """

    @abstractmethod
    def get_token(self) -> str: ...

    @classmethod
    def create(cls, config: Optional[dict]) -> "TokenProvider":
        raise NotImplementedError(
            f"{cls.__name__} is not constructable from declarative config"
        )


class StaticTokenProvider(TokenProvider):
    """Wraps a fixed token string. Back-compat for the existing PAT path."""

    def __init__(self, token: str) -> None:
        self._token = token

    def get_token(self) -> str:
        return self._token

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
        fetch: Callable[[], str],
        *,
        refresh_buffer_seconds: int = DEFAULT_REFRESH_BUFFER_SECONDS,
    ) -> None:
        self._fetch = fetch
        self._refresh_buffer_seconds = refresh_buffer_seconds
        self._lock = threading.Lock()
        self._token: Optional[str] = None
        self._expires_at: Optional[float] = None

    def get_token(self) -> str:
        with self._lock:
            if self._token is not None and not self._is_stale():
                return self._token
            token = self._fetch()
            self._token = token
            self._expires_at = self._parse_exp(token)
            return token

    def invalidate(self) -> None:
        with self._lock:
            self._token = None
            self._expires_at = None

    def _is_stale(self) -> bool:
        # Unknown expiry -> always re-fetch. Underlying providers (azure-identity,
        # a projected-token file read) do their own cheap caching, so this is safe.
        if self._expires_at is None:
            return True
        return time.time() >= (self._expires_at - self._refresh_buffer_seconds)

    @staticmethod
    def _parse_exp(token: str) -> Optional[float]:
        # Best-effort parse of the JWT 'exp' WITHOUT verifying the signature — we
        # are the client; GMS verifies. On any failure return None (re-fetch each call).
        try:
            payload_b64 = token.split(".")[1]
            payload_b64 += "=" * (-len(payload_b64) % 4)
            payload = json.loads(base64.urlsafe_b64decode(payload_b64))
            exp = payload.get("exp")
            return float(exp) if exp is not None else None
        except Exception:
            logger.debug("Could not parse 'exp' from token; will re-fetch each call")
            return None


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
        request.headers["Authorization"] = f"Bearer {self._provider.get_token()}"
        if self._retry_on_401:
            request.register_hook("response", self._handle_401)
        return request

    def _handle_401(
        self, response: requests.Response, **kwargs: object
    ) -> requests.Response:
        if response.status_code != 401 or getattr(self._thread_local, "retried", False):
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
        prepared.headers["Authorization"] = f"Bearer {self._provider.get_token()}"
        new_response = response.connection.send(prepared, **kwargs)  # type: ignore[attr-defined]
        new_response.history.append(response)
        new_response.request = prepared
        return new_response
