"""Caller authentication for /mcp, following GMS's own auth setting.

When GMS requires authentication, each request carries its caller's DataHub
token. It is checked with GraphQL ``me`` and the verdict cached briefly; a token
GMS rejects is a 401. When GMS auth is off, no token is needed: GMS runs every
request as its system user, as it does for direct calls, and any header the
caller sent is passed on unchanged.

Every request gets its own DataHub client built from its caller's credential, so
callers never share a client, a credential or a request context.

The service fails closed: while GMS can't be reached, or its auth mode can't be
told, requests get a 503 and an unchecked token is never treated as valid. In
``required`` mode, /mcp is unavailable unless GMS auth is on.
"""

import asyncio
import dataclasses
import hashlib
import json
import threading
import time
from typing import Callable, Optional

import requests
from datahub.configuration.common import GraphError
from loguru import logger
from starlette.types import ASGIApp, Receive, Scope, Send

from datahub_core_mcp_tools.graphql_helpers import http_status

from ..metrics import ServiceMetrics
from .auth_mode import GmsAuth, GmsAuthDetector
from .client_factory import CallerClientFactory
from .host_guard import HostGuard

_ME_QUERY = "query getMe { me { corpUser { urn } } }"
_MAX_CACHED_TOKENS = 1024
_MAX_CONCURRENT_VERIFICATIONS = 8

CALLER_STATE_KEY = "datahub_caller"


@dataclasses.dataclass(frozen=True)
class Caller:
    token: Optional[str] = dataclasses.field(repr=False)
    """The caller's credential, passed to GMS as is; None when none was sent."""

    actor_urn: Optional[str]
    """Who GMS said the token belongs to; None when GMS auth is off."""

    gms_auth: GmsAuth

    @property
    def verified(self) -> bool:
        return self.gms_auth is GmsAuth.ON


class InvalidCredentialError(Exception):
    """GMS rejected the caller's token."""


class GmsUnavailableError(Exception):
    """GMS couldn't say whether the caller's token is valid."""


def classify_gms_error(error: Exception) -> Exception:
    """Map a failed ``me`` call to InvalidCredentialError or GmsUnavailableError."""
    status = http_status(error)
    if status in (401, 403):
        return InvalidCredentialError(f"GMS rejected the token (HTTP {status})")
    if status is not None and (status >= 500 or status == 429):
        return GmsUnavailableError(f"GMS answered HTTP {status}")
    if isinstance(error, requests.RequestException):
        return GmsUnavailableError(f"GMS unreachable: {type(error).__name__}")
    if isinstance(error, GraphError):
        return InvalidCredentialError("GMS refused the identity query")
    return GmsUnavailableError(f"Unexpected error checking the token: {error!r}")


def _token_key(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


class DataHubTokenVerifier:
    """Checks caller tokens with GMS and remembers the verdict for a short TTL.

    Only successful verdicts are cached, keyed by a hash of the token, so a
    revoked or rejected token is checked again on its next use.
    """

    def __init__(
        self,
        client_factory: CallerClientFactory,
        *,
        cache_ttl_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client_factory = client_factory
        self._cache_ttl_seconds = cache_ttl_seconds
        self._clock = clock
        self._verdicts: dict[str, tuple[float, str]] = {}
        self._lock = threading.Lock()
        self._semaphore = asyncio.Semaphore(_MAX_CONCURRENT_VERIFICATIONS)

    def clear(self) -> None:
        """Forget every verdict: GMS's auth mode changed."""
        with self._lock:
            self._verdicts.clear()

    def invalidate(self, token: str) -> None:
        """Forget one token's verdict: GMS rejected it since."""
        with self._lock:
            self._verdicts.pop(_token_key(token), None)

    async def verify(self, token: str) -> Caller:
        key = _token_key(token)
        actor_urn = self._cached(key)
        if actor_urn is None:
            async with self._semaphore:
                # Requests queued behind the first check of a token reuse its verdict.
                actor_urn = self._cached(key)
                if actor_urn is None:
                    actor_urn = await asyncio.to_thread(self._verify_with_gms, token)
                    self._remember(key, actor_urn)
        return Caller(token=token, actor_urn=actor_urn, gms_auth=GmsAuth.ON)

    def _verify_with_gms(self, token: str) -> str:
        client = self._client_factory.build(token)
        try:
            result = client._graph.execute_graphql(_ME_QUERY)
        except Exception as e:
            raise classify_gms_error(e) from e
        finally:
            client._graph.close()
        corp_user = ((result or {}).get("me") or {}).get("corpUser") or {}
        actor_urn = corp_user.get("urn")
        if not isinstance(actor_urn, str) or not actor_urn:
            raise InvalidCredentialError("GMS returned no user for the token")
        return actor_urn

    def _cached(self, key: str) -> Optional[str]:
        with self._lock:
            entry = self._verdicts.get(key)
            if entry is None:
                return None
            if self._clock() >= entry[0]:
                del self._verdicts[key]
                return None
            return entry[1]

    def _remember(self, key: str, actor_urn: str) -> None:
        with self._lock:
            now = self._clock()
            if len(self._verdicts) >= _MAX_CACHED_TOKENS:
                for stale in [
                    k for k, (exp, _) in self._verdicts.items() if exp <= now
                ]:
                    del self._verdicts[stale]
            if len(self._verdicts) >= _MAX_CACHED_TOKENS:
                # dicts keep insertion order: drop the oldest verdict.
                del self._verdicts[next(iter(self._verdicts))]
            self._verdicts[key] = (now + self._cache_ttl_seconds, actor_urn)


def _bearer_token(scope: Scope) -> Optional[str]:
    for name, value in scope.get("headers", []):
        if name.lower() == b"authorization":
            scheme, _, token = value.decode("latin-1").partition(" ")
            if scheme.lower() == "bearer" and token.strip():
                return token.strip()
            return None
    return None


async def _send_error(send: Send, status: int, error: str, description: str) -> None:
    headers = [(b"content-type", b"application/json")]
    if status == 401:
        challenge = f'Bearer error="{error}"' if error != "missing_token" else "Bearer"
        headers.append((b"www-authenticate", challenge.encode()))
    body = json.dumps({"error": error, "error_description": description}).encode()
    headers.append((b"content-length", str(len(body)).encode()))
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


class McpAuthMiddleware:
    """ASGI middleware in front of the MCP app: admits the caller or refuses."""

    def __init__(
        self,
        app: ASGIApp,
        verifier: DataHubTokenVerifier,
        detector: GmsAuthDetector,
        host_guard: HostGuard,
        *,
        required: bool,
        metrics: Optional[ServiceMetrics] = None,
    ) -> None:
        self._app = app
        self._metrics = metrics
        self._verifier = verifier
        self._detector = detector
        self._host_guard = host_guard
        self._required = required
        self._gms_auth: Optional[GmsAuth] = None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        gms_auth = await asyncio.to_thread(self._detector.detect)
        if gms_auth is not self._gms_auth:
            # A verdict reached under one mode means nothing under another.
            self._verifier.clear()
            self._gms_auth = gms_auth

        if gms_auth is GmsAuth.UNKNOWN:
            await self._refuse(
                send,
                503,
                "datahub_unavailable",
                "Can't tell whether DataHub requires authentication: DataHub may be "
                "down. Try again shortly.",
            )
            return
        if gms_auth is GmsAuth.OFF:
            if self._required:
                await self._refuse(
                    send,
                    503,
                    "datahub_auth_off",
                    "DATAHUB_AI_AUTH_MODE is required, but DataHub authentication is "
                    "off, so tokens can't be checked. Turn on "
                    "METADATA_SERVICE_AUTH_ENABLED in DataHub, or set "
                    "DATAHUB_AI_AUTH_MODE=auto.",
                )
                return
            # Anyone who reaches /mcp now acts as the system user: refuse requests
            # a rebound web page could send.
            rejection = self._host_guard.rejection(scope)
            if rejection is not None:
                logger.warning(f"Refused an MCP request: {rejection}")
                await self._refuse(send, 403, "forbidden_host", rejection)
                return
            caller = Caller(
                token=_bearer_token(scope), actor_urn=None, gms_auth=GmsAuth.OFF
            )
        else:
            admitted = await self._verify(scope, send)
            if admitted is None:
                return
            caller = admitted

        scope.setdefault("state", {})[CALLER_STATE_KEY] = caller
        await self._app(scope, receive, send)

    async def _refuse(
        self, send: Send, status: int, error: str, description: str
    ) -> None:
        if self._metrics is not None:
            self._metrics.auth_failures.labels(error).inc()
        await _send_error(send, status, error, description)

    async def _verify(self, scope: Scope, send: Send) -> Optional[Caller]:
        token = _bearer_token(scope)
        if token is None:
            await self._refuse(
                send,
                401,
                "missing_token",
                "Send a DataHub personal access token as 'Authorization: Bearer <token>'.",
            )
            return None
        try:
            return await self._verifier.verify(token)
        except InvalidCredentialError as e:
            logger.info(f"Rejected an MCP request: {e}")
            await self._refuse(
                send, 401, "invalid_token", "DataHub rejected the token."
            )
        except GmsUnavailableError as e:
            logger.warning(f"Could not verify an MCP caller: {e}")
            await self._refuse(
                send,
                503,
                "datahub_unavailable",
                "DataHub could not be reached to check the token. Try again shortly.",
            )
        return None
