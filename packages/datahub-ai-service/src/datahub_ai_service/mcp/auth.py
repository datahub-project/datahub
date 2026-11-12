"""Caller authentication for /mcp: each request carries its caller's DataHub token.

The token is checked against GMS with GraphQL ``me`` and the verdict cached
briefly. Every request then gets its own DataHub client built from that token,
so callers never share a client, a credential or a request context.

A token GMS rejects is a 401. When GMS can't be reached or answers with a
server error, the request is a 503: the service fails closed and never treats
an unchecked token as valid.
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
from datahub.ingestion.graph.config import ClientMode, DatahubClientConfig
from datahub.sdk.main_client import DataHubClient
from loguru import logger
from starlette.types import ASGIApp, Receive, Scope, Send

from .._version import __version__

_ME_QUERY = "query getMe { me { corpUser { urn } } }"
_MAX_CACHED_TOKENS = 1024
_MAX_CONCURRENT_VERIFICATIONS = 8

CALLER_STATE_KEY = "datahub_caller"


@dataclasses.dataclass(frozen=True)
class VerifiedCaller:
    token: str = dataclasses.field(repr=False)
    actor_urn: str


class InvalidCredentialError(Exception):
    """GMS rejected the caller's token."""


class GmsUnavailableError(Exception):
    """GMS couldn't say whether the caller's token is valid."""


class CallerClientFactory:
    """Builds a DataHub client for one caller's token. Construction makes no calls."""

    def __init__(self, gms_url: str, *, timeout_seconds: float) -> None:
        self._gms_url = gms_url
        self._timeout_seconds = timeout_seconds

    def build(self, token: str) -> DataHubClient:
        return DataHubClient(
            config=DatahubClientConfig(
                server=self._gms_url,
                token=token,
                timeout_sec=self._timeout_seconds,
                client_mode=ClientMode.SDK,
                datahub_component=f"datahub-ai-service/{__version__}",
            )
        )


def _http_status(error: BaseException) -> Optional[int]:
    """The HTTP status behind an SDK error, if any (the SDK chains requests' HTTPError)."""
    seen: set[int] = set()
    cause: Optional[BaseException] = error
    while cause is not None and id(cause) not in seen:
        seen.add(id(cause))
        response = getattr(cause, "response", None)
        status = getattr(response, "status_code", None)
        if isinstance(status, int):
            return status
        cause = cause.__cause__ or cause.__context__
    return None


def classify_gms_error(error: Exception) -> Exception:
    """Map a failed ``me`` call to InvalidCredentialError or GmsUnavailableError."""
    status = _http_status(error)
    if status in (401, 403):
        return InvalidCredentialError(f"GMS rejected the token (HTTP {status})")
    if status is not None and (status >= 500 or status == 429):
        return GmsUnavailableError(f"GMS answered HTTP {status}")
    if isinstance(error, requests.RequestException):
        return GmsUnavailableError(f"GMS unreachable: {type(error).__name__}")
    if isinstance(error, GraphError):
        return InvalidCredentialError("GMS refused the identity query")
    return GmsUnavailableError(f"Unexpected error checking the token: {error!r}")


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

    async def verify(self, token: str) -> VerifiedCaller:
        key = hashlib.sha256(token.encode()).hexdigest()
        actor_urn = self._cached(key)
        if actor_urn is None:
            async with self._semaphore:
                # Requests queued behind the first check of a token reuse its verdict.
                actor_urn = self._cached(key)
                if actor_urn is None:
                    actor_urn = await asyncio.to_thread(self._verify_with_gms, token)
                    self._remember(key, actor_urn)
        return VerifiedCaller(token=token, actor_urn=actor_urn)

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
    """ASGI middleware in front of the MCP app: verifies the caller or refuses."""

    def __init__(self, app: ASGIApp, verifier: DataHubTokenVerifier) -> None:
        self._app = app
        self._verifier = verifier

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self._app(scope, receive, send)
            return

        token = _bearer_token(scope)
        if token is None:
            await _send_error(
                send,
                401,
                "missing_token",
                "Send a DataHub personal access token as 'Authorization: Bearer <token>'.",
            )
            return
        try:
            caller = await self._verifier.verify(token)
        except InvalidCredentialError as e:
            logger.info(f"Rejected an MCP request: {e}")
            await _send_error(send, 401, "invalid_token", "DataHub rejected the token.")
            return
        except GmsUnavailableError as e:
            logger.warning(f"Could not verify an MCP caller: {e}")
            await _send_error(
                send,
                503,
                "datahub_unavailable",
                "DataHub could not be reached to check the token. Try again shortly.",
            )
            return

        scope.setdefault("state", {})[CALLER_STATE_KEY] = caller
        await self._app(scope, receive, send)
