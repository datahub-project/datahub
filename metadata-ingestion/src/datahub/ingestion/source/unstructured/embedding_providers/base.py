"""Shared types and HTTP helpers for embedding providers.

Each provider lives in its own submodule and implements ``EmbeddingProvider``.
Providers either use a vendor SDK (only Bedrock, for SigV4 + the AWS credential
chain) or hit raw HTTP endpoints with ``requests`` — the latter mirrors the
wire-level contract used by the Java GMS embedding providers.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Default per-call timeout for HTTP-based providers. Embedding requests are
# typically quick; the long timeout exists only so we don't hang forever on a
# stuck server. Callers can override via ``EmbeddingConfig.request_timeout``.
DEFAULT_HTTP_TIMEOUT_SECONDS = 60

# Retry transient failures from upstream embedding services. urllib3's exponential
# backoff (0.5, 1, 2s) covers most transient blips; persistent outages still surface
# quickly because we cap at 3 retries.
_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def attach_retries(session: requests.Session) -> requests.Session:
    """Mount retry/backoff adapters for 429/5xx on an existing session.

    Used to wrap both vanilla ``requests.Session`` and ``AuthorizedSession``
    (google-auth) so all HTTP-based providers get the same transient-failure
    handling.
    """
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=_RETRY_STATUSES,
        allowed_methods=frozenset({"POST", "GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def build_retrying_session() -> requests.Session:
    """Return a fresh ``requests.Session`` with retry/backoff on 429/5xx."""
    return attach_retries(requests.Session())


@dataclass
class EmbeddingResult:
    """Provider-agnostic embedding response."""

    embeddings: list[list[float]]


class EmbeddingProvider(ABC):
    """Provider interface for generating text embeddings."""

    #: Stable identifier used in log messages and aspect ``modelVersion`` strings,
    #: e.g. ``"bedrock/cohere.embed-english-v3"``. Set by subclasses.
    model_id: str

    @abstractmethod
    def embed(self, texts: list[str]) -> EmbeddingResult:
        """Generate embeddings for the given input texts."""


def post_json(
    url: str,
    *,
    body: dict[str, Any],
    session: Optional[requests.Session] = None,
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """POST a JSON body and return the parsed JSON response.

    Raises an informative ``RuntimeError`` if the server returns a non-2xx
    status or a non-JSON body, including the response text so callers (and
    the embedding error mitigation helper) can pattern-match on it.
    """
    sender = session or requests
    response = sender.post(url, json=body, timeout=timeout)
    if not response.ok:
        raise RuntimeError(
            f"Embedding API call failed: {response.status_code} {response.reason}: "
            f"{response.text[:500]}"
        )
    try:
        return response.json()
    except ValueError as e:
        # 200 OK with HTML/plain-text body — usually a misconfigured proxy or
        # captive portal. Surface enough of the body to debug it.
        raise RuntimeError(
            f"Embedding API returned non-JSON body: {response.text[:500]}"
        ) from e
