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

# Per-call timeout for HTTP-based providers. Embedding requests are typically
# quick; a long timeout exists only so we don't hang forever on a stuck server.
HTTP_TIMEOUT_SECONDS = 60


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
) -> dict[str, Any]:
    """POST a JSON body and return the parsed JSON response.

    Raises an informative ``RuntimeError`` if the server returns a non-2xx
    status, including the response body so callers (and the embedding error
    mitigation helper) can pattern-match on it.
    """
    sender = session or requests
    response = sender.post(url, json=body, timeout=HTTP_TIMEOUT_SECONDS)
    if not response.ok:
        raise RuntimeError(
            f"Embedding API call failed: {response.status_code} {response.reason}: "
            f"{response.text[:500]}"
        )
    return response.json()
