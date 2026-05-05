"""OpenAI (and OpenAI-compatible) embedding provider (raw HTTP)."""

from __future__ import annotations

import os
from typing import Optional

import requests

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
    post_json,
)

OPENAI_API_BASE = "https://api.openai.com/v1"


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Embedding via the OpenAI ``/v1/embeddings`` HTTP endpoint.

    Also handles OpenAI-compatible local servers (e.g. Ollama) when
    ``base_url`` is overridden.
    """

    def __init__(
        self,
        model: str,
        api_key: Optional[str],
        base_url: Optional[str] = None,
        provider_label: str = "openai",
    ):
        # OpenAI proper requires a real key; OpenAI-compatible local servers
        # (Ollama, etc.) accept any token so we fall back to "local".
        resolved_key = api_key or os.environ.get("OPENAI_API_KEY") or "local"

        self._model = model
        self.model_id = f"{provider_label}/{model}"
        self._url = f"{(base_url or OPENAI_API_BASE).rstrip('/')}/embeddings"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {resolved_key}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        payload = post_json(
            self._url,
            body={"model": self._model, "input": texts},
            session=self._session,
        )
        data = payload.get("data")
        if not isinstance(data, list):
            raise RuntimeError(
                f"OpenAI-compatible response missing 'data' list: {payload}"
            )
        return EmbeddingResult(embeddings=[item["embedding"] for item in data])
