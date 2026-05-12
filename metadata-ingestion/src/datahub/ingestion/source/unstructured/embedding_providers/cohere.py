"""Cohere embedding provider (raw HTTP)."""

import os
from typing import Optional

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    EmbeddingProvider,
    EmbeddingResult,
    build_retrying_session,
    post_json,
)

COHERE_API_URL = "https://api.cohere.com/v1/embed"


class CohereEmbeddingProvider(EmbeddingProvider):
    """Embedding via the Cohere v1 ``/embed`` HTTP endpoint."""

    def __init__(
        self,
        model: str,
        api_key: Optional[str],
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        resolved_key = api_key or os.environ.get("COHERE_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Cohere API key is required. Set embedding.api_key or COHERE_API_KEY."
            )

        self._model = model
        self.model_id = f"cohere/{model}"
        self._timeout = timeout
        self._session = build_retrying_session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {resolved_key}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        payload = post_json(
            COHERE_API_URL,
            body={
                "texts": texts,
                "model": self._model,
                "input_type": "search_document",
            },
            session=self._session,
            timeout=self._timeout,
        )
        embeddings = payload.get("embeddings")
        if embeddings is None:
            raise RuntimeError(f"Cohere response missing 'embeddings' field: {payload}")
        return EmbeddingResult(embeddings=embeddings)
