"""Vertex AI embedding provider.

Authenticates with Application Default Credentials via ``google-auth``
(handles GCE/GKE workload identity, ADC files, service-account
impersonation, OAuth refresh), but does the call itself over plain HTTP.
Mirrors the Java ``VertexAiEmbeddingProvider`` wire format.
"""

from __future__ import annotations

from typing import Optional

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    EmbeddingProvider,
    EmbeddingResult,
    attach_retries,
    post_json,
)


class VertexAIEmbeddingProvider(EmbeddingProvider):
    """Embedding via the Vertex AI ``:predict`` REST endpoint."""

    _DEFAULT_LOCATION = "us-central1"

    def __init__(
        self,
        model: str,
        project_id: str,
        location: Optional[str],
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        try:
            import google.auth
            from google.auth.transport.requests import (
                AuthorizedSession,
                Request as GoogleAuthRequest,
            )
        except ImportError as e:
            raise ImportError(
                "google-auth is required for the vertex_ai embedding provider. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        self._model = model
        self.model_id = f"vertex_ai/{model}"
        self._location = location or self._DEFAULT_LOCATION
        self._project_id = project_id
        self._timeout = timeout

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # Eager refresh so misconfigured ADC fails fast at provider construction
        # (which is itself lazy — only happens on first embed call) rather than
        # mid-pipeline on the first batch.
        credentials.refresh(GoogleAuthRequest())
        self._session = attach_retries(AuthorizedSession(credentials))
        self._url = (
            f"https://{self._location}-aiplatform.googleapis.com/v1/"
            f"projects/{self._project_id}/locations/{self._location}/"
            f"publishers/google/models/{self._model}:predict"
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        # Asymmetric Gemini embedding models require a task_type. Mirrors the
        # search-side RETRIEVAL_QUERY in VertexAiEmbeddingProvider.java.
        body = {
            "instances": [
                {"task_type": "RETRIEVAL_DOCUMENT", "content": text} for text in texts
            ]
        }
        payload = post_json(
            self._url, body=body, session=self._session, timeout=self._timeout
        )
        predictions = payload.get("predictions")
        if not isinstance(predictions, list):
            raise RuntimeError(
                f"Vertex AI response missing 'predictions' list: {payload}"
            )
        return EmbeddingResult(
            embeddings=[p["embeddings"]["values"] for p in predictions]
        )
