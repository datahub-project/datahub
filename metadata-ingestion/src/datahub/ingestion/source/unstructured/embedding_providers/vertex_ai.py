"""Vertex AI embedding provider.

Authenticates with Application Default Credentials via ``google-auth``
(handles GCE/GKE workload identity, ADC files, service-account
impersonation, OAuth refresh), but does the call itself over plain HTTP.
Mirrors the Java ``VertexAiEmbeddingProvider`` wire format.
"""

from typing import Optional

import google.auth
from google.auth.transport.requests import (
    AuthorizedSession,
    Request as GoogleAuthRequest,
)

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
        project_id: Optional[str],
        location: Optional[str],
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        self._model = model
        self.model_id = f"vertex_ai/{model}"
        self._location = location or self._DEFAULT_LOCATION
        self._timeout = timeout

        credentials, adc_project = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        self._project_id = project_id or adc_project
        if not self._project_id:
            raise ValueError(
                "Could not determine GCP project for vertex_ai embedding provider. "
                "Set vertex_project_id, the VERTEX_AI_PROJECT_ID env var, or "
                "configure a project on your Application Default Credentials."
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
