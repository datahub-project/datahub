"""Embedding provider wrappers.

Provides a small abstraction over provider-specific transports so the chunking
source can call a single ``embed(texts)`` method regardless of provider.

Most providers use raw HTTP via ``requests`` to keep the dependency footprint
small and to mirror the wire-level contract used by the Java GMS embedding
providers. Bedrock is the exception: it uses ``boto3`` because SigV4 signing
plus the AWS credential chain (IRSA, AssumeRole, IMDS) are not worth
reimplementing.
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Optional

import requests

if TYPE_CHECKING:
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig

logger = logging.getLogger(__name__)

# Per-call timeout for HTTP-based providers. Embedding requests are typically
# quick; a long timeout exists only so we don't hang forever on a stuck server.
_HTTP_TIMEOUT_SECONDS = 60

_COHERE_API_URL = "https://api.cohere.com/v1/embed"
_OPENAI_API_BASE = "https://api.openai.com/v1"


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


def _post_json(
    url: str,
    *,
    headers: dict[str, str],
    body: dict[str, Any],
    session: Optional[requests.Session] = None,
) -> dict[str, Any]:
    """POST a JSON body and return the parsed JSON response.

    Raises an informative ``RuntimeError`` if the server returns a non-2xx
    status, including the response body so callers (and the
    ``_get_embedding_error_mitigation`` helper) can pattern-match on it.
    """
    sender = session or requests
    response = sender.post(
        url, headers=headers, json=body, timeout=_HTTP_TIMEOUT_SECONDS
    )
    if not response.ok:
        raise RuntimeError(
            f"Embedding API call failed: {response.status_code} {response.reason}: "
            f"{response.text[:500]}"
        )
    return response.json()


class BedrockEmbeddingProvider(EmbeddingProvider):
    """Embedding via AWS Bedrock Runtime (boto3).

    Uses boto3 because SigV4 + the AWS credential chain (IRSA, AssumeRole,
    IMDSv2) is the part where rolling our own would actually hurt.
    """

    def __init__(self, model: str, aws_region: Optional[str]):
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for the bedrock embedding provider. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        self._model = model
        self.model_id = f"bedrock/{model}"
        self._client = boto3.client(
            "bedrock-runtime",
            region_name=aws_region,
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        # Different Bedrock-hosted models have different request/response shapes.
        # We support the two families we ship defaults for: Cohere and Amazon Titan.
        model_lower = self._model.lower()

        if "cohere" in model_lower:
            body = {"texts": texts, "input_type": "search_document"}
            response = self._client.invoke_model(
                modelId=self._model,
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json",
            )
            payload = json.loads(response["body"].read())
            return EmbeddingResult(embeddings=payload["embeddings"])

        # Titan (and any other single-input-per-call models): one request per text.
        embeddings: list[list[float]] = []
        for text in texts:
            response = self._client.invoke_model(
                modelId=self._model,
                body=json.dumps({"inputText": text}),
                accept="application/json",
                contentType="application/json",
            )
            payload = json.loads(response["body"].read())
            embeddings.append(payload["embedding"])
        return EmbeddingResult(embeddings=embeddings)


class CohereEmbeddingProvider(EmbeddingProvider):
    """Embedding via the Cohere v1 ``/embed`` HTTP endpoint."""

    def __init__(self, model: str, api_key: Optional[str]):
        resolved_key = api_key or os.environ.get("COHERE_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Cohere API key is required. Set embedding.api_key or COHERE_API_KEY."
            )

        self._model = model
        self.model_id = f"cohere/{model}"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {resolved_key}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        payload = _post_json(
            _COHERE_API_URL,
            headers={},
            body={
                "texts": texts,
                "model": self._model,
                "input_type": "search_document",
            },
            session=self._session,
        )
        embeddings = payload.get("embeddings")
        if embeddings is None:
            raise RuntimeError(f"Cohere response missing 'embeddings' field: {payload}")
        return EmbeddingResult(embeddings=embeddings)


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
        self._url = f"{(base_url or _OPENAI_API_BASE).rstrip('/')}/embeddings"
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {resolved_key}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        payload = _post_json(
            self._url,
            headers={},
            body={"model": self._model, "input": texts},
            session=self._session,
        )
        data = payload.get("data")
        if not isinstance(data, list):
            raise RuntimeError(
                f"OpenAI-compatible response missing 'data' list: {payload}"
            )
        return EmbeddingResult(embeddings=[item["embedding"] for item in data])


class VertexAIEmbeddingProvider(EmbeddingProvider):
    """Embedding via the Vertex AI ``:predict`` REST endpoint.

    Authenticates with Application Default Credentials via ``google-auth``
    (handles GCE/GKE workload identity, ADC files, service-account
    impersonation, OAuth refresh), but does the call itself over plain HTTP.
    Mirrors the Java ``VertexAiEmbeddingProvider`` wire format.
    """

    _DEFAULT_LOCATION = "us-central1"

    def __init__(
        self,
        model: str,
        project_id: str,
        location: Optional[str],
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

        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        # Refresh once eagerly so that misconfigured ADC fails fast at provider
        # construction rather than on the first embed call.
        credentials.refresh(GoogleAuthRequest())
        self._session = AuthorizedSession(credentials)
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
        payload = _post_json(
            self._url,
            headers={"Content-Type": "application/json"},
            body=body,
            session=self._session,
        )
        predictions = payload.get("predictions")
        if not isinstance(predictions, list):
            raise RuntimeError(
                f"Vertex AI response missing 'predictions' list: {payload}"
            )
        return EmbeddingResult(
            embeddings=[p["embeddings"]["values"] for p in predictions]
        )


def _resolve_local_base_url(endpoint: Optional[str]) -> str:
    """Return the OpenAI-compatible base URL for the local provider.

    Strips a trailing ``/embeddings`` so the OpenAI provider can append its own
    path.
    """
    raw = (
        endpoint
        or os.environ.get("LOCAL_EMBEDDING_ENDPOINT")
        or "http://localhost:11434/v1/embeddings"
    )
    if raw.endswith("/embeddings"):
        return raw[: -len("/embeddings")]
    return raw


def create_embedding_provider(config: "EmbeddingConfig") -> EmbeddingProvider:
    """Build a provider instance from an :class:`EmbeddingConfig`.

    Validation of presence of provider/model/required fields is the caller's
    responsibility — this function assumes a fully resolved config.
    """
    provider = config.provider
    model = config.model
    api_key: Optional[str] = (
        config.api_key.get_secret_value() if config.api_key else None
    )

    if provider is None or model is None:
        raise ValueError("EmbeddingConfig.provider and .model must be set")

    if provider == "bedrock":
        return BedrockEmbeddingProvider(model=model, aws_region=config.aws_region)

    if provider == "cohere":
        return CohereEmbeddingProvider(model=model, api_key=api_key)

    if provider == "openai":
        return OpenAIEmbeddingProvider(model=model, api_key=api_key)

    if provider == "local":
        base_url = _resolve_local_base_url(config.endpoint)
        return OpenAIEmbeddingProvider(
            model=model,
            api_key="local",
            base_url=base_url,
            provider_label="openai",
        )

    if provider == "vertex_ai":
        project_id = config.vertex_project_id or os.environ.get("VERTEX_AI_PROJECT_ID")
        if not project_id:
            raise ValueError(
                "vertex_project_id is required for the vertex_ai provider "
                "(or set VERTEX_AI_PROJECT_ID env var)."
            )
        return VertexAIEmbeddingProvider(
            model=model,
            project_id=project_id,
            location=config.vertex_location,
        )

    raise ValueError(f"Unsupported embedding provider: {provider}")


# Allow tests / callers to introspect supported provider names.
SUPPORTED_PROVIDERS: tuple[str, ...] = (
    "bedrock",
    "cohere",
    "openai",
    "local",
    "vertex_ai",
)


def derive_model_id(provider: Optional[str], model: Optional[str]) -> Optional[str]:
    """Return the human-readable ``provider/model`` id, or None if not configured."""
    if not provider or not model:
        return None
    if provider == "local":
        return f"openai/{model}"
    return f"{provider}/{model}"
