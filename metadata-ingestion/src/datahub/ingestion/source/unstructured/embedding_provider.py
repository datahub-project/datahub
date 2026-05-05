"""Embedding provider wrappers.

Provides a small abstraction over provider-specific SDKs so the chunking
source can call a single ``embed(texts)`` method regardless of provider.

Each concrete provider is responsible for:
  * lazy-importing its SDK (so unrelated installs don't pull in everything)
  * normalising the response to a list[list[float]]
  * not leaking credentials into ``os.environ``
"""

from __future__ import annotations

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig

logger = logging.getLogger(__name__)


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


class BedrockEmbeddingProvider(EmbeddingProvider):
    """Embedding via AWS Bedrock Runtime (boto3)."""

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
    """Embedding via the Cohere REST SDK."""

    def __init__(self, model: str, api_key: Optional[str]):
        try:
            import cohere
        except ImportError as e:
            raise ImportError(
                "The 'cohere' package is required for the cohere embedding provider. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        resolved_key = api_key or os.environ.get("COHERE_API_KEY")
        if not resolved_key:
            raise ValueError(
                "Cohere API key is required. Set embedding.api_key or COHERE_API_KEY."
            )

        self._model = model
        self.model_id = f"cohere/{model}"
        # The Cohere SDK accepts an api_key argument directly so we never need
        # to write the secret into os.environ.
        self._client = cohere.Client(api_key=resolved_key)

    def embed(self, texts: list[str]) -> EmbeddingResult:
        response = self._client.embed(
            texts=texts,
            model=self._model,
            input_type="search_document",
        )
        # Modern SDK returns an object with .embeddings; older versions return
        # a dict-like. Handle both defensively.
        embeddings = getattr(response, "embeddings", None)
        if embeddings is None and isinstance(response, dict):
            embeddings = response["embeddings"]
        if embeddings is None:
            raise ValueError("Cohere SDK returned no embeddings")
        # Some SDK versions wrap embeddings in a typed object (.float_, .int8, ...).
        float_attr = getattr(embeddings, "float_", None)
        if float_attr is not None:
            embeddings = float_attr
        return EmbeddingResult(embeddings=list(embeddings))


class OpenAIEmbeddingProvider(EmbeddingProvider):
    """Embedding via the OpenAI Python SDK.

    Also used for OpenAI-compatible local servers (e.g. Ollama) by passing a
    custom ``base_url``.
    """

    def __init__(
        self,
        model: str,
        api_key: Optional[str],
        base_url: Optional[str] = None,
        provider_label: str = "openai",
    ):
        try:
            from openai import OpenAI
        except ImportError as e:
            raise ImportError(
                "The 'openai' package is required for the openai embedding provider. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        resolved_key = api_key or os.environ.get("OPENAI_API_KEY") or "local"
        self._model = model
        self.model_id = f"{provider_label}/{model}"
        self._client = OpenAI(api_key=resolved_key, base_url=base_url)

    def embed(self, texts: list[str]) -> EmbeddingResult:
        response = self._client.embeddings.create(model=self._model, input=texts)
        return EmbeddingResult(embeddings=[item.embedding for item in response.data])


class VertexAIEmbeddingProvider(EmbeddingProvider):
    """Embedding via Google Vertex AI."""

    def __init__(
        self,
        model: str,
        project_id: str,
        location: Optional[str],
    ):
        try:
            import vertexai
            from vertexai.language_models import TextEmbeddingModel
        except ImportError as e:
            raise ImportError(
                "google-cloud-aiplatform is required for the vertex_ai embedding provider. "
                "Install with: pip install 'acryl-datahub[vertexai]'"
            ) from e

        self._model_name = model
        self.model_id = f"vertex_ai/{model}"
        vertexai.init(project=project_id, location=location)
        self._model = TextEmbeddingModel.from_pretrained(model)

    def embed(self, texts: list[str]) -> EmbeddingResult:
        from vertexai.language_models import TextEmbeddingInput

        # Asymmetric Gemini embedding models require a task_type. Mirrors the
        # search-side RETRIEVAL_QUERY in VertexAiEmbeddingProvider.java.
        inputs = [
            TextEmbeddingInput(text=t, task_type="RETRIEVAL_DOCUMENT") for t in texts
        ]
        results = self._model.get_embeddings(inputs)
        return EmbeddingResult(embeddings=[r.values for r in results])


def _resolve_local_base_url(endpoint: Optional[str]) -> str:
    """Return the OpenAI-compatible base URL for the local provider.

    Strips a trailing ``/embeddings`` so the OpenAI SDK can append its own path.
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


# Helpers used by the chunking source to derive the model id without
# instantiating a provider (used by capability checks).
def derive_model_id(provider: Optional[str], model: Optional[str]) -> Optional[str]:
    """Return the human-readable ``provider/model`` id, or None if not configured."""
    if not provider or not model:
        return None
    if provider == "local":
        return f"openai/{model}"
    return f"{provider}/{model}"
