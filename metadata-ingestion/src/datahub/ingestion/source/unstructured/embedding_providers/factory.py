"""Factory and helpers for building :class:`EmbeddingProvider` instances."""

import os
from typing import TYPE_CHECKING, Optional

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
    BedrockEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.cohere import (
    CohereEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.openai import (
    OpenAIEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.vertex_ai import (
    VertexAIEmbeddingProvider,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig


SUPPORTED_PROVIDERS: tuple[str, ...] = (
    "bedrock",
    "cohere",
    "openai",
    "local",
    "vertex_ai",
)


def resolve_local_base_url(endpoint: Optional[str]) -> str:
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


def derive_model_id(provider: Optional[str], model: Optional[str]) -> Optional[str]:
    """Return the human-readable ``provider/model`` id, or None if not configured.

    The ``local`` provider serves OpenAI-compatible models, so we tag it
    ``openai/<model>`` for parity with the canonical id used by
    :class:`OpenAIEmbeddingProvider`.
    """
    if not provider or not model:
        return None
    if provider == "local":
        return f"openai/{model}"
    return f"{provider}/{model}"


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
    timeout = config.request_timeout

    if provider is None or model is None:
        raise ValueError("EmbeddingConfig.provider and .model must be set")

    if provider == "bedrock":
        return BedrockEmbeddingProvider(model=model, aws_region=config.aws_region)

    if provider == "cohere":
        return CohereEmbeddingProvider(model=model, api_key=api_key, timeout=timeout)

    if provider == "openai":
        # Mirror the Vertex/Cohere pattern: resolve env var here so the constructor's
        # "no key" guard sees the same value the recipe-level validator did.
        return OpenAIEmbeddingProvider(
            model=model,
            api_key=api_key or os.environ.get("OPENAI_API_KEY"),
            timeout=timeout,
        )

    if provider == "local":
        # Local OpenAI-compatible servers (Ollama, etc.) accept any token; the
        # placeholder key is plumbed through so the constructor's "no key"
        # guard only fires when targeting api.openai.com.
        return OpenAIEmbeddingProvider(
            model=model,
            api_key="local",
            base_url=resolve_local_base_url(config.endpoint),
            timeout=timeout,
        )

    if provider == "vertex_ai":
        # If neither config nor env var is set, the provider falls back to the
        # project on Application Default Credentials.
        project_id = config.vertex_project_id or os.environ.get("VERTEX_AI_PROJECT_ID")
        return VertexAIEmbeddingProvider(
            model=model,
            project_id=project_id,
            location=config.vertex_location,
            timeout=timeout,
        )

    raise ValueError(f"Unsupported embedding provider: {provider}")
