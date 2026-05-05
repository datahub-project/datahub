"""Embedding provider package.

Public exports:

* :class:`EmbeddingProvider` — provider interface
* :class:`EmbeddingResult` — provider-agnostic embedding response
* Concrete providers: ``BedrockEmbeddingProvider``, ``CohereEmbeddingProvider``,
  ``OpenAIEmbeddingProvider``, ``VertexAIEmbeddingProvider``
* :func:`create_embedding_provider` — factory from ``EmbeddingConfig``
* :func:`derive_model_id` — string id helper used in capability checks
"""

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
)
from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
    BedrockEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.cohere import (
    CohereEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.factory import (
    SUPPORTED_PROVIDERS,
    create_embedding_provider,
    derive_model_id,
    resolve_local_base_url,
)
from datahub.ingestion.source.unstructured.embedding_providers.openai import (
    OpenAIEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.vertex_ai import (
    VertexAIEmbeddingProvider,
)

__all__ = [
    "BedrockEmbeddingProvider",
    "CohereEmbeddingProvider",
    "EmbeddingProvider",
    "EmbeddingResult",
    "OpenAIEmbeddingProvider",
    "SUPPORTED_PROVIDERS",
    "VertexAIEmbeddingProvider",
    "create_embedding_provider",
    "derive_model_id",
    "resolve_local_base_url",
]
