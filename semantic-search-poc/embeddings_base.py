"""
Abstract base class for embeddings implementations used in this project.

Provides a nominal type that concrete embeddings classes must inherit from,
making implementation explicit and preventing accidental drift.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List


class BaseEmbeddings(ABC):
    """Embeddings interface implemented by concrete providers."""

    # Unified, readable model name for display/logging.
    # Concrete implementations MUST provide this property.
    @property
    @abstractmethod
    def model_name(self) -> str:  # pragma: no cover - abstract contract
        """Return a human-readable model name (e.g., "bedrock/cohere.embed-english-v3").

        Requirements
        - MUST be reliable, unique, and stable for the underlying embedding model
          configuration. This value is used to derive cache keys and for display,
          so collisions or instability will break caching semantics.
        - SHOULD include a provider prefix when applicable (e.g., "bedrock/<modelId>",
          "cohere/<model>") to avoid ambiguity across providers.
        - MUST reflect any relevant configuration that affects output vectors
          (e.g., dimension choices where applicable), ensuring distinct names map
          to distinct vector spaces.
        """
        raise NotImplementedError

    @abstractmethod
    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of texts as documents and return vectors for each."""
        raise NotImplementedError

    @abstractmethod
    def embed_query(self, text: str) -> List[float]:
        """Embed a single query string and return its vector."""
        raise NotImplementedError


