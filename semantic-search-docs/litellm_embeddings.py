"""
LiteLLM-based Embeddings adapter.

Provides a minimal analogue of LangChain's `Embeddings` interface with
`embed_documents` and `embed_query` methods so it can be used with the
existing caching wrapper (`CachedEmbeddings`).

Notes
- Supports any LiteLLM embedding model string (e.g., "text-embedding-3-large",
  "bedrock/amazon.titan-embed-text-v2:0", "cohere/embed-english-v3.0",
  "bedrock/cohere.embed-english-v3").
- For Cohere v3 models, we pass `input_type` appropriately for document vs query
  to match LangChain semantics (search_document vs search_query).
- For Bedrock, you can pass `aws_region_name` to route requests to a region.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from litellm import embedding as litellm_embedding
from embeddings_base import BaseEmbeddings


class LiteLLMEmbeddings(BaseEmbeddings):
    """A minimal embeddings client using LiteLLM.

    Implements `embed_documents` and `embed_query` compatible with the
    LangChain `Embeddings` shape used elsewhere in this repo.

    Model identification
    - Input `model` accepts any LiteLLM embedding model string. Examples:
      - Direct Cohere: "cohere/embed-english-v3.0"
      - Bedrock Titan: "bedrock/amazon.titan-embed-text-v2:0"
      - Bedrock Cohere: "bedrock/cohere.embed-english-v3"
    - The instance sets a single, stable `model_name` used for display and cache keys.
      We currently set `model_name` to the exact input `model` string, so callers
      can pass either the provider-prefixed form (preferred) or the raw Bedrock
      model id (when appropriate) and see that reflected consistently.
    - Guidance:
      - Prefer provider-prefixed strings (e.g., "bedrock/<modelId>", "cohere/<model>")
        to avoid ambiguity and ensure uniqueness across providers.
    """

    def __init__(
        self,
        model: str,
        *,
        aws_region_name: Optional[str] = None,
        default_kwargs: Optional[Dict[str, Any]] = None,
    ):
        self.model: str = model
        # Kept for compatibility with caching helpers which try common attribute names
        self.model_id: str = model
        # Store a stable, human-readable name for display/logging and cache keys.
        # We currently mirror the input string; callers should pass provider-prefixed
        # identifiers (e.g., "bedrock/cohere.embed-english-v3") for uniqueness.
        self._model_name: str = model
        self.aws_region_name = aws_region_name
        self._default_kwargs: Dict[str, Any] = default_kwargs or {}

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of texts as documents.

        For Cohere v3 models, sets input_type="search_document".
        """
        if not texts:
            return []

        kwargs = self._build_call_kwargs(is_query=False)
        response = litellm_embedding(model=self.model, input=texts, **kwargs)
        # OpenAI-compatible response: response.data is a list, each with `.embedding`
        return [item["embedding"] if isinstance(item, dict) else item.embedding for item in response.data]

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text.

        For Cohere v3 models, sets input_type="search_query".
        """
        kwargs = self._build_call_kwargs(is_query=True)
        model_l = self.model.lower()
        # Cohere direct API expects a list input; wrap single text to avoid provider errors
        if model_l.startswith("cohere/") or model_l == "cohere":
            response = litellm_embedding(model=self.model, input=[text], **kwargs)
        else:
            response = litellm_embedding(model=self.model, input=text, **kwargs)
        # For single input, response.data has one item
        item = response.data[0]
        return item["embedding"] if isinstance(item, dict) else item.embedding

    def _build_call_kwargs(self, *, is_query: bool) -> Dict[str, Any]:
        """Construct provider-specific kwargs, merging defaults.

        - Adds Cohere `input_type` when applicable
        - Adds Bedrock `aws_region_name` when provided
        """
        kwargs: Dict[str, Any] = dict(self._default_kwargs)

        # Region routing for Bedrock
        if self.aws_region_name:
            kwargs.setdefault("aws_region_name", self.aws_region_name)

        # Cohere v3 input_type mapping (works for direct Cohere or Bedrock Cohere models)
        model_l = self.model.lower()
        if ("cohere" in model_l) and ("embed" in model_l):
            kwargs.setdefault("input_type", "search_query" if is_query else "search_document")

        return kwargs

    @property
    def model_name(self) -> str:
        """Stable, unique identifier for this embeddings model.

        How it's produced
        - It is derived directly from the constructor's `model` argument and
          stored unchanged. Examples:
          - "cohere/embed-english-v3.0" → model_name == "cohere/embed-english-v3.0"
          - "bedrock/amazon.titan-embed-text-v2:0" → model_name == "bedrock/amazon.titan-embed-text-v2:0"
          - "bedrock/cohere.embed-english-v3" → model_name == "bedrock/cohere.embed-english-v3"

        Guidance
        - Prefer passing provider-prefixed strings (e.g., "bedrock/<modelId>",
          "cohere/<model>") to ensure uniqueness across providers.
        - This value is used in cache keys and for display; it MUST be stable and
          uniquely represent the underlying vector space.
        """
        return self._model_name
