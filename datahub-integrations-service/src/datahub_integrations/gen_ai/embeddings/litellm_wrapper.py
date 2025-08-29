from __future__ import annotations

from typing import Any, Dict, List, Optional

from litellm import embedding as litellm_embedding


class LiteLLMEmbeddings:
    """A direct embeddings client using LiteLLM.

    Provides `embed_documents` and `embed_query` methods for generating embeddings.
    Handles model-specific differences like Cohere's input_type parameter and
    character limits transparently.

    Model identification:
    - Accepts LiteLLM Bedrock embedding model strings. Examples:
      - Bedrock Titan: "bedrock/amazon.titan-embed-text-v2:0"
      - Bedrock Cohere: "bedrock/cohere.embed-english-v3"
    """

    def __init__(
        self,
        model_id: str,
        *,
        aws_region_name: Optional[str] = None,
        default_kwargs: Optional[Dict[str, Any]] = None,
        max_character_length: Optional[int] = None,
    ):
        self._model_id: str = model_id
        self._aws_region_name = aws_region_name
        self._default_kwargs: Dict[str, Any] = default_kwargs or {}
        # Optional hard character cap applied before calling the provider.
        # Useful for Bedrock Cohere which enforces a 2048-character validation limit
        # separate from the model's token context window. See:
        # https://github.com/deepset-ai/haystack-core-integrations/issues/912
        self._max_character_length: Optional[int] = max_character_length

    def embed_documents(self, texts: List[str]) -> List[List[float]]:
        """Embed a list of texts as documents.

        For Cohere v3 models, sets input_type="search_document".
        """
        if not texts:
            return []

        # If configured, enforce a hard character limit prior to provider call
        if self._max_character_length is not None:
            cap = self._max_character_length
            texts = [t[:cap] if len(t) > cap else t for t in texts]

        kwargs = self._build_call_kwargs(is_query=False)
        response = litellm_embedding(model=self._model_id, input=texts, **kwargs)
        # OpenAI-compatible response: response.data is a list, each with `.embedding`
        return [
            item["embedding"] if isinstance(item, dict) else item.embedding
            for item in response.data
        ]

    def embed_query(self, text: str) -> List[float]:
        """Embed a single query text.

        For Cohere v3 models, sets input_type="search_query".
        """
        # If configured, enforce a hard character limit prior to provider call
        if (
            self._max_character_length is not None
            and len(text) > self._max_character_length
        ):
            text = text[: self._max_character_length]

        kwargs = self._build_call_kwargs(is_query=True)
        response = litellm_embedding(model=self._model_id, input=text, **kwargs)
        # For single input, response.data has one item
        item = response.data[0]
        return item["embedding"] if isinstance(item, dict) else item.embedding

    def _build_call_kwargs(self, *, is_query: bool) -> Dict[str, Any]:
        """Construct provider-specific kwargs, merging defaults.

        - Adds Cohere `input_type` and `truncate` when applicable
        - Adds Bedrock `aws_region_name` when provided
        """
        kwargs: Dict[str, Any] = dict(self._default_kwargs)

        # Region routing for Bedrock
        if self._aws_region_name:
            kwargs.setdefault("aws_region_name", self._aws_region_name)

        # Cohere v3 input_type and truncate mapping for Bedrock Cohere models
        model_l = self._model_id.lower()
        if ("cohere" in model_l) and ("embed" in model_l):
            kwargs.setdefault(
                "input_type", "search_query" if is_query else "search_document"
            )
            # Enable automatic truncation for Cohere models to handle long texts
            # "END" truncates from the end, keeping the beginning of the text
            kwargs.setdefault("truncate", "END")

        return kwargs

    @property
    def model_id(self) -> str:
        """Return the model identifier.

        Returns the exact model string passed to the constructor.
        Examples:
        - "bedrock/amazon.titan-embed-text-v2:0"
        - "bedrock/cohere.embed-english-v3"
        """
        return self._model_id
