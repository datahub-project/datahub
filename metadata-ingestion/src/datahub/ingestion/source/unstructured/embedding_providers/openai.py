"""OpenAI (and OpenAI-compatible) embedding provider (raw HTTP)."""

from typing import Optional

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    EmbeddingProvider,
    EmbeddingResult,
    build_retrying_session,
    post_json,
)

OPENAI_API_BASE = "https://api.openai.com/v1"


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
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        # OpenAI proper requires a real key. OpenAI-compatible local servers
        # (Ollama, etc.) accept any token, so the factory passes a placeholder
        # when targeting a custom base_url. Raise here only when we'd be
        # talking to api.openai.com without a key — silent fallbacks would
        # surface as an opaque 401 on the first embed call.
        if not api_key and base_url is None:
            raise ValueError(
                "OpenAI API key is required. Set embedding.api_key or "
                "OPENAI_API_KEY, or configure provider='local' with an endpoint."
            )

        self._model = model
        self.model_id = f"{provider_label}/{model}"
        self._url = f"{(base_url or OPENAI_API_BASE).rstrip('/')}/embeddings"
        self._timeout = timeout
        self._session = build_retrying_session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {api_key or 'local'}",
                "Content-Type": "application/json",
            }
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        payload = post_json(
            self._url,
            body={"model": self._model, "input": texts},
            session=self._session,
            timeout=self._timeout,
        )
        data = payload.get("data")
        if not isinstance(data, list):
            raise RuntimeError(
                f"OpenAI-compatible response missing 'data' list: {payload}"
            )
        return EmbeddingResult(embeddings=[item["embedding"] for item in data])
