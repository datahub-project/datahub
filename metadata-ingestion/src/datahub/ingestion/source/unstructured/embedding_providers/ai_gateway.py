"""OAuth2-authenticated AI Gateway embedding provider.

Calls the platform-agnostic AI Gateway embedding endpoint
(``POST /platform/{platform}/model/{model}/embedding``). The AI Gateway fronts
multiple upstream LLM platforms (Google Vertex AI, Azure OpenAI, Amazon
Bedrock) behind one HTTP contract, so a single client works for all of them —
``platform``/``model`` are just routing parameters, not separate implementations.

Authentication is JWT via AWS Cognito's ``client_credentials`` grant. Mirrors
the token-caching + auth-retry behaviour of the Java ``AiGatewayEmbeddingProvider``
in metadata-io so both query-time (GMS) and document-time (this module) speak
the same protocol and end up in the same vector space.
"""

import base64
import logging
import os
import time

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    EmbeddingProvider,
    EmbeddingResult,
    build_retrying_session,
)

logger = logging.getLogger(__name__)

_EMBEDDING_CONTENT_TYPE = "application/vnd.ai.gateway.text.embedding.v1+json"
# Refresh ahead of actual expiry so an in-flight request never races a token that
# dies mid-call. Mirrors AiGatewayEmbeddingProvider.java's TOKEN_EXPIRY_BUFFER.
_TOKEN_EXPIRY_BUFFER_SECONDS = 30
_DEFAULT_TOKEN_EXPIRES_IN_SECONDS = 3600


class AiGatewayEmbeddingProvider(EmbeddingProvider):
    """Embedding via an OAuth2-authenticated AI Gateway, authenticated with AWS Cognito JWT."""

    def __init__(
        self,
        model: str,
        platform: str | None,
        base_url: str | None,
        token_url: str | None,
        client_id: str | None,
        client_secret: str | None,
        dimensions: int | None = None,
        timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
    ):
        resolved_platform = platform or os.environ.get("AI_GATEWAY_PLATFORM")
        resolved_base_url = base_url or os.environ.get("AI_GATEWAY_BASE_URL")
        resolved_token_url = token_url or os.environ.get("AI_GATEWAY_TOKEN_URL")
        resolved_client_id = client_id or os.environ.get("AI_GATEWAY_CLIENT_ID")
        resolved_client_secret = client_secret or os.environ.get(
            "AI_GATEWAY_CLIENT_SECRET"
        )

        missing = [
            name
            for name, value in (
                ("platform", resolved_platform),
                ("base_url", resolved_base_url),
                ("token_url", resolved_token_url),
                ("client_id", resolved_client_id),
                ("client_secret", resolved_client_secret),
            )
            if not value
        ]
        if missing:
            raise ValueError(
                f"AI Gateway embedding provider is missing required config: {', '.join(missing)}. "
                "Set embedding.ai_gateway_* fields or the AI_GATEWAY_PLATFORM / AI_GATEWAY_BASE_URL / "
                "AI_GATEWAY_TOKEN_URL / AI_GATEWAY_CLIENT_ID / AI_GATEWAY_CLIENT_SECRET env vars."
            )

        # Narrowed to `str` for mypy: the missing-value check above already guarantees
        # none of these are None/empty by this point.
        assert resolved_platform and resolved_base_url and resolved_token_url
        assert resolved_client_id and resolved_client_secret

        self._model = model
        self.model_id = f"ai-gateway/{resolved_platform}/{model}"
        self._platform = resolved_platform
        self._base_url = resolved_base_url.rstrip("/")
        self._token_url = resolved_token_url
        self._client_id = resolved_client_id
        self._client_secret = resolved_client_secret
        self._dimensions = dimensions
        self._timeout = timeout
        self._session = build_retrying_session()
        self._cached_token: str | None = None
        self._token_expires_at: float = 0.0

    def embed(self, texts: list[str]) -> EmbeddingResult:
        return EmbeddingResult(embeddings=[self._embed_one(text) for text in texts])

    def _embed_one(self, text: str) -> list[float]:
        body: dict = {"data": text}
        if self._dimensions:
            body["options"] = {"dimensions": self._dimensions}

        url = (
            f"{self._base_url}/platform/{self._platform}/model/{self._model}/embedding"
        )
        response = self._session.post(
            url,
            json=body,
            timeout=self._timeout,
            headers={
                "Content-Type": _EMBEDDING_CONTENT_TYPE,
                "Authorization": f"Bearer {self._get_access_token()}",
            },
        )

        if response.status_code in (401, 403):
            # Cached token may have been revoked server-side before its advertised
            # expiry; drop it and retry once with a freshly fetched token.
            self._cached_token = None
            response = self._session.post(
                url,
                json=body,
                timeout=self._timeout,
                headers={
                    "Content-Type": _EMBEDDING_CONTENT_TYPE,
                    "Authorization": f"Bearer {self._get_access_token()}",
                },
            )

        if not response.ok:
            logger.debug(
                "AI Gateway embedding error body (truncated): %s", response.text[:500]
            )
            raise RuntimeError(
                f"AI Gateway embedding call failed: {response.status_code} {response.reason}"
            )

        payload = response.json()
        embeddings = payload.get("embeddings")
        if not isinstance(embeddings, list) or not embeddings:
            raise RuntimeError(
                f"Invalid response from AI Gateway: missing or empty 'embeddings' for model {self._model}"
            )

        # AI Gateway may return a batch of embeddings (nested array) even for a single input
        if len(embeddings) > 0 and isinstance(embeddings[0], list):
            return embeddings[0]

        return embeddings

    def _get_access_token(self) -> str:
        if self._cached_token and time.monotonic() < self._token_expires_at:
            return self._cached_token

        basic_auth = base64.b64encode(
            f"{self._client_id}:{self._client_secret}".encode()
        ).decode()
        response = self._session.post(
            self._token_url,
            data={"grant_type": "client_credentials"},
            timeout=self._timeout,
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Authorization": f"Basic {basic_auth}",
            },
        )
        if not response.ok:
            raise RuntimeError(
                f"Cognito token request failed: {response.status_code} {response.reason}"
            )

        payload = response.json()
        access_token = payload.get("access_token")
        if not access_token:
            raise RuntimeError(
                "Cognito token response did not contain an 'access_token' field"
            )
        expires_in = payload.get("expires_in", _DEFAULT_TOKEN_EXPIRES_IN_SECONDS)

        self._cached_token = access_token
        self._token_expires_at = (
            time.monotonic() + expires_in - _TOKEN_EXPIRY_BUFFER_SECONDS
        )
        return access_token
