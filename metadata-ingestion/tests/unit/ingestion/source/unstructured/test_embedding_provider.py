"""Wire-format tests for the HTTP-based embedding providers.

Mocks the underlying ``requests.Session.post`` so we can verify each provider
sends the correct URL, headers, and body shape — which is the contract that
matters for parity with the Java GMS providers.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.unstructured.embedding_provider import (
    CohereEmbeddingProvider,
    OpenAIEmbeddingProvider,
)


def _ok_response(payload: dict) -> MagicMock:
    response = MagicMock()
    response.ok = True
    response.json.return_value = payload
    return response


def _err_response(status: int, text: str) -> MagicMock:
    response = MagicMock()
    response.ok = False
    response.status_code = status
    response.reason = "Bad Request"
    response.text = text
    return response


# ---------------------------------------------------------------------------
# Cohere
# ---------------------------------------------------------------------------


def test_cohere_provider_posts_v1_embed_with_bearer_auth():
    provider = CohereEmbeddingProvider(model="embed-english-v3.0", api_key="my-key")

    with patch.object(
        provider._session,
        "post",
        return_value=_ok_response({"embeddings": [[0.1, 0.2, 0.3]]}),
    ) as mock_post:
        result = provider.embed(["hello"])

    assert result.embeddings == [[0.1, 0.2, 0.3]]

    args, kwargs = mock_post.call_args
    assert args[0] == "https://api.cohere.com/v1/embed"
    assert kwargs["json"] == {
        "texts": ["hello"],
        "model": "embed-english-v3.0",
        "input_type": "search_document",
    }
    # Auth header is set on the session, not per-call.
    assert provider._session.headers["Authorization"] == "Bearer my-key"


def test_cohere_provider_uses_env_var_when_no_key():
    with patch.dict("os.environ", {"COHERE_API_KEY": "env-key"}, clear=True):
        provider = CohereEmbeddingProvider(model="embed-english-v3.0", api_key=None)
    assert provider._session.headers["Authorization"] == "Bearer env-key"


def test_cohere_provider_raises_without_key():
    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="Cohere API key is required"),
    ):
        CohereEmbeddingProvider(model="embed-english-v3.0", api_key=None)


def test_cohere_provider_surfaces_http_errors():
    provider = CohereEmbeddingProvider(model="embed-english-v3.0", api_key="k")
    with (
        patch.object(
            provider._session,
            "post",
            return_value=_err_response(401, '{"message":"invalid api token"}'),
        ),
        pytest.raises(RuntimeError, match="401"),
    ):
        provider.embed(["hello"])


# ---------------------------------------------------------------------------
# OpenAI / local
# ---------------------------------------------------------------------------


def test_openai_provider_posts_to_default_url():
    provider = OpenAIEmbeddingProvider(model="text-embedding-3-small", api_key="sk-x")

    with patch.object(
        provider._session,
        "post",
        return_value=_ok_response(
            {"data": [{"embedding": [0.1, 0.2]}, {"embedding": [0.3, 0.4]}]}
        ),
    ) as mock_post:
        result = provider.embed(["a", "b"])

    assert result.embeddings == [[0.1, 0.2], [0.3, 0.4]]

    args, kwargs = mock_post.call_args
    assert args[0] == "https://api.openai.com/v1/embeddings"
    assert kwargs["json"] == {"model": "text-embedding-3-small", "input": ["a", "b"]}
    assert provider._session.headers["Authorization"] == "Bearer sk-x"


def test_openai_provider_honours_custom_base_url_for_local_servers():
    provider = OpenAIEmbeddingProvider(
        model="nomic-embed-text",
        api_key="local",
        base_url="http://localhost:11434/v1",
        provider_label="openai",
    )
    assert provider._url == "http://localhost:11434/v1/embeddings"
    assert provider.model_id == "openai/nomic-embed-text"


def test_openai_provider_strips_trailing_slash_in_base_url():
    provider = OpenAIEmbeddingProvider(
        model="m",
        api_key="k",
        base_url="http://x.example/v1/",
    )
    assert provider._url == "http://x.example/v1/embeddings"


def test_openai_provider_surfaces_http_errors():
    provider = OpenAIEmbeddingProvider(model="m", api_key="k")
    with (
        patch.object(
            provider._session,
            "post",
            return_value=_err_response(429, "rate limit"),
        ),
        pytest.raises(RuntimeError, match="429"),
    ):
        provider.embed(["x"])
