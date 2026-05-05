"""Wire-format tests for the HTTP-based embedding providers.

Mocks the underlying ``requests.Session.post`` so we can verify each provider
sends the correct URL, headers, and body shape — which is the contract that
matters for parity with the Java GMS providers.
"""

from unittest.mock import MagicMock, patch

import pytest

from datahub.ingestion.source.unstructured.embedding_providers.cohere import (
    CohereEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.openai import (
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


def test_openai_provider_raises_when_no_key_and_no_base_url():
    """Targeting api.openai.com without a key must fail loudly, not silently 401."""
    with pytest.raises(ValueError, match="OpenAI API key is required"):
        OpenAIEmbeddingProvider(model="m", api_key=None)


def test_openai_provider_allows_missing_key_with_custom_base_url():
    """OpenAI-compatible local servers (Ollama) accept any token."""
    provider = OpenAIEmbeddingProvider(
        model="m", api_key=None, base_url="http://localhost:11434/v1"
    )
    assert provider._session.headers["Authorization"] == "Bearer local"


def test_openai_provider_passes_timeout_to_post_json():
    provider = OpenAIEmbeddingProvider(model="m", api_key="k", timeout=12.5)
    with patch.object(
        provider._session,
        "post",
        return_value=_ok_response({"data": [{"embedding": [0.0]}]}),
    ) as mock_post:
        provider.embed(["x"])
    assert mock_post.call_args.kwargs["timeout"] == 12.5


# ---------------------------------------------------------------------------
# base.post_json — JSON / error handling
# ---------------------------------------------------------------------------


def test_post_json_raises_runtime_error_on_non_json_body():
    """200 OK with HTML/plain text (proxy error pages) must surface as RuntimeError."""
    from datahub.ingestion.source.unstructured.embedding_providers.base import (
        post_json,
    )

    response = MagicMock()
    response.ok = True
    response.json.side_effect = ValueError("Expecting value")
    response.text = "<html>proxy error</html>"

    session = MagicMock()
    session.post.return_value = response

    with pytest.raises(RuntimeError, match="non-JSON"):
        post_json("https://example.com", body={}, session=session)


def test_post_json_includes_status_in_error_message():
    from datahub.ingestion.source.unstructured.embedding_providers.base import (
        post_json,
    )

    session = MagicMock()
    session.post.return_value = _err_response(503, "service unavailable")

    with pytest.raises(RuntimeError, match="503.*service unavailable"):
        post_json("https://example.com", body={}, session=session)


def test_post_json_default_timeout_is_60_seconds():
    """Default matches DEFAULT_HTTP_TIMEOUT_SECONDS so callers don't need to set it."""
    from datahub.ingestion.source.unstructured.embedding_providers.base import (
        DEFAULT_HTTP_TIMEOUT_SECONDS,
        post_json,
    )

    session = MagicMock()
    session.post.return_value = _ok_response({})

    post_json("https://example.com", body={}, session=session)
    assert session.post.call_args.kwargs["timeout"] == DEFAULT_HTTP_TIMEOUT_SECONDS
    assert DEFAULT_HTTP_TIMEOUT_SECONDS == 60


# ---------------------------------------------------------------------------
# base — retry adapter
# ---------------------------------------------------------------------------


def test_build_retrying_session_mounts_retry_adapter_on_both_schemes():
    from requests.adapters import HTTPAdapter

    from datahub.ingestion.source.unstructured.embedding_providers.base import (
        build_retrying_session,
    )

    session = build_retrying_session()
    http_adapter = session.get_adapter("http://x.example")
    https_adapter = session.get_adapter("https://x.example")

    assert isinstance(http_adapter, HTTPAdapter)
    assert isinstance(https_adapter, HTTPAdapter)
    # Same Retry config drives both schemes.
    retry = http_adapter.max_retries
    assert retry.total == 3
    assert 429 in retry.status_forcelist
    assert 503 in retry.status_forcelist


# ---------------------------------------------------------------------------
# Bedrock — dispatch by model-id prefix
# ---------------------------------------------------------------------------


def test_bedrock_provider_uses_cohere_batch_shape():
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )

    with patch("boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        mock_client.invoke_model.return_value = {
            "body": MagicMock(read=lambda: b'{"embeddings":[[0.1,0.2]]}')
        }

        provider = BedrockEmbeddingProvider(
            model="cohere.embed-english-v3", aws_region="us-east-1"
        )
        result = provider.embed(["hello"])

    assert result.embeddings == [[0.1, 0.2]]
    args, kwargs = mock_client.invoke_model.call_args
    body = kwargs["body"]
    import json

    parsed = json.loads(body)
    assert parsed == {"texts": ["hello"], "input_type": "search_document"}


def test_bedrock_provider_uses_titan_single_input_shape():
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )

    with patch("boto3.client") as mock_boto:
        mock_client = MagicMock()
        mock_boto.return_value = mock_client
        # Titan returns one embedding per call.
        mock_client.invoke_model.side_effect = [
            {"body": MagicMock(read=lambda: b'{"embedding":[1.0]}')},
            {"body": MagicMock(read=lambda: b'{"embedding":[2.0]}')},
        ]

        provider = BedrockEmbeddingProvider(
            model="amazon.titan-embed-text-v2:0", aws_region="us-east-1"
        )
        result = provider.embed(["a", "b"])

    assert result.embeddings == [[1.0], [2.0]]
    assert mock_client.invoke_model.call_count == 2


def test_bedrock_provider_raises_on_unknown_model_family():
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )

    with patch("boto3.client"):
        provider = BedrockEmbeddingProvider(
            model="mistral.mistral-embed-v1", aws_region="us-east-1"
        )
        with pytest.raises(ValueError, match="Unsupported Bedrock embedding model"):
            provider.embed(["hello"])


def test_bedrock_provider_does_not_match_substring_only():
    """A model name that *contains* 'cohere' but doesn't start with 'cohere.' is rejected."""
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )

    with patch("boto3.client"):
        provider = BedrockEmbeddingProvider(
            model="custom.cohere-lookalike-v1", aws_region="us-east-1"
        )
        with pytest.raises(ValueError, match="Unsupported Bedrock embedding model"):
            provider.embed(["hello"])


def test_bedrock_provider_model_id_is_namespaced():
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )

    with patch("boto3.client"):
        provider = BedrockEmbeddingProvider(
            model="cohere.embed-english-v3", aws_region="us-east-1"
        )
    assert provider.model_id == "bedrock/cohere.embed-english-v3"


# ---------------------------------------------------------------------------
# Factory — provider construction and id derivation
# ---------------------------------------------------------------------------


def _make_config(**overrides):
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig

    base = {
        "provider": "openai",
        "model": "text-embedding-3-small",
        "api_key": "sk-x",
        "allow_local_embedding_config": True,
    }
    base.update(overrides)
    return EmbeddingConfig(**base)


def test_derive_model_id_maps_local_to_openai_label():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        derive_model_id,
    )

    assert derive_model_id("local", "nomic-embed-text") == "openai/nomic-embed-text"
    assert derive_model_id("bedrock", "cohere.embed-english-v3") == (
        "bedrock/cohere.embed-english-v3"
    )
    assert derive_model_id("vertex_ai", "gemini-embed-001") == (
        "vertex_ai/gemini-embed-001"
    )


def test_derive_model_id_returns_none_when_unconfigured():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        derive_model_id,
    )

    assert derive_model_id(None, "m") is None
    assert derive_model_id("openai", None) is None


def test_create_embedding_provider_openai():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    provider = create_embedding_provider(_make_config(provider="openai"))
    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider.model_id == "openai/text-embedding-3-small"


def test_create_embedding_provider_local_strips_embeddings_suffix():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    provider = create_embedding_provider(
        _make_config(
            provider="local",
            api_key=None,
            endpoint="http://localhost:11434/v1/embeddings",
        )
    )
    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider._url == "http://localhost:11434/v1/embeddings"
    # Local provider gets the canonical "openai/<model>" label.
    assert provider.model_id == "openai/text-embedding-3-small"


def test_create_embedding_provider_cohere():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    provider = create_embedding_provider(
        _make_config(provider="cohere", model="embed-english-v3.0")
    )
    assert isinstance(provider, CohereEmbeddingProvider)
    assert provider.model_id == "cohere/embed-english-v3.0"


def test_create_embedding_provider_vertex_ai_falls_back_to_env_var():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    cfg = _make_config(
        provider="vertex_ai",
        model="gemini-embed-001",
        vertex_project_id=None,
        api_key=None,
    )

    with (
        patch.dict("os.environ", {"VERTEX_AI_PROJECT_ID": "env-proj"}, clear=True),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai."
            "VertexAIEmbeddingProvider.__init__",
            return_value=None,
        ) as mock_init,
    ):
        create_embedding_provider(cfg)

    kwargs = mock_init.call_args.kwargs
    assert kwargs["project_id"] == "env-proj"


def test_create_embedding_provider_vertex_ai_raises_without_project():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    cfg = _make_config(
        provider="vertex_ai",
        model="gemini-embed-001",
        vertex_project_id=None,
        api_key=None,
    )

    with (
        patch.dict("os.environ", {}, clear=True),
        pytest.raises(ValueError, match="vertex_project_id is required"),
    ):
        create_embedding_provider(cfg)


def test_create_embedding_provider_requires_provider_and_model():
    from datahub.ingestion.source.unstructured.chunking_config import EmbeddingConfig
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    with pytest.raises(ValueError, match="provider and .model must be set"):
        create_embedding_provider(EmbeddingConfig())


def test_create_embedding_provider_plumbs_request_timeout():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    provider = create_embedding_provider(
        _make_config(provider="openai", request_timeout=7.5)
    )
    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider._timeout == 7.5
