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

    # Status + reason only — body is logged at debug level so we don't
    # accidentally surface request input echoed back by upstream services
    # into pipeline reports.
    with pytest.raises(RuntimeError, match="503"):
        post_json("https://example.com", body={}, session=session)


def test_post_json_does_not_include_response_body_in_exception():
    """Defense-in-depth: response body must not appear in the user-visible message."""
    from datahub.ingestion.source.unstructured.embedding_providers.base import (
        post_json,
    )

    session = MagicMock()
    session.post.return_value = _err_response(401, "user-supplied secret echoed back")

    with pytest.raises(RuntimeError) as exc_info:
        post_json("https://example.com", body={}, session=session)
    assert "user-supplied secret" not in str(exc_info.value)


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


def test_create_embedding_provider_openai_falls_back_to_env_var():
    """Factory must resolve OPENAI_API_KEY itself; the provider constructor
    only sees the value the factory passes in, so reading the env var lazily
    inside __init__ would never run."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    cfg = _make_config(provider="openai", api_key=None)

    with patch.dict("os.environ", {"OPENAI_API_KEY": "sk-from-env"}, clear=True):
        provider = create_embedding_provider(cfg)

    assert isinstance(provider, OpenAIEmbeddingProvider)
    assert provider._session.headers["Authorization"] == "Bearer sk-from-env"


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


def test_create_embedding_provider_vertex_ai_falls_back_to_adc_project():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    cfg = _make_config(
        provider="vertex_ai",
        model="gemini-embed-001",
        vertex_project_id=None,
        api_key=None,
    )

    fake_creds = MagicMock()
    with (
        patch.dict("os.environ", {}, clear=True),
        patch(
            "google.auth.default",
            return_value=(fake_creds, "adc-proj"),
        ) as mock_default,
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.AuthorizedSession"
        ),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.GoogleAuthRequest"
        ),
    ):
        provider = create_embedding_provider(cfg)

    from datahub.ingestion.source.unstructured.embedding_providers.vertex_ai import (
        VertexAIEmbeddingProvider,
    )

    mock_default.assert_called_once()
    assert isinstance(provider, VertexAIEmbeddingProvider)
    assert provider._project_id == "adc-proj"


def test_vertex_ai_provider_raises_when_no_project_anywhere():
    from datahub.ingestion.source.unstructured.embedding_providers.vertex_ai import (
        VertexAIEmbeddingProvider,
    )

    fake_creds = MagicMock()
    with (
        patch("google.auth.default", return_value=(fake_creds, None)),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.AuthorizedSession"
        ),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.GoogleAuthRequest"
        ),
        pytest.raises(ValueError, match="Could not determine GCP project"),
    ):
        VertexAIEmbeddingProvider(
            model="gemini-embed-001", project_id=None, location=None
        )


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


def test_create_embedding_provider_bedrock_passes_region():
    from datahub.ingestion.source.unstructured.embedding_providers.bedrock import (
        BedrockEmbeddingProvider,
    )
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    cfg = _make_config(
        provider="bedrock",
        model="cohere.embed-english-v3",
        aws_region="us-west-2",
        api_key=None,
    )
    with patch("boto3.client") as mock_boto:
        provider = create_embedding_provider(cfg)

    assert isinstance(provider, BedrockEmbeddingProvider)
    assert mock_boto.call_args.kwargs["region_name"] == "us-west-2"
    assert provider.model_id == "bedrock/cohere.embed-english-v3"


def test_resolve_local_base_url_falls_back_to_env_var():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url,
    )

    with patch.dict(
        "os.environ",
        {"LOCAL_EMBEDDING_ENDPOINT": "http://other.example/v1/embeddings"},
        clear=True,
    ):
        assert (
            resolve_local_base_url(None) == "http://other.example/v1"
        )  # /embeddings stripped


def test_resolve_local_base_url_default_when_unset():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url,
    )

    with patch.dict("os.environ", {}, clear=True):
        assert resolve_local_base_url(None) == "http://localhost:11434/v1"


def test_resolve_local_base_url_passes_through_when_no_embeddings_suffix():
    """A bare base URL (without /embeddings) is returned as-is — OpenAI provider appends it."""
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        resolve_local_base_url,
    )

    assert resolve_local_base_url("http://x.example/v1") == "http://x.example/v1"


def test_create_embedding_provider_unsupported_provider_raises():
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    # Bypass EmbeddingConfig's _normalize_provider validation by mocking a config
    # that reports an unsupported provider name.
    cfg = MagicMock()
    cfg.provider = "huggingface"
    cfg.model = "bge-large"
    cfg.api_key = None
    cfg.request_timeout = 60

    with pytest.raises(ValueError, match="Unsupported embedding provider"):
        create_embedding_provider(cfg)


# ---------------------------------------------------------------------------
# Vertex AI provider — wire format and URL construction
# ---------------------------------------------------------------------------


def _make_vertex_provider(project_id="proj", location=None):
    from datahub.ingestion.source.unstructured.embedding_providers.vertex_ai import (
        VertexAIEmbeddingProvider,
    )

    fake_creds = MagicMock()
    with (
        patch("google.auth.default", return_value=(fake_creds, None)),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.AuthorizedSession"
        ),
        patch(
            "datahub.ingestion.source.unstructured.embedding_providers.vertex_ai.GoogleAuthRequest"
        ),
    ):
        return VertexAIEmbeddingProvider(
            model="gemini-embedding-001", project_id=project_id, location=location
        )


def test_vertex_ai_provider_builds_predict_url_with_default_location():
    provider = _make_vertex_provider(project_id="my-proj", location=None)
    assert provider._location == "us-central1"
    assert provider._url == (
        "https://us-central1-aiplatform.googleapis.com/v1/"
        "projects/my-proj/locations/us-central1/"
        "publishers/google/models/gemini-embedding-001:predict"
    )


def test_vertex_ai_provider_honours_custom_location():
    provider = _make_vertex_provider(project_id="my-proj", location="europe-west4")
    assert provider._location == "europe-west4"
    assert "europe-west4-aiplatform.googleapis.com" in provider._url
    assert "/locations/europe-west4/" in provider._url


def test_vertex_ai_provider_embed_sends_retrieval_document_task_type():
    provider = _make_vertex_provider(project_id="my-proj")
    payload = {
        "predictions": [
            {"embeddings": {"values": [0.1, 0.2]}},
            {"embeddings": {"values": [0.3, 0.4]}},
        ]
    }
    with patch.object(
        provider._session, "post", return_value=_ok_response(payload)
    ) as mock_post:
        result = provider.embed(["a", "b"])

    assert result.embeddings == [[0.1, 0.2], [0.3, 0.4]]
    body = mock_post.call_args.kwargs["json"]
    assert body == {
        "instances": [
            {"task_type": "RETRIEVAL_DOCUMENT", "content": "a"},
            {"task_type": "RETRIEVAL_DOCUMENT", "content": "b"},
        ]
    }


def test_vertex_ai_provider_raises_when_predictions_missing():
    provider = _make_vertex_provider(project_id="my-proj")
    with (
        patch.object(
            provider._session,
            "post",
            return_value=_ok_response({"unexpected": "shape"}),
        ),
        pytest.raises(RuntimeError, match="missing 'predictions' list"),
    ):
        provider.embed(["x"])


def test_vertex_ai_provider_model_id_is_namespaced():
    provider = _make_vertex_provider(project_id="my-proj")
    assert provider.model_id == "vertex_ai/gemini-embedding-001"


# ---------------------------------------------------------------------------
# Cohere / OpenAI — malformed payloads
# ---------------------------------------------------------------------------


def test_cohere_provider_raises_when_embeddings_field_missing():
    provider = CohereEmbeddingProvider(model="embed-english-v3.0", api_key="k")
    with (
        patch.object(
            provider._session,
            "post",
            return_value=_ok_response({"id": "x"}),
        ),
        pytest.raises(RuntimeError, match="missing 'embeddings' field"),
    ):
        provider.embed(["x"])


def test_openai_provider_raises_when_data_field_missing_or_wrong_type():
    provider = OpenAIEmbeddingProvider(model="m", api_key="k")
    with (
        patch.object(
            provider._session,
            "post",
            return_value=_ok_response({"object": "list"}),
        ),
        pytest.raises(RuntimeError, match="missing 'data' list"),
    ):
        provider.embed(["x"])
