import json
from io import BytesIO
from typing import Any, Dict, Optional
from unittest.mock import Mock, patch

import pytest
import requests
from urllib3.response import HTTPResponse

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
    DataHubDocumentsSource,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.embedding_providers.base import EmbeddingTask
from datahub.ingestion.source.unstructured.embedding_providers.datahub_server import (
    ServerEmbeddingModelMismatchError,
    ServerEmbeddingsUnavailableError,
    server_embedding_provider,
)
from datahub.ingestion.source.unstructured.embedding_providers.factory import (
    create_embedding_provider,
)
from datahub.metadata.schema_classes import SemanticContentClass
from datahub.utilities.server_config_util import RestServiceConfig, ServiceFeature

PATH = "/custom/embeddings"
SERVER = "http://example.invalid"


@pytest.fixture
def graph():
    graph = Mock(spec=DataHubGraph)
    graph.config = Mock(server=SERVER)
    graph.session = requests.Session()
    graph.session.trust_env = False
    graph.server_config = RestServiceConfig(
        raw_config={"embeddingService": {"enabled": True, "path": PATH}}
    )
    graph.execute_graphql.return_value = {
        "appConfig": {
            "semanticSearchConfig": {
                "enabled": True,
                "enabledEntities": ["document"],
                "embeddingConfig": {
                    "provider": "openai",
                    "modelId": "index-model",
                    "modelEmbeddingKey": "index_model",
                },
            }
        }
    }
    yield graph
    graph.session.close()


def response(
    payload: Dict[str, Any], status: int = 200, headers: Optional[Dict[str, str]] = None
) -> requests.Response:
    raw = requests.Response()
    raw.status_code = status
    raw.headers.update(headers or {})
    raw._content = json.dumps(payload).encode()
    return raw


def embedding_response(model="index-model"):
    return {
        "data": [
            {"index": 1, "embedding": [3.0, 4.0]},
            {"index": 0, "embedding": [1.0, 2.0]},
        ],
        "model": model,
        "x_datahub": {},
    }


@pytest.mark.parametrize("task", list(EmbeddingTask))
def test_request_shape_ordering_model_and_authenticated_session(graph, task):
    provider = server_embedding_provider(graph, timeout=12)
    with patch.object(
        graph.session, "post", return_value=response(embedding_response())
    ) as post:
        result = provider.embed(["first", "second"], task=task)
    post.assert_called_once_with(
        SERVER + PATH,
        json={
            "input": ["first", "second"],
            "x_datahub": {"task_type": task.value, "normalize": False},
        },
        timeout=12,
        allow_redirects=False,
    )
    assert result.embeddings == [[1.0, 2.0], [3.0, 4.0]]
    assert provider.model_id == "index-model"


@pytest.mark.parametrize(
    "raw_config",
    [
        {},
        {"embeddingService": {"enabled": False, "path": PATH}},
        {"embeddingService": {"enabled": True}},
        {"embeddingService": {"enabled": True, "path": "//example.invalid/embeddings"}},
    ],
)
def test_missing_capability_fails_before_embedding(graph, raw_config):
    graph.server_config = RestServiceConfig(raw_config=raw_config)
    assert not graph.server_config.supports_feature(ServiceFeature.EMBEDDING_SERVICE)
    with pytest.raises(ServerEmbeddingsUnavailableError, match="own credentials"):
        server_embedding_provider(graph)
    graph.execute_graphql.assert_not_called()
    ctx = PipelineContext(run_id="test", pipeline_name="test", graph=graph)
    with pytest.raises(ServerEmbeddingsUnavailableError, match="embeddingService"):
        DataHubDocumentsSource.create(
            {
                "embedding": {"provider": "datahub"},
                "stateful_ingestion": {"enabled": False},
            },
            ctx,
        )
    with patch(
        "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph",
        return_value=graph,
    ):
        report = DataHubDocumentsSource.test_connection(
            {"embedding": {"provider": "datahub"}}
        )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is not None
    assert "own credentials" in report.basic_connectivity.failure_reason


def test_discovery_is_advertised_not_version_inferred(graph):
    assert graph.server_config.supports_feature(ServiceFeature.EMBEDDING_SERVICE)
    assert graph.server_config.embedding_service_path == PATH


@pytest.mark.parametrize(
    "status, payload, headers, message",
    [
        (
            404,
            {"error": {"type": "not_enabled", "message": "private text"}},
            {},
            "does not offer",
        ),
        (403, {"error": "private text"}, {}, "Ask DataHub"),
        (429, {"error": "private text"}, {"x-should-retry": "false"}, "credits"),
        (500, {"error": "private text"}, {}, "HTTP 500"),
    ],
)
def test_actionable_errors_do_not_echo_input(graph, status, payload, headers, message):
    provider = server_embedding_provider(graph)
    with (
        patch.object(
            graph.session, "post", return_value=response(payload, status, headers)
        ) as post,
        pytest.raises((ValueError, RuntimeError), match=message) as error,
    ):
        provider.embed(["private text"], task=EmbeddingTask.DOCUMENT)
    assert "private text" not in str(error.value)
    assert post.call_count == 1


@pytest.mark.parametrize(
    "retry_header, expected_attempts", [("false", 1), ("true", 4), (None, 4)]
)
def test_http_adapter_never_retries_exhausted_credits(
    graph, retry_header, expected_attempts
):
    provider = server_embedding_provider(graph)
    headers = {"x-should-retry": retry_header} if retry_header is not None else {}

    def rate_limit(*args, **kwargs):
        return HTTPResponse(
            body=BytesIO(b"{}"), status=429, headers=headers, preload_content=False
        )

    with (
        patch(
            "urllib3.connectionpool.HTTPConnectionPool._make_request",
            side_effect=rate_limit,
        ) as http,
        patch("urllib3.util.retry.Retry.sleep"),
        pytest.raises(RuntimeError),
    ):
        provider.embed(["text"], task=EmbeddingTask.DOCUMENT)
    assert http.call_count == expected_attempts


def test_actual_session_preserves_per_request_authentication(graph):
    def authenticate(request):
        request.headers["Authorization"] = "Bearer refreshed-token"
        return request

    graph.session.auth = authenticate
    provider = server_embedding_provider(graph)
    with patch(
        "urllib3.connectionpool.HTTPConnectionPool._make_request",
        return_value=HTTPResponse(
            body=BytesIO(json.dumps(embedding_response()).encode()),
            status=200,
            preload_content=False,
        ),
    ) as http:
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
    assert http.call_args.kwargs["headers"]["Authorization"] == "Bearer refreshed-token"


def test_mismatch_rejected_on_every_response(graph):
    provider = server_embedding_provider(graph)
    with patch.object(
        graph.session,
        "post",
        side_effect=[
            response(embedding_response()),
            response(embedding_response("different-model")),
        ],
    ):
        provider.embed(["first", "second"], task=EmbeddingTask.QUERY)
        with pytest.raises(
            ServerEmbeddingModelMismatchError, match="semantic index model"
        ):
            provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
    assert provider.model_id == "different-model"


@pytest.mark.parametrize(
    "payload",
    [
        {
            "model": "index-model",
            "data": [{"index": 0, "embedding": [1]}, {"index": 0, "embedding": [2]}],
        },
        {"model": "index-model", "data": []},
        {"data": [{"index": 0, "embedding": [1]}]},
    ],
)
def test_invalid_responses_fail(graph, payload):
    provider = server_embedding_provider(graph)
    with (
        patch.object(graph.session, "post", return_value=response(payload)),
        pytest.raises(RuntimeError),
    ):
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)


@pytest.mark.parametrize(
    "index_provider, model_version",
    [("openai", "openai/index-model"), ("aws-bedrock", "bedrock/index-model")],
)
def test_recipe_uses_index_model_and_key_without_vendor_credentials(
    graph, index_provider, model_version
):
    graph.execute_graphql.return_value["appConfig"]["semanticSearchConfig"][
        "embeddingConfig"
    ]["provider"] = index_provider
    source = DocumentChunkingSource(
        ctx=PipelineContext(run_id="test"),
        config=DocumentChunkingSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"), incremental_mode=False
        ),
        standalone=False,
        graph=graph,
    )
    assert source.config.embedding.model == "index-model"
    assert source.config.embedding.model_embedding_key == "index_model"
    assert source.embedding_model == model_version
    with patch.object(
        graph.session, "post", return_value=response(embedding_response())
    ):
        vectors = source._generate_embeddings([{"text": "first"}, {"text": "second"}])
    workunits = list(
        source._emit_semantic_content(
            "urn:li:document:example", [{"text": "first"}, {"text": "second"}], vectors
        )
    )
    assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, SemanticContentClass)
    assert aspect.embeddings["index_model"].modelVersion == model_version


def test_connection_uses_same_discovery_and_embedding_checks(graph):
    with (
        patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph",
            return_value=graph,
        ),
        patch.object(
            graph.session,
            "post",
            return_value=response(
                {"model": "index-model", "data": [{"index": 0, "embedding": [1.0]}]}
            ),
        ),
    ):
        report = DataHubDocumentsSource.test_connection(
            {"embedding": {"provider": "datahub"}}
        )
    assert report.basic_connectivity is not None and report.basic_connectivity.capable
    assert report.capability_report is not None
    assert report.capability_report["semantic_search"].capable


def test_no_local_override_can_bypass_model_validation(graph):
    with pytest.raises(ValueError, match="local overrides"):
        DocumentChunkingSource.resolve_embedding_config(
            EmbeddingConfig(
                provider="datahub",
                model="different-model",
                allow_local_embedding_config=True,
            ),
            graph,
        )
    with pytest.raises(ValueError, match="model changed"):
        create_embedding_provider(
            EmbeddingConfig(provider="datahub", model="different-model"), graph=graph
        )


def test_default_provider_selection_is_unchanged(graph):
    resolved = DocumentChunkingSource.resolve_embedding_config(EmbeddingConfig(), graph)
    assert resolved.provider == "openai"
