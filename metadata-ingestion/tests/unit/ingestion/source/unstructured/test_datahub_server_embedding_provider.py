import json
from functools import partial
from io import BytesIO
from typing import Any, Callable, Dict, Optional
from unittest.mock import Mock, patch

import pytest
import requests
from urllib3.response import HTTPResponse

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DataHubRestEmitter, RequestsSessionConfig
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import CapabilityReport
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.embeddings import ServerEmbeddingsUnavailableError
from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
    DataHubDocumentsSource,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.embedding_providers.base import EmbeddingTask
from datahub.ingestion.source.unstructured.embedding_providers.classical import (
    MAX_CODE_POINTS as CLASSICAL_MAX_CODE_POINTS,
)
from datahub.ingestion.source.unstructured.embedding_providers.datahub_server import (
    DataHubServerEmbeddingProvider,
    server_embedding_provider,
)
from datahub.ingestion.source.unstructured.embedding_providers.factory import (
    create_embedding_provider,
)
from datahub.ingestion.source.unstructured.embedding_target import (
    ServerEmbeddingModelMismatchError,
    resolve_embedding_target,
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
    graph._session_config = RequestsSessionConfig()
    sessions = []

    def create_session(**kwargs):
        session = DataHubRestEmitter.create_session(graph, **kwargs)
        session.trust_env = False
        sessions.append(session)
        return session

    graph.create_session.side_effect = create_session
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
    for session in sessions:
        session.close()


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
        provider._client._session, "post", return_value=response(embedding_response())
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
        *[
            {"embeddingService": {"enabled": True, "path": path}}
            for path in [
                "//example.invalid/embeddings",
                "https://example.invalid/embeddings",
                "/custom/embeddings?version=1",
                "/custom/embeddings#fragment",
                "embeddings",
                # urlsplit strips the tab, so these parse as an authority or fail.
                "/\t/example.invalid/embeddings",
                "/\t/[",
            ]
        ],
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
    assert report.basic_connectivity.capable
    assert report.capability_report is not None
    capability = report.capability_report["semantic_search"]
    assert not capability.capable
    assert capability.failure_reason is not None
    assert "own credentials" in capability.failure_reason


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
            provider._client._session,
            "post",
            return_value=response(payload, status, headers),
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
        pytest.raises(
            RuntimeError, match="credits" if retry_header == "false" else "HTTP 429"
        ),
    ):
        provider.embed(["text"], task=EmbeddingTask.DOCUMENT)
    assert http.call_count == expected_attempts


def test_actual_session_preserves_per_request_authentication(graph):
    tokens = iter(["Bearer refreshed-token-1", "Bearer refreshed-token-2"])

    def authenticate(request):
        request.headers["Authorization"] = next(tokens)
        return request

    graph.session.auth = authenticate
    provider = server_embedding_provider(graph)
    with patch(
        "urllib3.connectionpool.HTTPConnectionPool._make_request",
        side_effect=lambda *args, **kwargs: HTTPResponse(
            body=BytesIO(json.dumps(embedding_response()).encode()),
            status=200,
            preload_content=False,
        ),
    ) as http:
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
        provider.embed(["first", "second"], task=EmbeddingTask.QUERY)
    assert [
        call.kwargs["headers"]["Authorization"] for call in http.call_args_list
    ] == ["Bearer refreshed-token-1", "Bearer refreshed-token-2"]


def test_mismatch_rejected_on_every_response(graph):
    provider = server_embedding_provider(graph)
    with patch.object(
        provider._client._session,
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
    assert provider.model_id == "index-model"


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
        patch.object(provider._client._session, "post", return_value=response(payload)),
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
        requests.Session, "post", return_value=response(embedding_response())
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
            requests.Session,
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
    with pytest.raises(ValueError, match="embedding.model"):
        create_embedding_provider(
            EmbeddingConfig(provider="datahub", model="different-model"), graph=graph
        )


def test_default_provider_selection_is_unchanged(graph):
    resolved = DocumentChunkingSource.resolve_embedding_config(EmbeddingConfig(), graph)
    assert resolved.provider == "openai"


def test_canonical_cloud_url(graph):
    graph.config.server = "http://example.acryl.io"
    provider = server_embedding_provider(graph)
    with patch.object(
        provider._client._session, "post", return_value=response(embedding_response())
    ) as post:
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
    assert post.call_args.args[0] == "https://example.acryl.io/gms" + PATH


def test_resolution_is_read_only_and_discovery_reused(graph):
    original_adapters = dict(graph.session.adapters)
    resolved = DocumentChunkingSource.resolve_embedding_config(
        EmbeddingConfig(provider="datahub"), graph
    )
    graph.create_session.assert_not_called()
    target = resolve_embedding_target(resolved, graph)
    provider = create_embedding_provider(resolved, graph=graph, target=target)
    assert isinstance(provider, DataHubServerEmbeddingProvider)
    with patch.object(
        provider._client._session, "post", return_value=response(embedding_response())
    ):
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
        provider.embed(["first", "second"], task=EmbeddingTask.QUERY)
    assert graph.execute_graphql.call_count == 1
    assert dict(graph.session.adapters) == original_adapters
    assert graph.create_session.call_count == 1
    assert provider._client._session is not graph.session


def test_owned_transport_preserves_sdk_settings(graph):
    from datahub.emitter.rest_emitter import _KeepAliveHTTPAdapter

    graph._session_config = RequestsSessionConfig(
        tcp_keepalive=True, pool_connections=7, pool_maxsize=19
    )
    graph.session.headers["Authorization"] = "Bearer static-token"
    original_adapters = dict(graph.session.adapters)
    provider = server_embedding_provider(graph)
    session = provider._client._session
    adapter = session.get_adapter(SERVER + PATH)
    assert isinstance(adapter, _KeepAliveHTTPAdapter)
    assert adapter.poolmanager.pools._maxsize == 7
    assert adapter.poolmanager.connection_pool_kw["maxsize"] == 19
    with patch(
        "urllib3.connectionpool.HTTPConnectionPool._make_request",
        return_value=HTTPResponse(
            body=BytesIO(json.dumps(embedding_response()).encode()),
            status=200,
            preload_content=False,
        ),
    ) as http:
        provider.embed(["first", "second"], task=EmbeddingTask.DOCUMENT)
    assert http.call_args.kwargs["headers"]["Authorization"] == "Bearer static-token"
    assert graph.session.adapters == original_adapters
    with patch.object(session, "close") as close:
        provider.close()
    close.assert_called_once()
    assert graph.session.adapters == original_adapters


@pytest.mark.parametrize("source_name", ["confluence", "google_drive", "notion"])
@pytest.mark.parametrize("available", [True, False])
@pytest.mark.parametrize("borrowed", [True, False])
def test_connector_capability_passes_graph_and_closes_only_owned(
    graph, source_name, available, borrowed
):
    check: Callable[[Optional[DataHubGraph]], CapabilityReport]
    if source_name == "confluence":
        from datahub.ingestion.source.confluence.confluence_config import (
            ConfluenceSourceConfig,
        )
        from datahub.ingestion.source.confluence.confluence_source import (
            ConfluenceSource,
        )

        confluence_config = ConfluenceSourceConfig.model_validate(
            {
                "url": "https://example.invalid/wiki",
                "username": "user",
                "api_token": "token",
                "embedding": {"provider": "datahub"},
                "datahub": {"server": SERVER, "token": "datahub-token"},
            }
        )
        check = partial(
            ConfluenceSource._test_semantic_search_capability, confluence_config
        )
    elif source_name == "notion":
        from datahub.ingestion.source.notion.notion_config import NotionSourceConfig
        from datahub.ingestion.source.notion.notion_source import NotionSource

        if borrowed:
            pytest.skip("Notion's connection test has no graph of its own to lend")
        notion_config = NotionSourceConfig.model_validate(
            {
                "api_key": "secret_test_key",
                "embedding": {"provider": "datahub"},
                "datahub": {"server": SERVER, "token": "datahub-token"},
            }
        )

        def check(_graph: Optional[DataHubGraph]) -> CapabilityReport:
            return NotionSource._test_semantic_search_capability(notion_config)

    else:
        pytest.importorskip("googleapiclient")
        from datahub.ingestion.source.google_drive.google_drive_config import (
            GoogleDriveSourceConfig,
        )
        from datahub.ingestion.source.google_drive.google_drive_source import (
            GoogleDriveSource,
        )

        drive_config = GoogleDriveSourceConfig.model_validate(
            {
                "root_folder_ids": ["root_folder_id_1234567890"],
                "embedding": {"provider": "datahub"},
                "datahub": {"server": SERVER, "token": "datahub-token"},
            }
        )
        check = partial(
            GoogleDriveSource._test_semantic_search_capability, drive_config
        )
    if not available:
        graph.server_config = RestServiceConfig(raw_config={})
    with (
        patch(
            "datahub.ingestion.source.unstructured.chunking_source.DataHubGraph",
            return_value=graph,
        ) as constructor,
        patch.object(
            requests.Session,
            "post",
            return_value=response(
                {"model": "index-model", "data": [{"index": 0, "embedding": [1.0]}]}
            ),
        ) as post,
    ):
        capability = check(graph if borrowed else None)
    assert capability.capable == available
    assert post.call_count == int(available)
    assert graph.close.call_count == int(not borrowed)
    if borrowed:
        constructor.assert_not_called()
    else:
        assert constructor.call_args.kwargs["config"].token == "datahub-token"


def test_connection_problem_is_basic_connectivity_failure():
    graph = Mock(spec=DataHubGraph)
    graph.test_connection.side_effect = RuntimeError("connection failed")
    with patch(
        "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph",
        return_value=graph,
    ):
        report = DataHubDocumentsSource.test_connection(
            {"embedding": {"provider": "datahub"}}
        )
    assert report.basic_connectivity is not None
    assert not report.basic_connectivity.capable


@pytest.mark.parametrize(
    "status,headers,payload",
    [
        (403, {}, {}),
        (429, {"x-should-retry": "false"}, {}),
        (404, {}, {"error": {"type": "not_enabled"}}),
        (
            200,
            {},
            {"model": "wrong-model", "data": [{"index": 0, "embedding": [1.0]}]},
        ),
    ],
)
def test_permanent_failure_stops_before_second_document(
    graph, status, headers, payload
):
    from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
        DataHubDocumentsSourceConfig,
    )

    source = DataHubDocumentsSource(
        PipelineContext(run_id="test", graph=graph),
        DataHubDocumentsSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"),
            locking={"enabled": False},
            stateful_ingestion={"enabled": False},
        ),
    )
    documents = [
        {"urn": "urn:li:document:first", "text": "first document " * 20},
        {"urn": "urn:li:document:second", "text": "second document " * 20},
    ]

    # Exercise the source's per-document error boundary and run-level failure report.
    def batch():
        for doc in documents:
            yield from source._process_document_with_throttle(doc)

    with (
        patch.object(source, "_process_batch_mode", side_effect=batch),
        # Exercises the per-document boundary; the startup probe has its own test.
        patch.object(source.chunking_source, "verify_embedding_service"),
        patch.object(
            source.text_partitioner, "partition_text", return_value=[{"text": "chunk"}]
        ),
        patch.object(
            source.chunking_source, "_chunk_elements", return_value=[{"text": "chunk"}]
        ),
        patch.object(
            requests.Session,
            "post",
            return_value=response(payload, status, headers),
        ) as post,
    ):
        list(source.get_workunits_internal())
    assert post.call_count == 1
    assert source.report.failures
    assert not source.document_state


def test_transport_change_preserves_stored_direct_hash(graph):
    from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
        DataHubDocumentsSourceConfig,
    )

    index = graph.execute_graphql.return_value["appConfig"]["semanticSearchConfig"][
        "embeddingConfig"
    ]
    index.update(
        provider="aws-bedrock",
        modelId="cohere.embed-english-v3",
        modelEmbeddingKey="cohere_embed_v3",
        awsRegion="us-east-1",
    )
    ctx = PipelineContext(run_id="test", graph=graph)
    direct = DataHubDocumentsSource(
        ctx,
        DataHubDocumentsSourceConfig(
            locking={"enabled": False}, stateful_ingestion={"enabled": False}
        ),
    )
    urn = "urn:li:document:example"
    text = "unchanged document content"
    stored_hash = direct._calculate_text_hash(text)
    # This is the previously persisted fingerprint shape; transport must add no fields.
    fingerprint = direct._get_processing_config_fingerprint()
    assert fingerprint["embedding_provider"] == "bedrock"
    assert fingerprint["embedding_model"] == "cohere.embed-english-v3"
    server = DataHubDocumentsSource(
        ctx,
        DataHubDocumentsSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"),
            locking={"enabled": False},
            stateful_ingestion={"enabled": False},
        ),
    )
    server.document_state[urn] = {"content_hash": stored_hash}
    assert server._get_processing_config_fingerprint() == fingerprint
    assert server._calculate_text_hash(text) == stored_hash
    assert not server._should_process(urn, text)
    server.config.chunking.max_characters += 1
    assert server._should_process(urn, text)
    server.config.chunking.max_characters -= 1
    index["modelId"] = "different-model"
    different = DataHubDocumentsSource(
        ctx,
        DataHubDocumentsSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"),
            locking={"enabled": False},
            stateful_ingestion={"enabled": False},
        ),
    )
    different.document_state[urn] = {"content_hash": stored_hash}
    assert different._should_process(urn, text)


def test_unknown_server_onnx_pooling_does_not_claim_equivalence(graph):
    config = EmbeddingConfig(provider="onnx", model="model", onnx_pooling="mean")
    direct = resolve_embedding_target(config)
    index = graph.execute_graphql.return_value["appConfig"]["semanticSearchConfig"][
        "embeddingConfig"
    ]
    index.update(provider="onnx", modelId="model")
    server = resolve_embedding_target(EmbeddingConfig(provider="datahub"), graph)
    assert direct.onnx_pooling == "mean"
    assert server.onnx_pooling is None


def test_permanent_event_failure_holds_offsets_without_batch_fallback(graph):
    from datahub.ingestion.graph.embeddings import ServerEmbeddingRunError
    from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
        DataHubDocumentsSourceConfig,
    )

    source = DataHubDocumentsSource(
        PipelineContext(run_id="test", graph=graph),
        DataHubDocumentsSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"),
            locking={"enabled": False},
            stateful_ingestion={"enabled": False},
        ),
    )
    source.state_handler = Mock()
    source.state_handler.is_checkpointing_enabled.return_value = True
    source.state_handler.get_event_offset.return_value = "offset-1"
    with (
        patch(
            "datahub.ingestion.source.datahub_documents.datahub_documents_source.DocumentEventConsumer"
        ) as consumer_class,
        patch.object(
            source,
            "_process_single_event",
            side_effect=ServerEmbeddingRunError("credits exhausted"),
        ) as process,
        patch.object(source, "_process_batch_mode") as batch,
    ):
        consumer = consumer_class.return_value
        consumer.consume_events.return_value = iter(
            [
                {"entityUrn": "urn:li:document:first"},
                {"entityUrn": "urn:li:document:second"},
            ]
        )
        consumer.suppress_offset_commits = False
        with pytest.raises(ServerEmbeddingRunError):
            list(source._process_event_mode())
    assert process.call_count == 1
    batch.assert_not_called()
    assert consumer.suppress_offset_commits
    consumer.close.assert_called_once()


def test_permanent_standalone_event_failure_holds_offsets(graph):
    from datahub.ingestion.graph.embeddings import ServerEmbeddingRunError

    source = DocumentChunkingSource(
        ctx=PipelineContext(run_id="test"),
        config=DocumentChunkingSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"), incremental_mode=False
        ),
        standalone=False,
        graph=graph,
    )
    event = {
        "entityUrn": "urn:li:document:first",
        "aspect": {"customProperties": {"unstructured_elements": "[]"}},
    }
    with (
        patch(
            "datahub.ingestion.source.unstructured.event_consumer.DocumentEventConsumer"
        ) as consumer_class,
        patch.object(
            source,
            "_process_single_document",
            side_effect=ServerEmbeddingRunError("credits exhausted"),
        ) as process,
    ):
        consumer = consumer_class.return_value
        consumer.consume_events.return_value = iter([event, event])
        consumer.suppress_offset_commits = False
        with pytest.raises(ServerEmbeddingRunError):
            list(source._process_event_driven())
    assert process.call_count == 1
    assert consumer.suppress_offset_commits
    consumer.close.assert_called_once()


def documents_source(graph):
    from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
        DataHubDocumentsSourceConfig,
    )

    return DataHubDocumentsSource(
        PipelineContext(run_id="test", graph=graph),
        DataHubDocumentsSourceConfig(
            embedding=EmbeddingConfig(provider="datahub"),
            locking={"enabled": False},
            stateful_ingestion={"enabled": False},
        ),
    )


def test_startup_probe_stops_run_before_any_document(graph):
    source = documents_source(graph)
    with (
        patch.object(source, "_process_batch_mode") as batch,
        patch.object(
            requests.Session,
            "post",
            return_value=response(embedding_response("different-model")),
        ) as post,
    ):
        list(source.get_workunits_internal())
    batch.assert_not_called()
    assert post.call_count == 1
    assert post.call_args.kwargs["json"]["x_datahub"]["task_type"] == "query"
    assert source.report.failures


def test_advertised_model_mismatch_fails_without_embedding(graph):
    def advertise(model):
        graph.server_config = RestServiceConfig(
            raw_config={
                "embeddingService": {"enabled": True, "path": PATH, "model": model}
            }
        )

    advertise("other-model")
    with (
        patch.object(requests.Session, "post") as post,
        pytest.raises(ServerEmbeddingModelMismatchError, match="other-model"),
    ):
        server_embedding_provider(graph)
    post.assert_not_called()
    advertise("index-model")
    assert server_embedding_provider(graph).model_id == "index-model"


def test_close_releases_lock_when_teardown_raises(graph):
    source = documents_source(graph)
    source.lock = Mock()
    with (
        patch.object(
            source.chunking_source, "close", side_effect=RuntimeError("teardown")
        ),
        pytest.raises(RuntimeError, match="teardown"),
    ):
        source.close()
    source.lock.release.assert_called_once()


def test_classical_cap_applies_to_the_resolved_server_model(graph):
    graph.execute_graphql.return_value["appConfig"]["semanticSearchConfig"][
        "embeddingConfig"
    ].update(provider="classical", modelId="hash-v1-2048")
    config = DocumentChunkingSourceConfig(
        embedding=EmbeddingConfig(provider="datahub"), incremental_mode=False
    )
    config.chunking.max_characters = CLASSICAL_MAX_CODE_POINTS + 1
    with pytest.raises(ValueError, match="code point limit"):
        DocumentChunkingSource(
            ctx=PipelineContext(run_id="test"),
            config=config,
            standalone=False,
            graph=graph,
        )
