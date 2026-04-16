"""Unit tests for DataHubDocumentsSource pending-chunk processing methods."""

from typing import Any
from unittest.mock import MagicMock, patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unstructured.chunking_source import PENDING_CHUNKS_KEY
from datahub.metadata.schema_classes import (
    EmbeddingChunkClass,
    EmbeddingModelDataClass,
    SemanticContentClass,
)


def _make_pending_model_data(*texts: str) -> EmbeddingModelDataClass:
    """Build an EmbeddingModelDataClass with empty-vector (pending) chunks."""
    return EmbeddingModelDataClass(
        modelVersion=PENDING_CHUNKS_KEY,
        generatedAt=0,
        chunkingStrategy="basic",
        totalChunks=len(texts),
        totalTokens=0,
        chunks=[
            EmbeddingChunkClass(
                position=i,
                vector=[],
                text=text,
                characterOffset=0,
                characterLength=len(text),
            )
            for i, text in enumerate(texts)
        ],
    )


def _semantic_content_with_pending(
    pending: EmbeddingModelDataClass,
) -> SemanticContentClass:
    return SemanticContentClass(embeddings={PENDING_CHUNKS_KEY: pending})


def _make_source(embedding_model: bool = True) -> Any:
    """Instantiate DataHubDocumentsSource with minimal mocks."""
    from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
        DataHubDocumentsSourceConfig,
    )
    from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
        DataHubDocumentsSource,
    )

    ctx = MagicMock(spec=PipelineContext)
    ctx.pipeline_name = "test"
    ctx.run_id = "test-run"
    ctx.graph = None

    config = DataHubDocumentsSourceConfig()

    with patch(
        "datahub.ingestion.source.datahub_documents.datahub_documents_source.DataHubGraph"
    ):
        source = DataHubDocumentsSource.__new__(DataHubDocumentsSource)
        source.ctx = ctx
        source.config = config
        source.graph = MagicMock()
        source.state_handler = None

        from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
            DataHubDocumentsReport,
        )

        source.report = DataHubDocumentsReport()

        chunking_source = MagicMock()
        chunking_source.embedding_model = (
            "bedrock/cohere.embed-english-v3" if embedding_model else None
        )
        source.chunking_source = chunking_source

    return source


class TestAspectHasPendingChunks:
    def test_returns_true_when_pending_key_present(self):
        source = _make_source()
        aspect_dict: dict[str, Any] = {
            "embeddings": {PENDING_CHUNKS_KEY: {"chunks": [{"text": "hi"}]}}
        }
        assert source._aspect_has_pending_chunks(aspect_dict)

    def test_returns_false_when_no_pending_key(self):
        source = _make_source()
        aspect_dict: dict[str, Any] = {"embeddings": {"some_model": {}}}
        assert not source._aspect_has_pending_chunks(aspect_dict)

    def test_returns_false_when_no_embeddings(self):
        source = _make_source()
        assert not source._aspect_has_pending_chunks({})


class TestGetPendingChunks:
    def test_returns_pending_model_data(self):
        source = _make_source()
        pending = _make_pending_model_data("chunk one", "chunk two")
        semantic_content = _semantic_content_with_pending(pending)
        source.graph.get_aspect.return_value = semantic_content

        result = source._get_pending_chunks("urn:li:document:(test,doc,PROD)")

        assert result is not None
        assert len(result.chunks) == 2

    def test_returns_none_when_no_aspect(self):
        source = _make_source()
        source.graph.get_aspect.return_value = None

        result = source._get_pending_chunks("urn:li:document:(test,doc,PROD)")

        assert result is None

    def test_returns_none_on_exception(self):
        source = _make_source()
        source.graph.get_aspect.side_effect = RuntimeError("connection refused")

        result = source._get_pending_chunks("urn:li:document:(test,doc,PROD)")

        assert result is None


class TestPendingChunksToDicts:
    def test_converts_chunks_to_dicts(self):
        source = _make_source()
        pending = _make_pending_model_data("hello world", "second chunk")

        dicts = source._pending_chunks_to_dicts(pending)

        assert len(dicts) == 2
        assert dicts[0]["text"] == "hello world"
        assert dicts[1]["text"] == "second chunk"

    def test_skips_chunks_without_text(self):
        source = _make_source()
        pending = EmbeddingModelDataClass(
            modelVersion=PENDING_CHUNKS_KEY,
            generatedAt=0,
            chunkingStrategy="basic",
            totalChunks=2,
            totalTokens=0,
            chunks=[
                EmbeddingChunkClass(
                    position=0,
                    vector=[],
                    text=None,
                    characterOffset=0,
                    characterLength=0,
                ),
                EmbeddingChunkClass(
                    position=1,
                    vector=[],
                    text="has text",
                    characterOffset=0,
                    characterLength=8,
                ),
            ],
        )

        dicts = source._pending_chunks_to_dicts(pending)

        assert len(dicts) == 1
        assert dicts[0]["text"] == "has text"


class TestProcessDocumentForPendingChunks:
    def test_generates_and_emits_embeddings(self):
        source = _make_source(embedding_model=True)
        pending = _make_pending_model_data("chunk one", "chunk two")
        semantic_content = _semantic_content_with_pending(pending)
        source.graph.get_aspect.return_value = semantic_content

        mock_embeddings = [[0.1, 0.2], [0.3, 0.4]]
        source.chunking_source._generate_embeddings.return_value = mock_embeddings
        source.chunking_source._emit_semantic_content.return_value = iter([])

        list(
            source._process_document_for_pending_chunks(
                "urn:li:document:(test,doc,PROD)"
            )
        )

        source.chunking_source._generate_embeddings.assert_called_once()
        source.chunking_source._emit_semantic_content.assert_called_once()
        assert source.report.num_embeddings_generated == 2

    def test_skips_when_no_embedding_model(self):
        source = _make_source(embedding_model=False)

        list(
            source._process_document_for_pending_chunks(
                "urn:li:document:(test,doc,PROD)"
            )
        )

        source.graph.get_aspect.assert_not_called()

    def test_skips_when_no_pending_chunks(self):
        source = _make_source(embedding_model=True)
        source.graph.get_aspect.return_value = None

        list(
            source._process_document_for_pending_chunks(
                "urn:li:document:(test,doc,PROD)"
            )
        )

        source.chunking_source._generate_embeddings.assert_not_called()

    def test_reports_error_on_embedding_failure(self):
        source = _make_source(embedding_model=True)
        pending = _make_pending_model_data("some text")
        source.graph.get_aspect.return_value = _semantic_content_with_pending(pending)
        source.chunking_source._generate_embeddings.side_effect = RuntimeError(
            "embedding API down"
        )

        list(
            source._process_document_for_pending_chunks(
                "urn:li:document:(test,doc,PROD)"
            )
        )

        assert len(source.report.processing_errors) == 1
        assert "embedding API down" in source.report.processing_errors[0]


class TestProcessSingleEventSemanticContent:
    def test_routes_semanticContent_event_with_pending_to_processor(self):
        source = _make_source(embedding_model=True)

        aspect_dict: dict[str, Any] = {
            "embeddings": {PENDING_CHUNKS_KEY: {"chunks": []}}
        }
        event = {
            "entityUrn": "urn:li:document:(test,doc,PROD)",
            "aspectName": "semanticContent",
            "aspect": aspect_dict,
        }

        with patch.object(
            source,
            "_process_document_for_pending_chunks",
            return_value=iter([]),
        ) as mock_proc:
            list(source._process_single_event(event))
            mock_proc.assert_called_once_with("urn:li:document:(test,doc,PROD)")

    def test_ignores_semanticContent_event_without_pending(self):
        source = _make_source(embedding_model=True)

        event = {
            "entityUrn": "urn:li:document:(test,doc,PROD)",
            "aspectName": "semanticContent",
            "aspect": {"embeddings": {"real_model": {"chunks": []}}},
        }

        with patch.object(
            source, "_process_document_for_pending_chunks", return_value=iter([])
        ) as mock_proc:
            list(source._process_single_event(event))
            mock_proc.assert_not_called()

    def test_ignores_semanticContent_event_when_no_embedding_model(self):
        source = _make_source(embedding_model=False)

        aspect_dict: dict[str, Any] = {
            "embeddings": {PENDING_CHUNKS_KEY: {"chunks": []}}
        }
        event = {
            "entityUrn": "urn:li:document:(test,doc,PROD)",
            "aspectName": "semanticContent",
            "aspect": aspect_dict,
        }

        with patch.object(
            source, "_process_document_for_pending_chunks", return_value=iter([])
        ) as mock_proc:
            list(source._process_single_event(event))
            mock_proc.assert_not_called()
