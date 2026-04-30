"""Unit tests for DataHubDocumentsSource internal event-processing methods.

_process_document_for_raw_chunks — every branch:
  - happy path: embedding model present, rawChunks found → SemanticContent emitted
  - no embedding model → early return, nothing emitted
  - _get_raw_chunks returns None → early return, nothing emitted
  - rawChunks.chunks is empty → early return, nothing emitted
  - _generate_embeddings raises → error captured in report, nothing raised

_process_single_event — routing logic (no Kafka/streaming required; method takes a plain dict):
  - semanticContent event with rawChunks → delegates to _process_document_for_raw_chunks
  - semanticContent event without rawChunks → returns silently, no delegation
  - semanticContent event with malformed aspect data → parse error handled gracefully
  - event missing entityUrn or aspectName → early return
"""

from unittest.mock import MagicMock, patch

import pytest

pytest.importorskip("unstructured")

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub_documents.datahub_documents_config import (
    DataHubDocumentsSourceConfig,
)
from datahub.ingestion.source.datahub_documents.datahub_documents_source import (
    DataHubDocumentsSource,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.metadata.schema_classes import (
    RawChunksClass,
    SemanticContentClass,
    TextChunkClass,
)

# ── Constants ─────────────────────────────────────────────────────────────────

ENTITY_URN = "urn:li:document:notion.abc123abc123abc123abc123abc123ab"
FAKE_EMBEDDING: list[float] = [0.1, 0.2, 0.3]

_DOCS_CONFIG = DataHubDocumentsSourceConfig(
    embedding={
        "provider": "openai",
        "model": "text-embedding-3-small",
        "api_key": "fake-openai-key",
        "allow_local_embedding_config": True,
    },
    stateful_ingestion={"enabled": False},
    incremental={"enabled": False},
)

# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_source(mock_graph: MagicMock) -> DataHubDocumentsSource:
    """Construct a DataHubDocumentsSource with a pre-wired mock graph.

    The autouse fixture in tests/unit/conftest.py keeps the startup probe
    from hitting real network endpoints, so this function needs no extra patching.
    """
    ctx = PipelineContext(run_id="test-run", pipeline_name="test")
    ctx.graph = mock_graph
    return DataHubDocumentsSource(ctx=ctx, config=_DOCS_CONFIG)


def _make_raw_chunks(*texts: str) -> RawChunksClass:
    """Build a RawChunksClass from one or more text strings."""
    chunks = [
        TextChunkClass(
            position=i,
            characterOffset=0,
            characterLength=len(t),
            text=t,
        )
        for i, t in enumerate(texts)
    ]
    return RawChunksClass(totalChunks=len(chunks), chunks=chunks)


def _semantic_content_with_raw(raw: RawChunksClass) -> SemanticContentClass:
    return SemanticContentClass(embeddings={}, rawChunks=raw)


# ── Tests ─────────────────────────────────────────────────────────────────────


class TestProcessDocumentForRawChunks:
    def test_happy_path_emits_work_unit_and_updates_report(self) -> None:
        """Embedding model present and rawChunks found → SemanticContent work unit emitted."""
        raw = _make_raw_chunks("hello world")
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = _semantic_content_with_raw(raw)

        source = _make_source(mock_graph)

        # Override the autouse probe-mock so _generate_embeddings returns real data.
        with patch.object(
            DocumentChunkingSource,
            "_generate_embeddings",
            return_value=[FAKE_EMBEDDING],
        ):
            workunits = list(source._process_document_for_raw_chunks(ENTITY_URN))

        assert len(workunits) == 1
        assert source.report.num_documents_processed == 1
        assert source.report.num_embeddings_generated == 1
        assert not source.report.processing_errors

    def test_no_embedding_model_returns_nothing(self) -> None:
        """When embedding_model is None the method returns early with no output."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)
        # Simulate the probe having disabled embeddings (e.g. bad credentials).
        source.chunking_source.embedding_model = None

        workunits = list(source._process_document_for_raw_chunks(ENTITY_URN))

        assert workunits == []
        assert source.report.num_documents_processed == 0
        # graph.get_aspect must not have been called — we returned before touching it.
        mock_graph.get_aspect.assert_not_called()

    def test_no_raw_chunks_in_graph_returns_nothing(self) -> None:
        """When the graph holds no SemanticContent for the URN, nothing is emitted."""
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = None  # entity has no SemanticContent yet

        source = _make_source(mock_graph)

        workunits = list(source._process_document_for_raw_chunks(ENTITY_URN))

        assert workunits == []
        assert source.report.num_documents_processed == 0

    def test_empty_raw_chunks_list_returns_nothing(self) -> None:
        """SemanticContent exists but rawChunks.chunks is empty → nothing emitted."""
        empty_raw = RawChunksClass(totalChunks=0, chunks=[])
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = _semantic_content_with_raw(empty_raw)

        source = _make_source(mock_graph)

        workunits = list(source._process_document_for_raw_chunks(ENTITY_URN))

        assert workunits == []
        assert source.report.num_documents_processed == 0

    def test_embedding_failure_is_recorded_not_raised(self) -> None:
        """When _generate_embeddings raises, error is captured in report; nothing is raised."""
        raw = _make_raw_chunks("some text to embed")
        mock_graph = MagicMock()
        mock_graph.get_aspect.return_value = _semantic_content_with_raw(raw)

        source = _make_source(mock_graph)

        with patch.object(
            DocumentChunkingSource,
            "_generate_embeddings",
            side_effect=RuntimeError("embedding API down"),
        ):
            workunits = list(source._process_document_for_raw_chunks(ENTITY_URN))

        assert workunits == []
        assert len(source.report.processing_errors) == 1
        assert "embedding API down" in source.report.processing_errors[0]
        assert source.report.num_documents_processed == 0


class TestProcessSingleEvent:
    """Tests for the MCL event routing logic in _process_single_event.

    The method takes a plain dict — no Kafka or streaming infrastructure needed.
    The DocumentEventConsumer is just a thin wrapper that feeds such dicts in;
    all the interesting routing logic lives here.
    """

    def test_semantic_content_with_raw_chunks_delegates_to_processor(self) -> None:
        """A semanticContent MCL event carrying rawChunks routes to _process_document_for_raw_chunks."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)

        event = {
            "entityUrn": ENTITY_URN,
            "aspectName": "semanticContent",
            # _parse_mcl_aspect returns a plain dict as-is; _aspect_has_raw_chunks
            # checks for a non-null/non-empty "rawChunks" key.
            "aspect": {"rawChunks": {"totalChunks": 1, "chunks": []}},
        }

        fake_wu = MagicMock()
        with patch.object(
            source,
            "_process_document_for_raw_chunks",
            return_value=iter([fake_wu]),
        ) as mock_processor:
            workunits = list(source._process_single_event(event))

        mock_processor.assert_called_once_with(ENTITY_URN)
        assert workunits == [fake_wu]

    def test_semantic_content_without_raw_chunks_returns_silently(self) -> None:
        """A semanticContent event with no rawChunks is ignored — _process_document_for_raw_chunks not called."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)

        event = {
            "entityUrn": ENTITY_URN,
            "aspectName": "semanticContent",
            "aspect": {},  # no rawChunks key → _aspect_has_raw_chunks returns False
        }

        with patch.object(source, "_process_document_for_raw_chunks") as mock_processor:
            workunits = list(source._process_single_event(event))

        mock_processor.assert_not_called()
        assert workunits == []

    def test_semantic_content_malformed_aspect_handled_gracefully(self) -> None:
        """A semanticContent event whose aspect cannot be parsed is silently skipped."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)

        event = {
            "entityUrn": ENTITY_URN,
            "aspectName": "semanticContent",
            "aspect": "not valid json {{{",  # _parse_mcl_aspect tries json.loads → raises
        }

        with patch.object(source, "_process_document_for_raw_chunks") as mock_processor:
            workunits = list(source._process_single_event(event))

        mock_processor.assert_not_called()
        assert workunits == []

    def test_missing_entity_urn_returns_early(self) -> None:
        """Event without entityUrn is dropped before any processing."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)

        event = {"aspectName": "semanticContent", "aspect": {"rawChunks": {}}}

        with patch.object(source, "_process_document_for_raw_chunks") as mock_processor:
            workunits = list(source._process_single_event(event))

        mock_processor.assert_not_called()
        assert workunits == []

    def test_missing_aspect_name_returns_early(self) -> None:
        """Event without aspectName is dropped before any processing."""
        mock_graph = MagicMock()
        source = _make_source(mock_graph)

        event = {"entityUrn": ENTITY_URN, "aspect": {"rawChunks": {}}}

        with patch.object(source, "_process_document_for_raw_chunks") as mock_processor:
            workunits = list(source._process_single_event(event))

        mock_processor.assert_not_called()
        assert workunits == []
