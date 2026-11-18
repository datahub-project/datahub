from typing import Any, Dict, List
from unittest.mock import Mock, patch

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.datahub_documents.document_embedding import (
    DocumentChunk,
    embed_document,
    resolve_embed_text,
)
from datahub.ingestion.source.datahub_documents.text_partitioner import TextPartitioner
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
    EmbeddingTask,
)
from datahub.ingestion.source.unstructured.semantic_chunks import (
    cap_chunks,
    chunk_elements,
    embed_chunks,
)
from datahub.metadata.schema_classes import SemanticContentClass


class TextLengthProvider(EmbeddingProvider):
    model_id = "classical/hash-v1-2048"

    def embed(self, texts: List[str], *, task: EmbeddingTask) -> EmbeddingResult:
        assert task is EmbeddingTask.DOCUMENT
        return EmbeddingResult(embeddings=[[float(len(text))] for text in texts])


@pytest.mark.parametrize("strategy", ["basic", "by_title"])
def test_embed_document_matches_source_output(strategy):
    pytest.importorskip("unstructured")
    text = "# A title\n\n" + "A paragraph about metadata. " * 30
    chunking = ChunkingConfig(
        strategy=strategy, max_characters=100, max_chunks_per_document=2
    )
    provider = TextLengthProvider()
    source = DocumentChunkingSource(
        ctx=PipelineContext(run_id="test"),
        config=DocumentChunkingSourceConfig(
            chunking=chunking,
            embedding=EmbeddingConfig(
                provider="classical",
                model="hash-v1-2048",
                allow_local_embedding_config=True,
            ),
            incremental_mode=False,
        ),
        standalone=False,
    )
    source._provider = provider
    elements = TextPartitioner().partition_text(text)
    raw = chunk_elements(elements, chunking)
    assert len(raw) > 2
    assert [chunk["text"] for chunk in source._chunk_elements(elements)] == [
        chunk["text"] for chunk in raw
    ]
    workunits = list(
        source.process_elements_inline("urn:li:document:example", elements)
    )
    assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
    aspect = workunits[0].metadata.aspect
    assert isinstance(aspect, SemanticContentClass)
    stored = next(iter(aspect.embeddings.values())).chunks
    assert embed_document(
        text,
        chunking=chunking,
        provider=provider,
        batch_size=1,
        min_text_length=0,
        skip_empty_text=True,
    ) == [DocumentChunk(chunk.text or "", chunk.vector) for chunk in stored]


@pytest.mark.parametrize(
    "tail, truncated", [([{"text": "last"}], True), ([{"text": " "}], False)]
)
def test_cap_preserves_blank_chunk_alignment(tail, truncated):
    chunks = [{"text": ""}, {"text": "a"}, {"text": " "}, {"text": "bb"}] + tail
    capped = cap_chunks(chunks, 2)
    assert capped.truncated == truncated
    assert capped.chunks == (chunks[:4] if truncated else chunks)
    assert embed_chunks(capped.chunks, TextLengthProvider(), 1) == [[1.0], [2.0]]


@pytest.mark.parametrize(
    "text, minimum, skip, expected",
    [
        ("", 0, False, []),
        ("short", 10, True, []),
        (" ", 0, True, []),
        ("short", 10, False, [DocumentChunk("short", [5.0])]),
    ],
)
def test_embed_document_skips_and_pairs_only_embeddable_chunks(
    text, minimum, skip, expected
):
    chunks: List[Dict[str, Any]] = [{"text": ""}, {"text": "short"}, {"text": " "}]
    with (
        patch(
            "datahub.ingestion.source.datahub_documents.document_embedding.TextPartitioner"
        ) as partitioner,
        patch(
            "datahub.ingestion.source.datahub_documents.document_embedding.chunk_elements",
            return_value=chunks,
        ),
    ):
        partitioner.return_value.partition_text.return_value = (
            [] if text.isspace() else [{"text": text}]
        )
        result = embed_document(
            text,
            chunking=ChunkingConfig(),
            provider=TextLengthProvider(),
            batch_size=1,
            min_text_length=minimum,
            skip_empty_text=skip,
        )
    assert result == expected


def test_embedding_count_mismatch_fails_before_next_batch():
    provider = Mock(spec=EmbeddingProvider)
    provider.embed.return_value = EmbeddingResult(embeddings=[])
    with pytest.raises(RuntimeError, match="vectors"):
        embed_chunks([{"text": "a"}, {"text": "b"}], provider, 1)
    provider.embed.assert_called_once_with(["a"], task=EmbeddingTask.DOCUMENT)


@pytest.mark.parametrize(
    "contents, expected",
    [
        ({"text": "body", "semanticText": " curated "}, " curated "),
        ({"text": " body ", "semanticText": "  "}, " body "),
        ({"text": " "}, ""),
        ({}, ""),
    ],
)
def test_resolve_text_preserves_content(contents, expected):
    assert resolve_embed_text(contents) == expected


@pytest.mark.parametrize("limit", [1000, 1])
def test_preview_matches_source_serialized_size_safeguard(monkeypatch, limit):
    from datahub.ingestion.source.datahub_documents import document_embedding
    from datahub.ingestion.source.unstructured import chunking_source
    from datahub.ingestion.source.unstructured.chunking_source import (
        compute_source_text_sha256,
    )
    from datahub.ingestion.source.unstructured.embedding_providers.factory import (
        create_embedding_provider,
    )

    pytest.importorskip("unstructured")
    text = "# A title\n\n" + "A paragraph about metadata. " * 30
    chunking = ChunkingConfig(strategy="basic", max_characters=100)
    embedding = EmbeddingConfig(
        provider="classical",
        model="hash-v1-64",
        model_embedding_key="custom_key",
        allow_local_embedding_config=True,
    )
    provider = create_embedding_provider(embedding)
    source = DocumentChunkingSource(
        ctx=PipelineContext(run_id="test"),
        config=DocumentChunkingSourceConfig(
            chunking=chunking, embedding=embedding, incremental_mode=False
        ),
        standalone=False,
    )
    source._provider = provider
    monkeypatch.setattr(chunking_source, "MAX_SERIALIZED_ASPECT_BYTES", limit)
    monkeypatch.setattr(document_embedding, "MAX_SERIALIZED_ASPECT_BYTES", limit)
    elements = TextPartitioner().partition_text(text)
    workunits = list(
        source.process_elements_inline(
            "urn:li:document:example",
            elements,
            source_text_sha256=compute_source_text_sha256(text),
        )
    )
    stored = []
    if workunits:
        assert isinstance(workunits[0].metadata, MetadataChangeProposalWrapper)
        aspect = workunits[0].metadata.aspect
        assert isinstance(aspect, SemanticContentClass)
        stored = aspect.embeddings["custom_key"].chunks
    preview = embed_document(
        text,
        chunking=chunking,
        provider=provider,
        batch_size=25,
        min_text_length=0,
        skip_empty_text=True,
    )
    assert preview == [
        DocumentChunk(chunk.text or "", chunk.vector) for chunk in stored
    ]
    assert len(preview) < len(chunk_elements(elements, chunking))
    if limit == 1:
        assert not preview
    else:
        assert preview


@pytest.mark.parametrize("batch_size", [0, -1])
def test_nonpositive_batch_size_rejected(batch_size):
    with pytest.raises(ValueError):
        EmbeddingConfig(batch_size=batch_size)
    with pytest.raises(ValueError):
        embed_chunks([{"text": "text"}], Mock(), batch_size)


def test_serialized_cap_keeps_largest_fitting_prefix():
    import json

    from datahub.ingestion.source.unstructured.semantic_chunks import (
        CappedSemanticContent,
        build_semantic_content,
    )

    chunks = [{"text": "chunk " * 20} for _ in range(5)]
    vectors = [[1.0] * 16 for _ in chunks]

    def build_prefix(
        chunks: List[Dict[str, Any]], vectors: List[List[float]], *, limit: int
    ) -> CappedSemanticContent:
        return build_semantic_content(
            chunks,
            vectors,
            model_version="openai/model",
            model_key="model",
            strategy="basic",
            generated_at=123,
            source_text_sha256=None,
            max_serialized_bytes=limit,
        )

    limit = 1000
    capped = build_prefix(chunks, vectors, limit=limit)
    assert capped.content is not None
    assert 0 < capped.kept_chunks < len(chunks)
    assert len(json.dumps(capped.content.to_obj())) <= limit
    next_prefix = build_prefix(
        chunks[: capped.kept_chunks + 1],
        vectors[: capped.kept_chunks + 1],
        limit=1000000,
    )
    assert next_prefix.content is not None
    assert len(json.dumps(next_prefix.content.to_obj())) > limit
