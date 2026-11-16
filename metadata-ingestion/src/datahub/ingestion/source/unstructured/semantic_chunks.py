import dataclasses
import json
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.unstructured.chunking_config import ChunkingConfig
from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingTask,
)
from datahub.metadata.schema_classes import (
    EmbeddingChunkClass,
    EmbeddingModelDataClass,
    SemanticContentClass,
)

# Keep the aspect below GMS's parse cap and the Kafka producer's 5MB limit.
MAX_SERIALIZED_ASPECT_BYTES = 4 * 1024 * 1024


def has_embeddable_text(chunk: Dict[str, Any]) -> bool:
    """Whether a chunk is sent to the embedding provider.

    Blank chunks are skipped. The same predicate must gate both the provider call
    and the aspect emission, or the returned vectors shift onto the wrong chunks.
    """
    return bool((chunk.get("text") or "").strip())


def chunk_elements(
    elements: List[Dict[str, Any]], chunking: ChunkingConfig
) -> List[Dict[str, Any]]:
    """Chunk unstructured elements with the configured strategy."""
    from unstructured.chunking.basic import chunk_elements as basic_chunk
    from unstructured.chunking.title import chunk_by_title
    from unstructured.staging.base import elements_from_dicts

    element_objects = elements_from_dicts(elements)
    if chunking.strategy == "by_title":
        chunks = chunk_by_title(
            elements=element_objects,
            max_characters=chunking.max_characters,
            combine_text_under_n_chars=chunking.combine_text_under_n_chars,
        )
    else:
        chunks = basic_chunk(
            elements=element_objects,
            max_characters=chunking.max_characters,
            overlap=chunking.overlap,
        )
    return [chunk.to_dict() for chunk in chunks]


@dataclasses.dataclass(frozen=True)
class CappedChunks:
    chunks: List[Dict[str, Any]]
    # True when embeddable chunks past the cap were dropped.
    truncated: bool


def cap_chunks(chunks: List[Dict[str, Any]], max_chunks: int) -> CappedChunks:
    """Keep the leading chunks up to `max_chunks` embeddable ones.

    Blanks interleaved with the kept chunks stay, so character offsets stay
    correct and chunk/vector alignment is preserved.
    """
    embeddable_seen = 0
    cutoff = len(chunks)
    for i, chunk in enumerate(chunks):
        if has_embeddable_text(chunk):
            embeddable_seen += 1
            if embeddable_seen == max_chunks:
                cutoff = i + 1
                break
    if cutoff >= len(chunks) or not any(
        has_embeddable_text(chunk) for chunk in chunks[cutoff:]
    ):
        return CappedChunks(chunks, truncated=False)
    return CappedChunks(chunks[:cutoff], truncated=True)


def embed_chunks(
    chunks: List[Dict[str, Any]], provider: EmbeddingProvider, batch_size: int
) -> List[List[float]]:
    """One document vector per embeddable chunk, in chunk order."""
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    texts = [chunk["text"] for chunk in chunks if has_embeddable_text(chunk)]
    embeddings: List[List[float]] = []
    for i in range(0, len(texts), batch_size):
        batch = texts[i : i + batch_size]
        result = provider.embed(batch, task=EmbeddingTask.DOCUMENT)
        # Checked per batch so a short batch fails before the remaining provider
        # calls are spent, and miscounts cannot net out across a batch boundary.
        if len(result.embeddings) != len(batch):
            raise RuntimeError(
                f"Embedding provider returned {len(result.embeddings)} "
                f"vectors for {len(batch)} chunks"
            )
        embeddings.extend(result.embeddings)
    return embeddings


@dataclasses.dataclass(frozen=True)
class CappedSemanticContent:
    content: Optional[SemanticContentClass]
    original_bytes: int
    original_chunks: int
    kept_chunks: int


def build_semantic_content(
    chunks: List[Dict[str, Any]],
    vectors: List[List[float]],
    *,
    model_version: str,
    model_key: str,
    strategy: str,
    generated_at: int,
    source_text_sha256: Optional[str],
    max_serialized_bytes: int,
) -> CappedSemanticContent:
    # Blank chunks consume offsets, but never consume vectors.
    offset = 0
    spans = []
    for chunk in chunks:
        if has_embeddable_text(chunk):
            spans.append((chunk["text"], offset))
        offset += len(chunk.get("text") or "")
    embedding_chunks = [
        EmbeddingChunkClass(
            position=i,
            vector=vector,
            characterOffset=start,
            characterLength=len(text),
            tokenCount=None,
            text=text,
        )
        for i, ((text, start), vector) in enumerate(zip(spans, vectors, strict=True))
    ]
    # Providers expose token usage per batch, so per-chunk token counts remain absent.
    model_data = EmbeddingModelDataClass(
        modelVersion=model_version,
        generatedAt=generated_at,
        sourceTextSha256=source_text_sha256,
        chunkingStrategy=strategy,
        totalChunks=len(embedding_chunks),
        chunks=embedding_chunks,
    )
    content = SemanticContentClass(embeddings={model_key: model_data})
    original_bytes = len(json.dumps(content.to_obj()))
    kept = len(embedding_chunks)
    if original_bytes > max_serialized_bytes:
        # High-dimensional vectors can exceed the byte limit even after count
        # capping. Keep the largest prefix, preserving text/vector alignment.
        lo, hi = 0, kept
        while lo < hi:
            mid = (lo + hi + 1) // 2
            model_data.chunks = embedding_chunks[:mid]
            model_data.totalChunks = mid
            if len(json.dumps(content.to_obj())) <= max_serialized_bytes:
                lo = mid
            else:
                hi = mid - 1
        kept = lo
        model_data.chunks = embedding_chunks[:kept]
        model_data.totalChunks = kept
        if kept == 0:
            return CappedSemanticContent(None, original_bytes, len(embedding_chunks), 0)
    return CappedSemanticContent(content, original_bytes, len(embedding_chunks), kept)
