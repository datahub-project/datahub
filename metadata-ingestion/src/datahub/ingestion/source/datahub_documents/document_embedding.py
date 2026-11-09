import dataclasses
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict, List

from datahub.ingestion.source.datahub_documents.text_partitioner import (
    TextPartitioner,
)
from datahub.ingestion.source.unstructured.chunking_config import ChunkingConfig
from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_target import model_storage_key
from datahub.ingestion.source.unstructured.semantic_chunks import (
    MAX_SERIALIZED_ASPECT_BYTES,
    build_semantic_content,
    cap_chunks,
    chunk_elements,
    embed_chunks,
    has_embeddable_text,
)


def resolve_embed_text(contents: Dict[str, Any]) -> str:
    """The text a document is embedded from: `semanticText` if it has any, else `text`.

    `semanticText` lets a curated representation drive retrieval while the full
    body stays in `text`. A whitespace-only value counts as missing, but the value
    itself is returned as-is so content hashes stay stable.
    """
    semantic_text = contents.get("semanticText") or ""
    if semantic_text.strip():
        return semantic_text
    text = contents.get("text") or ""
    return text if text.strip() else ""


def is_below_embed_minimum(
    text: str, *, min_text_length: int, skip_empty_text: bool
) -> bool:
    """Empty text is never embedded; `skip_empty_text` also skips text that is too short."""
    return not text or (skip_empty_text and len(text) < min_text_length)


@dataclasses.dataclass(frozen=True)
class DocumentChunk:
    text: str
    vector: List[float]


def embed_document(
    text: str,
    *,
    chunking: ChunkingConfig,
    provider: EmbeddingProvider,
    batch_size: int,
    min_text_length: int,
    skip_empty_text: bool,
    disable_markdown_link_parsing: bool = True,
    partition_timeout_seconds: float = 30.0,
) -> List[DocumentChunk]:
    """The chunks and vectors the source would write for `text`; empty when it skips it.

    Pass the recipe's ``disable_markdown_link_parsing`` and ``partition_timeout_seconds``
    to match its output; the defaults match the source's config defaults.

    Raises:
        PartitionTimeoutError: If partitioning exceeds ``partition_timeout_seconds``
            (enforced only on the main thread of a POSIX process).
    """
    if is_below_embed_minimum(
        text, min_text_length=min_text_length, skip_empty_text=skip_empty_text
    ):
        return []
    elements = TextPartitioner(
        disable_markdown_link_parsing=disable_markdown_link_parsing,
        timeout_seconds=partition_timeout_seconds,
    ).partition_text(text)
    if not elements:
        return []
    chunks = cap_chunks(
        chunk_elements(elements, chunking), chunking.max_chunks_per_document
    ).chunks
    embeddable = [chunk for chunk in chunks if has_embeddable_text(chunk)]
    if not embeddable:
        return []
    vectors = embed_chunks(chunks, provider, batch_size)
    model = provider.model_id.split("/", 1)[-1]
    model_key = model_storage_key(model, provider.model_embedding_key)
    capped = build_semantic_content(
        chunks,
        vectors,
        model_version=provider.semantic_model_version,
        model_key=model_key,
        strategy=chunking.strategy,
        generated_at=int(datetime.now(timezone.utc).timestamp() * 1000),
        source_text_sha256=hashlib.sha256(text.encode("utf-8")).hexdigest(),
        max_serialized_bytes=MAX_SERIALIZED_ASPECT_BYTES,
    )
    if capped.content is None:
        return []
    return [
        DocumentChunk(chunk.text or "", chunk.vector)
        for chunk in capped.content.embeddings[model_key].chunks
    ]
