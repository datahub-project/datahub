"""Tests for the classical (deterministic hashed-feature) embedding provider.

The GMS query side implements the same algorithm in Java. Both suites assert the
golden digests in ``GOLDEN_FIXTURE`` (the Java test loads it from its classpath):
each is the lowercase hex SHA-256 over the vector serialized as big-endian IEEE-754
binary32 components, so a change to either implementation that breaks
query/document parity fails on that side.
"""

import hashlib
import json
import struct
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unstructured.chunking_config import (
    ChunkingConfig,
    DocumentChunkingSourceConfig,
    EmbeddingConfig,
    ServerEmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.embedding_providers.classical import (
    ClassicalEmbeddingProvider,
)
from datahub.ingestion.source.unstructured.embedding_providers.factory import (
    create_embedding_provider,
)

MODEL = "hash-v1-2048"
DIMS = 2048

# Shared with ClassicalEmbeddingProviderTest.java, which reads the same file from its
# test classpath. Both suites read the same UTF-8 file, so the text is spelled with
# literal code points, escaped only where a code unit cannot be written literally
# (the lone surrogate row).
GOLDEN_FIXTURE = (
    Path(__file__).resolve().parents[6]
    / "metadata-io/src/test/resources/embedding/classical_hash_v1_2048_golden.json"
)


def _load_golden() -> list[tuple[str, str, str]]:
    """Empty when the fixture is not in this checkout (standalone metadata-ingestion
    tree), which skips the golden tests instead of failing collection."""
    if not GOLDEN_FIXTURE.is_file():
        return []
    rows = json.loads(GOLDEN_FIXTURE.read_text(encoding="utf-8"))
    assert rows, "shared golden fixture is empty"
    return [(row["id"], row["text"], row["sha256"]) for row in rows]


# (id, input text, expected digest)
GOLDEN: list[tuple[str, str, str]] = _load_golden()


def _embed(text: str, model: str = MODEL) -> list[float]:
    return ClassicalEmbeddingProvider(model=model).embed([text]).embeddings[0]


def _digest(vector: list[float]) -> str:
    return hashlib.sha256(b"".join(struct.pack(">f", v) for v in vector)).hexdigest()


@pytest.mark.skipif(
    not GOLDEN, reason=f"shared golden fixture not found at {GOLDEN_FIXTURE}"
)
@pytest.mark.parametrize(
    ("text", "expected"), [(t, d) for _, t, d in GOLDEN], ids=[i for i, _, _ in GOLDEN]
)
def test_golden_vector(text: str, expected: str) -> None:
    vector = _embed(text)
    assert len(vector) == DIMS
    assert _embed(text) == vector  # bit-identical on repeat
    assert all(isinstance(v, float) and v == int(v) for v in vector)
    assert any(vector)
    assert _digest(vector) == expected


def test_ascii_case_is_folded() -> None:
    assert _embed("Hello, World!") == _embed("hello, world!")


def test_combining_mark_is_not_normalized() -> None:
    # No NFC/NFKC: precomposed and decomposed forms stay distinct (documented limitation).
    assert _embed("\u00e9") != _embed("e\u0301")


def test_nbsp_is_not_a_separator() -> None:
    assert _embed("a\u00a0b") != _embed("a b")


def test_nul_is_not_a_separator() -> None:
    assert _embed("a\x00b") != _embed("a b")


def test_lone_surrogate_is_replaced_not_raised() -> None:
    assert _embed("\ud800x") == _embed("\ufffdx")


def test_rejects_input_over_max_code_points() -> None:
    assert len(_embed("a" * 16384)) == DIMS
    with pytest.raises(ValueError, match="16384"):
        _embed("a" * 16385)


@pytest.mark.parametrize("text", ["", "   \t\n"])
def test_empty_text_gives_sentinel(text: str) -> None:
    assert _embed(text) == [1.0] + [0.0] * (DIMS - 1)


def test_batch_returns_one_vector_per_input_in_order() -> None:
    provider = ClassicalEmbeddingProvider(model=MODEL)
    texts = ["Hello, World!", "id", "", "user_id customer_id"]
    assert provider.embed(texts).embeddings == [_embed(t) for t in texts]
    assert provider.embed([]).embeddings == []


def test_model_id_and_dimensions_come_from_model_name() -> None:
    provider = ClassicalEmbeddingProvider(model="hash-v1-1024")
    assert provider.model_id == "classical/hash-v1-1024"
    assert len(provider.embed(["id"]).embeddings[0]) == 1024


@pytest.mark.parametrize(
    "model",
    [
        "hash-v2-2048",  # unknown algorithm version
        "hash-v1-0",
        "hash-v1-01",  # leading zero
        "hash-v1-4097",  # above the dimension cap
        "hash-v1-",
        "hash-v1-2048x",
        "HASH-V1-2048",
        "text-embedding-3-small",
    ],
)
def test_bad_model_names_rejected(model: str) -> None:
    with pytest.raises(ValueError):
        ClassicalEmbeddingProvider(model=model)


def test_factory_dispatches_classical() -> None:
    provider = create_embedding_provider(
        EmbeddingConfig(provider="classical", model=MODEL)
    )
    assert isinstance(provider, ClassicalEmbeddingProvider)
    assert provider.model_id == f"classical/{MODEL}"


def test_server_config_resolves_classical() -> None:
    assert EmbeddingConfig._normalize_provider("CLASSICAL") == "classical"
    assert EmbeddingConfig._normalize_provider_from_server("classical") == "classical"

    cfg = EmbeddingConfig.from_server(
        ServerEmbeddingConfig(
            provider="classical", model_id=MODEL, model_embedding_key="hash_v1_2048"
        )
    )
    assert cfg.provider == "classical"
    assert cfg.model == MODEL
    assert cfg.model_embedding_key == "hash_v1_2048"


def test_default_embedding_key_matches_server_derivation() -> None:
    # With no server-supplied key the source derives it from the model name; it must
    # land on the same field the Java side derives ("-" -> "_") or vectors are written
    # under a field the query never reads.
    ctx = MagicMock(spec=PipelineContext)
    ctx.pipeline_name = "test_pipeline"
    source = DocumentChunkingSource(
        ctx=ctx,
        config=DocumentChunkingSourceConfig(
            embedding=EmbeddingConfig(
                provider="classical", model=MODEL, allow_local_embedding_config=True
            ),
            chunking=ChunkingConfig(strategy="basic"),
        ),
        standalone=False,
        graph=None,
    )
    assert source.embedding_model == f"classical/{MODEL}"
    assert source.get_model_embedding_key() == "hash_v1_2048"


def test_embedding_capability_runs_real_provider() -> None:
    # No network, no SDK: the capability check embeds a probe string for real.
    report = DocumentChunkingSource.test_embedding_capability(
        EmbeddingConfig(provider="classical", model=MODEL)
    )
    assert report.capable
    assert "dimension: 2048" in (report.mitigation_message or "")
