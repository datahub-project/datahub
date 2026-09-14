"""Tests for the classical (deterministic hashed-feature) embedding provider.

The GMS query side implements the same algorithm in Java. The golden digests in
``GOLDEN`` are shared with ``ClassicalEmbeddingProviderTest.java``: each is the
lowercase hex SHA-256 over the vector serialized as big-endian IEEE-754 binary32
components. Both suites must hard-code the same values for the same literals, so
a change to either implementation that breaks query/document parity fails here.
"""

import hashlib
import struct
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

# (id, input text, expected digest). Non-ASCII is spelled with escapes so the code
# points are unambiguous regardless of source-file normalization; the human-readable
# form is in the id. Must match ClassicalEmbeddingProviderTest.java literal for literal.
GOLDEN: list[tuple[str, str, str]] = [
    (
        "hello_world",
        "Hello, World!",
        "bec441fc81b6a708326c70ddfd5229c197bc315c450ac4cda1bfeb63826ce9ab",
    ),
    (
        "user_id_customer_id",
        "user_id customer_id",
        "0a803a46b7b0db404eeeb79344d9d56effcb6d81cdbce71a4e488e5978c78ec6",
    ),
    (
        "id",
        "id",
        "91d865d24040d06fb4298bb4d51ecb47a60d919a74c11a97364aeb5ecaf6b2f3",
    ),
    (
        "gruesse_tokyo_data",  # "Grüße 東京 data"
        "Grüße 東京 data",
        "97777de6cfe5a291c9711b1be592479ea6867b80aca515d950a2018835b2dd74",
    ),
    (
        "naive_cafe",  # "naïve café"
        "naïve café",
        "a8291f6e6798ef926025c47b85202c2734e66d025fa22b4bda60428104ae998e",
    ),
    (
        "e_acute_precomposed",  # "é" as U+00E9
        "é",
        "3376ecfc87f89b3d3d673b2ec8ce2645713dbcd47bd73b1c1af41d793c70cfbf",
    ),
    (
        "e_acute_combining",  # "e" + U+0301 combining acute
        "é",
        "26e0cf186ad15cc6a307f215cd94ffd41d2115b7b021cbd21a56be33a8ae4c5e",
    ),
    (
        "nbsp_one_word",  # "a" NBSP "b": NBSP is not a separator
        "a b",
        "6f3c34bb96eb1114f09ab6f8965df17a170225eecd2e1cd9b9636c213300151e",
    ),
    (
        "x_space_y",
        "x y",
        "7e9211ae1d54571d56bcda55c2b4b3595dcc7aa2b6aa4984943e6c12a9f90144",
    ),
    (
        "emoji",  # U+1F600 followed by " emoji"
        "\U0001f600 emoji",
        "c8fd64833ba1b40ec97efb673c00e92218750f1f748da766f32355bc6e10e766",
    ),
    (
        "lone_surrogate",  # unpaired U+D800 followed by "x"
        "\ud800x",
        "3b34bccb1a43951ed6567c13bc809713f7082688395f8504b41537147a59b9bc",
    ),
    (
        "slash_path",
        "a/b path",
        "cf00b52f191ebca9ac7dc94947f568176d943258f60275abbfe4edc9ab628de7",
    ),
    (
        "empty",
        "",
        "4fb362b7ae0cc6e8c1ee2ed26b3245c89937fa655d37f269ff3b1eb40db67033",
    ),
    (
        "whitespace_only",
        "   \t\n",
        "4fb362b7ae0cc6e8c1ee2ed26b3245c89937fa655d37f269ff3b1eb40db67033",
    ),
]


def _embed(text: str, model: str = MODEL) -> list[float]:
    return ClassicalEmbeddingProvider(model=model).embed([text]).embeddings[0]


def _digest(vector: list[float]) -> str:
    return hashlib.sha256(b"".join(struct.pack(">f", v) for v in vector)).hexdigest()


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
    assert _embed("é") != _embed("é")


def test_nbsp_is_not_a_separator() -> None:
    assert _embed("a b") != _embed("a b")


def test_lone_surrogate_is_replaced_not_raised() -> None:
    assert _embed("\ud800x") == _embed("�x")


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
