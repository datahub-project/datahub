"""Tests for the in-process ONNX document embedding provider and its wiring.

The parity-critical bits (model-file resolution order and pooling) are tested
directly; the heavy onnxruntime/tokenizers path is exercised only through the
config/factory wiring, which does not require those deps to be installed.
"""

import sys
from types import SimpleNamespace

import pytest

from datahub.ingestion.source.unstructured.chunking_config import (
    EmbeddingConfig,
    ServerEmbeddingConfig,
)
from datahub.ingestion.source.unstructured.chunking_source import DocumentChunkingSource
from datahub.ingestion.source.unstructured.embedding_providers.factory import (
    create_embedding_provider,
)
from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
    _resolve_model_file,
)


def test_resolve_model_file_prefers_quantized(tmp_path):
    # Must match the Java provider's order: quantized wins when both exist, so
    # the doc side and query side load the same weights.
    (tmp_path / "model.onnx").write_bytes(b"x")
    (tmp_path / "model_quantized.onnx").write_bytes(b"x")
    assert _resolve_model_file(tmp_path).name == "model_quantized.onnx"


def test_resolve_model_file_falls_back_to_standard(tmp_path):
    (tmp_path / "model.onnx").write_bytes(b"x")
    assert _resolve_model_file(tmp_path).name == "model.onnx"


def test_resolve_model_file_missing_raises(tmp_path):
    with pytest.raises(ValueError, match="No model.onnx"):
        _resolve_model_file(tmp_path)


def test_config_accepts_onnx_provider():
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    assert cfg.provider == "onnx"
    assert cfg.onnx_pooling == "cls"  # default matches the Java provider default


def test_normalize_provider_onnx():
    assert EmbeddingConfig._normalize_provider("onnx") == "onnx"
    assert EmbeddingConfig._normalize_provider_from_server("ONNX") == "onnx"


def test_from_server_reads_model_dir_env(monkeypatch):
    monkeypatch.setenv("ONNX_EMBEDDING_MODEL_DIR", "/models/arctic")
    server = ServerEmbeddingConfig(
        provider="onnx",
        model_id="snowflake_arctic_embed_s",
        model_embedding_key="snowflake_arctic_embed_s",
    )
    cfg = EmbeddingConfig.from_server(server)
    assert cfg.provider == "onnx"
    assert cfg.onnx_model_dir == "/models/arctic"
    assert cfg.model == "snowflake_arctic_embed_s"


def test_factory_onnx_requires_model_dir(monkeypatch):
    monkeypatch.delenv("ONNX_EMBEDDING_MODEL_DIR", raising=False)
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    with pytest.raises(ValueError, match="ONNX_EMBEDDING_MODEL_DIR"):
        create_embedding_provider(cfg)


def test_validate_provider_config_onnx(monkeypatch):
    monkeypatch.setenv("ONNX_EMBEDDING_MODEL_DIR", "/models/arctic")
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    model_id, err = DocumentChunkingSource._validate_provider_config(cfg)
    assert err is None
    assert model_id == "onnx/snowflake_arctic_embed_s"


def test_validate_provider_config_onnx_missing_dir(monkeypatch):
    monkeypatch.delenv("ONNX_EMBEDDING_MODEL_DIR", raising=False)
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    _, err = DocumentChunkingSource._validate_provider_config(cfg)
    assert err is not None and not err.capable


def test_cls_pool():
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import _cls_pool

    # [batch=1, seq=3, hidden=2]; CLS = first token.
    hidden = np.array([[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]]])
    assert _cls_pool(hidden).tolist() == [[1.0, 2.0]]


def test_mean_pool_excludes_padding():
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        _mean_pool,
    )

    # Third token is padding (mask 0) → mean of the first two only.
    hidden = np.array([[[1.0, 1.0], [3.0, 3.0], [999.0, 999.0]]])
    mask = np.array([[1, 1, 0]])
    assert _mean_pool(hidden, mask).tolist() == [[2.0, 2.0]]


def test_from_server_reads_pooling_env(monkeypatch):
    # GMS pooling must propagate to server-derived doc-side config or the two
    # sides embed with different pooling and kNN parity breaks.
    monkeypatch.setenv("ONNX_EMBEDDING_MODEL_DIR", "/models/arctic")
    monkeypatch.setenv("ONNX_EMBEDDING_POOLING", "mean")
    server = ServerEmbeddingConfig(
        provider="onnx",
        model_id="snowflake_arctic_embed_s",
        model_embedding_key="snowflake_arctic_embed_s",
    )
    cfg = EmbeddingConfig.from_server(server)
    assert cfg.onnx_pooling == "mean"


def test_init_requirements_onnx_missing_model_dir(monkeypatch):
    monkeypatch.delenv("ONNX_EMBEDDING_MODEL_DIR", raising=False)
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    with pytest.raises(ValueError, match="onnx_model_dir"):
        DocumentChunkingSource._validate_provider_init_requirements(cfg)


def test_init_requirements_onnx_env_satisfies(monkeypatch):
    monkeypatch.setenv("ONNX_EMBEDDING_MODEL_DIR", "/models/arctic")
    cfg = EmbeddingConfig(provider="onnx", model="snowflake_arctic_embed_s")
    DocumentChunkingSource._validate_provider_init_requirements(cfg)  # no raise


# --- OnnxEmbeddingProvider with stubbed onnxruntime/tokenizers -----------------
#
# These exercise OUR logic — feed assembly, token_type_ids gating, pooling
# dispatch — with fake session/tokenizer objects (real numpy). The real
# onnxruntime inference path is covered by the quickstart/smoke environment.


class _FakeEncoding:
    def __init__(self, ids, attention_mask, type_ids):
        self.ids = ids
        self.attention_mask = attention_mask
        self.type_ids = type_ids


class _FakeTokenizer:
    def __init__(self):
        self.truncation = None
        self.padding = False

    @classmethod
    def from_file(cls, path):
        return cls()

    def enable_truncation(self, max_length):
        self.truncation = max_length

    def enable_padding(self):
        self.padding = True

    def encode_batch(self, texts):
        # 3 tokens per text; every second text has one padding token (mask 0).
        return [
            _FakeEncoding(
                [101, 5 + i, 102], [1, 1, 0] if i % 2 else [1, 1, 1], [0, 0, 0]
            )
            for i in range(len(texts))
        ]


def _stub_onnx_modules(
    monkeypatch, output, input_names=("input_ids", "attention_mask")
):
    """Install fake onnxruntime/tokenizers modules; returns dict capturing session calls."""
    captured = {}

    class _FakeSession:
        def __init__(self, path, providers=None):
            captured["path"] = path
            captured["providers"] = providers

        def get_inputs(self):
            return [SimpleNamespace(name=n) for n in input_names]

        def run(self, _outputs, feeds):
            captured["feeds"] = feeds
            return [output]

    monkeypatch.setitem(
        sys.modules, "onnxruntime", SimpleNamespace(InferenceSession=_FakeSession)
    )
    monkeypatch.setitem(
        sys.modules, "tokenizers", SimpleNamespace(Tokenizer=_FakeTokenizer)
    )
    return captured


def _model_dir(tmp_path, tokenizer=True):
    (tmp_path / "model.onnx").write_bytes(b"x")
    if tokenizer:
        (tmp_path / "tokenizer.json").write_text("{}")
    return tmp_path


def test_provider_embed_cls_3d_with_token_type_ids(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    # [batch=2, seq=3, hidden=2]; CLS = first-token vector, no L2 normalization.
    out = np.array(
        [[[1.0, 2.0], [3.0, 4.0], [5.0, 6.0]], [[7.0, 8.0], [9.0, 10.0], [99.0, 99.0]]],
        dtype=np.float32,
    )
    cap = _stub_onnx_modules(
        monkeypatch, out, input_names=("input_ids", "attention_mask", "token_type_ids")
    )
    provider = OnnxEmbeddingProvider(model="m", model_dir=str(_model_dir(tmp_path)))
    assert provider.model_id == "onnx/m"
    result = provider.embed(["a", "b"])
    assert result.embeddings == [[1.0, 2.0], [7.0, 8.0]]
    assert set(cap["feeds"]) == {"input_ids", "attention_mask", "token_type_ids"}
    assert cap["feeds"]["input_ids"].dtype == np.int64


def test_provider_embed_omits_undeclared_token_type_ids(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    out = np.zeros((1, 3, 2), dtype=np.float32)
    cap = _stub_onnx_modules(monkeypatch, out)  # model declares no token_type_ids
    provider = OnnxEmbeddingProvider(model="m", model_dir=str(_model_dir(tmp_path)))
    provider.embed(["a"])
    assert "token_type_ids" not in cap["feeds"]


def test_provider_embed_mean_3d_excludes_padding(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    # Second text has mask [1,1,0] → padded third token excluded from the mean.
    out = np.array(
        [
            [[2.0, 2.0], [4.0, 4.0], [6.0, 6.0]],
            [[1.0, 1.0], [3.0, 3.0], [999.0, 999.0]],
        ],
        dtype=np.float32,
    )
    _stub_onnx_modules(monkeypatch, out)
    provider = OnnxEmbeddingProvider(
        model="m", model_dir=str(_model_dir(tmp_path)), pooling="mean"
    )
    assert provider.embed(["a", "b"]).embeddings == [[4.0, 4.0], [2.0, 2.0]]


def test_provider_embed_2d_output_used_as_is(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    out = np.array([[0.5, -0.5]], dtype=np.float32)  # model already pooled
    _stub_onnx_modules(monkeypatch, out)
    provider = OnnxEmbeddingProvider(model="m", model_dir=str(_model_dir(tmp_path)))
    assert provider.embed(["a"]).embeddings == [[0.5, -0.5]]


def test_provider_embed_empty_short_circuits(tmp_path, monkeypatch):
    pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    cap = _stub_onnx_modules(monkeypatch, None)
    provider = OnnxEmbeddingProvider(model="m", model_dir=str(_model_dir(tmp_path)))
    assert provider.embed([]).embeddings == []
    assert "feeds" not in cap  # session.run never invoked


def test_provider_pool_unexpected_rank_raises(tmp_path, monkeypatch):
    np = pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    _stub_onnx_modules(monkeypatch, None)
    provider = OnnxEmbeddingProvider(model="m", model_dir=str(_model_dir(tmp_path)))
    with pytest.raises(RuntimeError, match="rank"):
        provider._pool(np.zeros((2,)), np.ones((1, 2)))


def test_provider_init_invalid_pooling_raises(tmp_path, monkeypatch):
    pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    _stub_onnx_modules(monkeypatch, None)
    with pytest.raises(ValueError, match="pooling"):
        OnnxEmbeddingProvider(
            model="m", model_dir=str(_model_dir(tmp_path)), pooling="max"
        )


def test_provider_init_missing_tokenizer_raises(tmp_path, monkeypatch):
    pytest.importorskip("numpy")
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    _stub_onnx_modules(monkeypatch, None)
    with pytest.raises(ValueError, match="tokenizer.json"):
        OnnxEmbeddingProvider(
            model="m", model_dir=str(_model_dir(tmp_path, tokenizer=False))
        )


def test_provider_init_missing_deps_friendly_error(tmp_path, monkeypatch):
    from datahub.ingestion.source.unstructured.embedding_providers.onnx import (
        OnnxEmbeddingProvider,
    )

    # None in sys.modules makes `import onnxruntime` raise ImportError.
    monkeypatch.setitem(sys.modules, "onnxruntime", None)
    with pytest.raises(ImportError, match="onnx-embeddings"):
        OnnxEmbeddingProvider(model="m", model_dir=str(tmp_path))
