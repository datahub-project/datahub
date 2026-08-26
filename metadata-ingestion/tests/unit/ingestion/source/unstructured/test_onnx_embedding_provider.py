"""Tests for the in-process ONNX document embedding provider and its wiring.

The parity-critical bits (model-file resolution order and pooling) are tested
directly; the heavy onnxruntime/tokenizers path is exercised only through the
config/factory wiring, which does not require those deps to be installed.
"""

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
