"""In-process ONNX embedding provider for document-side embedding.

This mirrors the GMS Java ``OnnxEmbeddingProvider`` (query side) so that
document vectors written by the ingestion job match the query vectors GMS
produces at search time. kNN only returns hits when both sides embed with the
*same* model under the *same* pre/post-processing, so the parity contract here
is load-bearing:

- Same ``model.onnx`` (or ``model_quantized.onnx``) and same ``tokenizer.json``,
  resolved in the same order the Java provider uses.
- CLS pooling by default (first-token vector); attention-masked mean available.
- **No L2 normalization** — the Java provider returns the raw pooled vector.

Heavy dependencies (onnxruntime, tokenizers, numpy) are imported lazily in the
constructor so importing this module stays cheap when the provider is unused.
"""

import logging
from pathlib import Path
from typing import TYPE_CHECKING

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
)

if TYPE_CHECKING:
    import numpy as np

logger = logging.getLogger(__name__)

# Default max sequence length. Chunks are bounded well below this by the chunking
# config, so truncation effectively never fires; it exists only to keep an
# oversized input from exceeding the model's positional limit.
_DEFAULT_MAX_LENGTH = 512


def _resolve_model_file(model_dir: Path) -> Path:
    """Pick the model file, matching the Java provider's resolution order.

    The Java side prefers ``model_quantized.onnx`` over ``model.onnx``; we must
    make the identical choice or the two sides would embed with different
    weights when both files are present.
    """
    quantized = model_dir / "model_quantized.onnx"
    if quantized.is_file():
        return quantized
    standard = model_dir / "model.onnx"
    if standard.is_file():
        return standard
    raise ValueError(f"No model.onnx or model_quantized.onnx found in: {model_dir}")


def _cls_pool(last_hidden_state: "np.ndarray") -> "np.ndarray":
    """CLS pooling: the first-token vector for each sequence. Shape [batch, hidden]."""
    return last_hidden_state[:, 0, :]


def _mean_pool(
    last_hidden_state: "np.ndarray", attention_mask: "np.ndarray"
) -> "np.ndarray":
    """Attention-masked mean pooling. Shape [batch, hidden]."""
    import numpy as np

    mask = attention_mask[:, :, None].astype(np.float32)
    summed = (last_hidden_state * mask).sum(axis=1)
    counts = np.clip(mask.sum(axis=1), a_min=1e-9, a_max=None)
    return summed / counts


class OnnxEmbeddingProvider(EmbeddingProvider):
    """Embed text with a local ONNX model, matching the GMS query-side provider."""

    def __init__(
        self,
        model: str,
        model_dir: str,
        pooling: str = "cls",
        max_length: int = _DEFAULT_MAX_LENGTH,
    ):
        try:
            import numpy as np  # noqa: F401
            import onnxruntime as ort
            from tokenizers import Tokenizer
        except ImportError as e:
            raise ImportError(
                "The onnx embedding provider requires extra dependencies. "
                "Install them with: pip install 'acryl-datahub[onnx-embeddings]'"
            ) from e

        pooling_normalized = (pooling or "cls").lower()
        if pooling_normalized not in ("cls", "mean"):
            raise ValueError(
                f"Unsupported ONNX pooling strategy '{pooling}'. Use 'cls' or 'mean'."
            )
        self._pooling = pooling_normalized
        self.model_id = f"onnx/{model}"

        resolved_dir = Path(model_dir)
        model_file = _resolve_model_file(resolved_dir)
        tokenizer_file = resolved_dir / "tokenizer.json"
        if not tokenizer_file.is_file():
            raise ValueError(f"No tokenizer.json found in: {resolved_dir}")

        self._session = ort.InferenceSession(
            str(model_file), providers=["CPUExecutionProvider"]
        )
        # Only feed inputs the model actually declares (some models omit
        # token_type_ids), mirroring the Java provider's input map.
        self._input_names = {i.name for i in self._session.get_inputs()}

        self._tokenizer = Tokenizer.from_file(str(tokenizer_file))
        self._tokenizer.enable_truncation(max_length=max_length)
        # Padding lets a batch form a rectangular tensor. Padding is on the right,
        # so it never shifts token 0 — CLS pooling is unaffected — and mean
        # pooling excludes pad tokens via the attention mask.
        self._tokenizer.enable_padding()

        logger.info(
            "Initialized OnnxEmbeddingProvider: model=%s, file=%s, pooling=%s, inputs=%s",
            model,
            model_file.name,
            self._pooling,
            sorted(self._input_names),
        )

    def embed(self, texts: list[str]) -> EmbeddingResult:
        import numpy as np

        if not texts:
            return EmbeddingResult(embeddings=[])

        encodings = self._tokenizer.encode_batch(texts)
        input_ids = np.array([e.ids for e in encodings], dtype=np.int64)
        attention_mask = np.array([e.attention_mask for e in encodings], dtype=np.int64)

        feeds = {"input_ids": input_ids, "attention_mask": attention_mask}
        if "token_type_ids" in self._input_names:
            feeds["token_type_ids"] = np.array(
                [e.type_ids for e in encodings], dtype=np.int64
            )

        outputs = self._session.run(None, feeds)
        pooled = self._pool(outputs[0], attention_mask)
        return EmbeddingResult(embeddings=pooled.tolist())

    def _pool(
        self, raw_output: "np.ndarray", attention_mask: "np.ndarray"
    ) -> "np.ndarray":
        # Some models emit [batch, hidden] (already pooled); use as-is. Others
        # emit [batch, seq, hidden] (last_hidden_state); pool over the tokens.
        if raw_output.ndim == 2:
            return raw_output
        if raw_output.ndim == 3:
            return (
                _mean_pool(raw_output, attention_mask)
                if self._pooling == "mean"
                else _cls_pool(raw_output)
            )
        raise RuntimeError(
            f"Unexpected ONNX output rank {raw_output.ndim}; expected 2 or 3."
        )
