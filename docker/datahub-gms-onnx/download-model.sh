#!/usr/bin/env bash
# Downloads an ONNX embedding model from HuggingFace into models/<model_name>/.
#
# Usage: ./download-model.sh <model_name>
#
# Supported models:
#   snowflake_arctic_embed_s   — 33M params, ~130 MB, 384 dims (default, recommended)
#   snowflake_arctic_embed_l   — 137M params, ~550 MB, 1024 dims
#   bge_base_en_v1_5           — 109M params, ~438 MB, 768 dims

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODELS_DIR="${SCRIPT_DIR}/models"

MODEL_NAME="${1:-}"

if [ -z "$MODEL_NAME" ]; then
  echo "Usage: $0 <model_name>"
  echo ""
  echo "Supported models:"
  echo "  snowflake_arctic_embed_s (384 dims, recommended)"
  echo "  snowflake_arctic_embed_l (1024 dims)"
  echo "  bge_base_en_v1_5 (768 dims)"
  exit 1
fi

case "$MODEL_NAME" in
  snowflake_arctic_embed_s)
    REPO="Snowflake/snowflake-arctic-embed-s"
    DIMS="384"
    ONNX_PATH="onnx/model.onnx"
    ;;
  snowflake_arctic_embed_l)
    REPO="Snowflake/snowflake-arctic-embed-l"
    DIMS="1024"
    ONNX_PATH="onnx/model.onnx"
    ;;
  bge_base_en_v1_5)
    REPO="BAAI/bge-base-en-v1.5"
    DIMS="768"
    ONNX_PATH="onnx/model.onnx"
    ;;
  *)
    echo "Error: Unknown model '${MODEL_NAME}'"
    echo "Supported: snowflake_arctic_embed_s, snowflake_arctic_embed_l, bge_base_en_v1_5"
    exit 1
    ;;
esac

OUT_DIR="${MODELS_DIR}/${MODEL_NAME}"

mkdir -p "${OUT_DIR}"

HF_BASE="https://huggingface.co/${REPO}/resolve/main"

echo "Downloading ${MODEL_NAME} (${DIMS} dims) from ${REPO}..."

# Download to a temp file and rename atomically: an interrupted curl must not
# leave a partial file at the final path, or later runs would see it as done.
download() {
  local url="$1" dest="$2" tmp="$2.tmp"
  curl -fL --progress-bar "$url" -o "$tmp"
  if [ ! -s "$tmp" ]; then
    echo "Error: Downloaded file is empty: $dest"
    rm -f "$tmp"
    exit 1
  fi
  mv "$tmp" "$dest"
}

echo "  -> model.onnx"
download "${HF_BASE}/${ONNX_PATH}" "${OUT_DIR}/model.onnx"

echo "  -> tokenizer.json"
download "${HF_BASE}/tokenizer.json" "${OUT_DIR}/tokenizer.json"

echo ""
echo "Downloaded to: ${OUT_DIR}"
echo "Model dimensions: ${DIMS}"
echo ""
echo "To enable ONNX semantic search, set these environment variables:"
echo "  EMBEDDING_PROVIDER_TYPE=onnx"
echo "  ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true"
echo "  SEARCH_SERVICE_SEMANTIC_SEARCH_ENABLED=true"
echo "  ONNX_EMBEDDING_MODEL_NAME=${MODEL_NAME}"
echo "  ONNX_EMBEDDING_MODEL_DIR=/datahub/models/${MODEL_NAME}"
# All supported models are CLS-pooled asymmetric-retrieval models: queries must
# carry the instruction prefix or query/doc vectors diverge and recall degrades.
echo "  ONNX_EMBEDDING_POOLING=cls"
echo "  ONNX_EMBEDDING_QUERY_INSTRUCTION=\"Represent this sentence for searching relevant passages: \""
echo ""
echo "Or use the quickstart target:"
echo "  ./gradlew quickstartDebugBuiltinEmbedding"
