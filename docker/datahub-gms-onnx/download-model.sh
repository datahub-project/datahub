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

echo "  -> model.onnx"
curl -fL --progress-bar "${HF_BASE}/${ONNX_PATH}" -o "${OUT_DIR}/model.onnx"

echo "  -> tokenizer.json"
curl -fL --progress-bar "${HF_BASE}/tokenizer.json" -o "${OUT_DIR}/tokenizer.json"

# Verify downloads are not empty / error pages
for f in "${OUT_DIR}/model.onnx" "${OUT_DIR}/tokenizer.json"; do
  if [ ! -s "$f" ]; then
    echo "Error: Downloaded file is empty: $f"
    exit 1
  fi
done

echo ""
echo "Downloaded to: ${OUT_DIR}"
echo "Model dimensions: ${DIMS}"
echo ""
echo "To build the Docker image:"
echo "  cd ${SCRIPT_DIR}"
echo "  docker build --build-arg ONNX_MODEL_NAME=${MODEL_NAME} \\"
echo "               --build-arg ONNX_VECTOR_DIMENSION=${DIMS} \\"
echo "               -t datahub-gms-onnx ."
