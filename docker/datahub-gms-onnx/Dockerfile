# DataHub GMS with baked-in ONNX embedding model for in-process semantic search.
#
# Build:
#   1. Download a model:  ./download-model.sh snowflake_arctic_embed_s
#   2. Build the image:   docker build -t datahub-gms-onnx .
#
# Override model at build time:
#   docker build --build-arg ONNX_MODEL_NAME=bge_base_en_v1_5 \
#                --build-arg ONNX_VECTOR_DIMENSION=768 \
#                -t datahub-gms-onnx .

ARG BASE_IMAGE=acryldata/datahub-gms:head
FROM ${BASE_IMAGE}

ARG ONNX_MODEL_NAME=snowflake_arctic_embed_s
ARG ONNX_VECTOR_DIMENSION=384

# Model files must be pre-downloaded into models/<model_name>/ before building.
# Use download-model.sh to fetch supported models.
COPY models/${ONNX_MODEL_NAME} /datahub/models/${ONNX_MODEL_NAME}

# Configure ONNX embedding provider
ENV ELASTICSEARCH_SEMANTIC_SEARCH_ENABLED=true
ENV EMBEDDING_PROVIDER_TYPE=onnx
ENV ONNX_EMBEDDING_MODEL_NAME=${ONNX_MODEL_NAME}
ENV ONNX_EMBEDDING_MODEL_DIR=/datahub/models/${ONNX_MODEL_NAME}

# The default model (snowflake_arctic_embed_s) is registered in application.yaml with a matching
# env var placeholder. This env var provides the value, though 384 is also the YAML default.
# If using a different model, add a corresponding entry to application.yaml or override at runtime.
ENV ELASTICSEARCH_ENTITY_INDEX_SEMANTIC_SEARCH_MODELS_SNOWFLAKE_ARCTIC_EMBED_S_VECTORDIMENSION=384

# ONNX model loading adds 10-60s to startup; increase health check grace period
HEALTHCHECK --start-period=4m --retries=4 CMD curl --fail http://localhost:8080/${DATAHUB_GMS_BASE_PATH}health || exit 1
