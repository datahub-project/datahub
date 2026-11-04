import re
from typing import Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.graph.embeddings import (
    _NOT_OFFERED,
    ServerEmbeddingRunError,
    ServerEmbeddingsUnavailableError,
)
from datahub.ingestion.source.unstructured.chunking_config import (
    EmbeddingConfig,
    ResolvedEmbeddingTarget,
    get_semantic_search_config,
)
from datahub.utilities.server_config_util import ServiceFeature


class ServerEmbeddingModelMismatchError(ServerEmbeddingRunError):
    pass


def model_storage_key(model: str, override: Optional[str] = None) -> str:
    if override:
        return override
    if "embed-english-v3" in model:
        return "cohere_embed_v3"
    return re.sub(r"[^a-zA-Z0-9_]", "_", model)


def resolve_embedding_target(
    config: EmbeddingConfig, graph: Optional[DataHubGraph] = None
) -> ResolvedEmbeddingTarget:
    """Read canonical metadata without constructing a provider or changing transport.

    Resolved configs retain the frozen target for the run. Configuration changes
    require explicit resolution of a new config rather than implicit rediscovery.
    """
    if config._resolved_target is not None:
        return config._resolved_target
    provider: Optional[str] = config.provider
    model = config.model
    key = config.model_embedding_key
    path = None
    pooling = (config.onnx_pooling or "cls").lower() if provider == "onnx" else None
    if provider == "datahub":
        if graph is None:
            raise ValueError(
                "The datahub embedding provider requires a DataHubGraph connection."
            )
        service = graph.server_config
        if not service.supports_feature(ServiceFeature.EMBEDDING_SERVICE):
            raise ServerEmbeddingsUnavailableError(_NOT_OFFERED)
        path = service.embedding_service_path
        semantic = get_semantic_search_config(graph)
        if not semantic.enabled or semantic.embedding_config is None:
            raise ValueError(
                "Server embeddings require semantic search and its index model to be "
                "configured. Ask your administrator to enable semantic search."
            )
        index = semantic.embedding_config
        served = service.embedding_service_model
        if served is not None and served != index.model_id:
            raise ServerEmbeddingModelMismatchError(
                f"The DataHub embeddings endpoint serves {served!r}, but the semantic "
                f"index uses {index.model_id!r}. Ask your administrator to align the "
                "models before embedding."
            )
        if (model is not None and model != index.model_id) or (
            key is not None and key != index.model_embedding_key
        ):
            raise ValueError(
                f"embedding.model {model!r} or embedding.model_embedding_key {key!r} "
                f"differs from the DataHub semantic index model {index.model_id!r} "
                f"and key {index.model_embedding_key!r}. Remove the local overrides "
                "or align those fields with the server."
            )
        # Same normalization as the direct path, which rejects unknown providers.
        provider = EmbeddingConfig._normalize_provider_from_server(index.provider)
        model = index.model_id
        key = index.model_embedding_key
    if provider is None or model is None:
        raise ValueError("EmbeddingConfig.provider and .model must be set")
    semantic_provider = "openai" if provider == "local" else provider
    return ResolvedEmbeddingTarget(
        transport="datahub" if config.provider == "datahub" else "direct",
        model_provider=provider,
        model_id=model,
        model_embedding_key=model_storage_key(model, key),
        semantic_model_version=f"{semantic_provider}/{model}",
        endpoint_path=path,
        onnx_pooling=pooling,
    )
