from types import TracebackType
from typing import List, Optional
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.connectionpool import ConnectionPool
from urllib3.exceptions import MaxRetryError, ResponseError
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.unstructured.chunking_config import (
    EmbeddingConfig,
    ServerEmbeddingConfig,
    get_semantic_search_config,
)
from datahub.ingestion.source.unstructured.embedding_providers.base import (
    DEFAULT_HTTP_TIMEOUT_SECONDS,
    EmbeddingProvider,
    EmbeddingResult,
    EmbeddingTask,
)
from datahub.utilities.server_config_util import ServiceFeature

_NOT_OFFERED = (
    "This DataHub deployment does not offer server embeddings. "
    "It must advertise embeddingService.enabled=true and an endpoint path in /config. "
    "Use an embedding provider with its own credentials instead "
    "(for example bedrock, cohere, openai, or vertex_ai)."
)


class ServerEmbeddingsUnavailableError(ValueError):
    pass


class ServerEmbeddingModelMismatchError(RuntimeError):
    pass


class _EmbeddingRetry(Retry):
    def increment(
        self,
        method: Optional[str] = None,
        url: Optional[str] = None,
        response: Optional[HTTPResponse] = None,
        error: Optional[Exception] = None,
        _pool: Optional[ConnectionPool] = None,
        _stacktrace: Optional[TracebackType] = None,
    ) -> Retry:
        if (
            response is not None
            and response.status == 429
            and response.headers.get("x-should-retry", "").lower() == "false"
        ):
            # With raise_on_status=False, urllib3 returns this response to the
            # caller. Credits cannot recover through retries, unlike rate limits.
            assert _pool is not None and url is not None
            raise MaxRetryError(
                _pool, url, ResponseError("Embedding credits exhausted")
            )
        return super().increment(method, url, response, error, _pool, _stacktrace)


class DataHubServerEmbeddingProvider(EmbeddingProvider):
    def __init__(
        self,
        graph: DataHubGraph,
        *,
        path: str,
        index_config: ServerEmbeddingConfig,
        timeout: float,
    ) -> None:
        self.model_embedding_key = index_config.model_embedding_key
        self.index_config = index_config
        self.model_id = index_config.model_id
        self._url = graph.config.server.rstrip("/") + path
        self._session = graph.session
        self._timeout = timeout
        # Scope the adapter to this endpoint so the graph's other requests keep
        # their existing retry policy and per-request authentication.
        self._session.mount(
            self._url,
            HTTPAdapter(
                max_retries=_EmbeddingRetry(
                    total=3,
                    backoff_factor=0.5,
                    status_forcelist={429, 500, 502, 503, 504},
                    allowed_methods=frozenset({"POST"}),
                    raise_on_status=False,
                )
            ),
        )

    @property
    def semantic_model_version(self) -> str:
        provider = EmbeddingConfig._normalize_provider(self.index_config.provider)
        if provider == "local":
            provider = "openai"
        return f"{provider}/{self.index_config.model_id}"

    def embed(self, texts: List[str], *, task: EmbeddingTask) -> EmbeddingResult:
        if not texts:
            return EmbeddingResult(embeddings=[])
        try:
            response = self._session.post(
                self._url,
                json={
                    "input": texts,
                    # GMS stores provider vectors without normalization.
                    "x_datahub": {"task_type": task.value, "normalize": False},
                },
                timeout=self._timeout,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise RuntimeError(
                "DataHub embeddings request failed. Check server connectivity and retry."
            ) from None
        if response.status_code == 403:
            raise RuntimeError(
                "DataHub server embeddings require Ask DataHub to be enabled. "
                "Ask your administrator to enable it and check your access token."
            )
        if (
            response.status_code == 429
            and response.headers.get("x-should-retry", "").lower() == "false"
        ):
            raise RuntimeError(
                "DataHub embedding credits are exhausted. Add credits or contact "
                "your administrator before retrying."
            )
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if response.status_code == 404 and isinstance(payload, dict):
            error = payload.get("error", payload)
            if isinstance(error, dict) and error.get("type") == "not_enabled":
                raise ServerEmbeddingsUnavailableError(_NOT_OFFERED)
        if response.status_code != 200:
            raise RuntimeError(
                f"DataHub embeddings request failed (HTTP {response.status_code}). "
                "Check server availability and your DataHub access token."
            )
        if not isinstance(payload, dict):
            raise RuntimeError("DataHub embeddings returned an invalid JSON response.")
        model = payload.get("model")
        if not isinstance(model, str) or not model:
            raise RuntimeError("DataHub embeddings response is missing its model.")
        self.model_id = model
        if model != self.index_config.model_id:
            raise ServerEmbeddingModelMismatchError(
                "DataHub embeddings model differs from the semantic index model "
                "(appConfig.semanticSearchConfig.embeddingConfig.modelId). "
                "Ask your administrator to align the models before embedding."
            )
        data = payload.get("data")
        if (
            not isinstance(data, list)
            or len(data) != len(texts)
            or any(
                not isinstance(item, dict)
                or type(item.get("index")) is not int
                or not isinstance(item.get("embedding"), list)
                or not item["embedding"]
                or any(type(value) not in (int, float) for value in item["embedding"])
                for item in data
            )
            or sorted(item["index"] for item in data) != list(range(len(texts)))
        ):
            raise RuntimeError(
                "DataHub embeddings response has invalid vectors or indexes."
            )
        ordered = sorted(data, key=lambda item: item["index"])
        return EmbeddingResult(embeddings=[item["embedding"] for item in ordered])


def server_embedding_provider(
    graph: DataHubGraph,
    *,
    timeout: float = DEFAULT_HTTP_TIMEOUT_SECONDS,
) -> DataHubServerEmbeddingProvider:
    config = graph.server_config
    if not config.supports_feature(ServiceFeature.EMBEDDING_SERVICE):
        raise ServerEmbeddingsUnavailableError(_NOT_OFFERED)
    path = config.embedding_service_path
    assert path is not None
    parsed_path = urlsplit(path)
    if (
        parsed_path.netloc
        or parsed_path.scheme
        or parsed_path.query
        or parsed_path.fragment
    ):
        raise ServerEmbeddingsUnavailableError(
            "DataHub /config must advertise an embeddingService.path on this server."
        )
    semantic_config = get_semantic_search_config(graph)
    if not semantic_config.enabled or semantic_config.embedding_config is None:
        raise ValueError(
            "Server embeddings require semantic search and its index model to be "
            "configured. Ask your administrator to enable semantic search."
        )
    return DataHubServerEmbeddingProvider(
        graph,
        path=path,
        index_config=semantic_config.embedding_config,
        timeout=timeout,
    )
