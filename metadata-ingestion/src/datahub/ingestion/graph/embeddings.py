from dataclasses import dataclass
from types import TracebackType
from typing import List, Optional

import requests
from urllib3.connectionpool import ConnectionPool
from urllib3.exceptions import MaxRetryError, ResponseError
from urllib3.response import HTTPResponse
from urllib3.util.retry import Retry

from datahub.cli.cli_utils import fixup_gms_url
from datahub.ingestion.graph.client import DataHubGraph

_NOT_OFFERED = (
    "This DataHub deployment does not offer server embeddings. "
    "It must advertise embeddingService.enabled=true and an endpoint path in /config. "
    "Use an embedding provider with its own credentials instead "
    "(for example bedrock, cohere, openai, or vertex_ai)."
)


class ServerEmbeddingsUnavailableError(ValueError):
    pass


class ServerEmbeddingRunError(RuntimeError):
    """A server-wide failure that cannot recover by trying another document."""


@dataclass(frozen=True)
class DataHubEmbeddingsResponse:
    model: str
    embeddings: List[List[float]]


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


class DataHubEmbeddingsClient:
    def __init__(self, graph: DataHubGraph, *, path: str, timeout: float) -> None:
        self._url = fixup_gms_url(graph.config.server).rstrip("/") + path
        self._timeout = timeout
        self._session = graph.create_session(
            retry=_EmbeddingRetry(
                total=3,
                backoff_factor=0.5,
                status_forcelist={429, 500, 502, 503, 504},
                allowed_methods=frozenset({"POST"}),
                raise_on_status=False,
            )
        )

    def close(self) -> None:
        self._session.close()

    def embed(self, texts: List[str], *, task_type: str) -> DataHubEmbeddingsResponse:
        try:
            response = self._session.post(
                self._url,
                json={
                    "input": texts,
                    # GMS stores provider vectors without normalization.
                    "x_datahub": {"task_type": task_type, "normalize": False},
                },
                timeout=self._timeout,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise RuntimeError(
                "DataHub embeddings request failed. Check server connectivity and retry."
            ) from None
        if response.status_code == 403:
            raise ServerEmbeddingRunError(
                "DataHub server embeddings require Ask DataHub to be enabled. "
                "Ask your administrator to enable it and check your access token."
            )
        if (
            response.status_code == 429
            and response.headers.get("x-should-retry", "").lower() == "false"
        ):
            raise ServerEmbeddingRunError(
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
                # Discovery passed, so the service was turned off mid-run: no
                # later document can succeed either.
                raise ServerEmbeddingRunError(_NOT_OFFERED)
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
        return DataHubEmbeddingsResponse(
            model=model, embeddings=[item["embedding"] for item in ordered]
        )
