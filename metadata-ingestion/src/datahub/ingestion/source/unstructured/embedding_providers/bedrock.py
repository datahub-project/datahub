"""AWS Bedrock embedding provider.

Uses boto3 because SigV4 + the AWS credential chain (IRSA, AssumeRole, IMDSv2)
is the part where rolling our own would actually hurt.
"""

import json
from typing import Optional

import boto3

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
    EmbeddingTask,
)

# Bedrock model IDs are namespaced by the upstream provider, e.g.
# "cohere.embed-english-v3" or "amazon.titan-embed-text-v2:0".
_COHERE_PREFIX = "cohere."
_TITAN_PREFIX = "amazon.titan-embed"
# GMS truncates a query to this many characters before embedding it.
_QUERY_MAX_CHARS = 2048


def _java_prefix(text: str, length: int) -> str:
    """`text.substring(0, length)` as Java computes it, counting UTF-16 code units.

    Characters outside the Basic Multilingual Plane count twice, and a surrogate
    pair split at the boundary keeps its first half, so the query embedded here
    matches the one GMS embeds.
    """
    units = text.encode("utf-16-le", "surrogatepass")
    return units[: 2 * length].decode("utf-16-le", "surrogatepass")


class BedrockEmbeddingProvider(EmbeddingProvider):
    """Embedding via AWS Bedrock Runtime (boto3)."""

    def __init__(self, model: str, aws_region: Optional[str]):
        self._model = model
        self.model_id = f"bedrock/{model}"
        self._client = boto3.client("bedrock-runtime", region_name=aws_region)

    def embed(self, texts: list[str], *, task: EmbeddingTask) -> EmbeddingResult:
        # Different Bedrock-hosted model families use different request/response
        # shapes. Match by the official Bedrock model-id prefix rather than a
        # substring search so unrelated models don't get mis-formatted.
        if self._model.startswith(_COHERE_PREFIX):
            body = {
                "texts": [_java_prefix(text, _QUERY_MAX_CHARS) for text in texts]
                if task is EmbeddingTask.QUERY
                else texts,
                "input_type": "search_query"
                if task is EmbeddingTask.QUERY
                else "search_document",
            }
            if task is EmbeddingTask.QUERY:
                body["truncate"] = "END"
            response = self._client.invoke_model(
                modelId=self._model,
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json",
            )
            payload = json.loads(response["body"].read())
            return EmbeddingResult(embeddings=payload["embeddings"])

        if self._model.startswith(_TITAN_PREFIX):
            # Titan embedding models accept a single input per call.
            embeddings: list[list[float]] = []
            for text in texts:
                response = self._client.invoke_model(
                    modelId=self._model,
                    body=json.dumps({"inputText": text}),
                    accept="application/json",
                    contentType="application/json",
                )
                payload = json.loads(response["body"].read())
                embeddings.append(payload["embedding"])
            return EmbeddingResult(embeddings=embeddings)

        raise ValueError(
            f"Unsupported Bedrock embedding model family: {self._model!r}. "
            f"Supported prefixes: {_COHERE_PREFIX!r}, {_TITAN_PREFIX!r}."
        )
