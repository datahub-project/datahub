"""AWS Bedrock embedding provider.

Uses boto3 because SigV4 + the AWS credential chain (IRSA, AssumeRole, IMDSv2)
is the part where rolling our own would actually hurt.
"""

from __future__ import annotations

import json
from typing import Optional

from datahub.ingestion.source.unstructured.embedding_providers.base import (
    EmbeddingProvider,
    EmbeddingResult,
)


class BedrockEmbeddingProvider(EmbeddingProvider):
    """Embedding via AWS Bedrock Runtime (boto3)."""

    def __init__(self, model: str, aws_region: Optional[str]):
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for the bedrock embedding provider. "
                "Install with: pip install 'acryl-datahub[unstructured]'"
            ) from e

        self._model = model
        self.model_id = f"bedrock/{model}"
        self._client = boto3.client("bedrock-runtime", region_name=aws_region)

    def embed(self, texts: list[str]) -> EmbeddingResult:
        # Different Bedrock-hosted models have different request/response shapes.
        # We support the two families we ship defaults for: Cohere and Amazon Titan.
        model_lower = self._model.lower()

        if "cohere" in model_lower:
            body = {"texts": texts, "input_type": "search_document"}
            response = self._client.invoke_model(
                modelId=self._model,
                body=json.dumps(body),
                accept="application/json",
                contentType="application/json",
            )
            payload = json.loads(response["body"].read())
            return EmbeddingResult(embeddings=payload["embeddings"])

        # Titan (and any other single-input-per-call models): one request per text.
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
