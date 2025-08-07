"""Tests for the query embeddings endpoint."""

import math
import os
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from datahub_integrations.server import app

# Skip these integration tests in CI environments since they require AWS credentials
# Note: GitHub Actions automatically sets CI=true (since April 2020)
pytestmark = pytest.mark.skipif(
    os.environ.get("CI", "").lower() == "true",
    reason="Integration tests requiring AWS credentials are skipped in CI",
)


@pytest.mark.integration
class TestEmbeddingsEndpoint:
    """
    Integration test cases for the /ai/embeddings/query endpoint.

    These tests require AWS credentials to be configured since they make actual
    calls to AWS Bedrock services. They are automatically skipped in CI environments.

    To run locally, ensure you have AWS credentials configured (via AWS_PROFILE or
    standard AWS credential chain) and run:

        pytest tests/gen_ai/test_embeddings.py -m integration

    Or to run all tests including these:

        CI=false pytest tests/gen_ai/test_embeddings.py
    """

    def test_embed_query_success(self) -> None:
        """Test successful query embedding generation."""
        client = TestClient(app)

        response = client.post(
            "/private/ai/embeddings/query",
            json={"text": "test query for embeddings"},
        )

        assert response.status_code == 200
        data = response.json()

        # Check response structure
        assert "provider" in data
        assert "model" in data
        assert "embedding" in data
        assert "dimensionality" in data
        assert "created_at" in data

        # Check embedding properties
        assert isinstance(data["embedding"], list)
        assert len(data["embedding"]) == 1024
        assert data["dimensionality"] == 1024

        # Verify L2 normalization (should be close to 1.0)
        norm = math.sqrt(sum(x * x for x in data["embedding"]))
        assert abs(norm - 1.0) < 0.01, f"Embedding not properly normalized: norm={norm}"

        # Check timestamp format
        datetime.fromisoformat(data["created_at"].replace("Z", "+00:00"))

    def test_embed_query_with_model(self) -> None:
        """Test query embedding with explicit model specification."""
        client = TestClient(app)

        response = client.post(
            "/private/ai/embeddings/query",
            json={
                "text": "test query",
                "model": "bedrock:cohere.embed-english-v3",
            },
        )

        assert response.status_code == 200
        data = response.json()

        assert data["provider"] == "bedrock"
        assert data["model"] == "bedrock:cohere.embed-english-v3"

    def test_embed_query_empty_text(self) -> None:
        """Test that empty text is rejected."""
        client = TestClient(app)

        response = client.post(
            "/private/ai/embeddings/query",
            json={"text": ""},
        )

        assert response.status_code == 422  # Validation error

    def test_embed_query_missing_text(self) -> None:
        """Test that missing text field is rejected."""
        client = TestClient(app)

        response = client.post(
            "/private/ai/embeddings/query",
            json={},
        )

        assert response.status_code == 422  # Validation error

    def test_embed_query_long_text(self) -> None:
        """Test embedding generation with long text (truncation detection)."""
        client = TestClient(app)

        # Create text longer than 2048 chars (dummy truncation threshold)
        long_text = "a" * 3000

        response = client.post(
            "/private/ai/embeddings/query",
            json={
                "text": long_text,
                "model": "bedrock:dummy",  # Use bedrock provider to trigger truncation logic
            },
        )

        assert response.status_code == 200
        data = response.json()

        # Should indicate truncation for bedrock provider
        assert data["truncated"] is True

    def test_embed_query_text_too_long(self) -> None:
        """Test that text exceeding max length is rejected."""
        client = TestClient(app)

        # Create text longer than max allowed (128k chars)
        very_long_text = "a" * 140000  # ~137k chars, exceeds 128k limit

        response = client.post(
            "/private/ai/embeddings/query",
            json={"text": very_long_text},
        )

        assert response.status_code == 422  # Validation error

    def test_embed_query_consistent_output(self) -> None:
        """Test that the same input produces consistent embeddings."""
        client = TestClient(app)

        text = "consistent test query"

        response1 = client.post(
            "/private/ai/embeddings/query",
            json={"text": text},
        )

        response2 = client.post(
            "/private/ai/embeddings/query",
            json={"text": text},
        )

        assert response1.status_code == 200
        assert response2.status_code == 200

        # For dummy implementation, should return same embedding
        embedding1 = response1.json()["embedding"]
        embedding2 = response2.json()["embedding"]

        assert embedding1 == embedding2
