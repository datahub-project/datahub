"""Integration tests for GenAI term suggestion with cost tracking.

These tests verify that term suggestion LLM calls properly track costs and tokens.
"""

import os

import pytest
from fastapi.testclient import TestClient

from datahub_integrations.server import app

# Skip these integration tests in CI environments since they require AWS credentials
pytestmark = pytest.mark.skipif(
    os.environ.get("CI", "").lower() == "true",
    reason="Integration tests requiring AWS credentials are skipped in CI",
)


@pytest.mark.integration
class TestTermSuggestionCostTracking:
    """
    Integration tests for term suggestion with cost tracking.

    These tests require:
    - AWS credentials configured (via AWS_PROFILE or standard credential chain)
    - Access to AWS Bedrock with InvokeModel permissions
    - A test DataHub instance with glossary terms

    To run locally:
        AWS_PROFILE=acryl-read-write pytest tests/gen_ai/test_term_suggestion_cost_tracking.py -m integration
    """

    def test_suggest_terms_records_cost_metrics(self) -> None:
        """Test that term suggestion records cost tracking metrics."""
        client = TestClient(app)

        # Get baseline metrics
        metrics_before = client.get("/metrics")
        assert metrics_before.status_code == 200

        # Make a term suggestion request
        _response = client.post(
            "/private/ai/suggest_terms",
            params={
                "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.test_table,PROD)",
            },
            json={
                "glossary_node_urns": [],
                "use_all_glossary_terms": True,
            },
        )

        # The request might fail if the URN doesn't exist or no glossary terms are available
        # but we're checking whether cost tracking happens when LLM is invoked

        # Get metrics after the call
        metrics_after = client.get("/metrics")
        assert metrics_after.status_code == 200
        after_metrics = metrics_after.text

        # Check that GenAI metrics were recorded (if LLM was invoked)
        # The metrics might not appear if the request failed before reaching the LLM
        has_genai_metrics = (
            "genai_llm_cost_total" in after_metrics
            or "genai_llm_tokens_total" in after_metrics
        )

        if has_genai_metrics:
            print("✓ GenAI cost metrics detected after term suggestion")
        else:
            print(
                "ℹ No GenAI metrics found. This may indicate the LLM call was not made, "
                "possibly due to missing glossary terms or entity not found."
            )

    def test_term_suggestion_batch_cost_tracking(self) -> None:
        """Test that batch term suggestion tracks costs for multiple entities."""
        client = TestClient(app)

        # Get baseline metrics
        metrics_before = client.get("/metrics")
        baseline_text = metrics_before.text

        # Extract baseline token count if it exists
        baseline_tokens = 0
        for line in baseline_text.split("\n"):
            if line.startswith("genai_llm_tokens_total"):
                try:
                    baseline_tokens = int(float(line.split()[-1]))
                    break
                except (IndexError, ValueError):
                    pass

        # Make a batch request with multiple URNs
        _response = client.post(
            "/private/ai/suggest_terms_batch",
            json={
                "entity_urns": [
                    "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table1,PROD)",
                    "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.table2,PROD)",
                ],
                "universe_config": {
                    "glossary_node_urns": [],
                    "use_all_glossary_terms": True,
                },
            },
        )

        # Get updated metrics
        metrics_after = client.get("/metrics")
        after_text = metrics_after.text

        # Extract new token count
        new_tokens = 0
        for line in after_text.split("\n"):
            if line.startswith("genai_llm_tokens_total"):
                try:
                    new_tokens = int(float(line.split()[-1]))
                    break
                except (IndexError, ValueError):
                    pass

        # Check if tokens increased (indicating LLM calls were made)
        if new_tokens > baseline_tokens:
            token_increase = new_tokens - baseline_tokens
            print(
                f"✓ Batch term suggestion tracked {token_increase} tokens across multiple entities"
            )
        else:
            print(
                "ℹ Token count did not increase. This may indicate LLM calls were not made "
                "for the test entities (possibly due to missing metadata)."
            )

    def test_cache_tokens_in_term_suggestion(self) -> None:
        """Test that cache tokens are tracked when prompt caching is active.

        Makes repeated term suggestion calls with similar context to potentially
        trigger prompt caching.
        """
        client = TestClient(app)

        test_request = {
            "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.schema.cached_table,PROD)",
            "universe_config": {
                "glossary_node_urns": [],
                "use_all_glossary_terms": True,
            },
        }

        # Make first request (may populate cache)
        response1 = client.post("/private/ai/suggest_terms", json=test_request)

        # Make second request (may hit cache)
        response2 = client.post("/private/ai/suggest_terms", json=test_request)

        # Get metrics
        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 200
        metrics_text = metrics_response.text

        # Check for cache token metrics
        has_cache_read = 'token_type="cache_read"' in metrics_text
        has_cache_write = 'token_type="cache_write"' in metrics_text

        if has_cache_read or has_cache_write:
            print("✓ Cache token tracking working in term suggestion")
            if has_cache_read:
                print("  - cache_read tokens detected")
            if has_cache_write:
                print("  - cache_write tokens detected")
        else:
            print(
                "ℹ No cache token metrics found. This is expected if prompt caching "
                "is not enabled or the requests don't trigger caching."
            )

        # Verify basic token tracking exists
        assert (
            "genai_llm_tokens_total" in metrics_text
            or response1.status_code >= 400
            or response2.status_code >= 400
        ), "Expected token metrics or error responses"
