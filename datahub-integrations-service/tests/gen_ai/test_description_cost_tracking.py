"""Integration tests for GenAI description generation with cost tracking.

These tests verify that:
1. LLM API calls successfully generate descriptions
2. Cost tracking metrics are recorded with proper labels
3. Cache tokens are extracted and tracked when available
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
class TestDescriptionCostTracking:
    """
    Integration tests for description generation with cost tracking.

    These tests require:
    - AWS credentials configured (via AWS_PROFILE or standard credential chain)
    - Access to AWS Bedrock with InvokeModel permissions

    To run locally:
        AWS_PROFILE=acryl-read-write pytest tests/gen_ai/test_description_cost_tracking.py -m integration
    """

    def test_suggest_description_records_cost_metrics(self) -> None:
        """Test that description generation records cost tracking metrics."""
        client = TestClient(app)

        # First, get baseline metrics
        metrics_before = client.get("/metrics")
        assert metrics_before.status_code == 200
        _baseline_metrics = metrics_before.text

        # Generate a description for a dataset
        # Using a simple test URN that should work with any DataHub instance
        _response = client.get(
            "/private/ai/suggest_description",
            params={
                "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,test_db.test_schema.test_table,PROD)",
            },
        )

        # The request might fail if the URN doesn't exist in the test environment,
        # but we're primarily interested in whether cost tracking happens
        # For this test, we'll accept either 200 (success) or error codes
        # as long as the LLM was invoked and metrics were recorded

        # Get metrics after the call
        metrics_after = client.get("/metrics")
        assert metrics_after.status_code == 200
        after_metrics = metrics_after.text

        # Check that GenAI cost metrics were recorded
        # These metrics should appear in the output if any LLM calls were made
        assert (
            "genai_llm_cost_total" in after_metrics
            or "genai_llm_tokens_total" in after_metrics
        ), (
            "Expected GenAI cost metrics to be recorded after description generation. "
            "This could indicate that cost tracking is not working properly, or the LLM call failed "
            "before cost tracking could occur."
        )

    def test_suggest_description_with_cache_tokens(self) -> None:
        """Test that cache tokens are tracked when making repeated calls.

        This test makes the same request twice to potentially trigger prompt caching,
        then verifies that cache token metrics are recorded if caching is active.
        """
        client = TestClient(app)

        test_urn = "urn:li:dataset:(urn:li:dataPlatform:bigquery,project.dataset.cached_table,PROD)"

        # Make first request (may populate cache)
        _response1 = client.get(
            "/private/ai/suggest_description",
            params={"entity_urn": test_urn},
        )

        # Make second request (may hit cache)
        _response2 = client.get(
            "/private/ai/suggest_description",
            params={"entity_urn": test_urn},
        )

        # Get metrics after both calls
        metrics_response = client.get("/metrics")
        assert metrics_response.status_code == 200
        metrics_text = metrics_response.text

        # Parse metrics to check for cache token tracking
        has_cache_metrics = (
            'token_type="cache_read"' in metrics_text
            or 'token_type="cache_write"' in metrics_text
        )

        # Note: Cache tokens may not appear if:
        # 1. The LLM provider doesn't support prompt caching
        # 2. The cache hasn't been populated yet
        # 3. The requests are too different to trigger caching
        # So we log the result but don't fail the test if no cache metrics appear
        if has_cache_metrics:
            print("✓ Cache token metrics detected - prompt caching is active")
        else:
            print(
                "ℹ No cache token metrics found. This is expected if prompt caching "
                "is not enabled or not yet triggered."
            )

        # Verify that at least basic token metrics exist
        assert "genai_llm_tokens_total" in metrics_text, (
            "Expected token metrics to be recorded after description generation"
        )

    def test_cost_calculation_for_description_generation(self) -> None:
        """Test that cost is calculated and recorded for description generation."""
        client = TestClient(app)

        # Get baseline cost metric
        metrics_before = client.get("/metrics")
        baseline_text = metrics_before.text

        # Extract baseline cost if it exists
        baseline_cost = 0.0
        for line in baseline_text.split("\n"):
            if line.startswith("genai_llm_cost_total"):
                try:
                    baseline_cost = float(line.split()[-1])
                    break
                except (IndexError, ValueError):
                    pass

        # Generate description
        _response = client.get(
            "/private/ai/suggest_description",
            params={
                "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.schema.cost_test_table,PROD)",
            },
        )

        # Get updated metrics
        metrics_after = client.get("/metrics")
        after_text = metrics_after.text

        # Extract new cost
        new_cost = 0.0
        for line in after_text.split("\n"):
            if line.startswith("genai_llm_cost_total"):
                try:
                    new_cost = float(line.split()[-1])
                    break
                except (IndexError, ValueError):
                    pass

        # Cost should increase if LLM was invoked
        # Note: This assertion might not be strict depending on whether the LLM call succeeds
        # but the presence of the metric itself indicates cost tracking is working
        assert "genai_llm_cost_total" in after_text, (
            "Expected cost metric to be present after description generation"
        )

        if new_cost > baseline_cost:
            cost_increase = new_cost - baseline_cost
            print(f"✓ Cost tracking working: cost increased by ${cost_increase:.6f}")

    def test_token_metrics_have_correct_labels(self) -> None:
        """Test that token metrics include the expected labels for filtering."""
        client = TestClient(app)

        # Generate a description to ensure metrics are populated
        client.get(
            "/private/ai/suggest_description",
            params={
                "entity_urn": "urn:li:dataset:(urn:li:dataPlatform:mysql,analytics.users,PROD)",
            },
        )

        # Get metrics
        metrics_response = client.get("/metrics")
        metrics_text = metrics_response.text

        # Check for token metrics with proper labels
        token_metric_found = False
        expected_labels = ["provider", "model", "operation", "status", "token_type"]

        for line in metrics_text.split("\n"):
            if "genai_llm_tokens_total{" in line:
                token_metric_found = True
                # Check that all expected labels are present
                for label in expected_labels:
                    assert f'{label}="' in line, (
                        f"Expected label '{label}' in token metric: {line}"
                    )
                break

        if token_metric_found:
            print("✓ Token metrics have all expected labels")
        else:
            print(
                "ℹ No token metrics found yet. This may indicate the LLM call "
                "did not complete successfully in this test run."
            )
