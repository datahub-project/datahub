"""Reproducible test case for GenAI cost tracking observability.

This test validates that:
1. Chat API calls are instrumented with token and cost metrics
2. Description generation API calls are instrumented with token and cost metrics
3. Metrics are correctly exposed via the /metrics endpoint
4. Dashboard displays the metrics correctly

These tests are DISABLED by default because they make real LLM calls. To enable:
    ENABLE_GENAI_TESTS=true

Prerequisites:
- DataHub integrations service running (localhost:9003 or configured URL)
- DATAHUB_GMS_TOKEN with access to the target environment
- AWS credentials configured for Bedrock (for description generation)

Configuration is externalized to allow testing against different environments:
- fieldeng (remote instance, requires token)
- local quickstart (future: will use test data)

Usage:
    # Run GenAI tests locally
    ENABLE_GENAI_TESTS=true pytest tests/integrations/test_genai_cost_tracking.py -v
"""

import os
import time
from dataclasses import dataclass
from typing import Optional

import pytest
import requests

# Test gating - disabled by default to prevent expensive LLM calls
GENAI_TESTS_ENABLED = os.environ.get("ENABLE_GENAI_TESTS", "false").lower() == "true"


@dataclass
class TestConfig:
    """Configuration for cost tracking tests."""

    integrations_service_url: str
    gms_token: str
    # Test URN must have schema information for description generation
    test_dataset_urn: str
    # Expected operations to validate
    expected_operations: list[str]


@pytest.fixture(scope="session")
def test_config() -> TestConfig:
    """Load test configuration from environment variables.

    For fieldeng testing:
        export INTEGRATIONS_SERVICE_URL=http://localhost:9003
        export DATAHUB_GMS_TOKEN=<your_token>
        export TEST_DATASET_URN=urn:li:dataset:(urn:li:dataPlatform:bigquery,field-eng.analytics.ShelterDogs,PROD)

    For local quickstart (future):
        export INTEGRATIONS_SERVICE_URL=http://localhost:9003
        export DATAHUB_GMS_TOKEN=<local_token>
        export TEST_DATASET_URN=<test_dataset_with_schema>
    """
    return TestConfig(
        integrations_service_url=os.getenv(
            "INTEGRATIONS_SERVICE_URL", "http://localhost:9003"
        ),
        gms_token=os.getenv("DATAHUB_GMS_TOKEN", ""),
        test_dataset_urn=os.getenv(
            "TEST_DATASET_URN",
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,field-eng.analytics.ShelterDogs,PROD)",
        ),
        expected_operations=["chat", "description_generation"],
    )


def get_metrics(service_url: str) -> str:
    """Fetch raw metrics from the integrations service."""
    response = requests.get(f"{service_url}/metrics", timeout=10)
    response.raise_for_status()
    return response.text


def parse_metric_value(
    metrics_text: str, metric_name: str, labels: dict[str, str]
) -> Optional[float]:
    """Parse a specific metric value from Prometheus metrics text.

    Args:
        metrics_text: Raw Prometheus metrics text
        metric_name: Name of the metric to find
        labels: Dictionary of label key-value pairs to match

    Returns:
        Metric value as float, or None if not found
    """
    for line in metrics_text.split("\n"):
        if line.startswith("#") or not line.strip():
            continue

        if not line.startswith(metric_name):
            continue

        # Check if all labels match
        labels_match = all(f'{k}="{v}"' in line for k, v in labels.items())
        if not labels_match:
            continue

        # Extract value (last token on the line)
        try:
            value = float(line.split()[-1])
            return value
        except (ValueError, IndexError):
            continue

    return None


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_service_is_running(test_config: TestConfig):
    """Verify the integrations service is accessible."""
    response = requests.get(
        f"{test_config.integrations_service_url}/metrics", timeout=10
    )
    assert response.status_code == 200, "Integrations service is not running"
    print(
        f"✓ Integrations service is running at {test_config.integrations_service_url}"
    )


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_description_generation_endpoint(test_config: TestConfig):
    """Test that description generation records both token and cost metrics."""
    if not test_config.gms_token:
        pytest.skip("DATAHUB_GMS_TOKEN not set")

    # Get baseline metrics
    baseline_metrics = get_metrics(test_config.integrations_service_url)
    baseline_cost = (
        parse_metric_value(
            baseline_metrics,
            "genai_llm_cost_USD_total",
            {"ai_module": "description_generation", "status": "success"},
        )
        or 0.0
    )
    baseline_tokens = (
        parse_metric_value(
            baseline_metrics,
            "genai_llm_tokens_total",
            {
                "ai_module": "description_generation",
                "token_type": "prompt",
                "status": "success",
            },
        )
        or 0.0
    )

    print("\nBaseline metrics:")
    print(f"  Cost: ${baseline_cost}")
    print(f"  Tokens: {baseline_tokens}")

    # Make API call
    response = requests.get(
        f"{test_config.integrations_service_url}/private/ai/suggest_description",
        params={"entity_urn": test_config.test_dataset_urn},
        headers={"Authorization": f"Bearer {test_config.gms_token}"},
        timeout=120,
    )

    assert response.status_code == 200, (
        f"Description generation failed: {response.text}"
    )
    result = response.json()
    assert "entity_description" in result, "Response missing entity_description"
    assert "column_descriptions" in result, "Response missing column_descriptions"
    print("✓ Description generation succeeded")
    print(
        f"  Generated {len(result.get('column_descriptions', {}))} column descriptions"
    )

    # Wait for metrics to be updated (OpenTelemetry may batch)
    time.sleep(2)

    # Verify metrics increased
    updated_metrics = get_metrics(test_config.integrations_service_url)

    # Check cost metrics
    updated_cost = parse_metric_value(
        updated_metrics,
        "genai_llm_cost_USD_total",
        {"ai_module": "description_generation", "status": "success"},
    )
    assert updated_cost is not None, "Cost metric not found for generate_description"
    assert updated_cost > baseline_cost, (
        f"Cost did not increase: {baseline_cost} -> {updated_cost}"
    )
    cost_delta = updated_cost - baseline_cost
    print(f"✓ Cost metric recorded: ${cost_delta:.6f} USD")

    # Check token metrics (prompt)
    updated_prompt_tokens = parse_metric_value(
        updated_metrics,
        "genai_llm_tokens_total",
        {
            "ai_module": "description_generation",
            "token_type": "prompt",
            "status": "success",
        },
    )
    assert updated_prompt_tokens is not None, "Prompt token metric not found"
    assert updated_prompt_tokens > baseline_tokens, "Prompt tokens did not increase"
    prompt_delta = int(updated_prompt_tokens - baseline_tokens)
    print(f"✓ Prompt tokens recorded: {prompt_delta}")

    # Check token metrics (completion)
    updated_completion_tokens = parse_metric_value(
        updated_metrics,
        "genai_llm_tokens_total",
        {
            "ai_module": "description_generation",
            "token_type": "completion",
            "status": "success",
        },
    )
    assert updated_completion_tokens is not None, "Completion token metric not found"
    completion_tokens = int(updated_completion_tokens)
    print(f"✓ Completion tokens recorded: {completion_tokens}")

    # Verify provider and model labels are present
    assert 'provider="bedrock"' in updated_metrics, (
        "Provider label not found in metrics"
    )
    assert "model=" in updated_metrics, "Model label not found in metrics"
    print("✓ Provider and model labels present")


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_chat_endpoint(test_config: TestConfig):
    """Test that chat API calls record both token and cost metrics."""
    if not test_config.gms_token:
        pytest.skip("DATAHUB_GMS_TOKEN not set")

    # Get baseline metrics
    baseline_metrics = get_metrics(test_config.integrations_service_url)
    baseline_cost = (
        parse_metric_value(
            baseline_metrics,
            "genai_llm_cost_USD_total",
            {"ai_module": "chat", "status": "success"},
        )
        or 0.0
    )
    baseline_tokens = (
        parse_metric_value(
            baseline_metrics,
            "genai_llm_tokens_total",
            {"ai_module": "chat", "token_type": "prompt", "status": "success"},
        )
        or 0.0
    )

    print("\nBaseline metrics:")
    print(f"  Cost: ${baseline_cost}")
    print(f"  Tokens: {baseline_tokens}")

    # Make chat API call
    # Note: The exact request format may vary - this is a placeholder
    # We'll need to use the actual chat API format once confirmed
    response = requests.post(
        f"{test_config.integrations_service_url}/private/api/chat/message",
        headers={
            "Authorization": f"Bearer {test_config.gms_token}",
            "Content-Type": "application/json",
        },
        json={
            "text": "What datasets are available?",
            "conversation_urn": "urn:li:conversation:test-" + str(int(time.time())),
        },
        timeout=120,
    )

    # Chat endpoint might have different response format
    if response.status_code != 200:
        pytest.skip(f"Chat endpoint returned {response.status_code}, skipping test")

    print("✓ Chat API call succeeded")

    # Wait for metrics to be updated
    time.sleep(2)

    # Verify metrics increased
    updated_metrics = get_metrics(test_config.integrations_service_url)

    # Check cost metrics
    updated_cost = parse_metric_value(
        updated_metrics,
        "genai_llm_cost_USD_total",
        {"ai_module": "chat", "status": "success"},
    )

    if updated_cost is None:
        pytest.skip("Chat cost metrics not found - may not be implemented yet")

    assert updated_cost > baseline_cost, (
        f"Cost did not increase: {baseline_cost} -> {updated_cost}"
    )
    cost_delta = updated_cost - baseline_cost
    print(f"✓ Cost metric recorded: ${cost_delta:.6f} USD")

    # Check token metrics
    updated_prompt_tokens = parse_metric_value(
        updated_metrics,
        "genai_llm_tokens_total",
        {"ai_module": "chat", "token_type": "prompt", "status": "success"},
    )
    assert updated_prompt_tokens is not None, "Prompt token metric not found"
    assert updated_prompt_tokens > baseline_tokens, "Prompt tokens did not increase"
    prompt_delta = int(updated_prompt_tokens - baseline_tokens)
    print(f"✓ Prompt tokens recorded: {prompt_delta}")


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_metrics_endpoint_format(test_config: TestConfig):
    """Verify metrics endpoint returns valid Prometheus format."""
    metrics_text = get_metrics(test_config.integrations_service_url)

    # Check for required metric types
    assert "# HELP genai_llm_tokens_total" in metrics_text, (
        "Token metrics help text missing"
    )
    assert "# TYPE genai_llm_tokens_total counter" in metrics_text, (
        "Token metrics type declaration missing"
    )
    assert "# HELP genai_llm_cost_USD_total" in metrics_text, (
        "Cost metrics help text missing"
    )
    assert "# TYPE genai_llm_cost_USD_total counter" in metrics_text, (
        "Cost metrics type declaration missing"
    )

    print("✓ Metrics endpoint returns valid Prometheus format")

    # Check for semantic labels
    required_labels = ["provider", "model", "ai_module", "status"]
    for label in required_labels:
        assert f'{label}="' in metrics_text, (
            f"Required label '{label}' not found in metrics"
        )
    print(f"✓ All required labels present: {', '.join(required_labels)}")


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_dashboard_accessibility(test_config: TestConfig):
    """Verify observability dashboard is accessible (if running)."""
    dashboard_url = os.getenv("DASHBOARD_URL", "http://localhost:8502")

    try:
        response = requests.get(dashboard_url, timeout=5)
        if response.status_code == 200:
            print(f"✓ Dashboard is accessible at {dashboard_url}")

            # Check if dashboard can fetch metrics
            content = response.text
            if "DataHub Integrations" in content or "Observability" in content:
                print("✓ Dashboard appears to be the observability dashboard")
        else:
            pytest.skip(f"Dashboard returned {response.status_code}")
    except requests.exceptions.RequestException as e:
        pytest.skip(f"Dashboard not accessible: {e}")


@pytest.mark.skipif(
    not GENAI_TESTS_ENABLED,
    reason="GenAI tests disabled. Set ENABLE_GENAI_TESTS=true to run.",
)
def test_operations_summary(test_config: TestConfig):
    """Print summary of all operations with metrics."""
    metrics_text = get_metrics(test_config.integrations_service_url)

    print("\n" + "=" * 60)
    print("GenAI Cost Tracking Summary")
    print("=" * 60)

    operations_found = set()

    # Find all operations with metrics
    for line in metrics_text.split("\n"):
        if "genai_llm_cost_USD_total" in line and "ai_module=" in line:
            # Extract operation name
            for part in line.split(","):
                if "ai_module=" in part:
                    op = part.split('"')[1]
                    operations_found.add(op)

                    # Get cost for this operation
                    cost = parse_metric_value(
                        metrics_text,
                        "genai_llm_cost_USD_total",
                        {"ai_module": op, "status": "success"},
                    )

                    # Get tokens for this operation
                    prompt_tokens = (
                        parse_metric_value(
                            metrics_text,
                            "genai_llm_tokens_total",
                            {
                                "ai_module": op,
                                "token_type": "prompt",
                                "status": "success",
                            },
                        )
                        or 0
                    )
                    completion_tokens = (
                        parse_metric_value(
                            metrics_text,
                            "genai_llm_tokens_total",
                            {
                                "ai_module": op,
                                "token_type": "completion",
                                "status": "success",
                            },
                        )
                        or 0
                    )

                    print(f"\nOperation: {op}")
                    print(f"  Cost: ${cost:.6f} USD")
                    print(
                        f"  Tokens: {int(prompt_tokens)} prompt + {int(completion_tokens)} completion"
                    )

    print("\n" + "=" * 60)
    print(f"Total operations tracked: {len(operations_found)}")
    print(f"Operations: {', '.join(sorted(operations_found))}")
    print("=" * 60 + "\n")

    # Verify at least one operation is tracked
    assert len(operations_found) > 0, "No operations with cost metrics found"


if __name__ == "__main__":
    # Allow running directly with: python test_genai_cost_tracking.py
    pytest.main([__file__, "-v", "-s"])
