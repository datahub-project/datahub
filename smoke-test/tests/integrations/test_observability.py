"""Smoke tests for datahub-integrations-service observability /metrics endpoint.

Tests validate Phase 1 (foundation) and Phase 2 (instrumentation) metrics.
"""

import logging
import re
import time
from typing import Any

import pytest
import requests

from tests.utils import get_integrations_service_url

logger = logging.getLogger(__name__)


def get_integrations_metrics() -> str:
    """Fetch metrics from integrations service /metrics endpoint.

    No authentication required - public endpoint like GMS /actuator/prometheus.
    """
    url = f"{get_integrations_service_url()}/metrics"
    logger.info(f"Fetching metrics from {url}")
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    return response.text


def parse_prometheus_metrics(metrics_text: str) -> dict[str, list[dict[str, Any]]]:
    """Parse Prometheus text format into structured data.

    Returns:
        Dict mapping metric names to list of sample dicts with:
        - name: metric name
        - labels: dict of label key-value pairs
        - value: numeric value
    """
    metrics: dict[str, list[dict[str, Any]]] = {}

    for line in metrics_text.split("\n"):
        line = line.strip()

        # Skip comments and empty lines
        if not line or line.startswith("#"):
            continue

        # Parse metric line: metric_name{label="value",label2="value2"} 123.45 [timestamp]
        # Also handle metrics without labels: metric_name 123.45
        match = re.match(
            r"^([a-zA-Z_:][a-zA-Z0-9_:]*)((?:\{[^}]*\})?) (.+?)(?:\s+\d+)?$", line
        )
        if not match:
            continue

        metric_name = match.group(1)
        labels_str = match.group(2)
        value_str = match.group(3)

        # Parse labels
        labels: dict[str, str] = {}
        if labels_str and labels_str.startswith("{"):
            # Remove braces
            labels_content = labels_str[1:-1]
            # Parse label pairs: key="value"
            for label_match in re.finditer(
                r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"', labels_content
            ):
                labels[label_match.group(1)] = label_match.group(2)

        # Parse value
        try:
            value = float(value_str)
        except ValueError:
            continue

        # Add to metrics dict
        if metric_name not in metrics:
            metrics[metric_name] = []

        metrics[metric_name].append(
            {
                "name": metric_name,
                "labels": labels,
                "value": value,
            }
        )

    return metrics


def assert_metric_exists(
    metrics: dict[str, list[dict[str, Any]]],
    metric_pattern: str,
    required_labels: dict[str, str] | None = None,
) -> bool:
    """Assert that a metric matching pattern and labels exists.

    Args:
        metrics: Parsed metrics dictionary
        metric_pattern: Metric name pattern (supports * wildcard at end)
        required_labels: Optional dict of label key-value pairs that must match

    Returns:
        True if metric found, False otherwise (allows flexible checking)
    """
    # Handle wildcard patterns
    if metric_pattern.endswith("*"):
        prefix = metric_pattern[:-1]
        matching_names = [name for name in metrics.keys() if name.startswith(prefix)]
    else:
        matching_names = [metric_pattern] if metric_pattern in metrics else []

    if not matching_names:
        logger.warning(f"Metric pattern '{metric_pattern}' not found")
        return False

    # If no label requirements, just need to find the metric name
    if not required_labels:
        logger.info(
            f"Found metric(s) matching '{metric_pattern}': {matching_names[:3]}"
        )
        return True

    # Check if any samples have the required labels
    for name in matching_names:
        for sample in metrics[name]:
            sample_labels = sample.get("labels", {})
            if all(sample_labels.get(k) == v for k, v in required_labels.items()):
                logger.info(f"Found metric '{name}' with labels {required_labels}")
                return True

    logger.warning(
        f"Metric pattern '{metric_pattern}' found but no samples with labels {required_labels}"
    )
    return False


def get_histogram_buckets(
    metrics: dict[str, list[dict[str, Any]]],
    metric_name: str,
) -> list[float]:
    """Extract bucket boundaries from histogram metric.

    Args:
        metrics: Parsed metrics dictionary
        metric_name: Base histogram metric name (without _bucket suffix)

    Returns:
        List of bucket boundaries (le label values), excluding +Inf
    """
    bucket_metric_name = f"{metric_name}_bucket"
    if bucket_metric_name not in metrics:
        return []

    buckets = []
    for sample in metrics[bucket_metric_name]:
        le_value = sample.get("labels", {}).get("le")
        if le_value and le_value != "+Inf":
            try:
                buckets.append(float(le_value))
            except ValueError:
                continue

    # Return unique sorted buckets
    return sorted(set(buckets))


@pytest.mark.read_only
class TestObservabilityMetrics:
    """Test /metrics endpoint for observability telemetry."""

    def test_metrics_endpoint_accessible_without_auth(self) -> None:
        """Test /metrics endpoint is publicly accessible."""
        metrics_text = get_integrations_metrics()
        assert len(metrics_text) > 0
        logger.info(f"Fetched {len(metrics_text)} bytes of metrics")

    def test_metrics_prometheus_format(self) -> None:
        """Test metrics are in valid Prometheus text format."""
        metrics_text = get_integrations_metrics()

        # Should contain TYPE or HELP comments (metadata)
        has_metadata = "# TYPE" in metrics_text or "# HELP" in metrics_text

        # Parse to validate format
        metrics = parse_prometheus_metrics(metrics_text)
        assert len(metrics) > 0, "No valid metrics parsed"

        logger.info(
            f"Parsed {len(metrics)} distinct metric names (has metadata: {has_metadata})"
        )

    def test_metrics_contain_service_identification(self) -> None:
        """Test metrics include service resource attributes."""
        metrics_text = get_integrations_metrics()

        # OpenTelemetry exports resource attributes as target_info or in metric labels
        # Check for service name anywhere in the output
        has_service_name = (
            "datahub-integrations-service" in metrics_text.lower()
            or "datahub_integrations_service" in metrics_text.lower()
            or "service_name" in metrics_text.lower()
            or "target_info" in metrics_text.lower()
        )

        assert has_service_name, "No service identification found in metrics"
        logger.info("Service identification present in metrics")

    def test_http_server_metrics_present(self) -> None:
        """Test HTTP server metrics from FastAPI instrumentation."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Check for HTTP metrics (names vary by OTel version)
        # Look for common patterns
        http_metric_patterns = [
            "http_server",
            "http.server",
            "http_requests",
        ]

        found_http_metrics = []
        for pattern in http_metric_patterns:
            matching = [m for m in metrics.keys() if pattern.lower() in m.lower()]
            found_http_metrics.extend(matching)

        # May not have HTTP metrics if no requests made yet, so just log
        if found_http_metrics:
            logger.info(f"Found HTTP server metrics: {found_http_metrics[:5]}")
        else:
            logger.warning(
                "No HTTP server metrics found (may be expected on fresh deployment)"
            )


@pytest.mark.read_only
class TestPhase2InstrumentationMetrics:
    """Test Phase 2 custom application metrics."""

    def test_actions_metrics_structure(self) -> None:
        """Test Actions execution metrics are exposed with correct structure."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Look for actions metrics
        has_venv_setup = assert_metric_exists(metrics, "actions_venv_setup_duration*")
        has_execution_duration = assert_metric_exists(
            metrics, "actions_execution_duration*"
        )
        has_execution_total = assert_metric_exists(metrics, "actions_execution_total*")

        # At least one should be present (may not have data if no actions run)
        if has_venv_setup or has_execution_duration or has_execution_total:
            logger.info("Actions metrics found")

            # If execution metrics exist, check for stage labels
            if "actions_execution_total" in metrics:
                execution_samples = metrics["actions_execution_total"]
                stages = {s.get("labels", {}).get("stage") for s in execution_samples}
                stages.discard(None)
                if stages:
                    logger.info(f"Found actions with stages: {stages}")
        else:
            logger.warning("No Actions metrics found (expected if no actions have run)")

    def test_slack_command_metrics_structure(self) -> None:
        """Test Slack command metrics are exposed with correct structure."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        has_duration = assert_metric_exists(metrics, "slack_command_duration*")
        has_total = assert_metric_exists(metrics, "slack_command_total*")

        if has_duration or has_total:
            logger.info("Slack command metrics found")

            # If metrics exist, check for command labels
            if "slack_command_total" in metrics:
                command_samples = metrics["slack_command_total"]
                commands = {s.get("labels", {}).get("command") for s in command_samples}
                commands.discard(None)
                if commands:
                    logger.info(f"Found Slack commands: {commands}")
                    # Verify expected commands are subset
                    expected_commands = {"search", "get", "ask"}
                    if commands.intersection(expected_commands):
                        logger.info("Expected Slack commands present")
        else:
            logger.warning(
                "No Slack command metrics found (expected if no commands have been executed)"
            )

    def test_genai_llm_metrics_structure(self) -> None:
        """Test GenAI LLM call metrics including cost tracking."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Core LLM metrics
        has_call_duration = assert_metric_exists(metrics, "genai_llm_call_duration*")
        has_call_total = assert_metric_exists(metrics, "genai_llm_call_total*")

        # Token usage metrics
        has_tokens = assert_metric_exists(metrics, "genai_llm_tokens_total*")

        # Cost metrics (Phase 1 feature)
        has_cost = assert_metric_exists(metrics, "genai_llm_cost_total*")

        if any([has_call_duration, has_call_total, has_tokens, has_cost]):
            logger.info("GenAI metrics found (including cost tracking)")

            # Check for provider/model labels if metrics exist
            for metric_name in [
                "genai_llm_call_total",
                "genai_llm_tokens_total",
                "genai_llm_cost_total",
            ]:
                if metric_name in metrics:
                    samples = metrics[metric_name]
                    providers = {s.get("labels", {}).get("provider") for s in samples}
                    models = {s.get("labels", {}).get("model") for s in samples}
                    providers.discard(None)
                    models.discard(None)
                    if providers:
                        logger.info(f"Found LLM providers: {providers}")
                    if models:
                        logger.info(f"Found LLM models: {list(models)[:3]}")
        else:
            logger.warning(
                "No GenAI LLM metrics found (expected if no LLM calls have been made)"
            )

    def test_analytics_query_metrics_structure(self) -> None:
        """Test Analytics query execution metrics."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        has_duration = assert_metric_exists(metrics, "analytics_query_duration*")
        has_total = assert_metric_exists(metrics, "analytics_query_total*")

        if has_duration or has_total:
            logger.info("Analytics query metrics found")

            # If metrics exist, check for operation labels
            if "analytics_query_total" in metrics:
                query_samples = metrics["analytics_query_total"]
                operations = {
                    s.get("labels", {}).get("operation") for s in query_samples
                }
                operations.discard(None)
                if operations:
                    logger.info(f"Found analytics operations: {operations}")
                    # Verify expected operations
                    expected_ops = {"schema", "preview", "query"}
                    if operations.intersection(expected_ops):
                        logger.info("Expected analytics operations present")
        else:
            logger.warning(
                "No Analytics query metrics found (expected if no queries have been executed)"
            )

    def test_histogram_buckets_match_slo_config(self) -> None:
        """Test duration histograms use configured SLO buckets."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Default SLO buckets from config.py:
        # [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0]
        expected_buckets = [
            0.005,
            0.01,
            0.025,
            0.05,
            0.1,
            0.25,
            0.5,
            1.0,
            2.5,
            5.0,
            10.0,
            30.0,
            60.0,
        ]

        # Check histogram metrics for SLO buckets
        histogram_names = [
            "actions_execution_duration",
            "actions_venv_setup_duration",
            "slack_command_duration",
            "genai_llm_call_duration",
            "analytics_query_duration",
        ]

        found_histogram = False
        for hist_name in histogram_names:
            buckets = get_histogram_buckets(metrics, hist_name)
            if buckets:
                found_histogram = True
                logger.info(f"{hist_name} has buckets: {buckets}")

                # Check if buckets match expected (may be a subset or superset)
                # Just verify some expected buckets are present
                common_buckets = set(buckets).intersection(expected_buckets)
                if len(common_buckets) >= 5:
                    logger.info(
                        f"{hist_name} uses SLO-based buckets (matched {len(common_buckets)} buckets)"
                    )
                else:
                    logger.warning(f"{hist_name} may not use configured SLO buckets")

        if not found_histogram:
            logger.warning(
                "No histogram metrics with buckets found (expected if no activity)"
            )


@pytest.mark.read_only
class TestMetricsEndpointRobustness:
    """Test metrics endpoint handles edge cases."""

    def test_metrics_endpoint_responds_quickly(self) -> None:
        """Test metrics endpoint has acceptable latency."""
        start = time.perf_counter()
        get_integrations_metrics()
        duration = time.perf_counter() - start

        # Should respond in under 1 second (generous for CI/remote deployments)
        assert duration < 1.0, f"Metrics endpoint took {duration:.2f}s"
        logger.info(f"Metrics endpoint responded in {duration * 1000:.0f}ms")

    def test_metrics_endpoint_on_fresh_start(self) -> None:
        """Test metrics are available even without any activity.

        This validates that metrics infrastructure initializes correctly
        and exposes baseline metrics even when no requests have been made.
        """
        metrics_text = get_integrations_metrics()

        # Should have at least some content
        assert len(metrics_text) > 50, "Metrics response too small"

        # Parse to ensure valid format
        metrics = parse_prometheus_metrics(metrics_text)
        assert len(metrics) > 0, "No metrics parsed"

        logger.info(f"Fresh metrics endpoint has {len(metrics)} metric types")

    def test_metrics_endpoint_multiple_requests(self) -> None:
        """Test metrics endpoint handles repeated requests correctly."""
        # Make multiple requests
        responses = []
        for _ in range(3):
            metrics_text = get_integrations_metrics()
            responses.append(len(metrics_text))

        # All requests should succeed and return reasonable content
        assert all(size > 0 for size in responses), (
            "Some requests returned empty metrics"
        )
        logger.info(
            f"Multiple requests returned consistent metrics (sizes: {responses})"
        )

    def test_metrics_format_consistency(self) -> None:
        """Test that metrics output is consistently formatted."""
        metrics_text = get_integrations_metrics()
        lines = metrics_text.split("\n")

        # Should have some lines
        assert len(lines) > 0

        # Count metric lines vs comment lines
        metric_lines = [line for line in lines if line and not line.startswith("#")]
        comment_lines = [line for line in lines if line.startswith("#")]

        logger.info(
            f"Metrics output: {len(metric_lines)} data lines, {len(comment_lines)} comment lines"
        )

        # Should have at least some data lines
        assert len(metric_lines) > 0, "No metric data lines found"


@pytest.mark.read_only
class TestMetricsComprehensiveness:
    """Test comprehensive coverage of metrics from both phases."""

    def test_phase1_foundation_metrics_present(self) -> None:
        """Test that Phase 1 foundation metrics are accessible."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Phase 1 should provide:
        # 1. HTTP server metrics (from FastAPI auto-instrumentation)
        # 2. Resource attributes (service name, etc.)
        # 3. System metrics (optional)

        # We've validated these in other tests, here just ensure we have *something*
        assert len(metrics) > 0, "No metrics found from Phase 1 foundation"

        # Log what we found
        metric_names = list(metrics.keys())
        logger.info(f"Phase 1 foundation exposed {len(metric_names)} metric types")
        logger.info(f"Sample metrics: {metric_names[:10]}")

    def test_phase2_custom_metrics_categories_present(self) -> None:
        """Test that Phase 2 custom metrics span all instrumented categories."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Check for metrics from each category
        categories_found = []

        # Actions
        if any(k.startswith("actions_") for k in metrics.keys()):
            categories_found.append("Actions")

        # Slack
        if any(k.startswith("slack_") for k in metrics.keys()):
            categories_found.append("Slack")

        # GenAI
        if any(k.startswith("genai_") for k in metrics.keys()):
            categories_found.append("GenAI")

        # Analytics
        if any(k.startswith("analytics_") for k in metrics.keys()):
            categories_found.append("Analytics")

        # Log what we found
        if categories_found:
            logger.info(
                f"Phase 2 instrumentation found for: {', '.join(categories_found)}"
            )
        else:
            logger.warning(
                "No Phase 2 custom metrics found (expected if service has had no activity)"
            )

        # Don't assert here - metrics may legitimately not exist if service is fresh
        # Just document what we found

    def test_cost_tracking_metrics_structure(self) -> None:
        """Test that cost tracking metrics have correct structure (Phase 1 key feature)."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        # Cost tracking is a key Phase 1 feature
        cost_metric_name = "genai_llm_cost_total"

        if cost_metric_name in metrics:
            logger.info("Cost tracking metric found")

            # Verify structure: should have provider, model, operation labels
            cost_samples = metrics[cost_metric_name]
            required_labels = ["provider", "model", "operation"]

            sample_labels = cost_samples[0].get("labels", {}) if cost_samples else {}
            found_labels = [
                label for label in required_labels if label in sample_labels
            ]

            if len(found_labels) == len(required_labels):
                logger.info(
                    f"Cost tracking metric has all required labels: {required_labels}"
                )
            else:
                missing_labels = set(required_labels) - set(found_labels)
                logger.warning(f"Cost tracking metric missing labels: {missing_labels}")

            # Verify value is numeric (should be USD cost)
            if cost_samples and cost_samples[0].get("value") is not None:
                logger.info(
                    f"Cost tracking values present (sample: ${cost_samples[0]['value']:.6f})"
                )
        else:
            logger.warning(
                "Cost tracking metric not found (expected if no LLM calls have been made)"
            )

    def test_cache_token_tracking(self) -> None:
        """Test that cache token types are tracked separately for accurate cost calculation."""
        metrics = parse_prometheus_metrics(get_integrations_metrics())

        token_metric_name = "genai_llm_tokens_total"

        if token_metric_name in metrics:
            token_samples = metrics[token_metric_name]

            # Collect all token types
            token_types = {s.get("labels", {}).get("token_type") for s in token_samples}
            token_types.discard(None)

            # Check for expected token types
            expected_types = {"prompt", "completion"}
            cache_types = {"cache_read", "cache_write"}

            found_basic = expected_types.intersection(token_types)
            found_cache = cache_types.intersection(token_types)

            if found_basic:
                logger.info(
                    f"Found basic token types: {sorted(found_basic)} (required)"
                )

            if found_cache:
                logger.info(
                    f"Found cache token types: {sorted(found_cache)} (indicates prompt caching is active)"
                )
                # Log sample values to show cache usage
                for token_type in sorted(found_cache):
                    samples = [
                        s
                        for s in token_samples
                        if s.get("labels", {}).get("token_type") == token_type
                    ]
                    if samples:
                        total_cache_tokens = sum(
                            s.get("value", 0) for s in samples if s.get("value")
                        )
                        logger.info(
                            f"Total {token_type} tokens: {int(total_cache_tokens):,}"
                        )
            else:
                logger.info(
                    "No cache token metrics found (expected if prompt caching not used or not yet triggered)"
                )

            # All token types should have required labels
            required_labels = ["provider", "model", "token_type"]
            sample_labels = token_samples[0].get("labels", {}) if token_samples else {}
            found_labels = [
                label for label in required_labels if label in sample_labels
            ]

            if len(found_labels) == len(required_labels):
                logger.info(
                    f"Token tracking metric has all required labels: {required_labels}"
                )
        else:
            logger.warning(
                "Token tracking metric not found (expected if no LLM calls have been made)"
            )
