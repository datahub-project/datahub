"""
Smoke test for DataHub-specific metrics.

This test verifies that:
1. DataHub custom metrics are being generated
2. The new incrementMicrometer metrics are working
3. Request context metrics are being recorded
"""

import logging

import pytest
import requests

from tests.utils import get_gms_url

logger = logging.getLogger(__name__)


@pytest.mark.read_only
def test_datahub_request_count_metric_present():
    """Test that the new datahub_request_count metric is present in Prometheus output."""
    gms_url = get_gms_url()
    prometheus_url = f"{gms_url}/actuator/prometheus"

    # Service initialization should already induce requests that will generate
    # metrics. So we don't need to trigger any requests as part of test setup.
    response = requests.get(prometheus_url)
    content = response.text

    # Look specifically for the datahub_request_count metric
    metric_lines = []
    for line in content.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            if "datahub_request_count" in line:
                metric_lines.append(line)

    logger.info(f"✅ Found {len(metric_lines)} datahub_request_count metric lines")
    for line in metric_lines:
        logger.info(f"  - {line}")

    # The metric should be present
    assert len(metric_lines) > 0, (
        "datahub_request_count metric not found in Prometheus output"
    )

    # Verify that the metric has the expected tags
    expected_tags = ["user_category", "agent_class", "request_api"]
    logger.info(f"🔍 Checking for expected tags: {expected_tags}")

    for metric_line in metric_lines:
        # Check if the metric line contains the expected tags
        has_expected_tags = all(tag in metric_line for tag in expected_tags)
        assert has_expected_tags, (
            f"Metric line missing expected tags. Line: {metric_line}, Expected tags: {expected_tags}"
        )
        logger.info(f"✅ Metric line has all expected tags: {metric_line}")

    logger.info("🎉 All datahub_request_count metrics have the expected tags!")
