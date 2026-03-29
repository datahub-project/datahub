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

from tests.utils import get_gms_prometheus_base_url

logger = logging.getLogger(__name__)

# Micrometer logical name datahub.api.traffic → Prometheus counter
DATAHUB_API_TRAFFIC_PROM = "datahub_api_traffic"


@pytest.mark.read_only
def test_datahub_api_traffic_metric_present():
    """API traffic counter is present on Actuator Prometheus (datahub.api.traffic)."""
    prometheus_url = f"{get_gms_prometheus_base_url()}/actuator/prometheus"

    # Service initialization should already induce requests that will generate
    # metrics. So we don't need to trigger any requests as part of test setup.
    response = requests.get(prometheus_url)
    content = response.text

    metric_lines = []
    for line in content.split("\n"):
        line = line.strip()
        if line and not line.startswith("#"):
            if DATAHUB_API_TRAFFIC_PROM in line:
                metric_lines.append(line)

    logger.info(f"✅ Found {len(metric_lines)} {DATAHUB_API_TRAFFIC_PROM} metric lines")
    for line in metric_lines:
        logger.info(f"  - {line}")

    assert len(metric_lines) > 0, (
        f"{DATAHUB_API_TRAFFIC_PROM} metric not found in Prometheus output"
    )

    expected_tags = ["user_category", "agent_class", "request_api"]
    logger.info(f"🔍 Checking for expected tags: {expected_tags}")

    for metric_line in metric_lines:
        has_expected_tags = all(tag in metric_line for tag in expected_tags)
        assert has_expected_tags, (
            f"Metric line missing expected tags. Line: {metric_line}, Expected tags: {expected_tags}"
        )
        logger.info(f"✅ Metric line has all expected tags: {metric_line}")

    logger.info(f"🎉 All {DATAHUB_API_TRAFFIC_PROM} metrics have the expected tags!")
