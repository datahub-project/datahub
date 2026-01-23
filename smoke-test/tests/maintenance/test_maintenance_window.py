"""
Smoke tests for the Maintenance Window API.

These tests verify that the MaintenanceController is properly wired and functional.
The tests cover the full lifecycle: get status, enable, verify, disable, verify.
"""

import logging

import pytest

logger = logging.getLogger(__name__)

BASE_PATH = "/openapi/operations/maintenance"


@pytest.fixture(scope="module", autouse=True)
def cleanup_maintenance_mode(auth_session):
    """Ensure maintenance mode is disabled before and after tests."""
    base_url = auth_session.gms_url()

    # Disable maintenance mode before tests (in case previous test left it enabled)
    auth_session.post(f"{base_url}{BASE_PATH}/disable")

    yield

    # Disable maintenance mode after tests
    response = auth_session.post(f"{base_url}{BASE_PATH}/disable")
    logger.info(f"Cleanup: disabled maintenance mode, status={response.status_code}")


def test_maintenance_status_endpoint_works(auth_session):
    """Test that GET /status endpoint works (verifies controller bean wiring)."""
    base_url = auth_session.gms_url()

    response = auth_session.get(f"{base_url}{BASE_PATH}/status")

    logger.info(f"GET /status response: {response.status_code} - {response.text}")
    assert response.status_code == 200, (
        f"Expected 200, got {response.status_code}: {response.text}"
    )

    data = response.json()
    assert "enabled" in data, "Response should contain 'enabled' field"


def test_maintenance_enable_disable_lifecycle(auth_session):
    """Test the full enable/disable lifecycle of maintenance mode."""
    base_url = auth_session.gms_url()

    # Step 1: Enable maintenance mode
    enable_payload = {
        "message": "Smoke test maintenance window",
        "severity": "WARNING",
        "linkUrl": "https://status.example.com",
        "linkText": "Status Page",
    }

    response = auth_session.post(f"{base_url}{BASE_PATH}/enable", json=enable_payload)
    logger.info(f"POST /enable response: {response.status_code} - {response.text}")

    assert response.status_code == 200, f"Enable failed: {response.text}"

    data = response.json()
    assert data["enabled"] is True, "Expected enabled=True"
    assert data["message"] == "Smoke test maintenance window"
    assert data["severity"] == "WARNING"
    assert data["linkUrl"] == "https://status.example.com"
    assert data["linkText"] == "Status Page"
    assert "enabledAt" in data, "Response should contain 'enabledAt'"
    assert "enabledBy" in data, "Response should contain 'enabledBy'"

    # Step 2: Verify status shows enabled
    response = auth_session.get(f"{base_url}{BASE_PATH}/status")
    logger.info(f"GET /status (after enable) response: {response.status_code}")

    assert response.status_code == 200
    data = response.json()
    assert data["enabled"] is True, "Status should show enabled=True"
    assert data["message"] == "Smoke test maintenance window"

    # Step 3: Disable maintenance mode
    response = auth_session.post(f"{base_url}{BASE_PATH}/disable")
    logger.info(f"POST /disable response: {response.status_code} - {response.text}")

    assert response.status_code == 200, f"Disable failed: {response.text}"
    data = response.json()
    assert data["message"] == "Maintenance mode disabled"

    # Step 4: Verify status shows disabled
    response = auth_session.get(f"{base_url}{BASE_PATH}/status")
    logger.info(f"GET /status (after disable) response: {response.status_code}")

    assert response.status_code == 200
    data = response.json()
    assert data["enabled"] is False, "Status should show enabled=False"


def test_maintenance_enable_all_severities(auth_session):
    """Test that all severity levels work correctly."""
    base_url = auth_session.gms_url()

    for severity in ["INFO", "WARNING", "CRITICAL"]:
        # Enable with this severity
        enable_payload = {
            "message": f"Testing {severity} severity",
            "severity": severity,
        }

        response = auth_session.post(
            f"{base_url}{BASE_PATH}/enable", json=enable_payload
        )
        logger.info(f"POST /enable with {severity}: {response.status_code}")

        assert response.status_code == 200, (
            f"Enable with {severity} failed: {response.text}"
        )

        data = response.json()
        assert data["severity"] == severity, f"Expected severity={severity}"

        # Disable for next iteration
        auth_session.post(f"{base_url}{BASE_PATH}/disable")


def test_maintenance_enable_requires_message(auth_session):
    """Test that enabling without a message returns 400."""
    base_url = auth_session.gms_url()

    # Try to enable without message
    enable_payload = {
        "severity": "WARNING",
    }

    response = auth_session.post(f"{base_url}{BASE_PATH}/enable", json=enable_payload)
    logger.info(
        f"POST /enable without message: {response.status_code} - {response.text}"
    )

    assert response.status_code == 400, (
        f"Expected 400 for missing message, got {response.status_code}"
    )


def test_maintenance_enable_requires_severity(auth_session):
    """Test that enabling without severity returns 400."""
    base_url = auth_session.gms_url()

    # Try to enable without severity
    enable_payload = {
        "message": "Test message",
    }

    response = auth_session.post(f"{base_url}{BASE_PATH}/enable", json=enable_payload)
    logger.info(
        f"POST /enable without severity: {response.status_code} - {response.text}"
    )

    assert response.status_code == 400, (
        f"Expected 400 for missing severity, got {response.status_code}"
    )
