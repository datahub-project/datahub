import logging

import pytest
import requests

from tests.utils import get_gms_url

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# ==============================================
# DEV TOOLING API TESTS
# ==============================================
# Verifies that the DevToolingController is active and reachable in quickstartDebug.
#
# The controller is gated by devTooling.enabled (set via DEV_TOOLING_ENABLED env var),
# which is injected into the GMS container by the debug compose profile. If the env var
# is not wired through, Spring never registers the controller and every request returns
# 404. These tests are the primary regression guard for that wiring.
#
# Endpoints under test:
#   GET /openapi/operations/dev/featureFlags         -> all flags as a JSON object
#   GET /openapi/operations/dev/featureFlags/{name}  -> single flag value
# ==============================================

_DEV_FLAGS_URL = f"{get_gms_url()}/openapi/operations/dev/featureFlags"


def test_dev_feature_flags_endpoint_is_active(auth_session):
    """Verify the endpoint returns 200 — 404 means DEV_TOOLING_ENABLED isn't wired."""
    response = auth_session.get(_DEV_FLAGS_URL)
    assert response.status_code == 200, (
        f"Expected 200 but got {response.status_code}. "
        "A 404 means devTooling.enabled=false — check DEV_TOOLING_ENABLED is set "
        "in the GMS container environment (docker/profiles/docker-compose.gms.yml)."
    )

    flags = response.json()
    assert isinstance(flags, dict), "Feature flags response should be a JSON object"
    assert len(flags) > 0, "Feature flags response should not be empty"
    logger.info("Dev tooling endpoint active with %d flags", len(flags))


def test_dev_feature_flags_contain_stable_keys(auth_session):
    """Verify a handful of long-lived flags are present — guards against API structural regressions."""
    response = auth_session.get(_DEV_FLAGS_URL)
    response.raise_for_status()
    flags = response.json()

    # These flags have been present since the initial FeatureFlags model.
    # If one disappears it's a real API break, not test brittleness.
    stable_flags = [
        "showBrowseV2",
        "nestedDomainsEnabled",
        "dataContractsEnabled",
    ]
    missing = [f for f in stable_flags if f not in flags]
    assert not missing, (
        f"Expected stable feature flags missing from response: {missing}. "
        f"Available flags: {sorted(flags.keys())}"
    )
    logger.info("All stable flags present: %s", stable_flags)


def test_dev_feature_flag_single_lookup(auth_session):
    """Verify the single-flag endpoint returns a correctly shaped response."""
    flag_name = "showBrowseV2"
    response = auth_session.get(f"{_DEV_FLAGS_URL}/{flag_name}")
    assert response.status_code == 200, (
        f"Single flag lookup for '{flag_name}' returned {response.status_code}"
    )

    result = response.json()
    assert flag_name in result, (
        f"Response should contain the requested flag key '{flag_name}', got: {result}"
    )
    assert isinstance(result[flag_name], bool), (
        f"Flag value should be a boolean, got: {type(result[flag_name])}"
    )
    logger.info("Single flag lookup OK: %s=%s", flag_name, result[flag_name])


def test_dev_feature_flag_unknown_name_returns_404(auth_session):
    """Verify that requesting a nonexistent flag returns 404, not a silent empty response."""
    response = auth_session.get(f"{_DEV_FLAGS_URL}/thisFlagDoesNotExist")
    assert response.status_code == 404, (
        f"Unknown flag lookup should return 404, got {response.status_code}"
    )
    logger.info("Unknown flag correctly returns 404")


def test_dev_feature_flags_accessible_without_auth():
    """Verify the endpoint is publicly reachable — it is intentionally unauthenticated.

    /openapi/operations/dev/featureFlags exposes read-only feature flag state (no secrets,
    no mutations) and is only registered when DEV_TOOLING_ENABLED=true, which is restricted
    to debug profiles. Keeping it unauthenticated makes it easy for agents and developers
    to inspect flag state without needing a token.
    """
    response = requests.get(_DEV_FLAGS_URL)
    assert response.status_code == 200, (
        f"Expected /openapi/operations/dev/featureFlags to be publicly accessible, got {response.status_code}"
    )
    assert isinstance(response.json(), dict), "Response should be a JSON object"
    logger.info("Unauthenticated access correctly permitted (%d)", response.status_code)
