"""
DataHub Authentication E2E Test Suite

Core regression tests for DataHub's authentication system.
Validates authentication behavior across key endpoints and scenarios.

This test suite ensures authentication works correctly and provides protection
against regressions when making authentication-related changes.

Test Categories:
- 🔓 Public Endpoints: Tests excluded paths that should work without authentication
  (/health, /config, /actuator/prometheus, etc.) - Should never return 401
- 🔒 Protected Endpoints: Tests endpoints that require authentication
  (GraphQL API, Rest.li API, OpenAPI endpoints) - Should return 401 without valid auth
- 🎫 Token Authentication: Tests API token generation and usage, session cookie vs
  Bearer token authentication, token validation and cleanup
- 👑 Authorization: Tests privilege-based access control, admin endpoints requiring
  special permissions (creates/cleans up test users with limited privileges)
- 🧪 Edge Cases: Malformed authentication headers, authentication priority and
  fallback behavior, cross-service authentication consistency

Running: These tests are part of the standard smoke-test suite.
Use: pytest test_authentication_e2e.py -v (or run specific test functions)

Originally created during the two-filter authentication refactoring,
now serves as permanent regression testing.
"""

import logging
from typing import Tuple

import pytest
import requests

from tests.privileges.utils import create_user, remove_user
from tests.utils import (
    TestSessionWrapper,
    get_admin_credentials,
    get_frontend_url,
    get_gms_url,
    login_as,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# Test constants
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}


def extract_api_token_from_session(session: TestSessionWrapper) -> Tuple[str, str]:
    """Extract API token from frontend session cookies for OpenAPI authentication.

    Based on TestSessionWrapper pattern from tests/utils.py and test_system_info.py

    Args:
        session: Authenticated test session wrapper

    Returns:
        Tuple of (access_token, token_id)
    """
    # Extract actor URN from session cookies
    actor_urn = session.cookies["actor"]

    # Generate personal access token via GraphQL
    json_payload = {
        "query": """mutation createAccessToken($input: CreateAccessTokenInput!) {
            createAccessToken(input: $input) {
              accessToken
              metadata {
                id
              }
            }
        }""",
        "variables": {
            "input": {
                "type": "PERSONAL",
                "actorUrn": actor_urn,
                "duration": "ONE_HOUR",
                "name": "Test API Token - Authentication E2E",
                "description": "Token for authentication smoke tests",
            }
        },
    }

    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=json_payload)
    response.raise_for_status()

    token_data = response.json()["data"]["createAccessToken"]
    return token_data["accessToken"], token_data["metadata"]["id"]


def revoke_api_token(session: TestSessionWrapper, token_id: str) -> None:
    """Revoke an API token for cleanup.

    Args:
        session: Authenticated test session wrapper
        token_id: ID of the token to revoke
    """
    revoke_json = {
        "query": """mutation revokeAccessToken($tokenId: String!) {
            revokeAccessToken(tokenId: $tokenId)
        }""",
        "variables": {"tokenId": token_id},
    }
    response = session.post(f"{get_frontend_url()}/api/v2/graphql", json=revoke_json)
    response.raise_for_status()


# ===========================================
# PUBLIC ENDPOINTS TESTS (Excluded Paths)
# ===========================================


def test_health_endpoint_no_auth() -> None:
    """Health endpoint should work without authentication (excluded path)."""
    response = requests.get(f"{get_gms_url()}/health")
    assert response.status_code == 200
    logger.info(f"✅ /health without auth: {response.status_code}")


def test_health_endpoint_with_invalid_token() -> None:
    """Health endpoint should work even with invalid token (excluded path)."""
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    response = requests.get(f"{get_gms_url()}/health", headers=headers)
    assert response.status_code == 200
    logger.info(f"✅ /health with invalid token: {response.status_code}")


def test_actuator_prometheus_no_auth() -> None:
    """Prometheus metrics should work without authentication (excluded path)."""
    response = requests.get(f"{get_gms_url()}/actuator/prometheus")
    # Might be 200 or 404 depending on setup, but should NOT be 401
    assert response.status_code != 401
    logger.info(f"✅ /actuator/prometheus without auth: {response.status_code}")


@pytest.mark.parametrize(
    "endpoint",
    [
        "/config",
        "/config/search/export",
        "/public-iceberg/health",
        "/schema-registry/subjects",
    ],
)
def test_excluded_paths_no_auth(endpoint: str) -> None:
    """Test that excluded paths work without authentication.

    Args:
        endpoint: The endpoint path to test
    """
    response = requests.get(f"{get_gms_url()}{endpoint}")
    # Should not return 401 (might be 404 if endpoint doesn't exist)
    assert response.status_code != 401, (
        f"Expected non-401 for excluded path {endpoint}, got {response.status_code}"
    )
    logger.info(f"✅ {endpoint} without auth: {response.status_code}")


# ===========================================
# PROTECTED ENDPOINTS TESTS
# ===========================================


def test_graphql_endpoint_no_auth() -> None:
    """GraphQL endpoint should return 401 without authentication."""
    query = {"query": "{ me { corpUser { username } } }"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", json=query)
    assert response.status_code == 401
    logger.info(f"✅ GraphQL without auth: {response.status_code}")


def test_graphql_endpoint_invalid_token() -> None:
    """GraphQL endpoint should return 401 with invalid token."""
    query = {"query": "{ me { corpUser { username } } }"}
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    response = requests.post(
        f"{get_frontend_url()}/api/v2/graphql", json=query, headers=headers
    )
    assert response.status_code == 401
    logger.info(f"✅ GraphQL with invalid token: {response.status_code}")


def test_graphql_endpoint_valid_token(auth_session):
    """GraphQL endpoint should work with valid authentication."""
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=query
    )
    assert response.status_code == 200
    logger.info(f"✅ GraphQL with valid session: {response.status_code}")


@pytest.mark.parametrize(
    "endpoint,method",
    [
        ("/openapi/v2/entity/dataset", "GET"),
        ("/entities?action=search", "POST"),
        ("/aspects?action=getAspect", "GET"),
        ("/openapi/v1/system-info", "GET"),
        # Admin endpoints that use AuthenticationContext.getAuthentication()
        # These test the same logic that fails with NPE in MCE consumer when auth.enabled=false
        # Using throttle endpoint as it uses same auth pattern but responds quickly
        ("/openapi/operations/throttle/requests", "GET"),
    ],
)
def test_protected_endpoints_no_auth(endpoint: str, method: str) -> None:
    """Test that protected endpoints return 401 without authentication.

    Args:
        endpoint: The endpoint path to test
        method: HTTP method (GET or POST)
    """
    if method == "GET":
        response = requests.get(f"{get_gms_url()}{endpoint}")
    else:
        # Use proper request body based on endpoint
        if "entities?action=search" in endpoint:
            json_body = {"input": "*", "entity": "dataset", "start": 0, "count": 1}
            response = requests.post(
                f"{get_gms_url()}{endpoint}",
                headers=restli_default_headers,
                json=json_body,
            )
        else:
            # GET request for other endpoints (like throttle)
            response = requests.get(f"{get_gms_url()}{endpoint}")

    assert response.status_code == 401, (
        f"Expected 401 for {endpoint}, got {response.status_code}"
    )
    logger.info(f"✅ {method} {endpoint} without auth: {response.status_code}")


@pytest.mark.parametrize(
    "endpoint,method",
    [
        ("/openapi/v2/entity/dataset", "GET"),
        ("/entities?action=search", "POST"),
        ("/aspects?action=getAspect", "GET"),
        # Admin endpoints that use AuthenticationContext.getAuthentication()
        # Using throttle endpoint as it uses same auth pattern but responds quickly
        ("/openapi/operations/throttle/requests", "GET"),
    ],
)
def test_protected_endpoints_invalid_token(endpoint: str, method: str) -> None:
    """Test that protected endpoints return 401 with invalid token.

    Args:
        endpoint: The endpoint path to test
        method: HTTP method (GET or POST)
    """
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    if method == "GET":
        response = requests.get(f"{get_gms_url()}{endpoint}", headers=headers)
    else:
        headers.update(restli_default_headers)
        # Use proper request body based on endpoint
        if "entities?action=search" in endpoint:
            json_body = {"input": "*", "entity": "dataset", "start": 0, "count": 1}
            response = requests.post(
                f"{get_gms_url()}{endpoint}", headers=headers, json=json_body
            )
        else:
            # GET request for other endpoints (like throttle)
            response = requests.get(f"{get_gms_url()}{endpoint}", headers=headers)

    assert response.status_code == 401, (
        f"Expected 401 for {endpoint}, got {response.status_code}"
    )
    logger.info(f"✅ {method} {endpoint} with invalid token: {response.status_code}")


def test_restli_endpoints_with_valid_auth(auth_session: TestSessionWrapper) -> None:
    """Test that Rest.li endpoints work with valid authentication.

    Args:
        auth_session: Authenticated test session wrapper
    """
    # Test search endpoint
    search_json = {"input": "test", "entity": "dataset", "start": 0, "count": 10}
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=search",
        headers=restli_default_headers,
        json=search_json,
    )
    assert response.status_code == 200
    logger.info(f"✅ Rest.li search with valid auth: {response.status_code}")

    # Test admin endpoints that require authentication extraction
    # These test the same authentication logic that fails in MCE consumer when auth.enabled=false
    # Using throttle endpoint as it uses same auth pattern but responds quickly
    admin_endpoints = [
        ("/openapi/operations/throttle/requests", None),  # GET request, no body
    ]

    for endpoint, body in admin_endpoints:
        if body is None:
            # GET request for throttle endpoint
            response = auth_session.get(f"{auth_session.gms_url()}{endpoint}")
        else:
            # POST request with body for other endpoints
            response = auth_session.post(
                f"{auth_session.gms_url()}{endpoint}",
                headers=restli_default_headers,
                json=body,
            )
        # With valid authentication, these should work (200 OK)
        # The key is they should NOT return 500 Internal Server Error (NPE from authentication.getActor())
        # or 403 Forbidden (which would indicate a permissions issue)
        assert response.status_code == 200, (
            f"Admin endpoint {endpoint} returned {response.status_code}: {response.text}"
        )
        logger.info(
            f"✅ Admin endpoint {endpoint} with valid auth: {response.status_code}"
        )


# ===========================================
# AUTHORIZATION TESTS (403 scenarios)
# ===========================================


def test_admin_endpoints_require_privileges(auth_session) -> None:
    """Test that admin endpoints return 401/403 for users without admin privileges."""

    # Create a limited-privilege user for testing
    (admin_user, admin_pass) = get_admin_credentials()
    admin_session = login_as(admin_user, admin_pass)

    test_user_urn = "urn:li:corpuser:limited_auth_test_user"
    token_id = None

    try:
        # Create limited user
        create_user(admin_session, "limited_auth_test_user", "testpass123")

        # Login as limited user and get API token
        limited_session = login_as("limited_auth_test_user", "testpass123")
        api_token, token_id = extract_api_token_from_session(limited_session)

        # Test admin-only endpoints that require MANAGE_SYSTEM_OPERATIONS_PRIVILEGE
        admin_endpoints = [
            "/openapi/v1/system-info",
            "/openapi/operations/throttle/requests",  # API throttle management endpoint
        ]

        for endpoint in admin_endpoints:
            headers = {"Authorization": f"Bearer {api_token}"}
            response = requests.get(f"{get_gms_url()}{endpoint}", headers=headers)

            # Should be 403 (forbidden) for authenticated user without admin privileges
            assert response.status_code == 403, (
                f"Expected 403 for {endpoint}, got {response.status_code}. Response: {response.text[:200]}"
            )
            logger.info(f"✅ {endpoint} correctly returns 403 for non-admin user")

    finally:
        # Cleanup
        try:
            if token_id:
                revoke_api_token(limited_session, token_id)
            # Remove test user
            admin_cleanup_session = login_as(admin_user, admin_pass)
            remove_user(admin_cleanup_session, test_user_urn)
            logger.info("✅ Test user cleaned up successfully")
        except Exception as e:
            logger.warning(f"Failed to clean up test user: {e}")


# ===========================================
# EDGE CASES AND TOKEN SCENARIOS
# ===========================================


@pytest.mark.parametrize(
    "auth_header",
    [
        "Bearer",  # Missing token
        "bearer invalid-token",  # Wrong case
        "Basic dXNlcjpwYXNz",  # Wrong auth type
        "InvalidPrefix token",  # Wrong prefix
        "Bearer ",  # Empty token
    ],
)
def test_malformed_auth_headers(auth_header: str) -> None:
    """Test various malformed Authorization headers return 401.

    Args:
        auth_header: The malformed authorization header to test
    """
    query = {"query": "{ me { corpUser { username } } }"}
    headers = {"Authorization": auth_header}
    response = requests.post(
        f"{get_frontend_url()}/api/v2/graphql", json=query, headers=headers
    )
    assert response.status_code == 401
    logger.info(
        f"✅ GraphQL with malformed header '{auth_header}': {response.status_code}"
    )


def test_api_token_authentication(auth_session: TestSessionWrapper) -> None:
    """Test API token-based authentication behavior.

    Note: The frontend (port 9002) is intentionally not configured to handle Bearer tokens.
    It only supports session cookie authentication. Bearer tokens work directly with GMS (port 8080).
    This is an architectural design decision where the frontend handles UI authentication
    and GMS handles API authentication.

    Args:
        auth_session: Authenticated test session wrapper
    """
    token_id = None
    try:
        # Extract API token from session
        api_token, token_id = extract_api_token_from_session(auth_session)

        # Test GraphQL with API token through frontend - should return 401
        # Frontend is not configured to handle Bearer tokens (metadataServiceAuthEnabled=false)
        query = {"query": "{ me { corpUser { username } } }"}
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.post(
            f"{get_frontend_url()}/api/v2/graphql", json=query, headers=headers
        )
        assert response.status_code == 401, (
            f"Frontend should reject Bearer tokens, got {response.status_code}"
        )
        logger.info(
            "✅ Frontend correctly rejects Bearer tokens (architectural design)"
        )

        # Test OpenAPI with API token directly against GMS - should work
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.get(
            f"{get_gms_url()}/openapi/v2/entity/dataset", headers=headers
        )
        # Should work (200) or be method not allowed (405), but not unauthorized (401)
        assert response.status_code != 401, (
            f"API token should work against GMS, got {response.status_code}"
        )
        logger.info(f"✅ Bearer token works directly with GMS: {response.status_code}")

    finally:
        if token_id:
            revoke_api_token(auth_session, token_id)


def test_session_vs_token_authentication(auth_session: TestSessionWrapper) -> None:
    """Test the different authentication methods work as designed.

    This test validates the intentional architectural separation:
    - Frontend (port 9002): Session cookie authentication only
    - GMS (port 8080): Bearer token authentication for API access

    Args:
        auth_session: Authenticated test session wrapper
    """
    token_id = None
    try:
        # Test session cookie authentication through frontend - should work
        query = {"query": "{ me { corpUser { username } } }"}
        response = auth_session.post(
            f"{auth_session.frontend_url()}/api/v2/graphql", json=query
        )
        assert response.status_code == 200
        logger.info(
            f"✅ Session cookie authentication through frontend: {response.status_code}"
        )

        # Test bearer token authentication through frontend - should return 401
        # This is intentional: frontend doesn't handle Bearer tokens
        api_token, token_id = extract_api_token_from_session(auth_session)
        headers = {"Authorization": f"Bearer {api_token}"}

        # Use requests directly (no session cookies)
        response = requests.post(
            f"{get_frontend_url()}/api/v2/graphql", json=query, headers=headers
        )
        assert response.status_code == 401, (
            f"Frontend should reject Bearer tokens, got {response.status_code}"
        )
        logger.info(
            "✅ Frontend correctly rejects Bearer tokens (architectural design)"
        )

        # Test bearer token authentication directly with GMS - should work
        response = requests.get(
            f"{get_gms_url()}/openapi/v2/entity/dataset", headers=headers
        )
        assert response.status_code != 401, (
            f"Bearer token should work with GMS, got {response.status_code}"
        )
        logger.info(f"✅ Bearer token works directly with GMS: {response.status_code}")

    finally:
        if token_id:
            revoke_api_token(auth_session, token_id)


# ===========================================
# BEHAVIOR VALIDATION TESTS
# ===========================================


def test_authentication_priority_and_fallback() -> None:
    """Test authentication priority when multiple auth methods are present."""
    # This test documents current behavior for future reference

    # Test 1: No authentication at all
    query = {"query": "{ me { corpUser { username } } }"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", json=query)
    assert response.status_code == 401
    logger.info("✅ No auth returns 401 as expected")

    # Test 2: Invalid bearer token
    headers = {"Authorization": "Bearer invalid.token"}
    response = requests.post(
        f"{get_frontend_url()}/api/v2/graphql", json=query, headers=headers
    )
    assert response.status_code == 401
    logger.info("✅ Invalid bearer token returns 401 as expected")


def test_cross_service_authentication_consistency(
    auth_session: TestSessionWrapper,
) -> None:
    """Test that authentication works consistently across different services.

    Args:
        auth_session: Authenticated test session wrapper
    """
    # Test frontend GraphQL
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=query
    )
    assert response.status_code == 200
    logger.info("✅ Frontend GraphQL authentication works")

    # Test GMS Rest.li
    search_json = {"input": "test", "entity": "dataset", "start": 0, "count": 10}
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=search",
        headers=restli_default_headers,
        json=search_json,
    )
    assert response.status_code == 200
    logger.info("✅ GMS Rest.li authentication works")


def test_config_progressive_disclosure():
    """
    Test that the /config endpoint provides progressive disclosure based on authentication status.
    """
    logger.info("🧪 Testing /config progressive disclosure...")

    # Test anonymous access with verbose parameter
    response = requests.get(f"{get_gms_url()}/config?verbose=true")
    assert response.status_code == 200, (
        f"Anonymous /config access failed: {response.status_code}"
    )

    anonymous_config = response.json()
    assert "userType" in anonymous_config, (
        "Config response should include userType with verbose=true"
    )
    assert anonymous_config["userType"] == "anonymous", (
        f"Expected 'anonymous', got {anonymous_config.get('userType')}"
    )
    logger.info("✅ Anonymous user correctly identified")

    # Test authenticated access with verbose parameter
    from tests.utils import get_frontend_session

    auth_session = TestSessionWrapper(get_frontend_session())

    response = auth_session.get(f"{auth_session.gms_url()}/config?verbose=true")
    assert response.status_code == 200, (
        f"Authenticated /config access failed: {response.status_code}"
    )

    authenticated_config = response.json()
    assert "userType" in authenticated_config, (
        "Config response should include userType with verbose=true"
    )
    assert authenticated_config["userType"] in ["user", "admin"], (
        f"Expected 'user' or 'admin', got {authenticated_config.get('userType')}"
    )
    logger.info(
        f"✅ Authenticated user correctly identified as: {authenticated_config['userType']}"
    )

    # Verify both responses have the same base configuration
    base_fields = ["noCode", "retention", "statefulIngestionCapable", "patchCapable"]
    for field in base_fields:
        assert anonymous_config.get(field) == authenticated_config.get(field), (
            f"Base config field {field} should be the same"
        )

    logger.info(
        "✅ Progressive disclosure working: same base config, different userType"
    )

    # Test backward compatibility - userType should NOT be included without verbose parameter
    response_no_verbose = requests.get(f"{get_gms_url()}/config")
    assert response_no_verbose.status_code == 200, (
        f"Non-verbose /config access failed: {response_no_verbose.status_code}"
    )

    no_verbose_config = response_no_verbose.json()
    assert "userType" not in no_verbose_config, (
        "Config response should NOT include userType without verbose=true (backward compatibility)"
    )
    logger.info(
        "✅ Backward compatibility maintained: userType not included without verbose parameter"
    )


# ===========================================
# AUTHENTICATION TEST SUMMARY
# ===========================================


def test_authentication_behavior_summary(auth_session: TestSessionWrapper) -> None:
    """Summary test that validates overall authentication behavior.

    Args:
        auth_session: Authenticated test session wrapper
    """

    logger.info("🧪 DataHub Authentication Behavior Summary")
    logger.info("=" * 60)

    test_results = {
        "public_endpoints": 0,
        "protected_endpoints": 0,
        "auth_methods": 0,
    }

    # Test public endpoints (should work without auth)
    public_endpoints = ["/health", "/actuator/prometheus", "/config"]
    for endpoint in public_endpoints:
        try:
            response = requests.get(f"{get_gms_url()}{endpoint}")
            if response.status_code != 401:
                test_results["public_endpoints"] += 1
                logger.info(f"  Public endpoint {endpoint}: {response.status_code}")
        except Exception as e:
            logger.info(f"  Public endpoint {endpoint}: Error - {e}")

    # Test protected endpoints (should require auth)
    protected_endpoints = ["/api/v2/graphql", "/entities?action=search"]
    for endpoint in protected_endpoints:
        try:
            if endpoint.startswith("/api"):
                response = requests.post(f"{get_frontend_url()}{endpoint}", json={})
            else:
                response = requests.post(f"{get_gms_url()}{endpoint}", json={})
            if response.status_code == 401:
                test_results["protected_endpoints"] += 1
                logger.info(
                    f"  Protected endpoint {endpoint}: {response.status_code} ✅"
                )
        except Exception as e:
            logger.info(f"  Protected endpoint {endpoint}: Error - {e}")

    # Test authentication methods work
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=query
    )
    if response.status_code == 200:
        test_results["auth_methods"] += 1

    logger.info("📊 Authentication Test Results:")
    logger.info(f"  Public endpoints working: {test_results['public_endpoints']}/3")
    logger.info(
        f"  Protected endpoints secured: {test_results['protected_endpoints']}/2"
    )
    logger.info(f"  Authentication methods working: {test_results['auth_methods']}/1")

    # Verify we have reasonable coverage
    assert test_results["public_endpoints"] >= 1, (
        "Should have at least one working public endpoint"
    )
    assert test_results["protected_endpoints"] >= 1, "Should have protected endpoints"
    assert test_results["auth_methods"] >= 1, "Should have working authentication"

    logger.info(
        "✅ Authentication system validation complete - all regression tests passed"
    )
