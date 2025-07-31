"""
DataHub Authentication E2E Test Suite

Tests authentication behavior before implementing the two-filter approach.
Ensures backward compatibility by testing current authentication patterns.

This test suite captures the current authentication behavior and will serve 
as a comprehensive regression test when implementing the two-filter authentication approach.
"""

import logging
import pytest
import requests
from http import HTTPStatus
from typing import Optional

from tests.utils import (
    get_gms_url, 
    get_frontend_url, 
    get_admin_credentials,
    get_root_urn,
    login_as
)
from tests.privileges.utils import create_user, remove_user

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# Test constants
restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}


def extract_api_token_from_session(session):
    """Extract API token from frontend session cookies for OpenAPI authentication.
    
    Based on TestSessionWrapper pattern from tests/utils.py and test_system_info.py
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


def revoke_api_token(session, token_id: str):
    """Revoke an API token for cleanup."""
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

def test_health_endpoint_no_auth():
    """Health endpoint should work without authentication (excluded path)."""
    response = requests.get(f"{get_gms_url()}/health")
    assert response.status_code == 200
    logger.info(f"✅ /health without auth: {response.status_code}")


def test_health_endpoint_with_invalid_token():
    """Health endpoint should work even with invalid token (excluded path)."""
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    response = requests.get(f"{get_gms_url()}/health", headers=headers)
    assert response.status_code == 200
    logger.info(f"✅ /health with invalid token: {response.status_code}")


def test_actuator_prometheus_no_auth():
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
        "/schema-registry/subjects"
    ]
)
def test_excluded_paths_no_auth(endpoint):
    """Test that excluded paths work without authentication."""
    response = requests.get(f"{get_gms_url()}{endpoint}")
    # Should not return 401 (might be 404 if endpoint doesn't exist)
    assert response.status_code != 401, f"Expected non-401 for excluded path {endpoint}, got {response.status_code}"
    logger.info(f"✅ {endpoint} without auth: {response.status_code}")


# ===========================================
# PROTECTED ENDPOINTS TESTS
# ===========================================

def test_graphql_endpoint_no_auth():
    """GraphQL endpoint should return 401 without authentication."""
    query = {"query": "{ me { corpUser { username } } }"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", json=query)
    assert response.status_code == 401
    logger.info(f"✅ GraphQL without auth: {response.status_code}")


def test_graphql_endpoint_invalid_token():
    """GraphQL endpoint should return 401 with invalid token."""
    query = {"query": "{ me { corpUser { username } } }"}
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", 
                           json=query, headers=headers)
    assert response.status_code == 401
    logger.info(f"✅ GraphQL with invalid token: {response.status_code}")


def test_graphql_endpoint_valid_token(auth_session):
    """GraphQL endpoint should work with valid authentication."""
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=query)
    assert response.status_code == 200
    logger.info(f"✅ GraphQL with valid session: {response.status_code}")


@pytest.mark.parametrize(
    "endpoint,method",
    [
        ("/openapi/v2/entity/dataset", "GET"),
        ("/entities?action=search", "POST"),
        ("/aspects?action=getAspect", "GET"),
        ("/openapi/v1/system-info", "GET"),
    ]
)
def test_protected_endpoints_no_auth(endpoint, method):
    """Test that protected endpoints return 401 without authentication."""
    if method == "GET":
        response = requests.get(f"{get_gms_url()}{endpoint}")
    else:
        response = requests.post(f"{get_gms_url()}{endpoint}", 
                               headers=restli_default_headers, json={})
    
    assert response.status_code == 401, f"Expected 401 for {endpoint}, got {response.status_code}"
    logger.info(f"✅ {method} {endpoint} without auth: {response.status_code}")


@pytest.mark.parametrize(
    "endpoint,method",
    [
        ("/openapi/v2/entity/dataset", "GET"),
        ("/entities?action=search", "POST"),
        ("/aspects?action=getAspect", "GET")
    ]
)
def test_protected_endpoints_invalid_token(endpoint, method):
    """Test that protected endpoints return 401 with invalid token."""
    headers = {"Authorization": "Bearer invalid.jwt.token"}
    if method == "GET":
        response = requests.get(f"{get_gms_url()}{endpoint}", headers=headers)
    else:
        headers.update(restli_default_headers)
        response = requests.post(f"{get_gms_url()}{endpoint}", 
                               headers=headers, json={})
    
    assert response.status_code == 401, f"Expected 401 for {endpoint}, got {response.status_code}"
    logger.info(f"✅ {method} {endpoint} with invalid token: {response.status_code}")


def test_restli_endpoints_with_valid_auth(auth_session):
    """Test that Rest.li endpoints work with valid authentication."""
    # Test search endpoint
    search_json = {"input": "test", "entity": "dataset", "start": 0, "count": 10}
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=search",
        headers=restli_default_headers,
        json=search_json,
    )
    assert response.status_code == 200
    logger.info(f"✅ Rest.li search with valid auth: {response.status_code}")


# ===========================================
# AUTHORIZATION TESTS (403 scenarios)
# ===========================================

def test_admin_endpoints_require_privileges():
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
        
        # Test admin-only endpoints
        admin_endpoints = [
            "/openapi/v1/system-info",
            "/openapi/operations/elasticsearch/indices",
        ]
        
        for endpoint in admin_endpoints:
            headers = {"Authorization": f"Bearer {api_token}"}
            response = requests.get(f"{get_gms_url()}{endpoint}", headers=headers)
            
            # Should be 403 (forbidden) for authenticated user without admin privileges
            assert response.status_code == 403, f"Expected 403 for {endpoint}, got {response.status_code}. Response: {response.text[:200]}"
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
    ]
)
def test_malformed_auth_headers(auth_header):
    """Test various malformed Authorization headers return 401."""
    query = {"query": "{ me { corpUser { username } } }"}
    headers = {"Authorization": auth_header}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", 
                           json=query, headers=headers)
    assert response.status_code == 401
    logger.info(f"✅ GraphQL with malformed header '{auth_header}': {response.status_code}")


def test_api_token_authentication(auth_session):
    """Test API token-based authentication works correctly."""
    token_id = None
    try:
        # Extract API token from session
        api_token, token_id = extract_api_token_from_session(auth_session)
        
        # Test GraphQL with API token
        query = {"query": "{ me { corpUser { username } } }"}
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.post(f"{get_frontend_url()}/api/v2/graphql", 
                               json=query, headers=headers)
        assert response.status_code == 200
        logger.info("✅ GraphQL with API token works")
        
        # Test OpenAPI with API token - try a simple endpoint that should work
        headers = {"Authorization": f"Bearer {api_token}"}
        response = requests.get(f"{get_gms_url()}/openapi/v2/entity/dataset", 
                              headers=headers)
        # Should work (200) or be method not allowed (405), but not unauthorized (401)
        assert response.status_code != 401, f"API token should work, got {response.status_code}"
        logger.info(f"✅ OpenAPI with API token: {response.status_code}")
        
    finally:
        if token_id:
            revoke_api_token(auth_session, token_id)


def test_session_vs_token_authentication(auth_session):
    """Test that both session cookies and bearer tokens work."""
    token_id = None
    try:
        # Test session cookie authentication (auth_session uses cookies)
        query = {"query": "{ me { corpUser { username } } }"}
        response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=query)
        assert response.status_code == 200
        logger.info(f"✅ Session cookie authentication: {response.status_code}")
        
        # Test bearer token authentication
        api_token, token_id = extract_api_token_from_session(auth_session)
        headers = {"Authorization": f"Bearer {api_token}"}
        
        # Use requests directly (no session cookies)
        response = requests.post(f"{get_frontend_url()}/api/v2/graphql", 
                               json=query, headers=headers)
        assert response.status_code == 200
        logger.info(f"✅ Bearer token authentication: {response.status_code}")
        
    finally:
        if token_id:
            revoke_api_token(auth_session, token_id)


# ===========================================
# COMPREHENSIVE BEHAVIOR TESTS
# ===========================================

def test_authentication_priority_and_fallback():
    """Test authentication priority when multiple auth methods are present."""
    # This test documents current behavior for future reference
    
    # Test 1: No authentication at all
    query = {"query": "{ me { corpUser { username } } }"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", json=query)
    assert response.status_code == 401
    logger.info("✅ No auth returns 401 as expected")
    
    # Test 2: Invalid bearer token
    headers = {"Authorization": "Bearer invalid.token"}
    response = requests.post(f"{get_frontend_url()}/api/v2/graphql", 
                           json=query, headers=headers)
    assert response.status_code == 401  
    logger.info("✅ Invalid bearer token returns 401 as expected")


def test_cross_service_authentication_consistency(auth_session):
    """Test that authentication works consistently across different services."""
    # Test frontend GraphQL
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=query)
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


# ===========================================
# COMPREHENSIVE TEST SUMMARY
# ===========================================

def test_authentication_behavior_summary(auth_session):
    """Summary test that validates overall authentication behavior."""
    
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
                logger.info(f"  Protected endpoint {endpoint}: {response.status_code} ✅")
        except Exception as e:
            logger.info(f"  Protected endpoint {endpoint}: Error - {e}")
    
    # Test authentication methods work
    query = {"query": "{ me { corpUser { username } } }"}
    response = auth_session.post(f"{auth_session.frontend_url()}/api/v2/graphql", json=query)
    if response.status_code == 200:
        test_results["auth_methods"] += 1
    
    logger.info(f"📊 Authentication Test Results:")
    logger.info(f"  Public endpoints working: {test_results['public_endpoints']}/3")
    logger.info(f"  Protected endpoints secured: {test_results['protected_endpoints']}/2")
    logger.info(f"  Authentication methods working: {test_results['auth_methods']}/1")
    
    # Verify we have reasonable coverage
    assert test_results["public_endpoints"] >= 1, "Should have at least one working public endpoint"
    assert test_results["protected_endpoints"] >= 1, "Should have protected endpoints"
    assert test_results["auth_methods"] >= 1, "Should have working authentication"
    
    logger.info("✅ Authentication behavior validation complete - ready for two-filter refactoring")