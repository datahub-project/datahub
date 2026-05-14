"""
Smoke tests verifying entity-level authorization is enforced on Change History
(Timeline) code paths end-to-end.

Covers:
  - GraphQL getTimeline denied for unauthorized user on Dataset and Domain
  - GraphQL getTimeline succeeds for admin on Dataset (happy path)
  - REST /openapi/v2/timeline/v1/{urn} denied for unauthorized user (HTTP 401/403)
"""

import logging
import urllib.parse
import uuid

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    DomainPropertiesClass,
)
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    clear_polices,
    create_user,
    remove_user,
    set_base_platform_privileges_policy_status,
    set_view_dataset_sensitive_info_policy_status,
    set_view_entity_profile_privileges_policy_status,
)
from tests.utils import get_frontend_session, get_frontend_url, login_as

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# ---------------------------------------------------------------------------
# Constants — unique per test run so the test is idempotent
# ---------------------------------------------------------------------------
_UNIQUE = uuid.uuid4().hex[:8]

TEST_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,auth-test-{_UNIQUE},PROD)"
)
TEST_DOMAIN_URN = f"urn:li:domain:auth-test-domain-{_UNIQUE}"

TEST_USER_EMAIL = f"timeline.auth.test.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

GET_TIMELINE_QUERY = """
query getTimeline($input: GetTimelineInput!) {
    getTimeline(input: $input) {
        changeTransactions {
            timestampMillis
            lastSemanticVersion
            versionStamp
            changeType
            actor
            changes {
                urn
                category
                operation
            }
        }
    }
}
"""


# ---------------------------------------------------------------------------
# Module fixture: create entities, lock down policies, create restricted user
# ---------------------------------------------------------------------------
@pytest.fixture(scope="module", autouse=True)
def auth_test_setup(graph_client, auth_session):
    """
    Setup:
      1. Create test Dataset + Domain via the graph client (as admin).
      2. Disable the three "All Users" default policies so the test user has
         no VIEW_ENTITY_PAGE / GET_TIMELINE_PRIVILEGE by default.
      3. Create the restricted test user.

    Teardown restores policies and deletes created entities.
    """
    logger.info("auth_test_setup: creating test entities")

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TEST_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"auth-test-{_UNIQUE}",
                description="Dataset for timeline auth smoke test",
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=TEST_DOMAIN_URN,
            aspect=DomainPropertiesClass(
                name=f"Auth Test Domain {_UNIQUE}",
                description="Domain for timeline auth smoke test",
            ),
        )
    )
    wait_for_writes_to_sync()

    # Need a fresh admin session (not the shared auth_session fixture) so
    # cookie state is isolated from policy mutations.
    admin_session = get_frontend_session()

    logger.info("auth_test_setup: clearing leftover test policies")
    clear_polices(admin_session)

    logger.info("auth_test_setup: disabling All-Users default policies")
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync()

    logger.info(f"auth_test_setup: creating restricted user {TEST_USER_EMAIL}")
    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)

    yield

    logger.info("auth_test_setup: teardown — restoring policies and cleaning up")
    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session)

    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync()

    for urn in [TEST_DATASET_URN, TEST_DOMAIN_URN]:
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning(f"auth_test_setup: failed to delete {urn} during cleanup")


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------
def _graphql_as_user(email: str, password: str, urn: str) -> dict:
    """Log in as ``email`` and execute getTimeline against ``urn``.

    Uses the raw frontend session (cookie auth) directly rather than
    ``TestSessionWrapper``. The wrapper's ``__init__`` mints a PAT via
    ``createAccessToken``, which requires ``GENERATE_PERSONAL_ACCESS_TOKENS``
    — a privilege the restricted test user does not have (setup disables the
    All-Users base platform policy), causing tenacity to retry for ~156s.

    Returns the raw response JSON so callers can inspect errors without
    raising (unlike execute_graphql which asserts on errors).
    """
    user_session = login_as(email, password)
    payload = {
        "query": GET_TIMELINE_QUERY,
        "variables": {"input": {"urn": urn}},
    }
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
@pytest.mark.parametrize(
    "urn,entity_label",
    [
        (TEST_DATASET_URN, "Dataset"),
        (TEST_DOMAIN_URN, "Domain"),
    ],
)
def test_get_timeline_unauthorized_user_is_denied(urn, entity_label):
    """Unauthorized user must receive an authorization error from getTimeline."""
    logger.info(
        f"test_get_timeline_unauthorized_user_is_denied: {entity_label} ({urn})"
    )
    res = _graphql_as_user(TEST_USER_EMAIL, TEST_USER_PASSWORD, urn)

    errors = res.get("errors", [])
    assert errors, (
        f"[{entity_label}] Expected authorization errors in GraphQL response but got none. "
        f"Response: {res}"
    )

    # At least one error must be an auth/unauthorized error.
    error_messages = " ".join((e.get("message") or "") for e in errors).lower()
    assert any(
        keyword in error_messages
        for keyword in ("unauthorized", "forbidden", "not authorized", "permission")
    ), f"[{entity_label}] Expected an authorization error but got: {errors}"
    logger.info(
        f"[{entity_label}] Correctly denied — errors: {[e.get('message') for e in errors]}"
    )


def test_get_timeline_authorized_user_succeeds(auth_session):
    """Admin should receive a valid (non-error) getTimeline response for the Dataset."""
    logger.info(
        f"test_get_timeline_authorized_user_succeeds: Dataset ({TEST_DATASET_URN})"
    )
    payload = {
        "query": GET_TIMELINE_QUERY,
        "variables": {"input": {"urn": TEST_DATASET_URN}},
    }
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=payload
    )
    response.raise_for_status()
    res = response.json()

    assert "errors" not in res, (
        f"Admin getTimeline returned unexpected errors: {res.get('errors')}"
    )
    assert res.get("data") is not None, "Admin getTimeline returned null data"
    assert "getTimeline" in res["data"], (
        f"Admin getTimeline response missing getTimeline field: {res}"
    )
    logger.info("test_get_timeline_authorized_user_succeeds: admin access confirmed")


def test_get_timeline_rest_v2_unauthorized_user_is_denied():
    """Unauthorized user must be denied on the REST /openapi/v2/timeline/v1/{urn} endpoint."""
    encoded_urn = urllib.parse.quote(TEST_DATASET_URN, safe="")
    # `categories` is a required query param on the REST endpoint; without it Spring
    # returns 400 before the auth check fires, which would mask the auth behaviour.
    #
    # Route through the frontend proxy (`/openapi/*` → Application.proxy in
    # datahub-frontend/conf/routes) so cookie-based session auth works. Hitting
    # GMS directly would require a PAT, which this user cannot generate (see
    # _graphql_as_user).
    url = (
        f"{get_frontend_url()}/openapi/v2/timeline/v1/{encoded_urn}"
        f"?categories=TECHNICAL_SCHEMA"
    )
    logger.info(f"test_get_timeline_rest_v2_unauthorized_user_is_denied: GET {url}")

    user_session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    response = user_session.get(url)

    assert response.status_code in (401, 403), (
        f"Expected HTTP 401 or 403 from REST timeline endpoint for unauthorized user, "
        f"got {response.status_code}. Body: {response.text[:500]}"
    )
    logger.info(
        f"test_get_timeline_rest_v2_unauthorized_user_is_denied: "
        f"correctly denied with HTTP {response.status_code}"
    )
