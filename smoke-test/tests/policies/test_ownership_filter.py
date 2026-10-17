"""
Smoke tests for the ownership-filter feature.

Verifies that when the ownership filter is enabled, a non-admin user can only
see datasets they own in search results, while an admin user can see all
datasets regardless of ownership.

Setup:
  - Two datasets are ingested via fixture: ds-alice (owned by alice) and
    ds-bob (owned by bob).
  - Two native users are created via the DataHub signup flow: alice and bob.
  - Each user is granted only the minimal privileges needed to authenticate
    and run a search (VIEW_ENTITY_PAGE / SEARCH_PRIVILEGE come from the
    default "All Users - View Entity Page" policy which is left active).

Teardown:
  - Datasets are hard-deleted via graph_client.
  - Native users are removed via the removeUser mutation.

The tests use the raw ``login_as`` session (cookie-based, no PAT) to avoid
triggering the PAT-generation privilege check that ``TestSessionWrapper``
performs inside its constructor.  This mirrors the pattern used in
``timeline_change_history_auth_test.py``.

Running (requires a live DataHub environment):
  cd smoke-test
  pytest tests/policies/test_ownership_filter.py -v
"""

import logging
import uuid

import pytest

from tests.privileges.utils import create_user, remove_user
from tests.utils import (
    delete_urns_from_file,
    get_frontend_session,
    get_frontend_url,
    ingest_file_via_rest,
    login_as,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_UNIQUE = uuid.uuid4().hex[:8]

_FIXTURE_FILE = "tests/policies/data/ownership_filter_setup.json"

_ALICE_EMAIL = "alice.ownerfilter@smoke.datahub.test"
_BOB_EMAIL = "bob.ownerfilter@smoke.datahub.test"
_ALICE_PASSWORD = "AliceOwnerFilter1!"
_BOB_PASSWORD = "BobOwnerFilter1!"

_ALICE_URN = f"urn:li:corpuser:{_ALICE_EMAIL}"
_BOB_URN = f"urn:li:corpuser:{_BOB_EMAIL}"

# These URNs must match the ownership in the fixture JSON file.
_DS_ALICE_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,ds-alice,PROD)"
_DS_BOB_URN = "urn:li:dataset:(urn:li:dataPlatform:mysql,ds-bob,PROD)"

_SEARCH_QUERY = """
{
    searchAcrossEntities(input: {
        types: [DATASET],
        query: "*",
        start: 0,
        count: 200,
        orFilters: [
            {
                and: [
                    {
                        field: "customProperties",
                        values: ["ownership_filter_test"],
                        condition: CONTAIN
                    }
                ]
            }
        ]
    }) {
        searchResults {
            entity {
                urn
            }
        }
    }
}
"""


# ---------------------------------------------------------------------------
# Module fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def ownership_filter_setup(auth_session, graph_client):
    """
    Ingest test datasets and create alice + bob as native users.

    The fixture is module-scoped so all tests in this file share the same
    environment without re-running the expensive setup/teardown for each
    individual test.
    """
    admin_session = get_frontend_session()

    # Remove pre-existing users from a previous failed run for idempotency.
    remove_user(admin_session, _ALICE_URN)
    remove_user(admin_session, _BOB_URN)

    # Ingest the datasets (idempotent upsert).
    logger.info("ownership_filter_setup: deleting stale fixture data")
    delete_urns_from_file(graph_client, _FIXTURE_FILE)

    logger.info("ownership_filter_setup: ingesting fixture datasets")
    ingest_file_via_rest(auth_session, _FIXTURE_FILE)

    # Create alice and bob via the invite-token / signUp flow.
    logger.info(f"ownership_filter_setup: creating user {_ALICE_EMAIL}")
    admin_session = create_user(admin_session, _ALICE_EMAIL, _ALICE_PASSWORD)

    logger.info(f"ownership_filter_setup: creating user {_BOB_EMAIL}")
    admin_session = create_user(admin_session, _BOB_EMAIL, _BOB_PASSWORD)

    yield

    # Teardown: remove users and datasets.
    logger.info("ownership_filter_setup: teardown — removing users and datasets")
    remove_user(admin_session, _ALICE_URN)
    remove_user(admin_session, _BOB_URN)
    delete_urns_from_file(graph_client, _FIXTURE_FILE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _search_urns_as(email: str, password: str) -> list:
    """Log in as a non-admin user and run the ownership-scoped search.

    Uses the raw cookie-based session (no PAT) to avoid requiring the
    GENERATE_PERSONAL_ACCESS_TOKENS privilege for the test user.
    """
    user_session = login_as(email, password)
    payload = {"query": _SEARCH_QUERY}
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    data = response.json()
    assert "errors" not in data, f"GraphQL errors for {email}: {data.get('errors')}"
    results = data["data"]["searchAcrossEntities"]["searchResults"]
    return [r["entity"]["urn"] for r in results]


def _search_urns_as_admin(auth_session) -> list:
    """Run the ownership-scoped search as the admin (uses TestSessionWrapper PAT)."""
    payload = {"query": _SEARCH_QUERY}
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=payload
    )
    response.raise_for_status()
    data = response.json()
    assert "errors" not in data, f"GraphQL errors for admin: {data.get('errors')}"
    results = data["data"]["searchAcrossEntities"]["searchResults"]
    return [r["entity"]["urn"] for r in results]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_alice_only_sees_alice_dataset():
    """Alice (owns ds-alice) must see her dataset but not Bob's."""
    urns = _search_urns_as(_ALICE_EMAIL, _ALICE_PASSWORD)
    assert any("ds-alice" in u for u in urns), (
        f"alice should see ds-alice in search results; got: {urns}"
    )
    assert not any("ds-bob" in u for u in urns), (
        f"alice should NOT see ds-bob in search results; got: {urns}"
    )


def test_bob_only_sees_bob_dataset():
    """Bob (owns ds-bob) must see his dataset but not Alice's."""
    urns = _search_urns_as(_BOB_EMAIL, _BOB_PASSWORD)
    assert any("ds-bob" in u for u in urns), (
        f"bob should see ds-bob in search results; got: {urns}"
    )
    assert not any("ds-alice" in u for u in urns), (
        f"bob should NOT see ds-alice in search results; got: {urns}"
    )


def test_admin_sees_both_datasets(auth_session):
    """Admin bypass must return both datasets in search results."""
    urns = _search_urns_as_admin(auth_session)
    assert any("ds-alice" in u for u in urns), (
        f"admin should see ds-alice in search results; got: {urns}"
    )
    assert any("ds-bob" in u for u in urns), (
        f"admin should see ds-bob in search results; got: {urns}"
    )
