import logging
import urllib.parse

import pytest
import requests

from tests.privileges.utils import (
    clear_polices,
    create_user,
    remove_user,
    set_base_platform_privileges_policy_status,
)
from tests.utils import (
    get_frontend_session,
    get_frontend_url,
    login_as,
    wait_for_writes_to_sync,
)

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.no_cypress_suite1

TEST_USER_EMAIL = "system.data.user@smoke.datahub.test"
TEST_USER_PASSWORD = "user"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
SYSTEM_STATE_ENTITY = "dataHubSystemState"
SYSTEM_STATE_URN = "urn:li:dataHubSystemState:smoke-test"


@pytest.fixture(scope="module", autouse=True)
def system_data_test_setup(auth_session):
    """Prepare restricted user and policies; restore on teardown.

    Depends on auth_session so conftest sets DATAHUB_GMS_TOKEN for lag polling.
    Admin GraphQL always uses a frontend cookie session — not TestSessionWrapper.
    """
    _ = auth_session
    admin = get_frontend_session()
    clear_polices(admin)
    set_base_platform_privileges_policy_status("INACTIVE", admin)
    wait_for_writes_to_sync()
    create_user(admin, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    wait_for_writes_to_sync()

    yield

    admin = get_frontend_session()
    remove_user(admin, TEST_USER_URN)
    clear_polices(admin)
    set_base_platform_privileges_policy_status("ACTIVE", admin)
    wait_for_writes_to_sync()


@pytest.fixture(scope="module")
def user_session(system_data_test_setup):
    """Cookie session for a restricted user (no PAT — base platform policy is inactive)."""
    _ = system_data_test_setup
    return login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)


def _openapi_url(path: str) -> str:
    return f"{get_frontend_url()}{path}"


def _entity_by_urn_url(urn: str) -> str:
    encoded_urn = urllib.parse.quote(urn, safe="")
    return _openapi_url(f"/openapi/v3/entity/{SYSTEM_STATE_ENTITY}/{encoded_urn}")


def _entity_get(session: requests.Session, urn: str) -> int:
    resp = session.get(_entity_by_urn_url(urn), timeout=30)
    return resp.status_code


def _entity_exists(session: requests.Session, urn: str) -> int:
    resp = session.head(_entity_by_urn_url(urn), timeout=30)
    return resp.status_code


def test_system_data_hidden_from_default_user(user_session):
    assert _entity_get(user_session, SYSTEM_STATE_URN) == 403
    assert _entity_exists(user_session, SYSTEM_STATE_URN) == 403


def test_system_data_stays_hidden_without_allow_flags(user_session):
    """Even with broad platform policies disabled, hidden system data stays hidden."""
    assert _entity_get(user_session, SYSTEM_STATE_URN) == 403


def test_system_data_write_denied_for_user(user_session):
    resp = user_session.post(
        _openapi_url(f"/openapi/v3/entity/{SYSTEM_STATE_ENTITY}"),
        params={"async": "false"},
        json=[
            {
                "urn": SYSTEM_STATE_URN,
                "dataHubSystemStateProperties": {
                    "value": {"properties": {"smoke-test": "blocked"}}
                },
            }
        ],
        timeout=30,
    )
    assert resp.status_code in {403, 422}
