"""
E2E authorization smoke tests for container GraphQL view redaction (VBAC).

Requires ``VIEW_AUTHORIZATION_ENABLED=true`` on GMS; skipped when view authorization is off.

Covers:
  - Direct ``container(urn)`` loads field-strip unauthorized containers (real URN retained)
  - VIEW_ENTITY_PAGE on the container restores properties (e.g. description)
  - ``parentContainers`` still returns unauthorized parents as field-stripped stubs
"""

import logging
import uuid

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import ContainerClass, ContainerPropertiesClass
from tests.authorization.utils import is_view_authorization_enabled
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    clear_polices,
    create_metadata_policy,
    create_user,
    remove_policy,
    remove_user,
    set_base_platform_privileges_policy_status,
    set_view_dataset_sensitive_info_policy_status,
    set_view_entity_profile_privileges_policy_status,
)
from tests.utils import (
    get_frontend_session,
    get_frontend_url,
    login_as,
    with_test_retry,
)

logger = logging.getLogger(__name__)

pytestmark = [pytest.mark.no_cypress_suite1, pytest.mark.global_policy_mutator]

_UNIQUE = uuid.uuid4().hex[:8]
TEST_USER_EMAIL = f"container.view.auth.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

PARENT_CONTAINER_ID = f"vbac-parent-{_UNIQUE}"
CHILD_CONTAINER_ID = f"vbac-child-{_UNIQUE}"
PARENT_CONTAINER_URN = f"urn:li:container:{PARENT_CONTAINER_ID}"
CHILD_CONTAINER_URN = f"urn:li:container:{CHILD_CONTAINER_ID}"

PARENT_NAME = f"VBAC Parent {_UNIQUE}"
PARENT_DESCRIPTION = f"sensitive-parent-description-{_UNIQUE}"
CHILD_NAME = f"VBAC Child {_UNIQUE}"
CHILD_DESCRIPTION = f"sensitive-child-description-{_UNIQUE}"

GET_CONTAINER = """
query container($urn: String!) {
  container(urn: $urn) {
    urn
    properties {
      name
      description
    }
  }
}
"""

GET_PARENT_CONTAINERS = """
query parentContainersOnContainer($urn: String!) {
  container(urn: $urn) {
    urn
    parentContainers {
      count
      containers {
        urn
        properties {
          name
          description
        }
      }
    }
  }
}
"""

CONTAINER_VIEW_POLICY_PREFIXES = ["Test VIEW container"]


@pytest.fixture(scope="module", autouse=True)
def container_view_auth_setup(graph_client, auth_session):
    yield from _container_view_auth_setup_impl(graph_client, auth_session)


def _container_view_auth_setup_impl(graph_client, auth_session):
    if not is_view_authorization_enabled(auth_session):
        pytest.skip(
            "VIEW_AUTHORIZATION_ENABLED is false; "
            "container view authorization tests require view authorization"
        )

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=PARENT_CONTAINER_URN,
            aspect=ContainerPropertiesClass(
                name=PARENT_NAME,
                description=PARENT_DESCRIPTION,
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CHILD_CONTAINER_URN,
            aspect=ContainerPropertiesClass(
                name=CHILD_NAME,
                description=CHILD_DESCRIPTION,
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=CHILD_CONTAINER_URN,
            aspect=ContainerClass(container=PARENT_CONTAINER_URN),
        )
    )
    wait_for_writes_to_sync(mcp_only=True)

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefixes=CONTAINER_VIEW_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    yield

    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session, name_prefixes=CONTAINER_VIEW_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

    for urn in [CHILD_CONTAINER_URN, PARENT_CONTAINER_URN]:
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning("Failed to delete %s during cleanup", urn)


@with_test_retry(max_attempts=10)
def _fetch_container(email: str, password: str, urn: str) -> dict:
    user_session = login_as(email, password)
    payload = {"query": GET_CONTAINER, "variables": {"urn": urn}}
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


@with_test_retry(max_attempts=10)
def _parent_container_props(email: str, password: str) -> tuple[dict, dict]:
    """Return (full response, parent properties) once hierarchy includes the parent."""
    user_session = login_as(email, password)
    payload = {
        "query": GET_PARENT_CONTAINERS,
        "variables": {"urn": CHILD_CONTAINER_URN},
    }
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    res = response.json()
    container = (res.get("data") or {}).get("container")
    assert container is not None, res
    parents = (container.get("parentContainers") or {}).get("containers") or []
    parent_by_urn = {entry.get("urn"): entry for entry in parents if entry}
    assert PARENT_CONTAINER_URN in parent_by_urn, res
    return res, parent_by_urn[PARENT_CONTAINER_URN].get("properties") or {}


def test_container_field_stripped_without_view():
    """Unauthorized container loads keep the real URN but strip description."""
    res = _fetch_container(TEST_USER_EMAIL, TEST_USER_PASSWORD, PARENT_CONTAINER_URN)
    container = (res.get("data") or {}).get("container")
    assert container is not None, res
    assert container.get("urn") == PARENT_CONTAINER_URN, res
    props = container.get("properties") or {}
    assert props.get("name") == PARENT_NAME, res
    assert props.get("description") != PARENT_DESCRIPTION, res
    assert props.get("description") in (None, ""), res


def test_container_visible_with_view(auth_session):
    """VIEW_ENTITY_PAGE on the container restores the description."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test VIEW container {_UNIQUE}",
        description="Grant VIEW_ENTITY_PAGE on parent container",
        privileges=["VIEW_ENTITY_PAGE", "GET_ENTITY_PRIVILEGE"],
        user_urn=TEST_USER_URN,
        resource_urn=PARENT_CONTAINER_URN,
    )

    try:
        res = _fetch_container(
            TEST_USER_EMAIL, TEST_USER_PASSWORD, PARENT_CONTAINER_URN
        )
        container = (res.get("data") or {}).get("container")
        assert container is not None, res
        assert container.get("urn") == PARENT_CONTAINER_URN, res
        props = container.get("properties") or {}
        assert props.get("description") == PARENT_DESCRIPTION, res
    finally:
        remove_policy(policy_urn, admin_session)


def test_parent_containers_returns_stripped_unauthorized_parent():
    """parentContainers keeps unauthorized parents as field-stripped stubs."""
    res, props = _parent_container_props(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    assert props.get("name") == PARENT_NAME, res
    assert props.get("description") != PARENT_DESCRIPTION, res
    assert props.get("description") in (None, ""), res


def test_parent_containers_visible_with_view_on_parent(auth_session):
    """VIEW on the parent restores description via parentContainers stubs."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test VIEW container parent {_UNIQUE}",
        description="Grant VIEW_ENTITY_PAGE on parent for parentContainers",
        privileges=["VIEW_ENTITY_PAGE", "GET_ENTITY_PRIVILEGE"],
        user_urn=TEST_USER_URN,
        resource_urn=PARENT_CONTAINER_URN,
    )

    try:
        res, props = _parent_container_props(TEST_USER_EMAIL, TEST_USER_PASSWORD)
        assert props.get("description") == PARENT_DESCRIPTION, res
    finally:
        remove_policy(policy_urn, admin_session)
