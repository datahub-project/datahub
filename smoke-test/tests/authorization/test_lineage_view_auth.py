"""
E2E authorization smoke tests for GraphQL lineage under view-based access control.

Requires ``VIEW_AUTHORIZATION_ENABLED=true`` on GMS; skipped when view authorization is off.

Covers:
  - ``dataset.lineage`` returns a neighbor the user may not view as a ``Restricted``
    entity with an encrypted URN, never as a typed entity with its real URN
  - VIEW_ENTITY_PAGE on the neighbor restores the real entity
  - Lineage requested on a dataset the user may not view is empty
"""

import logging
import uuid
from typing import Any, Dict

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    DatasetPropertiesClass,
    UpstreamClass,
    UpstreamLineageClass,
)
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
from tests.utilities.domains import Domain
from tests.utils import (
    get_frontend_session,
    get_frontend_url,
    login_as,
    with_test_retry,
)

logger = logging.getLogger(__name__)

pytestmark = [
    pytest.mark.no_cypress_suite1,
    pytest.mark.global_policy_mutator,
    pytest.mark.domain(Domain.PLATFORM),
]

_UNIQUE = uuid.uuid4().hex[:8]
TEST_USER_EMAIL = f"lineage.view.auth.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

# VISIBLE is downstream of HIDDEN; the user is granted VIEW on VISIBLE only.
VISIBLE_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:hive,vbac_visible_{_UNIQUE},PROD)"
)
HIDDEN_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:hive,vbac_hidden_{_UNIQUE},PROD)"
)
LINEAGE_VIEW_POLICY_PREFIXES = ["Test VIEW lineage"]

GET_LINEAGE = """
query lineage($urn: String!, $direction: LineageDirection!) {
  dataset(urn: $urn) {
    urn
    lineage(input: { direction: $direction, start: 0, count: 10 }) {
      total
      relationships {
        type
        degree
        entity {
          urn
          type
        }
      }
    }
  }
}
"""


@pytest.fixture(scope="module", autouse=True)
def lineage_view_auth_setup(graph_client, auth_session):
    if not is_view_authorization_enabled(auth_session):
        pytest.skip(
            "VIEW_AUTHORIZATION_ENABLED is false; "
            "lineage view authorization tests require view authorization"
        )

    for urn in (VISIBLE_DATASET_URN, HIDDEN_DATASET_URN):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetPropertiesClass(name=f"vbac-{_UNIQUE}"),
            )
        )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=VISIBLE_DATASET_URN,
            aspect=UpstreamLineageClass(
                upstreams=[
                    UpstreamClass(
                        dataset=HIDDEN_DATASET_URN,
                        type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                ]
            ),
        )
    )
    wait_for_writes_to_sync()

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefixes=LINEAGE_VIEW_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    yield

    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session, name_prefixes=LINEAGE_VIEW_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

    for urn in (VISIBLE_DATASET_URN, HIDDEN_DATASET_URN):
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning("Failed to delete %s during cleanup", urn)


def _grant_view(admin_session, resource_urn: str, label: str) -> str:
    return create_metadata_policy(
        admin_session,
        name=f"Test VIEW lineage {label} {_UNIQUE}",
        description=f"Grant VIEW_ENTITY_PAGE on {label}",
        privileges=["VIEW_ENTITY_PAGE", "GET_ENTITY_PRIVILEGE"],
        user_urn=TEST_USER_URN,
        resource_urn=resource_urn,
    )


def _query_lineage(urn: str, direction: str) -> Dict[str, Any]:
    user_session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    response = user_session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": GET_LINEAGE, "variables": {"urn": urn, "direction": direction}},
    )
    response.raise_for_status()
    res = response.json()
    assert not res.get("errors"), res
    return res


@with_test_retry(max_attempts=10)
def _assert_upstream_is_restricted() -> None:
    """Retry until policy cache and lineage graph reflect VIEW on the source only."""
    res = _query_lineage(VISIBLE_DATASET_URN, "UPSTREAM")
    lineage = res["data"]["dataset"]["lineage"]
    assert lineage["total"] == 1, res
    entity = lineage["relationships"][0]["entity"]
    assert entity["type"] == "RESTRICTED", res
    assert entity["urn"].startswith("urn:li:restricted:"), res
    assert HIDDEN_DATASET_URN not in entity["urn"], res


@with_test_retry(max_attempts=10)
def _assert_upstream_is_visible() -> None:
    res = _query_lineage(VISIBLE_DATASET_URN, "UPSTREAM")
    lineage = res["data"]["dataset"]["lineage"]
    assert lineage["total"] == 1, res
    entity = lineage["relationships"][0]["entity"]
    assert entity["type"] == "DATASET", res
    assert entity["urn"] == HIDDEN_DATASET_URN, res


@with_test_retry(max_attempts=10)
def _assert_downstream_of_hidden_is_empty() -> None:
    res = _query_lineage(HIDDEN_DATASET_URN, "DOWNSTREAM")
    lineage = res["data"]["dataset"]["lineage"]
    assert lineage["total"] == 0, res
    assert lineage["relationships"] == [], res


def test_lineage_hides_unviewable_neighbor_as_restricted():
    """With VIEW on the source only, the upstream neighbor is a Restricted placeholder."""
    admin_session = get_frontend_session()
    policy_urn = _grant_view(admin_session, VISIBLE_DATASET_URN, "source")
    try:
        _assert_upstream_is_restricted()
    finally:
        remove_policy(policy_urn, admin_session)


def test_lineage_shows_neighbor_with_view_on_both():
    """VIEW on both endpoints restores the real neighbor."""
    admin_session = get_frontend_session()
    source_policy = _grant_view(admin_session, VISIBLE_DATASET_URN, "source")
    neighbor_policy = _grant_view(admin_session, HIDDEN_DATASET_URN, "neighbor")
    try:
        _assert_upstream_is_visible()
    finally:
        remove_policy(neighbor_policy, admin_session)
        remove_policy(source_policy, admin_session)


def test_lineage_of_unviewable_source_is_empty():
    """Lineage on a dataset the user may not view discloses nothing."""
    _assert_downstream_of_hidden_is_empty()
