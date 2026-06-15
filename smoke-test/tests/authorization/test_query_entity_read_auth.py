"""
E2E authorization smoke tests for AC-003 Query entity read visibility (subject-derived).

Requires ``VIEW_AUTHORIZATION_ENABLED=true`` on GMS; skipped when view authorization is off.
"""

import logging
import time
import uuid

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetPropertiesClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
)
from datahub.metadata.urns import CorpUserUrn, QueryUrn
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

pytestmark = pytest.mark.no_cypress_suite1

_UNIQUE = uuid.uuid4().hex[:8]
TEST_USER_EMAIL = f"query.auth.test.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

SUBJECT_DATASET_URN = (
    f"urn:li:dataset:(urn:li:dataPlatform:kafka,query-subject-{_UNIQUE},PROD)"
)
QUERY_ID = f"auth-query-{_UNIQUE}"
QUERY_ENTITY_URN = str(QueryUrn(QUERY_ID))
SENSITIVE_SQL = "SELECT secret_col FROM sensitive_table"

GET_QUERY_ENTITY = """
query entity($urn: String!) {
  entity(urn: $urn) {
    urn
    ... on QueryEntity {
      properties {
        statement { value }
      }
    }
  }
}
"""


@pytest.fixture(scope="module", autouse=True)
def query_auth_setup(graph_client, auth_session):
    if not is_view_authorization_enabled(auth_session):
        pytest.skip(
            "VIEW_AUTHORIZATION_ENABLED is false; "
            "query entity read authorization tests require view authorization"
        )

    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=SUBJECT_DATASET_URN,
            aspect=DatasetPropertiesClass(
                name=f"query-subject-{_UNIQUE}",
                description="Subject dataset for Query entity auth test",
            ),
        )
    )

    now = int(time.time() * 1000)
    actor = CorpUserUrn("datahub")
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=QUERY_ENTITY_URN,
            aspect=QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=SENSITIVE_SQL, language=QueryLanguageClass.SQL
                ),
                source=QuerySourceClass.MANUAL,
                name=f"Auth test query {_UNIQUE}",
                created=AuditStampClass(time=now, actor=str(actor)),
                lastModified=AuditStampClass(time=now, actor=str(actor)),
            ),
        )
    )
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=QUERY_ENTITY_URN,
            aspect=QuerySubjectsClass(
                subjects=[QuerySubjectClass(entity=SUBJECT_DATASET_URN)]
            ),
        )
    )
    wait_for_writes_to_sync()

    admin_session = get_frontend_session()
    clear_polices(admin_session)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync()

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    yield

    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync()

    for urn in [QUERY_ENTITY_URN, SUBJECT_DATASET_URN]:
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning("Failed to delete %s during cleanup", urn)


@with_test_retry(max_attempts=10)
def _fetch_query_entity(email: str, password: str) -> dict:
    user_session = login_as(email, password)
    payload = {"query": GET_QUERY_ENTITY, "variables": {"urn": QUERY_ENTITY_URN}}
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    return response.json()


def test_query_entity_hidden_without_subject_view():
    """AC-003 read: Query entity SQL not returned without VIEW_ENTITY_PAGE on subject dataset."""
    res = _fetch_query_entity(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    entity = (res.get("data") or {}).get("entity")
    if entity is None:
        return
    props = entity.get("properties") or {}
    statement = (props.get("statement") or {}).get("value")
    assert statement != SENSITIVE_SQL, res


def test_query_entity_visible_with_subject_view(auth_session):
    """AC-003 read: Query entity SQL visible when user can view subject dataset."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test VIEW subject {_UNIQUE}",
        description="Grant VIEW_ENTITY_PAGE on subject dataset",
        privileges=["VIEW_ENTITY_PAGE", "GET_ENTITY_PRIVILEGE"],
        user_urn=TEST_USER_URN,
        resource_urn=SUBJECT_DATASET_URN,
    )

    res = _fetch_query_entity(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    entity = (res.get("data") or {}).get("entity")
    assert entity is not None, res
    statement = entity.get("properties", {}).get("statement", {}).get("value")
    assert statement == SENSITIVE_SQL, res

    remove_policy(policy_urn, admin_session)


def test_query_entity_visible_with_edit_queries_on_subject(auth_session):
    """AC-003 read: Query entity SQL visible when user can edit queries on subject dataset."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test EDIT_ENTITY_QUERIES subject {_UNIQUE}",
        description="Grant EDIT_ENTITY_QUERIES on subject dataset",
        privileges=["EDIT_ENTITY_QUERIES"],
        user_urn=TEST_USER_URN,
        resource_urn=SUBJECT_DATASET_URN,
    )

    res = _fetch_query_entity(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    entity = (res.get("data") or {}).get("entity")
    assert entity is not None, res
    statement = entity.get("properties", {}).get("statement", {}).get("value")
    assert statement == SENSITIVE_SQL, res

    remove_policy(policy_urn, admin_session)
