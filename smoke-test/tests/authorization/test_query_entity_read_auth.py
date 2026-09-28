"""
E2E authorization smoke tests for Query entity read visibility (subject-derived).

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
    pytest.mark.p0,
]

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

GET_SUBJECT_DATASET = """
query dataset($urn: String!) {
  dataset(urn: $urn) { urn }
}
"""

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


QUERY_AUTH_POLICY_PREFIXES = ["Test VIEW", "Test EDIT_ENTITY_QUERIES"]


@pytest.fixture(scope="module", autouse=True)
def query_auth_setup(graph_client, auth_session):
    yield from _query_auth_setup_impl(graph_client, auth_session)


def _query_auth_setup_impl(graph_client, auth_session):
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
    wait_for_writes_to_sync(mcp_only=True)

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefixes=QUERY_AUTH_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)
    yield

    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session, name_prefixes=QUERY_AUTH_POLICY_PREFIXES)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)

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


@with_test_retry(max_attempts=10)
def _wait_until_subject_dataset_visible():
    """Proves a freshly granted VIEW_ENTITY_PAGE policy has reached the policy cache: under
    view authorization the subject dataset itself is hidden until it does. Call this before a
    negative assertion so "hidden" cannot be satisfied by a policy that isn't live yet."""
    user_session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    payload = {"query": GET_SUBJECT_DATASET, "variables": {"urn": SUBJECT_DATASET_URN}}
    response = user_session.post(f"{get_frontend_url()}/api/v2/graphql", json=payload)
    response.raise_for_status()
    res = response.json()
    assert (res.get("data") or {}).get("dataset") is not None, res


@with_test_retry(max_attempts=10)
def _assert_query_sql_hidden():
    res = _fetch_query_entity(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    entity = (res.get("data") or {}).get("entity")
    if entity is None:
        return
    props = entity.get("properties") or {}
    statement = (props.get("statement") or {}).get("value")
    assert statement != SENSITIVE_SQL, res


@with_test_retry(max_attempts=10)
def _assert_query_sql_visible():
    res = _fetch_query_entity(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    entity = (res.get("data") or {}).get("entity")
    assert entity is not None, res
    statement = entity.get("properties", {}).get("statement", {}).get("value")
    assert statement == SENSITIVE_SQL, res


def test_query_entity_hidden_without_subject_view():
    """Query entity SQL not returned for a user holding no privilege on the subject dataset."""
    _assert_query_sql_hidden()


def test_query_entity_hidden_with_only_view_entity_page_on_subject(auth_session):
    """VIEW_ENTITY_PAGE on the subject dataset no longer reveals the query's SQL: query
    reads require VIEW_ENTITY_QUERIES (or a privilege implying it). Upgraded policies get
    it backfilled; a policy created afterwards must grant it explicitly."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test VIEW subject {_UNIQUE}",
        description="Grant VIEW_ENTITY_PAGE on subject dataset",
        privileges=["VIEW_ENTITY_PAGE", "GET_ENTITY_PRIVILEGE"],
        user_urn=TEST_USER_URN,
        resource_urn=SUBJECT_DATASET_URN,
    )
    try:
        _wait_until_subject_dataset_visible()
        _assert_query_sql_hidden()
    finally:
        remove_policy(policy_urn, admin_session)


def test_query_entity_visible_with_view_entity_queries_on_subject(auth_session):
    """Query entity SQL visible when user holds VIEW_ENTITY_QUERIES on the subject dataset."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"Test VIEW_ENTITY_QUERIES subject {_UNIQUE}",
        description="Grant VIEW_ENTITY_QUERIES on subject dataset",
        privileges=["VIEW_ENTITY_QUERIES"],
        user_urn=TEST_USER_URN,
        resource_urn=SUBJECT_DATASET_URN,
    )
    try:
        _assert_query_sql_visible()
    finally:
        remove_policy(policy_urn, admin_session)


def test_query_entity_visible_with_edit_queries_on_subject(auth_session):
    """Query entity SQL visible when user can edit queries on subject dataset."""
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
