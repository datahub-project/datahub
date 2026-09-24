"""
E2E smoke tests for the VIEW_ENTITY_QUERIES / VIEW_ALL_QUERIES privileges.

Runs under the stock configuration (``QUERY_ENTITY_AUTHORIZATION_ENABLED=true``,
``QUERY_ENTITY_AUTHORIZATION_REQUIRE_ALL_SUBJECTS=compat``). The one assertion whose
expected value depends on ``VIEW_AUTHORIZATION_ENABLED`` reads the live flag and asserts
the COMPAT contract for whichever side is active, so the module exercises both legs
without needing a CI matrix.
"""

import logging
import time
import uuid

import pytest

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AuditStampClass,
    CalendarIntervalClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    QueryLanguageClass,
    QueryPropertiesClass,
    QuerySourceClass,
    QueryStatementClass,
    QuerySubjectClass,
    QuerySubjectsClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import CorpUserUrn, QueryUrn
from tests.authorization.utils import is_view_authorization_enabled
from tests.consistency_utils import wait_for_writes_to_sync
from tests.privileges.utils import (
    clear_polices,
    create_metadata_policy,
    create_user,
    create_user_policy,
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
TEST_USER_EMAIL = f"view.queries.test.{_UNIQUE}@smoke.datahub.test"
TEST_USER_URN = f"urn:li:corpuser:{TEST_USER_EMAIL}"
TEST_USER_PASSWORD = "user"

DS_A_URN = f"urn:li:dataset:(urn:li:dataPlatform:kafka,view-queries-a-{_UNIQUE},PROD)"
DS_B_URN = f"urn:li:dataset:(urn:li:dataPlatform:kafka,view-queries-b-{_UNIQUE},PROD)"
Q_SINGLE_URN = str(QueryUrn(f"view-queries-single-{_UNIQUE}"))
Q_JOIN_URN = str(QueryUrn(f"view-queries-join-{_UNIQUE}"))
Q_ORPHAN_URN = str(QueryUrn(f"view-queries-orphan-{_UNIQUE}"))

SQL_SINGLE = "SELECT id FROM my_db.table_a"
SQL_JOIN = "SELECT a.id FROM my_db.table_a a JOIN my_db.table_b b ON a.id = b.id"
SQL_ORPHAN = "SELECT col_a FROM unrecorded_table"
SQL_TOP = "SELECT col_a FROM my_db.table_a WHERE region = 'x'"
TOTAL_SQL_QUERIES = 5

POLICY_PREFIX = f"Test view-queries {_UNIQUE}"

LIST_QUERIES = """
query listQueries($datasetUrn: String!) {
  listQueries(input: {start: 0, count: 50, datasetUrn: $datasetUrn}) {
    total
    queries { urn }
  }
}
"""

GET_QUERY_ENTITY = """
query entity($urn: String!) {
  entity(urn: $urn) {
    urn
    ... on QueryEntity { properties { statement { value } } }
  }
}
"""

USAGE_WITH_TOP_SQL = """
query usage($urn: String!) {
  dataset(urn: $urn) {
    usageStats(range: YEAR) {
      aggregations { totalSqlQueries }
      buckets { metrics { topSqlQueries } }
    }
  }
}
"""

USAGE_WITHOUT_TOP_SQL = """
query usage($urn: String!) {
  dataset(urn: $urn) {
    usageStats(range: YEAR) { aggregations { totalSqlQueries } }
  }
}
"""


def _query_mcps(urn: str, sql: str, subjects: list[str] | None):
    now = int(time.time() * 1000)
    actor = str(CorpUserUrn("datahub"))
    mcps = [
        MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=QueryPropertiesClass(
                statement=QueryStatementClass(
                    value=sql, language=QueryLanguageClass.SQL
                ),
                source=QuerySourceClass.MANUAL,
                created=AuditStampClass(time=now, actor=actor),
                lastModified=AuditStampClass(time=now, actor=actor),
            ),
        )
    ]
    if subjects is not None:
        mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=QuerySubjectsClass(
                    subjects=[QuerySubjectClass(entity=s) for s in subjects]
                ),
            )
        )
    return mcps


@pytest.fixture(scope="module", autouse=True)
def view_queries_setup(graph_client, auth_session):
    for urn in (DS_A_URN, DS_B_URN):
        graph_client.emit_mcp(
            MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=DatasetPropertiesClass(name=urn.split(",")[1])
            )
        )
    for mcp in (
        *_query_mcps(Q_SINGLE_URN, SQL_SINGLE, [DS_A_URN]),
        *_query_mcps(Q_JOIN_URN, SQL_JOIN, [DS_A_URN, DS_B_URN]),
        *_query_mcps(Q_ORPHAN_URN, SQL_ORPHAN, None),
    ):
        graph_client.emit_mcp(mcp)
    now = int(time.time() * 1000)
    graph_client.emit_mcp(
        MetadataChangeProposalWrapper(
            entityUrn=DS_A_URN,
            aspect=DatasetUsageStatisticsClass(
                timestampMillis=now - (now % 86_400_000),
                eventGranularity=TimeWindowSizeClass(
                    unit=CalendarIntervalClass.DAY, multiple=1
                ),
                totalSqlQueries=TOTAL_SQL_QUERIES,
                topSqlQueries=[SQL_TOP],
            ),
        )
    )
    wait_for_writes_to_sync()

    admin_session = get_frontend_session()
    clear_polices(admin_session, name_prefix=POLICY_PREFIX)
    set_base_platform_privileges_policy_status("INACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("INACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("INACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)
    admin_session = create_user(admin_session, TEST_USER_EMAIL, TEST_USER_PASSWORD)

    # Reading usage numbers at all requires entity view plus VIEW_DATASET_USAGE; the tests
    # below check that these alone do NOT unlock query SQL or topSqlQueries.
    usage_policy = create_metadata_policy(
        admin_session,
        name=f"{POLICY_PREFIX} usage",
        description="VIEW_ENTITY_PAGE and VIEW_DATASET_USAGE on dataset A",
        privileges=["VIEW_ENTITY_PAGE", "VIEW_DATASET_USAGE"],
        user_urn=TEST_USER_URN,
        resource_urn=DS_A_URN,
    )
    yield

    remove_policy(usage_policy, admin_session)
    remove_user(admin_session, TEST_USER_URN)
    clear_polices(admin_session, name_prefix=POLICY_PREFIX)
    set_base_platform_privileges_policy_status("ACTIVE", admin_session)
    set_view_dataset_sensitive_info_policy_status("ACTIVE", admin_session)
    set_view_entity_profile_privileges_policy_status("ACTIVE", admin_session)
    wait_for_writes_to_sync(mae_only=True)
    for urn in (Q_SINGLE_URN, Q_JOIN_URN, Q_ORPHAN_URN, DS_A_URN, DS_B_URN):
        try:
            graph_client.hard_delete_entity(urn=urn)
        except Exception:
            logger.warning("Failed to delete %s during cleanup", urn)


def _gql(session, query: str, variables: dict) -> dict:
    response = session.post(
        f"{get_frontend_url()}/api/v2/graphql",
        json={"query": query, "variables": variables},
    )
    response.raise_for_status()
    return response.json()


def _visible_query_urns(session) -> set[str]:
    res = _gql(session, LIST_QUERIES, {"datasetUrn": DS_A_URN})
    assert "errors" not in res, res
    return {q["urn"] for q in res["data"]["listQueries"]["queries"]}


def _statement(session, urn: str) -> str | None:
    res = _gql(session, GET_QUERY_ENTITY, {"urn": urn})
    entity = (res.get("data") or {}).get("entity") or {}
    return ((entity.get("properties") or {}).get("statement") or {}).get("value")


def _top_sql(session) -> tuple[list[str], list[str]]:
    """Returns (topSqlQueries, error messages) for dataset A's usage stats."""
    res = _gql(session, USAGE_WITH_TOP_SQL, {"urn": DS_A_URN})
    errors = [e["message"] for e in res.get("errors") or []]
    stats = ((res.get("data") or {}).get("dataset") or {}).get("usageStats") or {}
    top = [
        sql
        for bucket in stats.get("buckets") or []
        for sql in (bucket.get("metrics") or {}).get("topSqlQueries") or []
    ]
    return top, errors


def _total_sql(session) -> int | None:
    res = _gql(session, USAGE_WITHOUT_TOP_SQL, {"urn": DS_A_URN})
    assert "errors" not in res, res
    return res["data"]["dataset"]["usageStats"]["aggregations"]["totalSqlQueries"]


@with_test_retry(max_attempts=10)
def _assert_no_query_access():
    session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    assert _visible_query_urns(session) == set()
    for urn in (Q_SINGLE_URN, Q_JOIN_URN, Q_ORPHAN_URN):
        assert _statement(session, urn) is None
    top, errors = _top_sql(session)
    assert top == [] and errors, (
        "topSqlQueries must be denied without VIEW_ENTITY_QUERIES"
    )
    assert _total_sql(session) == TOTAL_SQL_QUERIES


@with_test_retry(max_attempts=10)
def _assert_scoped_access(require_all_subjects: bool):
    session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    expected = {Q_SINGLE_URN} if require_all_subjects else {Q_SINGLE_URN, Q_JOIN_URN}
    assert _visible_query_urns(session) == expected
    assert _statement(session, Q_SINGLE_URN) == SQL_SINGLE
    assert _statement(session, Q_JOIN_URN) == (
        None if require_all_subjects else SQL_JOIN
    )
    assert _statement(session, Q_ORPHAN_URN) is None
    top, errors = _top_sql(session)
    assert top == [SQL_TOP] and not errors, (
        "topSqlQueries stays any-subject under COMPAT"
    )


@with_test_retry(max_attempts=10)
def _assert_view_all_access():
    session = login_as(TEST_USER_EMAIL, TEST_USER_PASSWORD)
    assert _visible_query_urns(session) == {Q_SINGLE_URN, Q_JOIN_URN}
    assert _statement(session, Q_ORPHAN_URN) == SQL_ORPHAN


def test_no_privilege_hides_queries_and_top_sql():
    """Without VIEW_ENTITY_QUERIES: no queries listed, no SQL, topSqlQueries denied,
    while plain usage numbers (entity view + VIEW_DATASET_USAGE) still come back."""
    _assert_no_query_access()


def test_scoped_privilege_follows_compat_contract(auth_session):
    """VIEW_ENTITY_QUERIES on dataset A only. Under COMPAT the join query (A+B) is
    visible when VIEW_AUTHORIZATION_ENABLED is off (any-subject) and hidden when it is
    on (require-all); the orphan is hidden either way and topSqlQueries is visible."""
    admin_session = get_frontend_session()
    policy_urn = create_metadata_policy(
        admin_session,
        name=f"{POLICY_PREFIX} scoped",
        description="VIEW_ENTITY_QUERIES on dataset A",
        privileges=["VIEW_ENTITY_QUERIES"],
        user_urn=TEST_USER_URN,
        resource_urn=DS_A_URN,
    )
    try:
        _assert_scoped_access(
            require_all_subjects=is_view_authorization_enabled(auth_session)
        )
    finally:
        remove_policy(policy_urn, admin_session)
    _assert_no_query_access()


def test_view_all_queries_grants_orphan():
    """VIEW_ALL_QUERIES alone reaches every query, including one with no subjects."""
    admin_session = get_frontend_session()
    policy_urn = create_user_policy(
        TEST_USER_URN,
        ["VIEW_ALL_QUERIES"],
        admin_session,
        name=f"{POLICY_PREFIX} view-all",
        description="Platform-wide VIEW_ALL_QUERIES",
    )
    try:
        _assert_view_all_access()
    finally:
        remove_policy(policy_urn, admin_session)
