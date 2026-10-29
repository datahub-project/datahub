"""
Creating a new glossary term version from an unversioned term: the term becomes v1, the new
version inherits its domain and owners and starts as a draft, and v1 stays the latest visible version until
the draft is published.
"""

import logging

import pytest

from tests.e2e.utils import execute_graphql, unique_suffix, with_test_retry
from utilities.domains import Domain

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)

DRAFT = "urn:li:lifecycleStageType:DRAFT"

CREATE_DOMAIN = """
    mutation CreateDomain($input: CreateDomainInput!) { createDomain(input: $input) }
"""
CREATE_TERM = """
    mutation CreateTerm($input: CreateGlossaryEntityInput!) { createGlossaryTerm(input: $input) }
"""
SET_DOMAIN = """
    mutation SetDomain($entityUrn: String!, $domainUrn: String!) {
        setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)
    }
"""
CREATE_GROUP = """
    mutation CreateGroup($input: CreateGroupInput!) { createGroup(input: $input) }
"""
ADD_OWNER = """
    mutation AddOwner($input: AddOwnerInput!) { addOwner(input: $input) }
"""
CREATE_VERSION = """
    mutation CreateVersion($input: CreateGlossaryTermVersionInput!) {
        createGlossaryTermVersion(input: $input) { urn }
    }
"""
SET_STAGE = """
    mutation SetStage($urn: String!, $lifecycleStageUrn: String) {
        setLifecycleStage(urn: $urn, lifecycleStageUrn: $lifecycleStageUrn)
    }
"""
GET_TERM = """
    query GetTerm($urn: String!) {
        glossaryTerm(urn: $urn) {
            domain { domain { urn } }
            ownership { owners { owner { ... on CorpGroup { urn } } } }
            status { lifecycleStage { urn } }
            versionProperties { isLatest version { versionTag } versionSet { urn } }
        }
    }
"""


def _owner_urns(term: dict) -> set:
    return {o["owner"]["urn"] for o in term["ownership"]["owners"] if o["owner"]}


def _term(auth_session, urn: str) -> dict:
    return execute_graphql(auth_session, GET_TERM, {"urn": urn})["data"]["glossaryTerm"]


@pytest.mark.p0
def test_new_version_inherits_metadata_and_starts_as_draft(auth_session, graph_client):
    suffix = unique_suffix()
    v1_urn = f"urn:li:glossaryTerm:smoketest_term_{suffix}"
    v2_id = f"smoketest_term_{suffix}_v2"
    v2_urn = f"urn:li:glossaryTerm:{v2_id}"
    v3_id = f"smoketest_term_{suffix}_v3"
    v3_urn = f"urn:li:glossaryTerm:{v3_id}"
    # Everything this test may create, hard-deleted in finally whether or not it got created.
    created = [
        v3_urn,
        v2_urn,
        v1_urn,
        f"urn:li:domain:smoketest_domain_{suffix}",
        f"urn:li:corpGroup:smoketest_owner_{suffix}",
    ]
    try:
        domain_urn = execute_graphql(
            auth_session,
            CREATE_DOMAIN,
            {"input": {"id": f"smoketest_domain_{suffix}", "name": f"Domain {suffix}"}},
        )["data"]["createDomain"]
        execute_graphql(
            auth_session,
            CREATE_TERM,
            {"input": {"id": f"smoketest_term_{suffix}", "name": f"Term {suffix}"}},
        )
        owner_urn = execute_graphql(
            auth_session,
            CREATE_GROUP,
            {"input": {"id": f"smoketest_owner_{suffix}", "name": f"Owner {suffix}"}},
        )["data"]["createGroup"]
        execute_graphql(
            auth_session, SET_DOMAIN, {"entityUrn": v1_urn, "domainUrn": domain_urn}
        )
        execute_graphql(
            auth_session,
            ADD_OWNER,
            {
                "input": {
                    "ownerUrn": owner_urn,
                    "ownerEntityType": "CORP_GROUP",
                    "ownershipTypeUrn": "urn:li:ownershipType:__system__technical_owner",
                    "resourceUrn": v1_urn,
                }
            },
        )
        version_set_urn = execute_graphql(
            auth_session,
            CREATE_VERSION,
            {
                "input": {
                    "previousTermUrn": v1_urn,
                    "id": v2_id,
                    "name": f"Term {suffix}",
                    "version": "v2",
                    "comment": "Smoke test revision",
                }
            },
        )["data"]["createGlossaryTermVersion"]["urn"]
        created.append(version_set_urn)

        @with_test_retry(max_attempts=12)
        def _assert_draft_inherits_and_v1_stays_latest():
            v1, v2 = _term(auth_session, v1_urn), _term(auth_session, v2_urn)
            assert v1["versionProperties"]["version"]["versionTag"] == "v1", v1
            assert v1["versionProperties"]["versionSet"]["urn"] == version_set_urn
            assert v2["versionProperties"]["versionSet"]["urn"] == version_set_urn
            assert v2["domain"]["domain"]["urn"] == domain_urn, v2
            assert owner_urn in _owner_urns(v2), v2
            # Relies on the bootstrapped DRAFT stage applying to glossaryTerm; without it the new
            # version is not drafted and becomes latest at once.
            assert v2["status"]["lifecycleStage"]["urn"] == DRAFT, v2
            assert v1["versionProperties"]["isLatest"] is True, v1
            assert v2["versionProperties"]["isLatest"] is False, v2

        _assert_draft_inherits_and_v1_stays_latest()

        execute_graphql(
            auth_session, SET_STAGE, {"urn": v2_urn, "lifecycleStageUrn": None}
        )

        @with_test_retry(max_attempts=12)
        def _assert_v2_latest_after_publish():
            assert _term(auth_session, v2_urn)["versionProperties"]["isLatest"] is True
            assert _term(auth_session, v1_urn)["versionProperties"]["isLatest"] is False

        _assert_v2_latest_after_publish()

        # Publishing v2 must also move the version set's latest to v2; otherwise the next
        # version is rejected as conflicting with an unpublished draft.
        execute_graphql(
            auth_session,
            CREATE_VERSION,
            {
                "input": {
                    "previousTermUrn": v2_urn,
                    "id": v3_id,
                    "name": f"Term {suffix}",
                    "version": "v3",
                }
            },
        )

        @with_test_retry(max_attempts=12)
        def _assert_v3_is_draft_and_v2_stays_latest():
            v2, v3 = _term(auth_session, v2_urn), _term(auth_session, v3_urn)
            assert v3["status"]["lifecycleStage"]["urn"] == DRAFT, v3
            assert v3["versionProperties"]["versionSet"]["urn"] == version_set_urn, v3
            assert v2["versionProperties"]["isLatest"] is True, v2
            assert v3["versionProperties"]["isLatest"] is False, v3

        _assert_v3_is_draft_and_v2_stays_latest()
    finally:
        for urn in created:
            try:
                graph_client.hard_delete_entity(urn)
            except Exception as exc:
                logger.warning(f"Cleanup failed for {urn}: {exc}")
