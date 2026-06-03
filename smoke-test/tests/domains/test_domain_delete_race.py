# Copyright 2026 Acryl Data, Inc.
# SPDX-License-Identifier: Apache-2.0
"""
Acceptance test demonstrating a race condition in domain deletion.

Root cause
----------
DeleteDomainResolver calls DomainUtils.hasChildDomains(), which filters via
entityClient.filter() -- an OpenSearch query (eventually consistent). When a
child domain is deleted and the parent is immediately deleted afterward,
OpenSearch may not yet have indexed the child's deletion. hasChildDomains()
therefore incorrectly returns True and the parent delete is rejected with:

    "Cannot delete domain <urn> which has child domains"

even though the child no longer exists in the primary store (MySQL).

The fix should make hasChildDomains() use a strongly-consistent read to
cross-check any results returned by the OpenSearch filter, discarding entries
that no longer exist in MySQL before making the blocking decision.

References
----------
- Linear: https://linear.app/acryl-data/issue/CAT-791
- DomainUtils.java: datahub-graphql-core/.../resolvers/mutate/util/DomainUtils.java
- DeleteDomainResolver.java: datahub-graphql-core/.../resolvers/domain/DeleteDomainResolver.java
"""

import uuid

from tests.utils import delete_entity, execute_graphql

_CREATE_DOMAIN = """
mutation createDomain($input: CreateDomainInput!) {
  createDomain(input: $input)
}
"""

_DELETE_DOMAIN = """
mutation deleteDomain($urn: String!) {
  deleteDomain(urn: $urn)
}
"""


def test_delete_parent_domain_immediately_after_child_deletion(auth_session):
    """
    A parent domain whose only child has just been deleted should be
    immediately deletable via a second deleteDomain call -- no sleep or
    page-refresh should be required.

    This test currently FAILS with:
        GraphQL errors: [{'message': '... Cannot delete domain <urn>
        which has child domains', ...}]

    because hasChildDomains() queries OpenSearch, which has not yet indexed
    the child domain's deletion by the time the parent delete is attempted.
    """
    run_id = uuid.uuid4().hex[:8]
    parent_id = f"test-domain-race-parent-{run_id}"
    child_id = f"test-domain-race-child-{run_id}"
    parent_urn = f"urn:li:domain:{parent_id}"
    child_urn = f"urn:li:domain:{child_id}"

    try:
        # 1. Create parent domain.
        res = execute_graphql(
            auth_session,
            _CREATE_DOMAIN,
            {"input": {"id": parent_id, "name": f"Race Test Parent {run_id}"}},
        )
        assert res["data"]["createDomain"] == parent_urn

        # 2. Create child domain nested under parent.
        res = execute_graphql(
            auth_session,
            _CREATE_DOMAIN,
            {
                "input": {
                    "id": child_id,
                    "name": f"Race Test Child {run_id}",
                    "parentDomain": parent_urn,
                }
            },
        )
        assert res["data"]["createDomain"] == child_urn

        # 3 & 4. Delete child then immediately delete parent, bypassing the
        # TestSessionWrapper sync wait entirely.
        #
        # TestSessionWrapper.post() calls wait_for_writes_to_sync() (a 3-second
        # static sleep) after every GraphQL POST. That sleep gives OpenSearch
        # enough time to index the child's deletion before the parent delete
        # runs, masking the race. Using _upstream directly skips the sleep so
        # both calls fire back-to-back with no consistency gap.
        endpoint = f"{auth_session.frontend_url()}/api/v2/graphql"
        headers = {"Authorization": f"Bearer {auth_session.gms_token()}"}

        def raw_graphql(query, variables):
            resp = auth_session._upstream.post(
                endpoint,
                json={"query": query, "variables": variables},
                headers=headers,
            )
            resp.raise_for_status()
            data = resp.json()
            assert "errors" not in data, f"GraphQL errors: {data.get('errors')}"
            return data

        # Delete child (writes to MySQL; MAE event queued to Kafka but not yet
        # indexed in OpenSearch).
        res = raw_graphql(_DELETE_DOMAIN, {"urn": child_urn})
        assert res["data"]["deleteDomain"] is True

        # Immediately delete parent -- no sleep between the two calls.
        # The child no longer exists in MySQL, so this SHOULD succeed.
        # It currently FAILS because hasChildDomains() queries OpenSearch,
        # which has not yet propagated the child's deletion.
        #
        # Expected (after fix): deleteDomain returns True.
        # Actual (before fix): GraphQL error "Cannot delete domain ... which
        #                      has child domains".
        res = raw_graphql(_DELETE_DOMAIN, {"urn": parent_urn})
        assert res["data"]["deleteDomain"] is True

    finally:
        # Best-effort cleanup via REST (bypasses the GraphQL child guard so
        # orphaned test domains are removed even when the test fails).
        for urn in (child_urn, parent_urn):
            try:
                delete_entity(auth_session, urn)
            except Exception:
                pass
