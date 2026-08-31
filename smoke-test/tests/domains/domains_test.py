import logging
import uuid
from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_unique_dataset_impl
from tests.utilities.domains import Domain
from tests.utils import delete_entity, execute_graphql, unique_suffix, with_test_retry

logger = logging.getLogger(__name__)

pytestmark = pytest.mark.domain(Domain.CATALOG)


@pytest.fixture(scope="module", autouse=False)
def dataset_urn(auth_session, graph_client, tmp_path_factory):
    yield from _ingest_cleanup_unique_dataset_impl(
        auth_session,
        graph_client,
        "tests/domains/data.json",
        "domains",
        "test-tags-terms-sample-kafka",
        tmp_path_factory.mktemp("domains"),
    )


@with_test_retry()
def _ensure_domain_readable(
    auth_session, domain_urn: str, domain_id: str
) -> Dict[str, Any]:
    """Wait until the domain is readable by URN — not via global listDomains.total.

    Concurrent modules also create domains under xdist, so asserting
    ``total == before + 1`` races. Get-by-URN is stable for a unique id.
    """
    get_domain_query = """query domain($urn: String!) {
            domain(urn: $urn) {
              urn
              id
              properties {
                name
                description
              }
            }
        }"""
    res_data = execute_graphql(auth_session, get_domain_query, {"urn": domain_urn})
    domain = res_data["data"]["domain"]
    assert domain is not None
    assert domain["urn"] == f"urn:li:domain:{domain_id}"
    assert domain["id"] == domain_id
    return domain


@pytest.mark.dependency()
def test_create_list_get_domain(auth_session):
    # Run-unique id so parallel workers never collide on urn:li:domain:test id.
    domain_id = f"test-id-{unique_suffix()}"
    domain_name = f"test name {domain_id}"
    domain_description = "test description"
    domain_urn = f"urn:li:domain:{domain_id}"

    try:
        # Create new Domain
        create_domain_query = """mutation createDomain($input: CreateDomainInput!) {
                createDomain(input: $input)
            }"""
        create_domain_variables: Dict[str, Any] = {
            "input": {
                "id": domain_id,
                "name": domain_name,
                "description": domain_description,
            }
        }

        res_data = execute_graphql(
            auth_session, create_domain_query, create_domain_variables
        )

        assert res_data["data"]["createDomain"] is not None
        assert res_data["data"]["createDomain"] == domain_urn

        domain = _ensure_domain_readable(auth_session, domain_urn, domain_id)
        assert domain["properties"]["name"] == domain_name
        assert domain["properties"]["description"] == domain_description
    finally:
        # Always delete — assertion failures must not leak domains into CI.
        try:
            delete_entity(auth_session, domain_urn)
        except Exception as exc:
            logger.warning("Failed to clean up domain %s: %s", domain_urn, exc)


@pytest.mark.dependency(depends=["test_create_list_get_domain"])
def test_set_unset_domain(auth_session, dataset_urn):
    # Set and Unset a Domain for a dataset. Note that this doesn't test for adding domains to charts, dashboards, charts, & jobs.
    domain_urn = "urn:li:domain:engineering"

    # First unset to be sure.
    unset_domain_query = """mutation unsetDomain($entityUrn: String!) {
            unsetDomain(entityUrn: $entityUrn)}"""
    unset_domain_variables: Dict[str, Any] = {"entityUrn": dataset_urn}

    # Skip the sync wait -- setDomain follows immediately with no intermediate
    # read, and only the combined state after both mutations is checked below.
    res_data = execute_graphql(
        auth_session, unset_domain_query, unset_domain_variables, no_sync_wait=True
    )

    assert res_data["data"]["unsetDomain"] is True

    # Set a new domain
    set_domain_query = """mutation setDomain($entityUrn: String!, $domainUrn: String!) {
            setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)}"""
    set_domain_variables: Dict[str, Any] = {
        "entityUrn": dataset_urn,
        "domainUrn": domain_urn,
    }

    res_data = execute_graphql(auth_session, set_domain_query, set_domain_variables)

    assert res_data["data"]["setDomain"] is True

    # Now, fetch the dataset's domain and confirm it was set.
    get_dataset_query = """query dataset($urn: String!) {
            dataset(urn: $urn) {
              urn
              domain {
                domain {
                  urn
                  properties{
                    name
                  }
                }
              }
            }
        }"""
    get_dataset_variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, get_dataset_query, get_dataset_variables)

    assert res_data["data"]["dataset"]["domain"]["domain"]["urn"] == domain_urn
    assert (
        res_data["data"]["dataset"]["domain"]["domain"]["properties"]["name"]
        == "Engineering"
    )


_CREATE_DOMAIN_MUTATION = """
mutation createDomain($input: CreateDomainInput!) {
  createDomain(input: $input)
}
"""

_DELETE_DOMAIN_MUTATION = """
mutation deleteDomain($urn: String!) {
  deleteDomain(urn: $urn)
}
"""


def test_delete_parent_domain_immediately_after_child_deletion(auth_session):
    """
    A parent domain whose only child has just been deleted should be
    immediately deletable -- no sleep or page-refresh should be required.

    Demonstrates a race condition in DomainUtils.hasChildDomains(): it
    queries OpenSearch (eventually consistent) rather than the primary
    store (MySQL). When a child is deleted and the parent delete follows
    immediately, OpenSearch may not yet have indexed the child's removal,
    causing the parent delete to be rejected with "Cannot delete domain
    which has child domains" even though the child is already gone.

    The test bypasses TestSessionWrapper's post-mutation consistency sleep
    so both deletes fire back-to-back with no gap for OpenSearch to catch up.

    References
    ----------
    - DomainUtils.java: datahub-graphql-core/.../resolvers/mutate/util/DomainUtils.java
    - DeleteDomainResolver.java: datahub-graphql-core/.../resolvers/domain/DeleteDomainResolver.java
    """
    run_id = uuid.uuid4().hex[:8]
    parent_id = f"test-domain-race-parent-{run_id}"
    child_id = f"test-domain-race-child-{run_id}"
    parent_urn = f"urn:li:domain:{parent_id}"
    child_urn = f"urn:li:domain:{child_id}"

    try:
        # Skip the sync wait -- CreateDomainResolver only checks parent existence
        # via entityClient.exists() (primary store), not search, so the child
        # create below doesn't need the parent indexed yet. The child create
        # DOES need a real wait (kept below): the race test needs the child
        # actually indexed as a child of the parent before the delete race window
        # starts, or there'd be no child to race against.
        res = execute_graphql(
            auth_session,
            _CREATE_DOMAIN_MUTATION,
            {"input": {"id": parent_id, "name": f"Race Test Parent {run_id}"}},
            no_sync_wait=True,
        )
        assert res["data"]["createDomain"] == parent_urn

        res = execute_graphql(
            auth_session,
            _CREATE_DOMAIN_MUTATION,
            {
                "input": {
                    "id": child_id,
                    "name": f"Race Test Child {run_id}",
                    "parentDomain": parent_urn,
                }
            },
        )
        assert res["data"]["createDomain"] == child_urn

        # Delete child then immediately delete parent via raw_post,
        # bypassing TestSessionWrapper's post-mutation consistency sleep.
        # Preserves the race window between child deletion (MySQL write)
        # and parent deletion (OpenSearch child-guard check).
        endpoint = f"{auth_session.frontend_url()}/api/v2/graphql"

        def raw_graphql(query, variables):
            # raw_post skips TestSessionWrapper sync wait (preserves race window).
            resp = auth_session.raw_post(
                endpoint,
                json={"query": query, "variables": variables},
            )
            resp.raise_for_status()
            data = resp.json()
            assert "errors" not in data, f"GraphQL errors: {data.get('errors')}"
            return data

        res = raw_graphql(_DELETE_DOMAIN_MUTATION, {"urn": child_urn})
        assert res["data"]["deleteDomain"] is True

        res = raw_graphql(_DELETE_DOMAIN_MUTATION, {"urn": parent_urn})
        assert res["data"]["deleteDomain"] is True

    finally:
        # Best-effort cleanup via REST, which bypasses the GraphQL child guard.
        for urn in (child_urn, parent_urn):
            try:
                delete_entity(auth_session, urn)
            except Exception:
                pass
