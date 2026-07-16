import logging
import uuid
from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import delete_entity, execute_graphql, with_test_retry

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/domains/data.json", "domains"
    )


@with_test_retry()
def _ensure_more_domains(
    auth_session, query: str, variables: Dict[str, Any], before_count: int
) -> None:
    # Get new count of Domains
    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["listDomains"]["total"] is not None

    # Assert that there are more domains now.
    after_count = res_data["data"]["listDomains"]["total"]
    logger.info(f"after_count is {after_count}")
    assert after_count == before_count + 1


@pytest.mark.dependency()
def test_create_list_get_domain(auth_session):
    # Setup: Delete the domain (if exists)
    delete_entity(auth_session, "urn:li:domain:test id")

    # Get count of existing secrets
    list_domains_query = """query listDomains($input: ListDomainsInput!) {
            listDomains(input: $input) {
              start
              count
              total
              domains {
                urn
                properties {
                  name
                }
              }
            }
        }"""
    list_domains_variables: Dict[str, Any] = {"input": {"start": 0, "count": 20}}

    res_data = execute_graphql(auth_session, list_domains_query, list_domains_variables)

    assert res_data["data"]["listDomains"]["total"] is not None
    logger.info(f"domains resp is {res_data}")

    before_count = res_data["data"]["listDomains"]["total"]
    logger.info(f"before_count is {before_count}")

    domain_id = "test id"
    domain_name = "test name"
    domain_description = "test description"

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

    domain_urn = res_data["data"]["createDomain"]

    _ensure_more_domains(
        auth_session=auth_session,
        query=list_domains_query,
        variables=list_domains_variables,
        before_count=before_count,
    )

    # Get the domain value back
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
    get_domain_variables: Dict[str, Any] = {"urn": domain_urn}

    res_data = execute_graphql(auth_session, get_domain_query, get_domain_variables)

    assert res_data["data"]["domain"] is not None

    domain = res_data["data"]["domain"]
    assert domain["urn"] == f"urn:li:domain:{domain_id}"
    assert domain["id"] == domain_id
    assert domain["properties"]["name"] == domain_name
    assert domain["properties"]["description"] == domain_description

    delete_entity(auth_session, domain_urn)


@pytest.mark.dependency(depends=["test_create_list_get_domain"])
def test_set_unset_domain(auth_session, ingest_cleanup_data):
    # Set and Unset a Domain for a dataset. Note that this doesn't test for adding domains to charts, dashboards, charts, & jobs.
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )
    domain_urn = "urn:li:domain:engineering"

    # First unset to be sure.
    unset_domain_query = """mutation unsetDomain($entityUrn: String!) {
            unsetDomain(entityUrn: $entityUrn)}"""
    unset_domain_variables: Dict[str, Any] = {"entityUrn": dataset_urn}

    res_data = execute_graphql(auth_session, unset_domain_query, unset_domain_variables)

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
        res = execute_graphql(
            auth_session,
            _CREATE_DOMAIN_MUTATION,
            {"input": {"id": parent_id, "name": f"Race Test Parent {run_id}"}},
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

        # Delete child then immediately delete parent using the underlying
        # session directly, bypassing TestSessionWrapper's post-mutation
        # consistency sleep. This preserves the race window between the
        # child deletion (MySQL write) and the parent deletion attempt
        # (OpenSearch child-guard check).
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
