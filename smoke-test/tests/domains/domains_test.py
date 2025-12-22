import logging
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
