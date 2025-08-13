import pytest
import tenacity

from tests.utils import (
    delete_urns_from_file,
    execute_graphql_mutation,
    execute_graphql_query,
    get_sleep_info,
    ingest_file_via_rest,
)

sleep_sec, sleep_times = get_sleep_info()


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting domains test data")
    ingest_file_via_rest(auth_session, "tests/domains/data.json")
    yield
    print("removing domains test data")
    delete_urns_from_file(graph_client, "tests/domains/data.json")


@tenacity.retry(
    stop=tenacity.stop_after_attempt(sleep_times), wait=tenacity.wait_fixed(sleep_sec)
)
def _ensure_more_domains(
    auth_session, list_domains_query, list_domains_variables, before_count
):
    # Get new count of Domains
    res_data = execute_graphql_query(
        auth_session,
        list_domains_query,
        variables=list_domains_variables,
        expected_data_key="listDomains",
    )

    # Assert that there are more domains now.
    after_count = res_data["data"]["listDomains"]["total"]
    print(f"after_count is {after_count}")
    assert after_count == before_count + 1


@pytest.mark.dependency()
def test_create_list_get_domain(auth_session):
    # Setup: Delete the domain (if exists)
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=delete",
        json={"urn": "urn:li:domain:test id"},
    )

    # Get count of existing domains
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

    res_data = execute_graphql_query(
        auth_session,
        list_domains_query,
        variables={"input": {"start": 0, "count": 20}},
        expected_data_key="listDomains",
    )
    print(f"domains resp is {res_data}")

    before_count = res_data["data"]["listDomains"]["total"]
    print(f"before_count is {before_count}")

    domain_id = "test id"
    domain_name = "test name"
    domain_description = "test description"

    # Create new Domain
    create_domain_mutation = """mutation createDomain($input: CreateDomainInput!) {
        createDomain(input: $input)
    }"""

    create_domain_variables = {
        "input": {
            "id": domain_id,
            "name": domain_name,
            "description": domain_description,
        }
    }

    res_data = execute_graphql_mutation(
        auth_session, create_domain_mutation, create_domain_variables, "createDomain"
    )

    domain_urn = res_data["data"]["createDomain"]

    _ensure_more_domains(
        auth_session=auth_session,
        list_domains_query=list_domains_query,
        list_domains_variables={"input": {"start": 0, "count": 20}},
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

    res_data = execute_graphql_query(
        auth_session,
        get_domain_query,
        variables={"urn": domain_urn},
        expected_data_key="domain",
    )

    domain = res_data["data"]["domain"]
    assert domain["urn"] == f"urn:li:domain:{domain_id}"
    assert domain["id"] == domain_id
    assert domain["properties"]["name"] == domain_name
    assert domain["properties"]["description"] == domain_description

    delete_json = {"urn": domain_urn}

    # Cleanup: Delete the domain
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=delete", json=delete_json
    )

    response.raise_for_status()


@pytest.mark.dependency(depends=["test_create_list_get_domain"])
def test_set_unset_domain(auth_session, ingest_cleanup_data):
    # Set and Unset a Domain for a dataset. Note that this doesn't test for adding domains to charts, dashboards, charts, & jobs.
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )
    domain_urn = "urn:li:domain:engineering"

    # First unset to be sure.
    unset_domain_mutation = """mutation unsetDomain($entityUrn: String!) {
        unsetDomain(entityUrn: $entityUrn)
    }"""

    res_data = execute_graphql_mutation(
        auth_session, unset_domain_mutation, {"entityUrn": dataset_urn}, "unsetDomain"
    )

    assert res_data["data"]["unsetDomain"] is True

    # Set a new domain
    set_domain_mutation = """mutation setDomain($entityUrn: String!, $domainUrn: String!) {
        setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)
    }"""

    res_data = execute_graphql_mutation(
        auth_session,
        set_domain_mutation,
        {"entityUrn": dataset_urn, "domainUrn": domain_urn},
        "setDomain",
    )

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

    res_data = execute_graphql_query(
        auth_session,
        get_dataset_query,
        variables={"urn": dataset_urn},
        expected_data_key="dataset",
    )
    assert res_data["data"]["dataset"]["domain"]["domain"]["urn"] == domain_urn
    assert (
        res_data["data"]["dataset"]["domain"]["domain"]["properties"]["name"]
        == "Engineering"
    )
