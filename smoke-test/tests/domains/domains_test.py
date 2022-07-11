import time

import pytest
from tests.utils import (
    delete_urns_from_file,
    get_frontend_url,
    get_gms_url,
    ingest_file_via_rest,
)


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(request):
    print("ingesting domains test data")
    ingest_file_via_rest("tests/domains/data.json")
    yield
    print("removing domains test data")
    delete_urns_from_file("tests/domains/data.json")


@pytest.mark.dependency()
def test_healthchecks(wait_for_healthchecks):
    # Call to wait_for_healthchecks fixture will do the actual functionality.
    pass


@pytest.mark.dependency(depends=["test_healthchecks"])
def test_create_list_get_domain(frontend_session):

    # Get count of existing secrets
    list_domains_json = {
        "query": """query listDomains($input: ListDomainsInput!) {\n
            listDomains(input: $input) {\n
              start\n
              count\n
              total\n
              domains {\n
                urn\n
                properties {\n
                  name\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"input": {"start": "0", "count": "20"}},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_domains_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listDomains"]["total"] is not None
    assert "errors" not in res_data

    before_count = res_data["data"]["listDomains"]["total"]
    print(before_count)

    domain_id = "test id"
    domain_name = "test name"
    domain_description = "test description"

    # Create new Domain
    create_domain_json = {
        "query": """mutation createDomain($input: CreateDomainInput!) {\n
            createDomain(input: $input)
        }""",
        "variables": {
            "input": {
                "id": domain_id,
                "name": domain_name,
                "description": domain_description,
            }
        },
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=create_domain_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["createDomain"] is not None
    assert "errors" not in res_data

    domain_urn = res_data["data"]["createDomain"]

    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    # Get new count of Domains
    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=list_domains_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["listDomains"]["total"] is not None
    assert "errors" not in res_data

    # Assert that there are more domains now.
    after_count = res_data["data"]["listDomains"]["total"]
    print(after_count)
    assert after_count == before_count + 1

    # Get the domain value back
    get_domain_json = {
        "query": """query domain($urn: String!) {\n
            domain(urn: $urn) {\n
              urn\n
              id\n
              properties {\n
                name\n
                description\n
              }\n
            }\n
        }""",
        "variables": {"urn": domain_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_domain_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["domain"] is not None
    assert "errors" not in res_data

    domain = res_data["data"]["domain"]
    assert domain["urn"] == f"urn:li:domain:{domain_id}"
    assert domain["id"] == domain_id
    assert domain["properties"]["name"] == domain_name
    assert domain["properties"]["description"] == domain_description

    delete_json = {"urn": domain_urn}

    # Cleanup: Delete the domain
    response = frontend_session.post(
        f"{get_gms_url()}/entities?action=delete", json=delete_json
    )

    response.raise_for_status()


@pytest.mark.dependency(depends=["test_healthchecks", "test_create_list_get_domain"])
def test_set_unset_domain(frontend_session, ingest_cleanup_data):

    # Set and Unset a Domain for a dataset. Note that this doesn't test for adding domains to charts, dashboards, charts, & jobs.
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )
    domain_urn = "urn:li:domain:engineering"

    # First unset to be sure.
    unset_domain_json = {
        "query": """mutation unsetDomain($entityUrn: String!) {\n
            unsetDomain(entityUrn: $entityUrn)}""",
        "variables": {"entityUrn": dataset_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=unset_domain_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["unsetDomain"] is True
    assert "errors" not in res_data

    # Set a new domain
    set_domain_json = {
        "query": """mutation setDomain($entityUrn: String!, $domainUrn: String!) {\n
            setDomain(entityUrn: $entityUrn, domainUrn: $domainUrn)}""",
        "variables": {"entityUrn": dataset_urn, "domainUrn": domain_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=set_domain_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["setDomain"] is True
    assert "errors" not in res_data

    # Now, fetch the dataset's domain and confirm it was set.
    get_dataset_json = {
        "query": """query dataset($urn: String!) {\n
            dataset(urn: $urn) {\n
              urn\n
              domain {\n
                urn\n
                properties{\n
                  name\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    response = frontend_session.post(
        f"{get_frontend_url()}/api/v2/graphql", json=get_dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]["dataset"]["domain"]["urn"] == domain_urn
    assert res_data["data"]["dataset"]["domain"]["properties"]["name"] == "Engineering"
