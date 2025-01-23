import pytest

from tests.utils import delete_urns_from_file, ingest_file_via_rest


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting containers test data")
    ingest_file_via_rest(auth_session, "tests/containers/data.json")
    yield
    print("removing containers test data")
    delete_urns_from_file(graph_client, "tests/containers/data.json")


@pytest.mark.dependency()
def test_get_full_container(auth_session, ingest_cleanup_data):
    container_urn = "urn:li:container:SCHEMA"
    container_name = "datahub_schema"
    container_description = "The DataHub schema"
    editable_container_description = "custom description"

    # Get a full container
    get_container_json = {
        "query": """query container($urn: String!) {\n
            container(urn: $urn) {\n
              urn\n
              type\n
              platform {\n
                urn\n
                properties{\n
                  displayName\n
                }\n
              }\n
              container {\n
                urn\n
                properties {\n
                  name\n
                  description\n
                }\n
              }\n
              properties {\n
                name\n
                description\n
              }\n
              editableProperties {\n
                description\n
              }\n
              ownership {\n
                owners {\n
                  owner {\n
                    ...on CorpUser {\n
                      urn\n
                    }\n
                  }\n
                }\n
              }\n
              institutionalMemory {\n
                elements {\n
                  url\n
                }\n
              }\n
              tags {\n
                tags {\n
                  tag {\n
                    urn\n
                  }\n
                }\n
              }\n
              glossaryTerms {\n
                terms {\n
                  term {\n
                    urn\n
                  }\n
                }\n
              }\n
              subTypes {\n
                typeNames\n
              }\n
              entities(input: {}) {\n
                total\n
                searchResults {\n
                  entity {\n
                    ...on Dataset {\n
                      urn\n
                    }\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"urn": container_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=get_container_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["container"] is not None
    assert "errors" not in res_data

    container = res_data["data"]["container"]
    assert container["urn"] == container_urn
    assert container["type"] == "CONTAINER"
    assert container["platform"]["urn"] == "urn:li:dataPlatform:mysql"
    assert container["properties"]["name"] == container_name
    assert container["properties"]["description"] == container_description
    assert container["subTypes"]["typeNames"][0] == "Schema"
    assert (
        container["editableProperties"]["description"] == editable_container_description
    )
    assert container["ownership"] is None
    assert container["institutionalMemory"] is None
    assert container["tags"] is None
    assert container["glossaryTerms"] is None


@pytest.mark.dependency(depends=["test_get_full_container"])
def test_get_parent_container(auth_session):
    dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:hive,SampleHiveDataset,PROD)"

    # Get count of existing secrets
    get_dataset_json = {
        "query": """query dataset($urn: String!) {\n
          dataset(urn: $urn) {\n
            urn\n
            container {\n
              urn\n
              properties {\n
                name\n
              }\n
            }\n
          }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=get_dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"] is not None
    assert "errors" not in res_data

    dataset = res_data["data"]["dataset"]
    assert dataset["container"]["properties"]["name"] == "datahub_schema"


@pytest.mark.dependency(depends=["test_get_full_container"])
def test_update_container(auth_session):
    container_urn = "urn:li:container:SCHEMA"

    new_tag = "urn:li:tag:Test"

    add_tag_json = {
        "query": """mutation addTag($input: TagAssociationInput!) {\n
            addTag(input: $input)
        }""",
        "variables": {
            "input": {
                "tagUrn": new_tag,
                "resourceUrn": container_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_tag_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTag"] is True

    new_term = "urn:li:glossaryTerm:Term"

    add_term_json = {
        "query": """mutation addTerm($input: TermAssociationInput!) {\n
            addTerm(input: $input)
        }""",
        "variables": {
            "input": {
                "termUrn": new_term,
                "resourceUrn": container_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_term_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addTerm"] is True

    new_owner = "urn:li:corpuser:jdoe"

    add_owner_json = {
        "query": """mutation addOwner($input: AddOwnerInput!) {\n
            addOwner(input: $input)
        }""",
        "variables": {
            "input": {
                "ownerUrn": new_owner,
                "resourceUrn": container_urn,
                "ownerEntityType": "CORP_USER",
                "ownershipTypeUrn": "urn:li:ownershipType:__system__technical_owner",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_owner_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addOwner"] is True

    new_link = "https://www.test.com"

    add_link_json = {
        "query": """mutation addLink($input: AddLinkInput!) {\n
            addLink(input: $input)
        }""",
        "variables": {
            "input": {
                "linkUrl": new_link,
                "resourceUrn": container_urn,
                "label": "Label",
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=add_link_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["addLink"] is True

    new_description = "New description"

    update_description_json = {
        "query": """mutation updateDescription($input: DescriptionUpdateInput!) {\n
            updateDescription(input: $input)
        }""",
        "variables": {
            "input": {
                "description": new_description,
                "resourceUrn": container_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=update_description_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDescription"] is True

    # Now fetch the container to ensure it was updated
    # Get the container
    get_container_json = {
        "query": """query container($urn: String!) {\n
           container(urn: $urn) {\n
              editableProperties {\n
                description\n
              }\n
              ownership {\n
                owners {\n
                  owner {\n
                    ...on CorpUser {\n
                      urn\n
                    }\n
                  }\n
                }\n
              }\n
              institutionalMemory {\n
                elements {\n
                  url\n
                }\n
              }\n
              tags {\n
                tags {\n
                  tag {\n
                    urn\n
                  }\n
                }\n
              }\n
              glossaryTerms {\n
                terms {\n
                  term {\n
                    urn\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"urn": container_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=get_container_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["container"] is not None
    assert "errors" not in res_data

    container = res_data["data"]["container"]
    assert container["editableProperties"]["description"] == new_description
    assert container["ownership"]["owners"][0]["owner"]["urn"] == new_owner
    assert container["institutionalMemory"]["elements"][0]["url"] == new_link
    assert container["tags"]["tags"][0]["tag"]["urn"] == new_tag
    assert container["glossaryTerms"]["terms"][0]["term"]["urn"] == new_term
