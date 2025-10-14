from typing import Any, Dict

import pytest

from tests.utils import delete_urns_from_file, execute_graphql, ingest_file_via_rest


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
    get_container_query = """query container($urn: String!) {
            container(urn: $urn) {
              urn
              type
              platform {
                urn
                properties{
                  displayName
                }
              }
              container {
                urn
                properties {
                  name
                  description
                }
              }
              properties {
                name
                description
              }
              editableProperties {
                description
              }
              ownership {
                owners {
                  owner {
                    ...on CorpUser {
                      urn
                    }
                  }
                }
              }
              institutionalMemory {
                elements {
                  url
                }
              }
              tags {
                tags {
                  tag {
                    urn
                  }
                }
              }
              glossaryTerms {
                terms {
                  term {
                    urn
                  }
                }
              }
              subTypes {
                typeNames
              }
              entities(input: {}) {
                total
                searchResults {
                  entity {
                    ...on Dataset {
                      urn
                    }
                  }
                }
              }
            }
        }"""
    get_container_variables: Dict[str, Any] = {"urn": container_urn}

    res_data = execute_graphql(
        auth_session, get_container_query, get_container_variables
    )

    assert res_data["data"]["container"] is not None

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
    get_dataset_query = """query dataset($urn: String!) {
          dataset(urn: $urn) {
            urn
            container {
              urn
              properties {
                name
              }
            }
          }
        }"""
    get_dataset_variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, get_dataset_query, get_dataset_variables)

    assert res_data["data"]["dataset"] is not None

    dataset = res_data["data"]["dataset"]
    assert dataset["container"]["properties"]["name"] == "datahub_schema"


@pytest.mark.dependency(depends=["test_get_full_container"])
def test_update_container(auth_session):
    container_urn = "urn:li:container:SCHEMA"

    new_tag = "urn:li:tag:Test"

    add_tag_query = """mutation addTag($input: TagAssociationInput!) {
            addTag(input: $input)
        }"""
    add_tag_variables: Dict[str, Any] = {
        "input": {
            "tagUrn": new_tag,
            "resourceUrn": container_urn,
        }
    }

    res_data = execute_graphql(auth_session, add_tag_query, add_tag_variables)

    assert res_data["data"]["addTag"] is True

    new_term = "urn:li:glossaryTerm:Term"

    add_term_query = """mutation addTerm($input: TermAssociationInput!) {
            addTerm(input: $input)
        }"""
    add_term_variables: Dict[str, Any] = {
        "input": {
            "termUrn": new_term,
            "resourceUrn": container_urn,
        }
    }

    res_data = execute_graphql(auth_session, add_term_query, add_term_variables)

    assert res_data["data"]["addTerm"] is True

    new_owner = "urn:li:corpuser:jdoe"

    add_owner_query = """mutation addOwner($input: AddOwnerInput!) {
            addOwner(input: $input)
        }"""
    add_owner_variables: Dict[str, Any] = {
        "input": {
            "ownerUrn": new_owner,
            "resourceUrn": container_urn,
            "ownerEntityType": "CORP_USER",
            "ownershipTypeUrn": "urn:li:ownershipType:__system__technical_owner",
        }
    }

    res_data = execute_graphql(auth_session, add_owner_query, add_owner_variables)

    assert res_data["data"]["addOwner"] is True

    new_link = "https://www.test.com"

    add_link_query = """mutation addLink($input: AddLinkInput!) {
            addLink(input: $input)
        }"""
    add_link_variables: Dict[str, Any] = {
        "input": {
            "linkUrl": new_link,
            "resourceUrn": container_urn,
            "label": "Label",
        }
    }

    res_data = execute_graphql(auth_session, add_link_query, add_link_variables)

    assert res_data["data"]["addLink"] is True

    new_description = "New description"

    update_description_query = """mutation updateDescription($input: DescriptionUpdateInput!) {
            updateDescription(input: $input)
        }"""
    update_description_variables: Dict[str, Any] = {
        "input": {
            "description": new_description,
            "resourceUrn": container_urn,
        }
    }

    res_data = execute_graphql(
        auth_session, update_description_query, update_description_variables
    )

    assert res_data["data"]["updateDescription"] is True

    # Now fetch the container to ensure it was updated
    # Get the container
    get_container_query = """query container($urn: String!) {
           container(urn: $urn) {
              editableProperties {
                description
              }
              ownership {
                owners {
                  owner {
                    ...on CorpUser {
                      urn
                    }
                  }
                }
              }
              institutionalMemory {
                elements {
                  url
                }
              }
              tags {
                tags {
                  tag {
                    urn
                  }
                }
              }
              glossaryTerms {
                terms {
                  term {
                    urn
                  }
                }
              }
            }
        }"""
    get_container_variables: Dict[str, Any] = {"urn": container_urn}

    res_data = execute_graphql(
        auth_session, get_container_query, get_container_variables
    )

    assert res_data["data"]["container"] is not None

    container = res_data["data"]["container"]
    assert container["editableProperties"]["description"] == new_description
    assert container["ownership"]["owners"][0]["owner"]["urn"] == new_owner
    assert container["institutionalMemory"]["elements"][0]["url"] == new_link
    assert container["tags"]["tags"][0]["tag"]["urn"] == new_tag
    assert container["glossaryTerms"]["terms"][0]["term"]["urn"] == new_term
