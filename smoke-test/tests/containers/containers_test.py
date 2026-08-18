import logging
from dataclasses import dataclass
from typing import Any, Dict

import pytest

from tests.utilities.metadata_operations import add_tag, add_term, update_description
from tests.utils import (
    delete_urns_from_file,
    execute_graphql,
    ingest_file_via_rest,
    materialize_with_unique_name,
    with_test_retry,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ContainerUrns:
    """Run-unique URNs of the containers ingested by :func:`ingest_cleanup_data`."""

    schema_urn: str
    database_urn: str


@pytest.fixture(scope="module", autouse=False)
def ingest_cleanup_data(auth_session, graph_client, tmp_path_factory):
    """Ingest the container fixtures under run-unique container URNs.

    data.json used to hardcode ``urn:li:container:SCHEMA`` and
    ``urn:li:container:DATABASE``. Under pytest-xdist ``--dist=loadscope`` (see
    smoke-test/smoke.sh) test modules run concurrently against the same GMS, so
    another module's teardown could delete those containers while this module
    was mid-query. Rewriting both keys to a run-unique value gives this module
    sole ownership of the entities it asserts on.

    ``SCHEMA`` and ``DATABASE`` are uppercase and occur in data.json only inside
    the container URNs, which is what ``materialize_with_unique_name`` requires
    (its substitution is a plain, case-sensitive replace over the whole file).
    The human-readable ``datahub_schema``/``datahub_db`` names are lowercase and
    so are deliberately left untouched.
    """
    tmp_dir = tmp_path_factory.mktemp("containers")
    data_file, schema_key = materialize_with_unique_name(
        "tests/containers/data.json", "SCHEMA", tmp_dir
    )
    data_file, database_key = materialize_with_unique_name(
        data_file, "DATABASE", tmp_dir
    )
    urns = ContainerUrns(
        schema_urn=f"urn:li:container:{schema_key}",
        database_urn=f"urn:li:container:{database_key}",
    )

    # No pre-ingest idempotency delete: the URNs are freshly unique per run, so
    # nothing pre-exists to clean up.
    logger.info(f"ingesting containers test data (schema={urns.schema_urn})")
    ingest_file_via_rest(auth_session, data_file)
    yield urns
    logger.info("removing containers test data")
    delete_urns_from_file(graph_client, data_file)


@pytest.mark.dependency()
# The query below reads platform, subTypes, editableProperties and related
# entities in one shot, and each aspect is indexed independently -- a single
# lagging aspect fails an otherwise correct run. The test is a pure read, so
# retrying it is safe.
@with_test_retry()
def test_get_full_container(auth_session, ingest_cleanup_data):
    container_urn = ingest_cleanup_data.schema_urn
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
    assert container["container"]["urn"] == ingest_cleanup_data.database_urn
    assert container["subTypes"]["typeNames"][0] == "Schema"
    assert (
        container["editableProperties"]["description"] == editable_container_description
    )
    assert container["ownership"] is None
    assert container["institutionalMemory"] is None
    assert container["tags"] is None
    assert container["glossaryTerms"] is None


@pytest.mark.dependency(depends=["test_get_full_container"])
def test_get_parent_container(auth_session, ingest_cleanup_data):
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
def test_update_container(auth_session, ingest_cleanup_data):
    container_urn = ingest_cleanup_data.schema_urn

    # add_tag/add_term/addOwner/addLink below are back-to-back writes to the same
    # container with no intermediate read -- only the combined state after the
    # whole batch (checked once via get_container_query below) matters, so all
    # but the last write skip the sync wait.
    new_tag = "urn:li:tag:Test"
    assert add_tag(auth_session, container_urn, new_tag, no_sync_wait=True)

    new_term = "urn:li:glossaryTerm:Term"
    assert add_term(auth_session, container_urn, new_term, no_sync_wait=True)

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

    res_data = execute_graphql(
        auth_session, add_owner_query, add_owner_variables, no_sync_wait=True
    )

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

    res_data = execute_graphql(
        auth_session, add_link_query, add_link_variables, no_sync_wait=True
    )

    assert res_data["data"]["addLink"] is True

    # Last write in the batch -- keeps the real sync wait.
    new_description = "New description"
    assert update_description(auth_session, container_urn, new_description)

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
