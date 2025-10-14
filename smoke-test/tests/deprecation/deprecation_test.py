from typing import Any, Dict

import pytest

from tests.utils import (
    delete_urns_from_file,
    execute_graphql,
    get_root_urn,
    ingest_file_via_rest,
)


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting deprecation test data")
    ingest_file_via_rest(auth_session, "tests/deprecation/data.json")
    yield
    print("removing deprecation test data")
    delete_urns_from_file(graph_client, "tests/deprecation/data.json")


@pytest.mark.dependency()
def test_update_deprecation_all_fields(auth_session):
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )

    query = """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                deprecation {\n
                    deprecated\n
                    decommissionTime\n
                    note\n
                    actor\n
                }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"urn": dataset_urn}

    # Fetch tags
    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["dataset"]["deprecation"] is None

    update_query = """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }"""
    update_variables: Dict[str, Any] = {
        "input": {
            "urn": dataset_urn,
            "deprecated": True,
            "note": "My test note",
            "decommissionTime": 0,
        }
    }

    res_data = execute_graphql(auth_session, update_query, update_variables)

    assert res_data["data"]["updateDeprecation"] is True

    # Refetch the dataset
    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["dataset"]["deprecation"] == {
        "deprecated": True,
        "decommissionTime": 0,
        "note": "My test note",
        "actor": get_root_urn(),
    }


@pytest.mark.dependency(depends=["test_update_deprecation_all_fields"])
def test_update_deprecation_partial_fields(auth_session, ingest_cleanup_data):
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )

    update_query = """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }"""
    update_variables: Dict[str, Any] = {
        "input": {"urn": dataset_urn, "deprecated": False}
    }

    res_data = execute_graphql(auth_session, update_query, update_variables)

    assert res_data["data"]["updateDeprecation"] is True

    # Refetch the dataset
    query = """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                deprecation {\n
                    deprecated\n
                    decommissionTime\n
                    note\n
                    actor\n
                }\n
            }\n
        }"""
    variables: Dict[str, Any] = {"urn": dataset_urn}

    res_data = execute_graphql(auth_session, query, variables)

    assert res_data["data"]["dataset"]["deprecation"] == {
        "deprecated": False,
        "note": "",
        "actor": get_root_urn(),
        "decommissionTime": None,
    }
