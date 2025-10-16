from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import execute_graphql, get_root_urn


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/deprecation/data.json", "deprecation"
    )


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


@pytest.mark.dependency(depends=["test_update_deprecation_all_fields"])
def test_update_deprecation_with_replacement(auth_session, ingest_cleanup_data):
    dataset_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,test-tags-terms-sample-kafka,PROD)"
    )
    replacement_urn = (
        "urn:li:dataset:(urn:li:dataPlatform:kafka,replacement-dataset,PROD)"
    )

    # Add replacement
    update_deprecation_json = {
        "query": """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }""",
        "variables": {
            "input": {
                "urn": dataset_urn,
                "deprecated": True,
                "note": "Replaced by new dataset",
                "decommissionTime": 0,
                "replacement": replacement_urn,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=update_deprecation_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDeprecation"] is True

    # Verify replacement was set
    dataset_json = {
        "query": """query getDataset($urn: String!) {\n
            dataset(urn: $urn) {\n
                deprecation {\n
                    deprecated\n
                    decommissionTime\n
                    note\n
                    actor\n
                    replacement {\n
                        urn\n
                    }\n
                }\n
            }\n
        }""",
        "variables": {"urn": dataset_urn},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["deprecation"] == {
        "deprecated": True,
        "decommissionTime": 0,
        "note": "Replaced by new dataset",
        "actor": get_root_urn(),
        "replacement": {"urn": replacement_urn},
    }

    # Now remove replacement
    update_deprecation_json = {
        "query": """mutation updateDeprecation($input: UpdateDeprecationInput!) {\n
            updateDeprecation(input: $input)
        }""",
        "variables": {
            "input": {
                "urn": dataset_urn,
                "deprecated": True,
                "note": "No longer replaced",
                "decommissionTime": 0,
                "replacement": None,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=update_deprecation_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["updateDeprecation"] is True

    # Verify replacement was removed
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=dataset_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]
    assert res_data["data"]["dataset"]["deprecation"] == {
        "deprecated": True,
        "decommissionTime": 0,
        "note": "No longer replaced",
        "actor": get_root_urn(),
        "replacement": None,
    }
