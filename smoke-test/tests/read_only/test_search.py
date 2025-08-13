from urllib.parse import quote

import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utils import execute_graphql_query, get_gms_url

BASE_URL_V3 = f"{get_gms_url()}/openapi/v3"

default_headers = {
    "Content-Type": "application/json",
}

ENTITY_TO_MAP = {
    "chart": "CHART",
    "dataset": "DATASET",
    "dashboard": "DASHBOARD",
    "dataJob": "DATA_JOB",
    "dataFlow": "DATA_FLOW",
    "container": "CONTAINER",
    "tag": "TAG",
    "corpUser": "CORP_USER",
    "mlFeature": "MLFEATURE",
    "glossaryTerm": "GLOSSARY_TERM",
    "domain": "DOMAIN",
    "mlPrimaryKey": "MLPRIMARY_KEY",
    "corpGroup": "CORP_GROUP",
    "mlFeatureTable": "MLFEATURE_TABLE",
    "glossaryNode": "GLOSSARY_NODE",
    "mlModel": "MLMODEL",
}


def _get_search_result(auth_session, entity: str):
    search_query = """
    query search($input: SearchInput!) {
        search(input: $input) {
            total
            searchResults {
                entity {
                    urn
                }
            }
        }
    }
    """

    res_data = execute_graphql_query(
        auth_session,
        search_query,
        variables={"input": {"type": ENTITY_TO_MAP.get(entity), "query": "*"}},
        expected_data_key="search",
    )
    print(f"Response text was {res_data}")
    return res_data["data"]["search"]


@pytest.mark.read_only
@pytest.mark.parametrize(
    "entity_type,api_name",
    [
        ("chart", "chart"),
        ("dataset", "dataset"),
        ("dashboard", "dashboard"),
        ("dataJob", "dataJob"),
        ("dataFlow", "dataFlow"),
        ("container", "container"),
        ("tag", "tag"),
        ("corpUser", "corpUser"),
        ("mlFeature", "mlFeature"),
        ("glossaryTerm", "glossaryTerm"),
        ("domain", "domain"),
        ("mlPrimaryKey", "mlPrimaryKey"),
        ("corpGroup", "corpGroup"),
        ("mlFeatureTable", "mlFeatureTable"),
        ("glossaryNode", "glossaryNode"),
        ("mlModel", "mlModel"),
    ],
)
def test_search_works(auth_session, entity_type, api_name):
    search_result = _get_search_result(auth_session, entity_type)
    num_entities = search_result["total"]
    add_datahub_stats(f"num-{entity_type}", num_entities)
    if num_entities == 0:
        print(f"[WARN] No results for {entity_type}")
        return
    entities = search_result["searchResults"]

    first_urn = entities[0]["entity"]["urn"]

    entity_query = f"""
        query {api_name}($input: String!) {{
            {api_name}(urn: $input) {{
                urn
            }}
        }}
    """

    res_data = execute_graphql_query(
        auth_session,
        entity_query,
        variables={"input": first_urn},
        expected_data_key=api_name,
    )
    assert res_data["data"][api_name]["urn"] == first_urn, f"res_data was {res_data}"


@pytest.mark.read_only
@pytest.mark.parametrize(
    "entity_type",
    [
        "chart",
        "dataset",
        "dashboard",
        "dataJob",
        "dataFlow",
        "container",
        "tag",
        "corpUser",
        "mlFeature",
        "glossaryTerm",
        "domain",
        "mlPrimaryKey",
        "corpGroup",
        "mlFeatureTable",
        "glossaryNode",
        "mlModel",
    ],
)
def test_openapi_v3_entity(auth_session, entity_type):
    search_result = _get_search_result(auth_session, entity_type)
    num_entities = search_result["total"]
    if num_entities == 0:
        print(f"[WARN] No results for {entity_type}")
        return
    entities = search_result["searchResults"]

    first_urn = entities[0]["entity"]["urn"]

    encoded_urn = quote(first_urn, safe="")
    url = f"{BASE_URL_V3}/entity/{entity_type}/{encoded_urn}"
    response = auth_session.get(url, headers=default_headers)
    response.raise_for_status()
    actual_data = response.json()
    print(f"Entity Data for URN {first_urn}: {actual_data}")

    expected_data = {"urn": first_urn}

    assert actual_data["urn"] == expected_data["urn"], (
        f"Mismatch: expected {expected_data}, got {actual_data}"
    )
