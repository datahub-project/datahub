import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utils import get_frontend_session, get_frontend_url

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
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


def _get_search_result(frontend_session, entity: str):
    json = {
        "query": """
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
        """,
        "variables": {"input": {"type": ENTITY_TO_MAP.get(entity), "query": "*"}},
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    print(f"Response text was {response.text}")
    res_data = response.json()
    assert res_data, f"response data was {res_data}"
    assert res_data["data"], f"response data was {res_data}"
    assert res_data["data"]["search"], f"response data was {res_data}"
    return res_data["data"]["search"]


@pytest.mark.read_only
@pytest.mark.parametrize(
    "entity_type,api_name",
    [
        ("chart", "chart"),
        ("dataset", "dataset"),
        ("dashboard", "dashboard"),
        (
            # Task
            "dataJob",
            "dataJob",
        ),
        (
            # Pipeline
            "dataFlow",
            "dataFlow",
        ),
        ("container", "container"),
        ("tag", "tag"),
        ("corpUser", "corpUser"),
        ("mlFeature", "mlFeature"),
        ("glossaryTerm", "glossaryTerm"),
        ("domain", "domain"),
        ("mlPrimaryKey", "mlPrimaryKey"),
        ("corpGroup", "corpGroup"),
        ("mlFeatureTable", "mlFeatureTable"),
        (
            # Term group
            "glossaryNode",
            "glossaryNode",
        ),
        ("mlModel", "mlModel"),
    ],
)
def test_search_works(entity_type, api_name):
    frontend_session = get_frontend_session()
    search_result = _get_search_result(frontend_session, entity_type)
    num_entities = search_result["total"]
    add_datahub_stats(f"num-{entity_type}", num_entities)
    if num_entities == 0:
        print(f"[WARN] No results for {entity_type}")
        return
    entities = search_result["searchResults"]

    first_urn = entities[0]["entity"]["urn"]

    json = {
        "query": """
            query """
        + api_name
        + """($input: String!) {
                """
        + api_name
        + """(urn: $input) {
                    urn
                }
            }
        """,
        "variables": {"input": first_urn},
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    response.raise_for_status()
    res_data = response.json()
    assert res_data["data"], f"res_data was {res_data}"
    assert res_data["data"][api_name]["urn"] == first_urn, f"res_data was {res_data}"
