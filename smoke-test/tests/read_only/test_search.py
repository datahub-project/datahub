import pytest
import requests

from tests.test_result_msg import add_datahub_stats
from tests.utils import get_frontend_url, get_gms_url

restli_default_headers = {
    "X-RestLi-Protocol-Version": "2.0.0",
}


def _get_search_result(entity: str):
    json = {"input": "*", "entity": entity, "start": 0, "count": 1}
    response = requests.post(
        f"{get_gms_url()}/entities?action=search",
        headers=restli_default_headers,
        json=json,
    )
    res_data = response.json()
    assert res_data, f"response data was {res_data}"
    assert res_data["value"], f"response data was {res_data}"
    return res_data["value"]


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
def test_search_works(frontend_session, entity_type, api_name):
    search_result = _get_search_result(entity_type)
    num_entities = search_result["numEntities"]
    add_datahub_stats(f"num-{entity_type}", num_entities)
    if num_entities == 0:
        print(f"[WARN] No results for {entity_type}")
        return
    entities = search_result["entities"]

    first_urn = entities[0]["entity"]

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
