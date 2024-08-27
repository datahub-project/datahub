import pytest

from tests.test_result_msg import add_datahub_stats
from tests.utils import get_frontend_url


@pytest.mark.read_only
def test_policies_are_accessible(frontend_session):
    json = {
        "query": """
            query listPolicies($input: ListPoliciesInput!) {
                listPolicies(input: $input) {
                    total
                    policies {
                        urn
                        name
                        state
                    }
                }
            }
        """,
        "variables": {"input": {"query": "*"}},
    }

    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"

    res_data = res_json.get("data", {}).get("listPolicies", {})
    assert res_data, f"Received listPolicies were {res_data}"
    assert res_data["total"] > 0, f"Total was {res_data['total']}"
    add_datahub_stats("num-policies", res_data["total"])
