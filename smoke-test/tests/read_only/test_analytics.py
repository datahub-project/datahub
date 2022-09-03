import pytest

from tests.utils import get_frontend_url


@pytest.mark.read_only
def test_highlights_is_accessible(frontend_session):
    json = {
        "query": """
            query getHighlights {
                getHighlights {
                    value
                    title
                    body
                }
            }
        """,
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"


@pytest.mark.read_only
def test_analytics_chart_is_accessible(frontend_session):
    json = {
        "query": """
            query getAnalyticsCharts {
                getAnalyticsCharts {
                    groupId
                    title
                }
            }
        """,
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"


@pytest.mark.read_only
def test_metadata_analytics_chart_is_accessible(frontend_session):
    json = {
        "query": """
            query getMetadataAnalyticsCharts($input: MetadataAnalyticsInput!) {
                getMetadataAnalyticsCharts(input: $input) {
                    groupId
                    title
                }
            }
        """,
    }
    response = frontend_session.post(f"{get_frontend_url()}/api/v2/graphql", json=json)
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"
