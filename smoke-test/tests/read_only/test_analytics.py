import pytest


@pytest.mark.read_only
def test_highlights_is_accessible(auth_session):
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
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"


@pytest.mark.read_only
def test_analytics_chart_is_accessible(auth_session):
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
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"


@pytest.mark.read_only
def test_metadata_analytics_chart_is_accessible(auth_session):
    json = {
        "query": """
            query getMetadataAnalyticsCharts($input: MetadataAnalyticsInput!) {
                getMetadataAnalyticsCharts(input: $input) {
                    groupId
                    title
                }
            }
        """,
        "variables": {"input": {"query": "*"}},
    }
    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=json
    )
    res_json = response.json()
    assert res_json, f"Received JSON was {res_json}"
