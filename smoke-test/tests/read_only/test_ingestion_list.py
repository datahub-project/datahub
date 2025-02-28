import pytest

from tests.test_result_msg import add_datahub_stats


@pytest.mark.read_only
def test_policies_are_accessible(auth_session):
    json = {
        "query": """
            query listIngestionSources($input: ListIngestionSourcesInput!) {
                listIngestionSources(input: $input) {
                    total
                    ingestionSources {
                        urn
                        name
                        type
                        config {
                            version
                        }
                    }
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

    res_data = res_json.get("data", {}).get("listIngestionSources", {})
    assert res_data, f"Received listIngestionSources were {res_data}"
    add_datahub_stats("num-ingestion-sources", res_data["total"])

    if res_data["total"] > 0:
        for ingestion_source in res_data.get("ingestionSources"):
            name = ingestion_source.get("name")
            source_type = ingestion_source.get("type")
            urn = ingestion_source.get("urn")
            version = ingestion_source.get("config", {}).get("version")
            add_datahub_stats(
                f"ingestion-source-{urn}",
                {"name": name, "version": version, "source_type": source_type},
            )
