import time

import pytest

from tests.utils import delete_urns_from_file, ingest_file_via_rest


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client, request):
    print("ingesting incidents test data")
    ingest_file_via_rest(auth_session, "tests/incidents/data.json")
    yield
    print("removing incidents test data")
    delete_urns_from_file(graph_client, "tests/incidents/data.json")


TEST_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)"
)
TEST_INCIDENT_URN = "urn:li:incident:test"


@pytest.mark.dependency()
def test_list_dataset_incidents(auth_session):
    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    list_dataset_incidents_json = {
        "query": """query dataset($urn: String!) {\n
            dataset(urn: $urn) {\n
              incidents(state: ACTIVE, start: 0, count: 10) {\n
                start\n
                count\n
                total\n
                incidents {\n
                  urn\n
                  type\n
                  incidentType\n
                  title\n
                  description\n
                  status {\n
                    state\n
                    message\n
                    lastUpdated {\n
                      time\n
                      actor\n
                    }\n
                  }\n
                  source {\n
                    type\n
                    source {\n
                      ... on Assertion {\n
                        urn\n
                        info {\n
                          type
                        }\n
                      }\n
                    }\n
                  }\n
                  entity {\n
                    urn\n
                  }\n
                  created {\n
                    time\n
                    actor\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"urn": TEST_DATASET_URN},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=list_dataset_incidents_json,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]["incidents"] == {
        "start": 0,
        "count": 10,
        "total": 1,
        "incidents": [
            {
                "urn": TEST_INCIDENT_URN,
                "type": "INCIDENT",
                "incidentType": "OPERATIONAL",
                "title": "test title",
                "description": "test description",
                "status": {
                    "state": "ACTIVE",
                    "message": None,
                    "lastUpdated": {"time": 0, "actor": "urn:li:corpuser:admin"},
                },
                "source": {
                    "type": "MANUAL",
                    "source": None,
                },
                "entity": {"urn": TEST_DATASET_URN},
                "created": {"time": 0, "actor": "urn:li:corpuser:admin"},
            }
        ],
    }


@pytest.mark.dependency(depends=["test_list_dataset_incidents"])
def test_raise_resolve_incident(auth_session):
    # Raise new incident
    raise_incident_json = {
        "query": """mutation raiseIncident($input: RaiseIncidentInput!) {\n
            raiseIncident(input: $input)
        }""",
        "variables": {
            "input": {
                "type": "OPERATIONAL",
                "title": "test title 2",
                "description": "test description 2",
                "resourceUrn": TEST_DATASET_URN,
                "priority": 0,
            }
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=raise_incident_json
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["raiseIncident"] is not None

    new_incident_urn = res_data["data"]["raiseIncident"]

    # Resolve the incident.
    update_incident_status = {
        "query": """mutation updateIncidentStatus($urn: String!, $input: UpdateIncidentStatusInput!) {\n
            updateIncidentStatus(urn: $urn, input: $input)
        }""",
        "variables": {
            "urn": new_incident_urn,
            "input": {
                "state": "RESOLVED",
                "message": "test message 2",
            },
        },
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql", json=update_incident_status
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert "errors" not in res_data
    assert res_data["data"]
    assert res_data["data"]["updateIncidentStatus"] is True

    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    # Fetch the dataset's incidents to confirm there's a resolved incident.new_incident_urn
    list_dataset_incidents_json = {
        "query": """query dataset($urn: String!) {\n
            dataset(urn: $urn) {\n
              incidents(state: RESOLVED, start: 0, count: 10) {\n
                start\n
                count\n
                total\n
                incidents {\n
                  urn\n
                  type\n
                  incidentType\n
                  title\n
                  description\n
                  priority\n
                  status {\n
                    state\n
                    message\n
                    lastUpdated {\n
                      time\n
                      actor\n
                    }\n
                  }\n
                  entity {\n
                    urn\n
                  }\n
                  created {\n
                    time\n
                    actor\n
                  }\n
                }\n
              }\n
            }\n
        }""",
        "variables": {"urn": TEST_DATASET_URN},
    }

    response = auth_session.post(
        f"{auth_session.frontend_url()}/api/v2/graphql",
        json=list_dataset_incidents_json,
    )
    response.raise_for_status()
    res_data = response.json()

    assert res_data
    assert res_data["data"]
    assert res_data["data"]["dataset"]["incidents"]["total"] is not None
    assert "errors" not in res_data

    # Find the new incident and do the comparison.
    active_incidents = res_data["data"]["dataset"]["incidents"]["incidents"]
    filtered_incidents = list(
        filter(lambda incident: incident["urn"] == new_incident_urn, active_incidents)
    )
    assert len(filtered_incidents) == 1
    new_incident = filtered_incidents[0]
    assert new_incident["title"] == "test title 2"
    assert new_incident["description"] == "test description 2"
    assert new_incident["status"]["state"] == "RESOLVED"
    assert new_incident["priority"] == 0

    delete_json = {"urn": new_incident_urn}

    # Cleanup: Delete the incident
    response = auth_session.post(
        f"{auth_session.gms_url()}/entities?action=delete", json=delete_json
    )

    response.raise_for_status()
