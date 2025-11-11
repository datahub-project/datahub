import time
from typing import Any, Dict

import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utils import delete_entity, execute_graphql


@pytest.fixture(scope="module", autouse=True)
def ingest_cleanup_data(auth_session, graph_client):
    yield from _ingest_cleanup_data_impl(
        auth_session, graph_client, "tests/incidents/data.json", "incidents"
    )


TEST_DATASET_URN = (
    "urn:li:dataset:(urn:li:dataPlatform:kafka,incidents-sample-dataset,PROD)"
)
TEST_INCIDENT_URN = "urn:li:incident:test"


@pytest.mark.dependency()
def test_list_dataset_incidents(auth_session):
    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    list_dataset_incidents_query = """query dataset($urn: String!) {
            dataset(urn: $urn) {
              incidents(state: ACTIVE, start: 0, count: 10) {
                start
                count
                total
                incidents {
                  urn
                  type
                  incidentType
                  title
                  description
                  incidentStatus {
                    state
                    message
                    lastUpdated {
                      time
                      actor
                    }
                  }
                  source {
                    type
                    source {
                      ... on Assertion {
                        urn
                        info {
                          type
                        }
                      }
                    }
                  }
                  entity {
                    urn
                  }
                  created {
                    time
                    actor
                  }
                }
              }
            }
        }"""
    list_dataset_incidents_variables: Dict[str, Any] = {"urn": TEST_DATASET_URN}

    res_data = execute_graphql(
        auth_session, list_dataset_incidents_query, list_dataset_incidents_variables
    )

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
                "incidentStatus": {
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
    raise_incident_query = """mutation raiseIncident($input: RaiseIncidentInput!) {
            raiseIncident(input: $input)
        }"""
    raise_incident_variables: Dict[str, Any] = {
        "input": {
            "type": "OPERATIONAL",
            "title": "test title 2",
            "description": "test description 2",
            "resourceUrn": TEST_DATASET_URN,
            "priority": "CRITICAL",
        }
    }

    res_data = execute_graphql(
        auth_session, raise_incident_query, raise_incident_variables
    )

    assert res_data["data"]["raiseIncident"] is not None

    new_incident_urn = res_data["data"]["raiseIncident"]

    # Resolve the incident.
    update_incident_status_query = """mutation updateIncidentStatus($urn: String!, $input: IncidentStatusInput!) {
            updateIncidentStatus(urn: $urn, input: $input)
        }"""
    update_incident_status_variables: Dict[str, Any] = {
        "urn": new_incident_urn,
        "input": {
            "state": "RESOLVED",
            "message": "test message 2",
        },
    }

    res_data = execute_graphql(
        auth_session, update_incident_status_query, update_incident_status_variables
    )

    assert res_data["data"]["updateIncidentStatus"] is True

    # Sleep for eventual consistency (not ideal)
    time.sleep(2)

    # Fetch the dataset's incidents to confirm there's a resolved incident.new_incident_urn
    list_resolved_incidents_query = """query dataset($urn: String!) {
            dataset(urn: $urn) {
              incidents(state: RESOLVED, start: 0, count: 10) {
                start
                count
                total
                incidents {
                  urn
                  type
                  incidentType
                  title
                  description
                  priority
                  incidentStatus {
                    state
                    message
                    lastUpdated {
                      time
                      actor
                    }
                  }
                  entity {
                    urn
                  }
                  created {
                    time
                    actor
                  }
                }
              }
            }
        }"""
    list_resolved_incidents_variables: Dict[str, Any] = {"urn": TEST_DATASET_URN}

    res_data = execute_graphql(
        auth_session, list_resolved_incidents_query, list_resolved_incidents_variables
    )

    assert res_data["data"]["dataset"]["incidents"]["total"] is not None

    # Find the new incident and do the comparison.
    active_incidents = res_data["data"]["dataset"]["incidents"]["incidents"]
    filtered_incidents = list(
        filter(lambda incident: incident["urn"] == new_incident_urn, active_incidents)
    )
    assert len(filtered_incidents) == 1
    new_incident = filtered_incidents[0]
    assert new_incident["title"] == "test title 2"
    assert new_incident["description"] == "test description 2"
    assert new_incident["incidentStatus"]["state"] == "RESOLVED"
    assert new_incident["priority"] == "CRITICAL"

    # Cleanup: Delete the incident
    delete_entity(auth_session, new_incident_urn)
