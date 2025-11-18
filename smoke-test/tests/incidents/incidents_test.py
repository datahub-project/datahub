import pytest

from conftest import _ingest_cleanup_data_impl
from tests.utilities.metadata_operations import (
    list_incidents,
    raise_incident,
    update_incident_status,
)
from tests.utils import delete_entity


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
    incidents_data = list_incidents(auth_session, TEST_DATASET_URN)

    assert incidents_data == {
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
                "priority": None,
                "incidentStatus": {
                    "state": "ACTIVE",
                    "message": None,
                    "lastUpdated": {"time": 0, "actor": "urn:li:corpuser:admin"},
                },
                "source": {
                    "type": "ASSERTION_FAILURE",
                    "source": {
                        "urn": "urn:li:assertion:assertion-test",
                        "info": None,
                    },
                },
                "entity": {"urn": TEST_DATASET_URN},
                "created": {"time": 0, "actor": "urn:li:corpuser:admin"},
            }
        ],
    }


@pytest.mark.dependency(depends=["test_list_dataset_incidents"])
def test_raise_resolve_incident(auth_session, graph_client):
    new_incident_urn = raise_incident(
        auth_session,
        TEST_DATASET_URN,
        "OPERATIONAL",
        "test title 2",
        "test description 2",
        "CRITICAL",
    )

    assert new_incident_urn is not None

    result = update_incident_status(
        auth_session, new_incident_urn, "RESOLVED", "test message 2"
    )

    assert result is True

    incidents_data = list_incidents(auth_session, TEST_DATASET_URN, state="RESOLVED")

    assert incidents_data["total"] is not None

    resolved_incidents = incidents_data["incidents"]
    filtered_incidents = [
        incident
        for incident in resolved_incidents
        if incident["urn"] == new_incident_urn
    ]
    assert len(filtered_incidents) == 1
    new_incident = filtered_incidents[0]
    assert new_incident["title"] == "test title 2"
    assert new_incident["description"] == "test description 2"
    assert new_incident["incidentStatus"]["state"] == "RESOLVED"
    assert new_incident["priority"] == "CRITICAL"

    delete_entity(auth_session, new_incident_urn)
