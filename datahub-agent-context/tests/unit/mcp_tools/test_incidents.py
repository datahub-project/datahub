"""Tests for incident management tools."""

from unittest.mock import Mock

import pytest

from datahub_agent_context.context import DataHubContext
from datahub_agent_context.mcp_tools.incidents import (
    _build_incident_summary,
    list_incidents,
    raise_incident,
    resolve_incident,
)

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.schema.users,PROD)"
INCIDENT_URN = "urn:li:incident:9caa8a45-42c4-45ec-91d5-9cbbb5f2be40"


def _make_incident(
    urn: str = INCIDENT_URN,
    incident_type: str = "OPERATIONAL",
    title: str = "Pipeline failure",
    description: str = "The nightly load failed",
    state: str = "ACTIVE",
    priority: str | None = "HIGH",
) -> dict:
    return {
        "urn": urn,
        "incidentType": incident_type,
        "customType": None,
        "title": title,
        "description": description,
        "priority": priority,
        "incidentStatus": {
            "state": state,
            "stage": "INVESTIGATION" if state == "ACTIVE" else "FIXED",
            "message": None,
            "lastUpdated": {"time": 1700000000000, "actor": "urn:li:corpuser:datahub"},
        },
        "source": {"type": "MANUAL"},
        "created": {"time": 1699999000000, "actor": "urn:li:corpuser:datahub"},
    }


def _make_list_response(
    incidents: list[dict],
    total: int | None = None,
    entity_type: str = "DATASET",
) -> dict:
    return {
        "entity": {
            "urn": DATASET_URN,
            "type": entity_type,
            "incidents": {
                "start": 0,
                "count": len(incidents),
                "total": total if total is not None else len(incidents),
                "incidents": incidents,
            },
        }
    }


@pytest.fixture
def mock_client():
    """Create a mock DataHubClient."""
    mock = Mock()
    mock._graph = Mock()
    mock.graph.execute_graphql = Mock()
    return mock


# --- Unit tests for helper functions ---


class TestBuildIncidentSummary:
    def test_basic_summary(self):
        incident = _make_incident()
        summary = _build_incident_summary(incident)

        assert summary["urn"] == INCIDENT_URN
        assert summary["type"] == "OPERATIONAL"
        assert summary["title"] == "Pipeline failure"
        assert summary["state"] == "ACTIVE"
        assert summary["stage"] == "INVESTIGATION"
        assert summary["priority"] == "HIGH"
        assert summary["source"] == "MANUAL"
        assert summary["createdMillis"] == 1699999000000
        assert summary["createdBy"] == "urn:li:corpuser:datahub"

    def test_missing_nested_fields(self):
        incident = {"urn": INCIDENT_URN}
        summary = _build_incident_summary(incident)

        assert summary["urn"] == INCIDENT_URN
        assert summary["state"] is None
        assert summary["source"] is None
        assert summary["createdMillis"] is None


# --- Tests for list_incidents ---


class TestListIncidents:
    def test_basic_fetch(self, mock_client):
        incidents = [_make_incident(), _make_incident(urn="urn:li:incident:other")]
        mock_client._graph.execute_graphql.return_value = _make_list_response(incidents)

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN)

        assert result["success"] is True
        assert result["data"]["total"] == 2
        assert len(result["data"]["incidents"]) == 2
        assert result["data"]["incidents"][0]["state"] == "ACTIVE"

    def test_default_state_filter_is_active(self, mock_client):
        mock_client._graph.execute_graphql.return_value = _make_list_response([])

        with DataHubContext(mock_client):
            list_incidents(urn=DATASET_URN)

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        assert call_kwargs["variables"]["state"] == "ACTIVE"

    def test_resolved_state_filter(self, mock_client):
        mock_client._graph.execute_graphql.return_value = _make_list_response(
            [_make_incident(state="RESOLVED")]
        )

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN, state="RESOLVED")

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        assert call_kwargs["variables"]["state"] == "RESOLVED"
        assert result["data"]["incidents"][0]["state"] == "RESOLVED"

    def test_no_state_filter(self, mock_client):
        mock_client._graph.execute_graphql.return_value = _make_list_response([])

        with DataHubContext(mock_client):
            list_incidents(urn=DATASET_URN, state=None)

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        assert call_kwargs["variables"]["state"] is None

    def test_no_incidents(self, mock_client):
        mock_client._graph.execute_graphql.return_value = _make_list_response([])

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN)

        assert result["success"] is True
        assert result["data"]["total"] == 0
        assert result["data"]["incidents"] == []

    def test_entity_not_found(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {"entity": None}

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN)

        assert result["success"] is False
        assert "not found" in result["message"]

    def test_unsupported_entity_type(self, mock_client):
        # An entity type without the incidents field (e.g. Tag) matches none of
        # the inline fragments, so the incidents key is absent from the response.
        mock_client._graph.execute_graphql.return_value = {
            "entity": {"urn": "urn:li:tag:pii", "type": "TAG"}
        }

        with DataHubContext(mock_client):
            result = list_incidents(urn="urn:li:tag:pii")

        assert result["success"] is False
        assert "does not support incidents" in result["message"]

    def test_empty_urn_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="urn cannot be empty"):
                list_incidents(urn="")

    def test_pagination_clamped(self, mock_client):
        mock_client._graph.execute_graphql.return_value = _make_list_response([])

        with DataHubContext(mock_client):
            list_incidents(urn=DATASET_URN, start=-5, count=1000)

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        assert call_kwargs["variables"]["start"] == 0
        assert call_kwargs["variables"]["count"] == 50

    def test_graphql_error(self, mock_client):
        mock_client._graph.execute_graphql.side_effect = Exception("Connection failed")

        with DataHubContext(mock_client):
            with pytest.raises(RuntimeError, match="Connection failed"):
                list_incidents(urn=DATASET_URN)

    def test_long_descriptions_truncated(self, mock_client):
        incident = _make_incident(description="x" * 2000)
        mock_client._graph.execute_graphql.return_value = _make_list_response(
            [incident]
        )

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN)

        description = result["data"]["incidents"][0]["description"]
        assert len(description) <= 1000
        assert description.endswith("...")

    def test_not_affected_by_incident_tools_disabled(self, mock_client, monkeypatch):
        monkeypatch.setenv("INCIDENT_TOOLS_ENABLED", "false")
        mock_client._graph.execute_graphql.return_value = _make_list_response(
            [_make_incident()]
        )

        with DataHubContext(mock_client):
            result = list_incidents(urn=DATASET_URN)

        assert result["success"] is True
        assert result["data"]["total"] == 1


# --- Tests for raise_incident ---


class TestRaiseIncident:
    def test_basic_raise(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {
            "raiseIncident": INCIDENT_URN
        }

        with DataHubContext(mock_client):
            result = raise_incident(
                urn=DATASET_URN,
                title="Stale data",
                description="Table not updated since yesterday",
            )

        assert result["success"] is True
        assert result["incidentUrn"] == INCIDENT_URN
        assert result["urn"] == DATASET_URN

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        incident_input = call_kwargs["variables"]["input"]
        assert incident_input["type"] == "OPERATIONAL"
        assert incident_input["title"] == "Stale data"
        assert incident_input["description"] == "Table not updated since yesterday"
        assert incident_input["resourceUrn"] == DATASET_URN
        assert "priority" not in incident_input

    def test_explicit_type_and_priority(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {
            "raiseIncident": INCIDENT_URN
        }

        with DataHubContext(mock_client):
            raise_incident(
                urn=DATASET_URN,
                title="Null spike in customer_email",
                description="23% of rows have null customer_email",
                incident_type="FIELD",
                priority="HIGH",
            )

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        incident_input = call_kwargs["variables"]["input"]
        assert incident_input["type"] == "FIELD"
        assert incident_input["priority"] == "HIGH"

    def test_empty_urn_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="urn cannot be empty"):
                raise_incident(urn="", title="t", description="d")

    def test_empty_title_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="title cannot be empty"):
                raise_incident(urn=DATASET_URN, title="  ", description="d")

    def test_empty_description_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="description cannot be empty"):
                raise_incident(urn=DATASET_URN, title="t", description="")

    def test_invalid_incident_type_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="Invalid incident_type"):
                raise_incident(
                    urn=DATASET_URN,
                    title="t",
                    description="d",
                    incident_type="BOGUS",  # type: ignore[arg-type]
                )

    def test_invalid_priority_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="Invalid priority"):
                raise_incident(
                    urn=DATASET_URN,
                    title="t",
                    description="d",
                    priority="URGENT",  # type: ignore[arg-type]
                )

    def test_no_urn_returned_raises(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {"raiseIncident": None}

        with DataHubContext(mock_client):
            with pytest.raises(RuntimeError, match="no incident urn returned"):
                raise_incident(urn=DATASET_URN, title="t", description="d")

    def test_graphql_error(self, mock_client):
        mock_client._graph.execute_graphql.side_effect = Exception("Connection failed")

        with DataHubContext(mock_client):
            with pytest.raises(RuntimeError, match="Connection failed"):
                raise_incident(urn=DATASET_URN, title="t", description="d")


# --- Tests for resolve_incident ---


class TestResolveIncident:
    def test_basic_resolve(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {"updateIncidentStatus": True}

        with DataHubContext(mock_client):
            result = resolve_incident(
                incident_urn=INCIDENT_URN,
                message="Backfilled missing partitions",
            )

        assert result["success"] is True
        assert result["incidentUrn"] == INCIDENT_URN

        call_kwargs = mock_client._graph.execute_graphql.call_args.kwargs
        assert call_kwargs["variables"]["urn"] == INCIDENT_URN
        status_input = call_kwargs["variables"]["input"]
        assert status_input["state"] == "RESOLVED"
        assert status_input["message"] == "Backfilled missing partitions"

    def test_empty_incident_urn_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="incident_urn cannot be empty"):
                resolve_incident(incident_urn="", message="fixed")

    def test_invalid_incident_urn_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="Invalid incident_urn format"):
                resolve_incident(incident_urn=DATASET_URN, message="fixed")

    def test_empty_message_raises(self, mock_client):
        with DataHubContext(mock_client):
            with pytest.raises(ValueError, match="message cannot be empty"):
                resolve_incident(incident_urn=INCIDENT_URN, message="  ")

    def test_operation_returned_false_raises(self, mock_client):
        mock_client._graph.execute_graphql.return_value = {
            "updateIncidentStatus": False
        }

        with DataHubContext(mock_client):
            with pytest.raises(RuntimeError, match="operation returned false"):
                resolve_incident(incident_urn=INCIDENT_URN, message="fixed")

    def test_graphql_error(self, mock_client):
        mock_client._graph.execute_graphql.side_effect = Exception("Connection failed")

        with DataHubContext(mock_client):
            with pytest.raises(RuntimeError, match="Connection failed"):
                resolve_incident(incident_urn=INCIDENT_URN, message="fixed")


# --- Tests for the INCIDENT_TOOLS_ENABLED kill-switch ---


class TestIncidentToolsEnabledGuard:
    def test_raise_incident_disabled(self, mock_client, monkeypatch):
        monkeypatch.setenv("INCIDENT_TOOLS_ENABLED", "false")

        with DataHubContext(mock_client):
            result = raise_incident(urn=DATASET_URN, title="t", description="d")

        assert result["success"] is False
        assert "disabled" in result["message"]
        mock_client._graph.execute_graphql.assert_not_called()

    def test_resolve_incident_disabled(self, mock_client, monkeypatch):
        monkeypatch.setenv("INCIDENT_TOOLS_ENABLED", "false")

        with DataHubContext(mock_client):
            result = resolve_incident(incident_urn=INCIDENT_URN, message="fixed")

        assert result["success"] is False
        assert "disabled" in result["message"]
        mock_client._graph.execute_graphql.assert_not_called()

    def test_enabled_by_default(self, mock_client, monkeypatch):
        monkeypatch.delenv("INCIDENT_TOOLS_ENABLED", raising=False)
        mock_client._graph.execute_graphql.return_value = {
            "raiseIncident": INCIDENT_URN
        }

        with DataHubContext(mock_client):
            result = raise_incident(urn=DATASET_URN, title="t", description="d")

        assert result["success"] is True
