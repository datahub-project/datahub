"""
Tests for Teams event handlers.
"""

import unittest
from typing import Any
from unittest.mock import Mock, patch

from datahub_integrations.teams.event_handlers import TeamsIncidentEventHandler


class TestTeamsIncidentEventHandler(unittest.TestCase):
    """Test TeamsIncidentEventHandler functionality."""

    def setUp(self) -> None:
        """Set up test fixtures."""
        self.mock_graph_client = Mock()
        self.mock_identity_provider = Mock()
        self.handler = TeamsIncidentEventHandler(
            self.mock_graph_client, self.mock_identity_provider
        )

    def test_parse_teams_submit_data(self) -> None:
        """Test parsing Teams Action.Submit data."""
        submit_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        parsed_data = self.handler.parse_teams_submit_data(submit_data)

        self.assertEqual(parsed_data["action"], "resolve_incident")
        self.assertEqual(parsed_data["incident_urn"], "urn:li:incident:123")
        self.assertEqual(parsed_data["incident_stage"], "TRIAGE")

    def test_parse_teams_submit_data_with_missing_fields(self) -> None:
        """Test parsing Teams submit data with missing fields."""
        submit_data = {
            "action": "resolve_incident"
            # Missing incident_urn and incident_stage
        }

        parsed_data = self.handler.parse_teams_submit_data(submit_data)

        self.assertEqual(parsed_data["action"], "resolve_incident")
        self.assertIsNone(parsed_data["incident_urn"])
        self.assertIsNone(parsed_data["incident_stage"])

    def test_validate_action_data_valid(self) -> None:
        """Test validation of valid action data."""
        action_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        error = self.handler.validate_action_data(action_data)
        self.assertIsNone(error)

    def test_validate_action_data_missing_action(self) -> None:
        """Test validation of action data missing action field."""
        action_data = {
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        error = self.handler.validate_action_data(action_data)
        self.assertEqual(error, "Missing required field: action")

    def test_validate_action_data_missing_incident_urn(self) -> None:
        """Test validation of action data missing incident_urn field."""
        action_data = {"action": "resolve_incident", "incident_stage": "TRIAGE"}

        error = self.handler.validate_action_data(action_data)
        self.assertEqual(error, "Missing required field: incident_urn")

    def test_validate_action_data_invalid_action(self) -> None:
        """Test validation of action data with invalid action."""
        action_data = {
            "action": "invalid_action",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        error = self.handler.validate_action_data(action_data)
        self.assertIsNotNone(error)
        assert error is not None  # Type assertion for mypy
        self.assertIn("Invalid action: invalid_action", error)

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_missing_urn(self, mock_logger: Any) -> None:
        """Test handling incident action with missing URN."""
        action_data = {
            "action": "resolve_incident"
            # Missing incident_urn
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Missing incident URN")

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_missing_action(
        self, mock_logger: Any
    ) -> None:
        """Test handling incident action with missing action."""
        action_data = {
            "incident_urn": "urn:li:incident:123"
            # Missing action
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Missing action")

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_unknown_action(
        self, mock_logger: Any
    ) -> None:
        """Test handling incident action with unknown action."""
        action_data = {
            "action": "unknown_action",
            "incident_urn": "urn:li:incident:123",
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertEqual(result["error"], "Unknown action: unknown_action")

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_resolve_success(
        self, mock_logger: Any
    ) -> None:
        """Test successful incident resolution."""
        action_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        # Mock successful GraphQL response
        self.mock_graph_client.execute_graphql.return_value = {
            "resolveIncident": {"success": True}
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], "Incident resolved successfully")
        self.assertEqual(result["action"], "resolve_incident")

        # Verify GraphQL was called correctly
        self.mock_graph_client.execute_graphql.assert_called_once()
        call_args = self.mock_graph_client.execute_graphql.call_args
        self.assertIn(
            "resolveIncident", call_args[0][0]
        )  # Check mutation contains resolveIncident
        self.assertEqual(call_args[0][1]["urn"], "urn:li:incident:123")

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_resolve_failure(
        self, mock_logger: Any
    ) -> None:
        """Test failed incident resolution."""
        action_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        # Mock failed GraphQL response
        self.mock_graph_client.execute_graphql.return_value = {
            "resolveIncident": {"success": False}
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertIn("Failed to resolve incident", result["error"])

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_reopen_success(
        self, mock_logger: Any
    ) -> None:
        """Test successful incident reopening."""
        action_data = {
            "action": "reopen_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "FIXED",
        }

        # Mock successful GraphQL response
        self.mock_graph_client.execute_graphql.return_value = {
            "reopenIncident": {"success": True}
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertTrue(result["success"])
        self.assertEqual(result["message"], "Incident reopened successfully")
        self.assertEqual(result["action"], "reopen_incident")

        # Verify GraphQL was called correctly
        self.mock_graph_client.execute_graphql.assert_called_once()
        call_args = self.mock_graph_client.execute_graphql.call_args
        self.assertIn(
            "reopenIncident", call_args[0][0]
        )  # Check mutation contains reopenIncident
        self.assertEqual(call_args[0][1]["urn"], "urn:li:incident:123")

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_reopen_failure(
        self, mock_logger: Any
    ) -> None:
        """Test failed incident reopening."""
        action_data = {
            "action": "reopen_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "FIXED",
        }

        # Mock failed GraphQL response
        self.mock_graph_client.execute_graphql.return_value = {
            "reopenIncident": {"success": False}
        }

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertIn("Failed to reopen incident", result["error"])

    @patch("datahub_integrations.teams.event_handlers.logger")
    async def test_handle_incident_action_graphql_exception(
        self, mock_logger: Any
    ) -> None:
        """Test handling GraphQL exceptions during incident resolution."""
        action_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        # Mock GraphQL exception
        self.mock_graph_client.execute_graphql.side_effect = Exception("GraphQL error")

        result = await self.handler.handle_incident_action(action_data, "user123")

        self.assertFalse(result["success"])
        self.assertIn("Failed to resolve incident", result["error"])
        self.assertIn("GraphQL error", result["error"])

    # Note: _resolve_incident and _reopen_incident are private methods that don't exist
    # These tests have been removed as they were testing non-existent methods


if __name__ == "__main__":
    unittest.main()
