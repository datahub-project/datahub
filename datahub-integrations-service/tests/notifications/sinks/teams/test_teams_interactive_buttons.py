"""
Tests for Teams-specific interactive button functionality.
"""

import json
import unittest

from datahub_integrations.notifications.sinks.shared.interactive_buttons import (
    IncidentContext,
    ReopenIncidentButton,
    ResolveIncidentButton,
    ViewDetailsButton,
    create_incident_buttons,
)


class TestTeamsInteractiveButtons(unittest.TestCase):
    """Test Teams-specific interactive button functionality."""

    def test_resolve_button_teams_format_structure(self) -> None:
        """Test that resolve button produces correct Teams Action.Submit structure."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        teams_format = button.to_teams_format()

        # Verify Teams-specific structure
        self.assertEqual(teams_format["type"], "Action.Submit")
        self.assertEqual(teams_format["title"], "Mark as Resolved")
        self.assertIn("data", teams_format)

        # Verify data structure
        data = teams_format["data"]
        self.assertEqual(data["action"], "resolve_incident")
        self.assertEqual(data["incident_urn"], "urn:li:incident:123")
        self.assertEqual(data["incident_stage"], "TRIAGE")

    def test_reopen_button_teams_format_structure(self) -> None:
        """Test that reopen button produces correct Teams Action.Submit structure."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        button = ReopenIncidentButton(context)
        teams_format = button.to_teams_format()

        # Verify Teams-specific structure
        self.assertEqual(teams_format["type"], "Action.Submit")
        self.assertEqual(teams_format["title"], "Reopen Incident")
        self.assertIn("data", teams_format)

        # Verify data structure
        data = teams_format["data"]
        self.assertEqual(data["action"], "reopen_incident")
        self.assertEqual(data["incident_urn"], "urn:li:incident:123")
        self.assertEqual(data["incident_stage"], "FIXED")

    def test_view_details_button_teams_format_structure(self) -> None:
        """Test that view details button produces correct Teams Action.OpenUrl structure."""
        context = IncidentContext(urn="urn:li:incident:123")
        url = "https://datahub.example.com/incidents/123"
        button = ViewDetailsButton(context, url)
        teams_format = button.to_teams_format()

        # Verify Teams-specific structure
        self.assertEqual(teams_format["type"], "Action.OpenUrl")
        self.assertEqual(teams_format["title"], "View Details")
        self.assertEqual(teams_format["url"], url)
        self.assertNotIn("data", teams_format)  # OpenUrl doesn't have data field

    def test_teams_adaptive_card_integration(self) -> None:
        """Test that buttons can be integrated into Teams adaptive cards."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)

        # Convert to Teams format
        teams_actions = [button.to_teams_format() for button in buttons]

        # Verify we have the right number of actions
        self.assertEqual(len(teams_actions), 2)

        # Verify action types
        action_types = [action["type"] for action in teams_actions]
        self.assertIn("Action.OpenUrl", action_types)
        self.assertIn("Action.Submit", action_types)

        # Verify that the adaptive card structure would be valid
        adaptive_card = {
            "type": "AdaptiveCard",
            "version": "1.4",
            "body": [{"type": "TextBlock", "text": "Test incident"}],
            "actions": teams_actions,
        }

        # Verify the card structure is valid JSON
        json_str = json.dumps(adaptive_card)
        parsed_card = json.loads(json_str)
        self.assertEqual(len(parsed_card["actions"]), 2)

    def test_teams_button_data_serialization(self) -> None:
        """Test that Teams button data can be properly serialized and deserialized."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        teams_format = button.to_teams_format()

        # Simulate what Teams would send back when button is clicked
        submitted_data = teams_format["data"]

        # Verify we can extract the action and incident info
        self.assertEqual(submitted_data["action"], "resolve_incident")
        self.assertEqual(submitted_data["incident_urn"], "urn:li:incident:123")
        self.assertEqual(submitted_data["incident_stage"], "TRIAGE")

        # Verify the data can be JSON serialized (as Teams would do)
        json_str = json.dumps(submitted_data)
        parsed_data = json.loads(json_str)
        self.assertEqual(parsed_data["action"], "resolve_incident")

    def test_teams_button_with_none_stage(self) -> None:
        """Test Teams button format with None stage."""
        context = IncidentContext(urn="urn:li:incident:123")
        button = ResolveIncidentButton(context)
        teams_format = button.to_teams_format()

        data = teams_format["data"]
        self.assertIsNone(data["incident_stage"])

        # Verify it can still be serialized
        json_str = json.dumps(teams_format)
        parsed_format = json.loads(json_str)
        self.assertIsNone(parsed_format["data"]["incident_stage"])


class TestTeamsIncidentButtonIntegration(unittest.TestCase):
    """Test integration of incident buttons with Teams adaptive cards."""

    def test_teams_incident_card_with_resolve_button(self) -> None:
        """Test creating a Teams incident card with resolve button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Create a mock Teams adaptive card
        adaptive_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "🚨 New Data Incident",
                        "weight": "Bolder",
                        "size": "Large",
                    }
                ],
                "actions": teams_actions,
            },
        }

        # Verify the card structure
        self.assertEqual(
            adaptive_card["contentType"], "application/vnd.microsoft.card.adaptive"
        )
        content = adaptive_card["content"]
        self.assertIsInstance(content, dict)
        assert isinstance(content, dict)  # Type assertion for mypy
        self.assertEqual(content["type"], "AdaptiveCard")
        self.assertEqual(content["version"], "1.4")
        actions = content["actions"]
        self.assertIsInstance(actions, list)
        self.assertEqual(len(actions), 2)

        # Verify action types
        action_types = [action["type"] for action in actions]
        self.assertIn("Action.OpenUrl", action_types)
        self.assertIn("Action.Submit", action_types)

    def test_teams_incident_card_with_reopen_button(self) -> None:
        """Test creating a Teams incident card with reopen button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "RESOLVED", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Create a mock Teams adaptive card
        adaptive_card = {
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [
                    {
                        "type": "TextBlock",
                        "text": "✅ Incident Resolved",
                        "weight": "Bolder",
                        "size": "Large",
                    }
                ],
                "actions": teams_actions,
            },
        }

        # Verify the card structure
        content = adaptive_card["content"]
        self.assertIsInstance(content, dict)
        assert isinstance(content, dict)  # Type assertion for mypy
        actions = content["actions"]
        self.assertIsInstance(actions, list)
        self.assertEqual(len(actions), 2)

        # Find the submit action
        submit_actions = [
            action for action in actions if action["type"] == "Action.Submit"
        ]
        self.assertEqual(len(submit_actions), 1)
        self.assertEqual(submit_actions[0]["data"]["action"], "reopen_incident")

    def test_teams_button_action_parsing(self) -> None:
        """Test parsing Teams button action data for incident management."""
        # Simulate what Teams sends when a button is clicked
        teams_submit_data = {
            "action": "resolve_incident",
            "incident_urn": "urn:li:incident:123",
            "incident_stage": "TRIAGE",
        }

        # Verify we can extract the necessary information
        action = teams_submit_data["action"]
        incident_urn = teams_submit_data["incident_urn"]
        incident_stage = teams_submit_data["incident_stage"]

        self.assertEqual(action, "resolve_incident")
        self.assertEqual(incident_urn, "urn:li:incident:123")
        self.assertEqual(incident_stage, "TRIAGE")

        # Verify we can determine the action type
        if action == "resolve_incident":
            action_type = "resolve"
        elif action == "reopen_incident":
            action_type = "reopen"
        else:
            action_type = "unknown"

        self.assertEqual(action_type, "resolve")


if __name__ == "__main__":
    unittest.main()
