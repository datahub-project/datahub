"""
Tests for Teams sink integration with interactive buttons.
"""

import json
import unittest

from datahub_integrations.notifications.sinks.shared.interactive_buttons import (
    IncidentContext,
    ResolveIncidentButton,
    create_incident_buttons,
)


class TestTeamsSinkIntegration(unittest.TestCase):
    """Test Teams sink integration with interactive buttons."""

    def test_teams_sink_button_creation(self) -> None:
        """Test that Teams sink can create appropriate buttons for different incident statuses."""
        # Test active incident - should get resolve button
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Should have 2 actions: View Details and Resolve
        self.assertEqual(len(teams_actions), 2)

        # Find the submit action (resolve button)
        submit_actions = [
            action for action in teams_actions if action["type"] == "Action.Submit"
        ]
        self.assertEqual(len(submit_actions), 1)
        self.assertEqual(submit_actions[0]["data"]["action"], "resolve_incident")

        # Find the open URL action (view details button)
        url_actions = [
            action for action in teams_actions if action["type"] == "Action.OpenUrl"
        ]
        self.assertEqual(len(url_actions), 1)
        self.assertEqual(url_actions[0]["url"], details_url)

    def test_teams_sink_resolved_incident_buttons(self) -> None:
        """Test that Teams sink creates reopen button for resolved incidents."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "RESOLVED", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Should have 2 actions: View Details and Reopen
        self.assertEqual(len(teams_actions), 2)

        # Find the submit action (reopen button)
        submit_actions = [
            action for action in teams_actions if action["type"] == "Action.Submit"
        ]
        self.assertEqual(len(submit_actions), 1)
        self.assertEqual(submit_actions[0]["data"]["action"], "reopen_incident")

    def test_teams_adaptive_card_with_interactive_buttons(self) -> None:
        """Test creating a complete Teams adaptive card with interactive buttons."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Create a complete adaptive card
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
                        "color": "attention",
                    },
                    {
                        "type": "TextBlock",
                        "text": "An incident has been raised on **test_entity**.",
                        "wrap": True,
                        "spacing": "Small",
                    },
                    {
                        "type": "TextBlock",
                        "text": "**Test Incident Title**",
                        "weight": "Bolder",
                        "size": "Medium",
                        "wrap": True,
                        "spacing": "Medium",
                    },
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

        # Verify the card can be JSON serialized
        json_str = json.dumps(adaptive_card)
        parsed_card = json.loads(json_str)
        parsed_actions = parsed_card["content"]["actions"]
        self.assertIsInstance(parsed_actions, list)
        self.assertEqual(len(parsed_actions), 2)

    def test_teams_button_data_structure(self) -> None:
        """Test that Teams button data has the correct structure for event handling."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        teams_format = button.to_teams_format()

        # Verify the structure that Teams will send back
        self.assertEqual(teams_format["type"], "Action.Submit")
        self.assertEqual(teams_format["title"], "Mark as Resolved")
        self.assertIn("data", teams_format)

        data = teams_format["data"]
        self.assertEqual(data["action"], "resolve_incident")
        self.assertEqual(data["incident_urn"], "urn:li:incident:123")
        self.assertEqual(data["incident_stage"], "TRIAGE")

        # Verify this data can be processed by an event handler
        # This simulates what Teams sends when the button is clicked
        submitted_data = data

        # Verify we can extract the necessary information
        action = submitted_data["action"]
        incident_urn = submitted_data["incident_urn"]
        incident_stage = submitted_data["incident_stage"]

        self.assertEqual(action, "resolve_incident")
        self.assertEqual(incident_urn, "urn:li:incident:123")
        self.assertEqual(incident_stage, "TRIAGE")

    def test_teams_button_consistency_across_statuses(self) -> None:
        """Test that Teams buttons are consistent across different incident statuses."""
        details_url = "https://datahub.example.com/incidents/123"

        # Test active incident
        active_context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        active_buttons = create_incident_buttons(active_context, "ACTIVE", details_url)
        active_actions = [button.to_teams_format() for button in active_buttons]

        # Test resolved incident
        resolved_context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        resolved_buttons = create_incident_buttons(
            resolved_context, "RESOLVED", details_url
        )
        resolved_actions = [button.to_teams_format() for button in resolved_buttons]

        # Both should have the same number of actions
        self.assertEqual(len(active_actions), 2)
        self.assertEqual(len(resolved_actions), 2)

        # Both should have a View Details button
        active_url_actions = [
            action for action in active_actions if action["type"] == "Action.OpenUrl"
        ]
        resolved_url_actions = [
            action for action in resolved_actions if action["type"] == "Action.OpenUrl"
        ]

        self.assertEqual(len(active_url_actions), 1)
        self.assertEqual(len(resolved_url_actions), 1)
        self.assertEqual(active_url_actions[0]["url"], details_url)
        self.assertEqual(resolved_url_actions[0]["url"], details_url)

        # Active should have resolve, resolved should have reopen
        active_submit_actions = [
            action for action in active_actions if action["type"] == "Action.Submit"
        ]
        resolved_submit_actions = [
            action for action in resolved_actions if action["type"] == "Action.Submit"
        ]

        self.assertEqual(len(active_submit_actions), 1)
        self.assertEqual(len(resolved_submit_actions), 1)
        self.assertEqual(active_submit_actions[0]["data"]["action"], "resolve_incident")
        self.assertEqual(
            resolved_submit_actions[0]["data"]["action"], "reopen_incident"
        )

    def test_teams_button_with_none_stage(self) -> None:
        """Test Teams buttons with None stage."""
        context = IncidentContext(urn="urn:li:incident:123")  # No stage
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)
        teams_actions = [button.to_teams_format() for button in buttons]

        # Find the submit action
        submit_actions = [
            action for action in teams_actions if action["type"] == "Action.Submit"
        ]
        self.assertEqual(len(submit_actions), 1)

        # Verify the stage is None in the data
        self.assertIsNone(submit_actions[0]["data"]["incident_stage"])

        # Verify it can still be JSON serialized
        json_str = json.dumps(teams_actions)
        parsed_actions = json.loads(json_str)

        # Find the submit action (second action)
        submit_action = parsed_actions[1]  # View Details is first, Submit is second
        self.assertIsNone(submit_action["data"]["incident_stage"])


if __name__ == "__main__":
    unittest.main()
