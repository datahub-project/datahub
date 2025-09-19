"""
Tests for shared interactive button components.
"""

import json
import unittest

from datahub_integrations.notifications.sinks.shared.interactive_buttons import (
    IncidentContext,
    InteractiveButton,
    ReopenIncidentButton,
    ResolveIncidentButton,
    ViewDetailsButton,
    create_incident_buttons,
)


class TestIncidentContext(unittest.TestCase):
    """Test IncidentContext dataclass."""

    def test_incident_context_creation(self) -> None:
        """Test creating an incident context with required fields."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        self.assertEqual(context.urn, "urn:li:incident:123")
        self.assertEqual(context.stage, "TRIAGE")

    def test_incident_context_optional_stage(self) -> None:
        """Test creating an incident context with optional stage."""
        context = IncidentContext(urn="urn:li:incident:123")
        self.assertEqual(context.urn, "urn:li:incident:123")
        self.assertIsNone(context.stage)


class TestResolveIncidentButton(unittest.TestCase):
    """Test ResolveIncidentButton functionality."""

    def test_resolve_button_creation(self) -> None:
        """Test creating a resolve incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        self.assertEqual(button.incident_context, context)
        self.assertEqual(button.get_action_id(), "resolve_incident")

    def test_resolve_button_slack_format(self) -> None:
        """Test Slack format for resolve incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        slack_format = button.to_slack_format()

        self.assertEqual(slack_format["type"], "button")
        self.assertEqual(slack_format["text"]["text"], "Mark as Resolved")
        self.assertEqual(slack_format["style"], "primary")
        self.assertEqual(slack_format["action_id"], "resolve_incident")

        # Test that value contains serialized context
        value_data = json.loads(slack_format["value"])
        self.assertEqual(value_data["urn"], "urn:li:incident:123")
        self.assertEqual(value_data["stage"], "TRIAGE")

    def test_resolve_button_teams_format(self) -> None:
        """Test Teams format for resolve incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)
        teams_format = button.to_teams_format()

        self.assertEqual(teams_format["type"], "Action.Submit")
        self.assertEqual(teams_format["title"], "Mark as Resolved")
        self.assertEqual(teams_format["data"]["action"], "resolve_incident")
        self.assertEqual(teams_format["data"]["incident_urn"], "urn:li:incident:123")
        self.assertEqual(teams_format["data"]["incident_stage"], "TRIAGE")

    def test_resolve_button_with_none_stage(self) -> None:
        """Test resolve button with None stage."""
        context = IncidentContext(urn="urn:li:incident:123")
        button = ResolveIncidentButton(context)

        slack_format = button.to_slack_format()
        value_data = json.loads(slack_format["value"])
        self.assertIsNone(value_data["stage"])

        teams_format = button.to_teams_format()
        self.assertIsNone(teams_format["data"]["incident_stage"])


class TestReopenIncidentButton(unittest.TestCase):
    """Test ReopenIncidentButton functionality."""

    def test_reopen_button_creation(self) -> None:
        """Test creating a reopen incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        button = ReopenIncidentButton(context)
        self.assertEqual(button.incident_context, context)
        self.assertEqual(button.get_action_id(), "reopen_incident")

    def test_reopen_button_slack_format(self) -> None:
        """Test Slack format for reopen incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        button = ReopenIncidentButton(context)
        slack_format = button.to_slack_format()

        self.assertEqual(slack_format["type"], "button")
        self.assertEqual(slack_format["text"]["text"], "Reopen Incident")
        self.assertEqual(slack_format["style"], "primary")
        self.assertEqual(slack_format["action_id"], "reopen_incident")

        # Test that value contains serialized context
        value_data = json.loads(slack_format["value"])
        self.assertEqual(value_data["urn"], "urn:li:incident:123")
        self.assertEqual(value_data["stage"], "FIXED")

    def test_reopen_button_teams_format(self) -> None:
        """Test Teams format for reopen incident button."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        button = ReopenIncidentButton(context)
        teams_format = button.to_teams_format()

        self.assertEqual(teams_format["type"], "Action.Submit")
        self.assertEqual(teams_format["title"], "Reopen Incident")
        self.assertEqual(teams_format["data"]["action"], "reopen_incident")
        self.assertEqual(teams_format["data"]["incident_urn"], "urn:li:incident:123")
        self.assertEqual(teams_format["data"]["incident_stage"], "FIXED")


class TestViewDetailsButton(unittest.TestCase):
    """Test ViewDetailsButton functionality."""

    def test_view_details_button_creation(self) -> None:
        """Test creating a view details button."""
        context = IncidentContext(urn="urn:li:incident:123")
        url = "https://datahub.example.com/incidents/123"
        button = ViewDetailsButton(context, url)
        self.assertEqual(button.incident_context, context)
        self.assertEqual(button.url, url)
        self.assertEqual(button.get_action_id(), "external_redirect")

    def test_view_details_button_slack_format(self) -> None:
        """Test Slack format for view details button."""
        context = IncidentContext(urn="urn:li:incident:123")
        url = "https://datahub.example.com/incidents/123"
        button = ViewDetailsButton(context, url)
        slack_format = button.to_slack_format()

        self.assertEqual(slack_format["type"], "button")
        self.assertEqual(slack_format["text"]["text"], "View Details")
        self.assertEqual(slack_format["url"], url)
        self.assertEqual(slack_format["action_id"], "external_redirect")

    def test_view_details_button_teams_format(self) -> None:
        """Test Teams format for view details button."""
        context = IncidentContext(urn="urn:li:incident:123")
        url = "https://datahub.example.com/incidents/123"
        button = ViewDetailsButton(context, url)
        teams_format = button.to_teams_format()

        self.assertEqual(teams_format["type"], "Action.OpenUrl")
        self.assertEqual(teams_format["title"], "View Details")
        self.assertEqual(teams_format["url"], url)


class TestCreateIncidentButtons(unittest.TestCase):
    """Test create_incident_buttons helper function."""

    def test_create_buttons_for_active_incident(self) -> None:
        """Test creating buttons for an active incident."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "ACTIVE", details_url)

        self.assertEqual(len(buttons), 2)
        self.assertIsInstance(buttons[0], ViewDetailsButton)
        self.assertIsInstance(buttons[1], ResolveIncidentButton)
        view_button = buttons[0]
        resolve_button = buttons[1]
        assert isinstance(view_button, ViewDetailsButton)  # Type assertion for mypy
        assert isinstance(
            resolve_button, ResolveIncidentButton
        )  # Type assertion for mypy
        self.assertEqual(view_button.url, details_url)
        self.assertEqual(resolve_button.get_action_id(), "resolve_incident")

    def test_create_buttons_for_resolved_incident(self) -> None:
        """Test creating buttons for a resolved incident."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "RESOLVED", details_url)

        self.assertEqual(len(buttons), 2)
        self.assertIsInstance(buttons[0], ViewDetailsButton)
        self.assertIsInstance(buttons[1], ReopenIncidentButton)
        view_button = buttons[0]
        reopen_button = buttons[1]
        assert isinstance(view_button, ViewDetailsButton)  # Type assertion for mypy
        assert isinstance(
            reopen_button, ReopenIncidentButton
        )  # Type assertion for mypy
        self.assertEqual(view_button.url, details_url)
        self.assertEqual(reopen_button.get_action_id(), "reopen_incident")

    def test_create_buttons_for_unknown_status(self) -> None:
        """Test creating buttons for an incident with unknown status."""
        context = IncidentContext(urn="urn:li:incident:123")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, "UNKNOWN", details_url)

        self.assertEqual(len(buttons), 2)
        self.assertIsInstance(buttons[0], ViewDetailsButton)
        self.assertIsInstance(
            buttons[1], ReopenIncidentButton
        )  # Default to reopen for unknown status

    def test_create_buttons_for_none_status(self) -> None:
        """Test creating buttons for an incident with None status."""
        context = IncidentContext(urn="urn:li:incident:123")
        details_url = "https://datahub.example.com/incidents/123"

        buttons = create_incident_buttons(context, None, details_url)

        self.assertEqual(len(buttons), 2)
        self.assertIsInstance(buttons[0], ViewDetailsButton)
        self.assertIsInstance(
            buttons[1], ResolveIncidentButton
        )  # Default to resolve for None status


class TestInteractiveButtonAbstract(unittest.TestCase):
    """Test InteractiveButton abstract base class."""

    def test_interactive_button_is_abstract(self) -> None:
        """Test that InteractiveButton cannot be instantiated directly."""
        context = IncidentContext(urn="urn:li:incident:123")

        with self.assertRaises(TypeError):
            InteractiveButton(context)  # type: ignore[abstract]

    def test_concrete_button_inheritance(self) -> None:
        """Test that concrete button classes properly inherit from InteractiveButton."""
        context = IncidentContext(urn="urn:li:incident:123")

        resolve_button = ResolveIncidentButton(context)
        reopen_button = ReopenIncidentButton(context)
        view_button = ViewDetailsButton(context, "https://example.com")

        # All should be instances of InteractiveButton
        self.assertIsInstance(resolve_button, InteractiveButton)
        self.assertIsInstance(reopen_button, InteractiveButton)
        self.assertIsInstance(view_button, InteractiveButton)

        # All should have required methods
        self.assertTrue(hasattr(resolve_button, "to_slack_format"))
        self.assertTrue(hasattr(resolve_button, "to_teams_format"))
        self.assertTrue(hasattr(resolve_button, "get_action_id"))

        self.assertTrue(hasattr(reopen_button, "to_slack_format"))
        self.assertTrue(hasattr(reopen_button, "to_teams_format"))
        self.assertTrue(hasattr(reopen_button, "get_action_id"))

        self.assertTrue(hasattr(view_button, "to_slack_format"))
        self.assertTrue(hasattr(view_button, "to_teams_format"))
        self.assertTrue(hasattr(view_button, "get_action_id"))


class TestButtonFormatConsistency(unittest.TestCase):
    """Test consistency between different button formats."""

    def test_resolve_button_format_consistency(self) -> None:
        """Test that resolve button formats are consistent."""
        context = IncidentContext(urn="urn:li:incident:123", stage="TRIAGE")
        button = ResolveIncidentButton(context)

        slack_format = button.to_slack_format()
        teams_format = button.to_teams_format()

        # Both should reference the same incident
        slack_value = json.loads(slack_format["value"])
        self.assertEqual(slack_value["urn"], teams_format["data"]["incident_urn"])
        self.assertEqual(slack_value["stage"], teams_format["data"]["incident_stage"])

        # Both should have the same action identifier
        self.assertEqual(slack_format["action_id"], teams_format["data"]["action"])

    def test_reopen_button_format_consistency(self) -> None:
        """Test that reopen button formats are consistent."""
        context = IncidentContext(urn="urn:li:incident:123", stage="FIXED")
        button = ReopenIncidentButton(context)

        slack_format = button.to_slack_format()
        teams_format = button.to_teams_format()

        # Both should reference the same incident
        slack_value = json.loads(slack_format["value"])
        self.assertEqual(slack_value["urn"], teams_format["data"]["incident_urn"])
        self.assertEqual(slack_value["stage"], teams_format["data"]["incident_stage"])

        # Both should have the same action identifier
        self.assertEqual(slack_format["action_id"], teams_format["data"]["action"])


if __name__ == "__main__":
    unittest.main()
