"""
Comprehensive tests for Teams progress message utilities.
Tests cover all functions and edge cases for progress messaging.
"""

from datahub_integrations.teams.utils.progress import (
    build_teams_initial_progress_message,
    build_teams_progress_message,
)


class TestBuildTeamsProgressMessage:
    """Test build_teams_progress_message function with various scenarios."""

    def test_empty_steps_returns_starting_message(self) -> None:
        """Test that empty steps list returns default starting message."""
        result = build_teams_progress_message([])

        assert result["type"] == "message"
        assert result["text"] == "🤔 Starting to process your question..."

    def test_single_step_shows_current_step_only(self) -> None:
        """Test progress message with single step."""
        steps = ["Searching for datasets"]
        result = build_teams_progress_message(steps)

        assert result["type"] == "message"
        assert "⏳ *Searching for datasets*" in result["text"]
        assert "✅" not in result["text"]  # No completed steps

    def test_two_steps_shows_one_completed_one_current(self) -> None:
        """Test progress message with two steps."""
        steps = ["Searching for datasets", "Analyzing results"]
        result = build_teams_progress_message(steps)

        assert result["type"] == "message"
        text = result["text"]
        assert "✅ *Searching for datasets*" in text
        assert "⏳ *Analyzing results*" in text

    def test_multiple_steps_shows_all_previous_as_completed(self) -> None:
        """Test progress message with multiple steps."""
        steps = [
            "Step 1: Initialize",
            "Step 2: Search",
            "Step 3: Filter",
            "Step 4: Current work",
        ]
        result = build_teams_progress_message(steps)

        text = result["text"]
        assert "✅ *Step 1: Initialize*" in text
        assert "✅ *Step 2: Search*" in text
        assert "✅ *Step 3: Filter*" in text
        assert "⏳ *Step 4: Current work*" in text

    def test_many_steps_limits_to_last_8_previous(self) -> None:
        """Test that only last 8 previous steps are shown to avoid message length limits."""
        steps = [f"Step {i}" for i in range(1, 12)]  # 11 steps total
        result = build_teams_progress_message(steps)

        text = result["text"]

        # Should not include the first 2 steps (steps 1-2)
        assert "✅ *Step 1*" not in text
        assert "✅ *Step 2*" not in text

        # Should include steps 3-10 as completed (8 previous steps)
        for i in range(3, 11):
            assert f"✅ *Step {i}*" in text

        # Should include step 11 as current
        assert "⏳ *Step 11*" in text

    def test_exactly_9_steps_shows_8_previous_1_current(self) -> None:
        """Test edge case with exactly 9 steps (8 previous + 1 current)."""
        steps = [f"Step {i}" for i in range(1, 10)]  # 9 steps total
        result = build_teams_progress_message(steps)

        text = result["text"]

        # Should include all 8 previous steps
        for i in range(1, 9):
            assert f"✅ *Step {i}*" in text

        # Should include step 9 as current
        assert "⏳ *Step 9*" in text

    def test_message_formatting_uses_double_newlines(self) -> None:
        """Test that progress steps are separated by double newlines for Teams."""
        steps = ["First step", "Second step", "Third step"]
        result = build_teams_progress_message(steps)

        text = result["text"]
        # Should use double newlines for Teams formatting
        assert "\n\n" in text

        # Verify the exact format
        expected_parts = ["✅ *First step*", "✅ *Second step*", "⏳ *Third step*"]
        expected_text = "\n\n".join(expected_parts)
        assert text == expected_text

    def test_steps_with_special_characters(self) -> None:
        """Test progress message handles steps with special characters."""
        steps = [
            "Searching for 'datasets' with @symbols",
            "Analyzing results: 100% complete!",
            "Finalizing query... (almost done)",
        ]
        result = build_teams_progress_message(steps)

        text = result["text"]
        assert "✅ *Searching for 'datasets' with @symbols*" in text
        assert "✅ *Analyzing results: 100% complete!*" in text
        assert "⏳ *Finalizing query... (almost done)*" in text

    def test_steps_with_empty_strings(self) -> None:
        """Test progress message handles empty step strings."""
        steps = ["Valid step", "", "Another valid step"]
        result = build_teams_progress_message(steps)

        text = result["text"]
        assert "✅ *Valid step*" in text
        assert "✅ **" in text  # Empty step becomes "✅ **"
        assert "⏳ *Another valid step*" in text

    def test_steps_with_very_long_text(self) -> None:
        """Test progress message with very long step descriptions."""
        long_step = "A" * 200  # Very long step
        steps = ["Short step", long_step]
        result = build_teams_progress_message(steps)

        text = result["text"]
        assert "✅ *Short step*" in text
        assert f"⏳ *{long_step}*" in text

    def test_unicode_characters_in_steps(self) -> None:
        """Test progress message handles Unicode characters in steps."""
        steps = [
            "🔍 Searching datasets",
            "📊 Analyzing data with émojis",
            "✨ Finalizing résults",
        ]
        result = build_teams_progress_message(steps)

        text = result["text"]
        assert "✅ *🔍 Searching datasets*" in text
        assert "✅ *📊 Analyzing data with émojis*" in text
        assert "⏳ *✨ Finalizing résults*" in text


class TestBuildTeamsInitialProgressMessage:
    """Test build_teams_initial_progress_message function."""

    def test_default_initial_message(self) -> None:
        """Test that default initial message is returned when no custom message provided."""
        result = build_teams_initial_progress_message()

        assert result["type"] == "message"
        expected_text = "⏳ *Sure thing! I'm looking through the available data to answer your question. Hold on a second...*"
        assert result["text"] == expected_text

    def test_custom_initial_message(self) -> None:
        """Test that custom initial message is used when provided."""
        custom_message = "Processing your request..."
        result = build_teams_initial_progress_message(custom_message)

        assert result["type"] == "message"
        assert result["text"] == "⏳ *Processing your request...*"

    def test_empty_custom_message_uses_default(self) -> None:
        """Test that empty string uses default message."""
        result = build_teams_initial_progress_message("")

        assert result["type"] == "message"
        expected_text = "⏳ *Sure thing! I'm looking through the available data to answer your question. Hold on a second...*"
        assert result["text"] == expected_text

    def test_none_custom_message_uses_default(self) -> None:
        """Test that None uses default message."""
        result = build_teams_initial_progress_message(None)

        assert result["type"] == "message"
        expected_text = "⏳ *Sure thing! I'm looking through the available data to answer your question. Hold on a second...*"
        assert result["text"] == expected_text

    def test_custom_message_with_special_characters(self) -> None:
        """Test custom message with special characters."""
        custom_message = "Analyzing data: 50% complete! @user"
        result = build_teams_initial_progress_message(custom_message)

        assert result["type"] == "message"
        assert result["text"] == "⏳ *Analyzing data: 50% complete! @user*"

    def test_custom_message_with_unicode(self) -> None:
        """Test custom message with Unicode characters."""
        custom_message = "🚀 Starting analysis with advanced AI..."
        result = build_teams_initial_progress_message(custom_message)

        assert result["type"] == "message"
        assert result["text"] == "⏳ *🚀 Starting analysis with advanced AI...*"

    def test_very_long_custom_message(self) -> None:
        """Test custom message with very long text."""
        long_message = "A" * 500  # Very long message
        result = build_teams_initial_progress_message(long_message)

        assert result["type"] == "message"
        assert result["text"] == f"⏳ *{long_message}*"

    def test_message_format_consistency(self) -> None:
        """Test that initial message format is consistent with progress message format."""
        initial_result = build_teams_initial_progress_message("Test message")
        progress_result = build_teams_progress_message(["Test step"])

        # Both should have the same structure
        assert initial_result["type"] == progress_result["type"] == "message"
        assert "text" in initial_result
        assert "text" in progress_result

        # Both should use the ⏳ emoji for current/active state
        assert initial_result["text"].startswith("⏳")
        assert "⏳" in progress_result["text"]


class TestProgressMessageIntegration:
    """Test integration scenarios between initial and progress messages."""

    def test_progress_flow_simulation(self) -> None:
        """Test a complete progress flow from initial to completion."""
        # Start with initial message
        initial = build_teams_initial_progress_message("Starting data analysis...")
        assert "⏳ *Starting data analysis...*" == initial["text"]

        # Progress through multiple steps
        step1 = build_teams_progress_message(["Connecting to database"])
        assert "⏳ *Connecting to database*" == step1["text"]

        step2 = build_teams_progress_message(
            ["Connecting to database", "Querying datasets"]
        )
        expected_step2 = "✅ *Connecting to database*\n\n⏳ *Querying datasets*"
        assert expected_step2 == step2["text"]

        step3 = build_teams_progress_message(
            ["Connecting to database", "Querying datasets", "Processing results"]
        )
        expected_step3 = "✅ *Connecting to database*\n\n✅ *Querying datasets*\n\n⏳ *Processing results*"
        assert expected_step3 == step3["text"]

    def test_message_type_consistency(self) -> None:
        """Test that all progress functions return consistent message structure."""
        messages = [
            build_teams_initial_progress_message(),
            build_teams_initial_progress_message("Custom"),
            build_teams_progress_message([]),
            build_teams_progress_message(["Step 1"]),
            build_teams_progress_message(["Step 1", "Step 2"]),
        ]

        for msg in messages:
            assert isinstance(msg, dict)
            assert msg["type"] == "message"
            assert "text" in msg
            assert isinstance(msg["text"], str)
