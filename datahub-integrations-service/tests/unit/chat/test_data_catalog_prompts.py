"""Unit tests for data catalog prompts and system message building."""

from unittest.mock import Mock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.agents.data_catalog_prompts import (
    DataHubSystemPromptBuilder,
    PlanningMode,
    _get_system_prompt,
    get_tool_instructions,
)
from datahub_integrations.mcp_integration.external_mcp_manager import (
    ExternalToolWrapper,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


class TestPlanningMode:
    """Test the PlanningMode enum and its effect on system prompts."""

    def test_planning_mode_enum_values(self) -> None:
        """PlanningMode enum has expected string values."""
        assert PlanningMode.STRICT.value == "strict"
        assert PlanningMode.AUTO.value == "auto"
        assert PlanningMode.DISABLED.value == "disabled"

    def test_strict_mode_requires_planning(self) -> None:
        """STRICT mode requires create_plan as first tool."""
        prompt = _get_system_prompt(PlanningMode.STRICT)
        assert "MUST call create_plan as the FIRST tool" in prompt

    def test_auto_mode_recommends_planning(self) -> None:
        """AUTO mode recommends planning for complex tasks."""
        prompt = _get_system_prompt(PlanningMode.AUTO)
        assert "SHOULD use create_plan" in prompt
        assert "SQL generation" in prompt
        assert "3 or more tool calls" in prompt

    def test_disabled_mode_has_no_planning_instruction(self) -> None:
        """DISABLED mode has no planning instruction."""
        prompt = _get_system_prompt(PlanningMode.DISABLED)
        assert "MUST call create_plan" not in prompt
        assert "SHOULD use create_plan" not in prompt

    def test_strict_mode_includes_plan_xml_fields(self) -> None:
        """STRICT mode includes plan-related XML reasoning fields."""
        prompt = _get_system_prompt(PlanningMode.STRICT)
        assert "<plan_id>" in prompt
        assert "<plan_step>" in prompt
        assert "<done_criteria_met>" in prompt

    def test_auto_mode_includes_plan_xml_fields(self) -> None:
        """AUTO mode includes plan-related XML reasoning fields."""
        prompt = _get_system_prompt(PlanningMode.AUTO)
        assert "<plan_id>" in prompt
        assert "<plan_step>" in prompt

    def test_disabled_mode_excludes_plan_xml_fields(self) -> None:
        """DISABLED mode excludes plan-related XML reasoning fields."""
        prompt = _get_system_prompt(PlanningMode.DISABLED)
        assert "<plan_id>" not in prompt
        assert "<plan_step>" not in prompt
        assert "<done_criteria_met>" not in prompt

    def test_disabled_mode_excludes_revise_plan_instructions(self) -> None:
        """DISABLED mode excludes revise_plan-related instructions."""
        prompt = _get_system_prompt(PlanningMode.DISABLED)
        assert "revise_plan" not in prompt

    def test_strict_mode_includes_revise_plan_instructions(self) -> None:
        """STRICT mode includes revise_plan-related instructions."""
        prompt = _get_system_prompt(PlanningMode.STRICT)
        assert "revise_plan" in prompt


class TestDataHubSystemPromptBuilder:
    """Test the DataHub system prompt builder."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mock_client = Mock(spec=DataHubClient)
        # Mock the _graph attribute that is accessed during prompt building
        mock_graph = Mock()
        self.mock_client._graph = mock_graph

    def test_base_system_prompt_always_included(self) -> None:
        """Base system prompt is always included regardless of mutation flag."""
        for env_value in ["true", "false", None]:
            env_dict = {"TOOLS_IS_MUTATION_ENABLED": env_value} if env_value else {}
            with patch.dict("os.environ", env_dict, clear=True):
                with patch(
                    "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                    return_value=None,
                ):
                    builder = DataHubSystemPromptBuilder()
                    system_messages = builder.build_system_messages(self.mock_client)

                    # First message should always be the base system prompt
                    assert len(system_messages) >= 1
                    assert "DataHub AI" in system_messages[0]["text"]
                    assert "Acryl Data" in system_messages[0]["text"]

    def test_context_included_when_provided(self) -> None:
        """Context message is included when context is provided."""
        with patch.dict("os.environ", {}, clear=True):
            with patch(
                "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                return_value=None,
            ):
                test_context = "User is viewing the Sales Dashboard"
                builder = DataHubSystemPromptBuilder(context=test_context)
                system_messages = builder.build_system_messages(self.mock_client)

                # Extract all text content from system messages
                all_text = " ".join(msg["text"] for msg in system_messages)

                # Verify context is present
                assert test_context in all_text
                assert "The following context is provided from our UI" in all_text

    def test_extra_instructions_override(self) -> None:
        """Extra instructions override works."""
        with patch.dict("os.environ", {"TOOLS_IS_MUTATION_ENABLED": "true"}):
            # Don't patch get_extra_llm_instructions - it should not be called
            override_instructions = "Custom company guidelines"
            builder = DataHubSystemPromptBuilder(
                extra_instructions_override=override_instructions
            )
            system_messages = builder.build_system_messages(self.mock_client)

            # Extract all text content from system messages
            all_text = " ".join(msg["text"] for msg in system_messages)

            # Verify override instructions are present
            assert override_instructions in all_text

    def test_message_ordering(self) -> None:
        """System messages are ordered correctly: base, context, mutations, extra."""
        with patch.dict("os.environ", {"TOOLS_IS_MUTATION_ENABLED": "true"}):
            with patch(
                "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
                return_value="Extra instructions from API",
            ):
                test_context = "User context"
                builder = DataHubSystemPromptBuilder(context=test_context)
                system_messages = builder.build_system_messages(self.mock_client)

                # Should have 4 messages: base, context, extra
                assert len(system_messages) == 3

                # Verify ordering
                assert "DataHub AI" in system_messages[0]["text"]
                assert test_context in system_messages[1]["text"]
                assert "Extra instructions from API" in system_messages[2]["text"]

    def test_tool_instructions_included_when_tools_provided(self) -> None:
        """Tool-specific instructions are included when tools with instructions are provided."""
        with patch(
            "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
            return_value=None,
        ):
            # Create a mock ExternalToolWrapper with instructions
            mock_tool = Mock(spec=ExternalToolWrapper)
            mock_tool.instructions = "Always include repo:owner/repo in queries"
            mock_tool.tool_prefix = "github"

            builder = DataHubSystemPromptBuilder(tools=[mock_tool])
            system_messages = builder.build_system_messages(self.mock_client)

            # Extract all text content
            all_text = " ".join(msg["text"] for msg in system_messages)

            # Verify tool instructions are present with prefix
            assert "TOOL-SPECIFIC INSTRUCTIONS" in all_text
            assert "[github]" in all_text
            assert "Always include repo:owner/repo in queries" in all_text

    def test_no_tool_instructions_when_no_external_tools(self) -> None:
        """No tool instructions section when tools have no instructions."""
        with patch(
            "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
            return_value=None,
        ):
            # Create a mock ToolWrapper (internal tool, no instructions)
            mock_tool = Mock(spec=ToolWrapper)

            builder = DataHubSystemPromptBuilder(tools=[mock_tool])
            system_messages = builder.build_system_messages(self.mock_client)

            # Extract all text content
            all_text = " ".join(msg["text"] for msg in system_messages)

            # Verify no tool instructions section
            assert "TOOL-SPECIFIC INSTRUCTIONS" not in all_text

    def test_planning_mode_strict(self) -> None:
        """Planning mode STRICT includes planning instruction in system prompt."""
        with patch(
            "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
            return_value=None,
        ):
            builder = DataHubSystemPromptBuilder(planning_mode=PlanningMode.STRICT)
            system_messages = builder.build_system_messages(self.mock_client)

            all_text = " ".join(msg["text"] for msg in system_messages)
            assert "MUST call create_plan as the FIRST tool" in all_text

    def test_planning_mode_auto(self) -> None:
        """Planning mode AUTO includes recommendation in system prompt."""
        with patch(
            "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
            return_value=None,
        ):
            builder = DataHubSystemPromptBuilder(planning_mode=PlanningMode.AUTO)
            system_messages = builder.build_system_messages(self.mock_client)

            all_text = " ".join(msg["text"] for msg in system_messages)
            assert "SHOULD use create_plan" in all_text

    def test_planning_mode_disabled(self) -> None:
        """Planning mode DISABLED excludes planning instructions."""
        with patch(
            "datahub_integrations.chat.agents.data_catalog_prompts.get_extra_llm_instructions",
            return_value=None,
        ):
            builder = DataHubSystemPromptBuilder(planning_mode=PlanningMode.DISABLED)
            system_messages = builder.build_system_messages(self.mock_client)

            all_text = " ".join(msg["text"] for msg in system_messages)
            assert "MUST call create_plan" not in all_text
            assert "SHOULD use create_plan" not in all_text
            assert "<plan_id>" not in all_text


class TestGetToolInstructions:
    """Test the get_tool_instructions helper function."""

    def test_extracts_instructions_from_external_tools(self) -> None:
        """Instructions are extracted from ExternalToolWrapper with prefix."""
        mock_tool = Mock(spec=ExternalToolWrapper)
        mock_tool.instructions = "Use environment ID for production"
        mock_tool.tool_prefix = "dbt_alex"

        result = get_tool_instructions([mock_tool])

        assert len(result) == 1
        assert result[0] == "[dbt_alex] Use environment ID for production"

    def test_skips_internal_tools(self) -> None:
        """ToolWrapper (internal tools) are skipped - they have no instructions."""
        mock_internal = Mock(spec=ToolWrapper)
        mock_external = Mock(spec=ExternalToolWrapper)
        mock_external.instructions = "External instructions"
        mock_external.tool_prefix = "github"

        result = get_tool_instructions([mock_internal, mock_external])

        assert len(result) == 1
        assert "[github]" in result[0]

    def test_deduplicates_instructions(self) -> None:
        """Duplicate instructions from same plugin are deduplicated."""
        # Two tools from same plugin with same instructions
        mock_tool1 = Mock(spec=ExternalToolWrapper)
        mock_tool1.instructions = "Same instructions"
        mock_tool1.tool_prefix = "github"

        mock_tool2 = Mock(spec=ExternalToolWrapper)
        mock_tool2.instructions = "Same instructions"
        mock_tool2.tool_prefix = "github"

        result = get_tool_instructions([mock_tool1, mock_tool2])

        assert len(result) == 1

    def test_multiple_plugins_with_different_instructions(self) -> None:
        """Multiple plugins each contribute their instructions."""
        mock_github = Mock(spec=ExternalToolWrapper)
        mock_github.instructions = "GitHub specific guidance"
        mock_github.tool_prefix = "github"

        mock_dbt = Mock(spec=ExternalToolWrapper)
        mock_dbt.instructions = "dbt specific guidance"
        mock_dbt.tool_prefix = "dbt_prod"

        result = get_tool_instructions([mock_github, mock_dbt])

        assert len(result) == 2
        assert "[github] GitHub specific guidance" in result
        assert "[dbt_prod] dbt specific guidance" in result

    def test_tool_without_instructions_skipped(self) -> None:
        """ExternalToolWrapper with no instructions is skipped."""
        mock_tool = Mock(spec=ExternalToolWrapper)
        mock_tool.instructions = None
        mock_tool.tool_prefix = "github"

        result = get_tool_instructions([mock_tool])

        assert len(result) == 0

    def test_tool_without_prefix_still_included(self) -> None:
        """Instructions without prefix are still included (no bracket prefix)."""
        mock_tool = Mock(spec=ExternalToolWrapper)
        mock_tool.instructions = "Some instructions"
        mock_tool.tool_prefix = ""

        result = get_tool_instructions([mock_tool])

        assert len(result) == 1
        assert result[0] == "Some instructions"
        assert "[" not in result[0]
