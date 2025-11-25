"""Unit tests for ingestion troubleshooting agent."""

from unittest.mock import Mock, patch

from datahub.sdk.main_client import DataHubClient

from datahub_integrations.chat.chat_history import ChatHistory
from datahub_integrations.chat.examples.ingestion_troubleshooting_agent import (
    check_entity_freshness,
    create_ingestion_troubleshooting_agent,
    diagnose_missing_entity,
    find_ingestion_sources,
    get_ingestion_runs,
    get_run_logs,
)


class TestGetIngestionRuns:
    """Test get_ingestion_runs placeholder function."""

    def test_returns_expected_structure(self) -> None:
        """Test that function returns expected data structure."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test")

        assert "source_urn" in result
        assert "source_name" in result
        assert "source_type" in result
        assert "total_runs" in result
        assert "runs" in result
        assert "success_rate" in result
        assert "average_duration_ms" in result

    def test_returns_placeholder_data(self) -> None:
        """Test that function returns placeholder data."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test")

        assert result["source_urn"] == "urn:li:dataHubIngestionSource:test"
        assert result["total_runs"] == 0
        assert result["runs"] == []
        assert "_note" in result
        assert "PLACEHOLDER" in result["_note"]

    def test_accepts_count_parameter(self) -> None:
        """Test that function accepts count parameter."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test", count=5)

        assert isinstance(result, dict)

    def test_accepts_status_filter_parameter(self) -> None:
        """Test that function accepts status_filter parameter."""
        result = get_ingestion_runs(
            "urn:li:dataHubIngestionSource:test", status_filter="FAILURE"
        )

        assert isinstance(result, dict)


class TestGetRunLogs:
    """Test get_run_logs placeholder function."""

    def test_returns_expected_structure(self) -> None:
        """Test that function returns expected data structure."""
        result = get_run_logs("urn:li:executionRequest:abc123")

        assert "execution_request_urn" in result
        assert "status" in result
        assert "start_time_ms" in result
        assert "duration_ms" in result
        assert "raw_logs" in result
        assert "structured_report" in result
        assert "common_errors" in result

    def test_structured_report_has_expected_fields(self) -> None:
        """Test that structured report contains expected fields."""
        result = get_run_logs("urn:li:executionRequest:abc123")

        report = result["structured_report"]
        assert "entities_written" in report
        assert "entities_read" in report
        assert "warnings" in report
        assert "errors" in report
        assert "failure_reason" in report

    def test_returns_placeholder_data(self) -> None:
        """Test that function returns placeholder data."""
        result = get_run_logs("urn:li:executionRequest:abc123")

        assert result["execution_request_urn"] == "urn:li:executionRequest:abc123"
        assert result["status"] == "UNKNOWN"
        assert "_note" in result
        assert "PLACEHOLDER" in result["_note"]


class TestDiagnoseMissingEntity:
    """Test diagnose_missing_entity placeholder function."""

    def test_returns_expected_structure(self) -> None:
        """Test that function returns expected data structure."""
        result = diagnose_missing_entity("prod.sales.orders", "snowflake")

        assert "expected_entity" in result
        assert "entity_exists" in result
        assert "similar_entities" in result
        assert "relevant_sources" in result
        assert "recent_errors" in result
        assert "diagnosis" in result
        assert "soft_deleted" in result

    def test_expected_entity_structure(self) -> None:
        """Test expected_entity field structure."""
        result = diagnose_missing_entity("prod.sales.orders", "snowflake", env="DEV")

        expected = result["expected_entity"]
        assert expected["qualified_name"] == "prod.sales.orders"
        assert expected["platform"] == "snowflake"
        assert expected["env"] == "DEV"

    def test_diagnosis_structure(self) -> None:
        """Test diagnosis field structure."""
        result = diagnose_missing_entity("prod.sales.orders", "snowflake")

        diagnosis = result["diagnosis"]
        assert "likely_cause" in diagnosis
        assert "confidence" in diagnosis
        assert "evidence" in diagnosis
        assert "recommendations" in diagnosis
        assert isinstance(diagnosis["recommendations"], list)

    def test_returns_placeholder_data(self) -> None:
        """Test that function returns placeholder data."""
        result = diagnose_missing_entity("test.table", "postgres")

        assert result["entity_exists"] is False
        assert result["similar_entities"] == []
        assert result["relevant_sources"] == []
        assert result["soft_deleted"] is False
        assert "_note" in result
        assert "PLACEHOLDER" in result["_note"]

    def test_default_env_is_prod(self) -> None:
        """Test that default environment is PROD."""
        result = diagnose_missing_entity("test.table", "postgres")

        assert result["expected_entity"]["env"] == "PROD"


class TestCheckEntityFreshness:
    """Test check_entity_freshness placeholder function."""

    def test_returns_expected_structure(self) -> None:
        """Test that function returns expected data structure."""
        result = check_entity_freshness("urn:li:dataset:(test)")

        assert "entity_urn" in result
        assert "entity_name" in result
        assert "last_modified_ms" in result
        assert "hours_since_update" in result
        assert "is_stale" in result
        assert "ingestion_source" in result
        assert "freshness_assessment" in result

    def test_freshness_assessment_structure(self) -> None:
        """Test freshness_assessment field structure."""
        result = check_entity_freshness("urn:li:dataset:(test)")

        assessment = result["freshness_assessment"]
        assert "status" in assessment
        assert "message" in assessment
        assert "recommendation" in assessment

    def test_returns_placeholder_data(self) -> None:
        """Test that function returns placeholder data."""
        result = check_entity_freshness("urn:li:dataset:(test)")

        assert result["entity_urn"] == "urn:li:dataset:(test)"
        assert result["entity_name"] == "unknown"
        assert result["last_modified_ms"] is None
        assert result["freshness_assessment"]["status"] == "UNKNOWN"
        assert "_note" in result
        assert "PLACEHOLDER" in result["_note"]


class TestFindIngestionSources:
    """Test find_ingestion_sources placeholder function."""

    def test_returns_expected_structure(self) -> None:
        """Test that function returns expected data structure."""
        result = find_ingestion_sources()

        assert "total" in result
        assert "sources" in result
        assert isinstance(result["sources"], list)

    def test_accepts_platform_filter(self) -> None:
        """Test that function accepts platform filter."""
        result = find_ingestion_sources(platform="snowflake")

        assert isinstance(result, dict)

    def test_accepts_name_pattern_filter(self) -> None:
        """Test that function accepts name_pattern filter."""
        result = find_ingestion_sources(name_pattern="prod")

        assert isinstance(result, dict)

    def test_accepts_both_filters(self) -> None:
        """Test that function accepts both filters."""
        result = find_ingestion_sources(platform="bigquery", name_pattern="analytics")

        assert isinstance(result, dict)

    def test_returns_placeholder_data(self) -> None:
        """Test that function returns placeholder data."""
        result = find_ingestion_sources()

        assert result["total"] == 0
        assert result["sources"] == []
        assert "_note" in result
        assert "PLACEHOLDER" in result["_note"]


class TestRespondWithTroubleshootingContext:
    """Test respond_with_troubleshooting_context internal tool."""

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_function_is_available_in_agent(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that troubleshooting response function is available."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        # Check that respond_to_user tool exists in config
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        tool_names = [tool.name for tool in config.tools]
        assert "respond_to_user" in tool_names


class TestCreateIngestionTroubleshootingAgent:
    """Test create_ingestion_troubleshooting_agent function."""

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_creates_agent_runner(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that function creates an AgentRunner."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        assert mock_agent_runner.called

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_has_correct_name(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent is configured with correct name."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        assert config.agent_name == "Ingestion Troubleshooting Agent"
        assert "ingestion" in config.agent_description.lower()

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_has_ingestion_tools(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent has ingestion-specific tools."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        # Check for ingestion-specific tool names
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        tool_names = [tool.name for tool in config.tools]
        assert "get_ingestion_runs" in tool_names
        assert "get_run_logs" in tool_names
        assert "diagnose_missing_entity" in tool_names
        assert "check_entity_freshness" in tool_names
        assert "find_ingestion_sources" in tool_names
        assert "respond_to_user" in tool_names

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_has_moderate_temperature(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent uses moderate temperature for balanced behavior."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        # Troubleshooting should balance creativity and focus
        assert config.temperature == 0.4

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_with_existing_history(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test creating agent with existing chat history."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}
        history = ChatHistory()

        create_ingestion_troubleshooting_agent(mock_client, history=history)

        call_args = mock_agent_runner.call_args
        assert call_args[1]["history"] == history

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_has_higher_max_tool_calls(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent allows more tool calls for proactive investigation."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        # Proactive troubleshooting needs more tool calls
        assert config.max_tool_calls == 20

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_plannable_tools_excludes_respond_to_user(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that plannable tools exclude internal control-flow tools."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        plannable_tool_names = [tool.name for tool in config.plannable_tools]
        assert "respond_to_user" not in plannable_tool_names
        assert "get_ingestion_runs" in plannable_tool_names
        assert "diagnose_missing_entity" in plannable_tool_names

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_uses_correct_model(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent uses specified model."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        # Should use Claude Sonnet
        assert "claude" in config.model_id.lower()
        assert "sonnet" in config.model_id.lower()


class TestIngestionTroubleshootingAgentIntegration:
    """Integration tests for ingestion troubleshooting agent."""

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_system_prompt_mentions_troubleshooting(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that system prompt is appropriate for troubleshooting."""
        mock_client = Mock(spec=DataHubClient)
        mock_client._graph = Mock()
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        # Get system prompt builder from config
        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        system_messages = config.system_prompt_builder.build_system_messages(
            mock_client
        )

        # Check that prompt mentions troubleshooting and ingestion
        prompt_text = " ".join(msg.get("text", "") for msg in system_messages)
        assert (
            "troubleshoot" in prompt_text.lower() or "diagnosis" in prompt_text.lower()
        )
        assert "ingestion" in prompt_text.lower()

    @patch(
        "datahub_integrations.chat.examples.ingestion_troubleshooting_agent.AgentRunner"
    )
    @patch("datahub_integrations.chat.examples.ingestion_troubleshooting_agent.mcp")
    def test_agent_has_all_tool_categories(
        self, mock_mcp: Mock, mock_agent_runner: Mock
    ) -> None:
        """Test that agent has tools from all categories."""
        mock_client = Mock(spec=DataHubClient)
        mock_mcp._tool_manager = Mock()
        mock_mcp._tool_manager._tools = {}

        create_ingestion_troubleshooting_agent(mock_client)

        call_args = mock_agent_runner.call_args
        config = call_args[1]["config"]
        tool_names = [tool.name for tool in config.tools]

        # Should have MCP tools (via mock)
        # Should have ingestion tools
        assert "get_ingestion_runs" in tool_names
        assert "get_run_logs" in tool_names
        assert "diagnose_missing_entity" in tool_names
        assert "check_entity_freshness" in tool_names
        assert "find_ingestion_sources" in tool_names

        # Should have internal tools
        assert "respond_to_user" in tool_names


class TestPlaceholderToolsDocumentation:
    """Test that placeholder tools have proper documentation."""

    def test_get_ingestion_runs_has_docstring(self) -> None:
        """Test that get_ingestion_runs has comprehensive docstring."""
        assert get_ingestion_runs.__doc__ is not None
        assert "ingestion run history" in get_ingestion_runs.__doc__.lower()
        assert "TODO" in get_ingestion_runs.__doc__

    def test_get_run_logs_has_docstring(self) -> None:
        """Test that get_run_logs has comprehensive docstring."""
        assert get_run_logs.__doc__ is not None
        assert "logs" in get_run_logs.__doc__.lower()
        assert "TODO" in get_run_logs.__doc__

    def test_diagnose_missing_entity_has_docstring(self) -> None:
        """Test that diagnose_missing_entity has comprehensive docstring."""
        assert diagnose_missing_entity.__doc__ is not None
        assert "diagnose" in diagnose_missing_entity.__doc__.lower()
        assert "missing" in diagnose_missing_entity.__doc__.lower()
        assert "TODO" in diagnose_missing_entity.__doc__

    def test_check_entity_freshness_has_docstring(self) -> None:
        """Test that check_entity_freshness has comprehensive docstring."""
        assert check_entity_freshness.__doc__ is not None
        assert "freshness" in check_entity_freshness.__doc__.lower()
        assert "TODO" in check_entity_freshness.__doc__

    def test_find_ingestion_sources_has_docstring(self) -> None:
        """Test that find_ingestion_sources has comprehensive docstring."""
        assert find_ingestion_sources.__doc__ is not None
        assert "ingestion source" in find_ingestion_sources.__doc__.lower()
        assert "TODO" in find_ingestion_sources.__doc__


class TestPlaceholderToolsReturnTypes:
    """Test that all placeholder tools return expected types."""

    def test_all_placeholder_tools_return_dict(self) -> None:
        """Test that all placeholder tools return dictionaries."""
        assert isinstance(get_ingestion_runs("urn"), dict)
        assert isinstance(get_run_logs("urn"), dict)
        assert isinstance(diagnose_missing_entity("name", "platform"), dict)
        assert isinstance(check_entity_freshness("urn"), dict)
        assert isinstance(find_ingestion_sources(), dict)

    def test_all_placeholder_tools_include_note(self) -> None:
        """Test that all placeholder tools include implementation note."""
        results = [
            get_ingestion_runs("urn"),
            get_run_logs("urn"),
            diagnose_missing_entity("name", "platform"),
            check_entity_freshness("urn"),
            find_ingestion_sources(),
        ]

        for result in results:
            assert "_note" in result
            assert "PLACEHOLDER" in result["_note"]


class TestDiagnoseMissingEntityAdvanced:
    """Advanced tests for diagnose_missing_entity function."""

    def test_diagnosis_includes_recommendations(self) -> None:
        """Test that diagnosis always includes recommendations."""
        result = diagnose_missing_entity("test.table", "postgres")

        assert len(result["diagnosis"]["recommendations"]) > 0
        assert all(
            isinstance(rec, str) for rec in result["diagnosis"]["recommendations"]
        )

    def test_diagnosis_confidence_levels(self) -> None:
        """Test that confidence level is provided."""
        result = diagnose_missing_entity("test.table", "postgres")

        assert result["diagnosis"]["confidence"] in ["HIGH", "MEDIUM", "LOW"]

    def test_handles_various_platform_types(self) -> None:
        """Test handling different platform types."""
        platforms = ["snowflake", "bigquery", "postgres", "redshift", "databricks"]

        for platform in platforms:
            result = diagnose_missing_entity("test.table", platform)
            assert result["expected_entity"]["platform"] == platform


class TestGetIngestionRunsAdvanced:
    """Advanced tests for get_ingestion_runs function."""

    def test_success_rate_calculation_placeholder(self) -> None:
        """Test that success_rate field is present."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test")

        assert "success_rate" in result
        assert isinstance(result["success_rate"], (int, float))

    def test_average_duration_placeholder(self) -> None:
        """Test that average_duration_ms field is present."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test")

        assert "average_duration_ms" in result
        assert isinstance(result["average_duration_ms"], (int, float))

    def test_runs_list_structure(self) -> None:
        """Test that runs list is properly structured."""
        result = get_ingestion_runs("urn:li:dataHubIngestionSource:test")

        assert isinstance(result["runs"], list)


class TestCheckEntityFreshnessAdvanced:
    """Advanced tests for check_entity_freshness function."""

    def test_freshness_statuses(self) -> None:
        """Test that status is one of expected values."""
        result = check_entity_freshness("urn:li:dataset:(test)")

        status = result["freshness_assessment"]["status"]
        assert status in ["FRESH", "AGING", "STALE", "UNKNOWN"]

    def test_provides_recommendation(self) -> None:
        """Test that recommendation is always provided."""
        result = check_entity_freshness("urn:li:dataset:(test)")

        assert "recommendation" in result["freshness_assessment"]
        assert isinstance(result["freshness_assessment"]["recommendation"], str)
        assert len(result["freshness_assessment"]["recommendation"]) > 0


class TestIngestionAgentSystemPrompt:
    """Test the ingestion troubleshooting system prompt."""

    def test_prompt_includes_troubleshooting_patterns(self) -> None:
        """Test that system prompt includes troubleshooting patterns."""
        from datahub_integrations.chat.examples.ingestion_troubleshooting_agent import (
            INGESTION_TROUBLESHOOTING_PROMPT,
        )

        assert "Missing Entity" in INGESTION_TROUBLESHOOTING_PROMPT
        assert "Stale Metadata" in INGESTION_TROUBLESHOOTING_PROMPT
        assert "Failed Ingestion" in INGESTION_TROUBLESHOOTING_PROMPT
        assert "Schema Differences" in INGESTION_TROUBLESHOOTING_PROMPT

    def test_prompt_includes_communication_style(self) -> None:
        """Test that prompt includes communication guidelines."""
        from datahub_integrations.chat.examples.ingestion_troubleshooting_agent import (
            INGESTION_TROUBLESHOOTING_PROMPT,
        )

        assert "conversational" in INGESTION_TROUBLESHOOTING_PROMPT.lower()
        assert "friendly" in INGESTION_TROUBLESHOOTING_PROMPT.lower()

    def test_prompt_includes_proactive_behavior(self) -> None:
        """Test that prompt encourages proactive behavior."""
        from datahub_integrations.chat.examples.ingestion_troubleshooting_agent import (
            INGESTION_TROUBLESHOOTING_PROMPT,
        )

        assert "PROACTIVE" in INGESTION_TROUBLESHOOTING_PROMPT
        assert "Investigate automatically" in INGESTION_TROUBLESHOOTING_PROMPT
