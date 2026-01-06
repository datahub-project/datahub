"""Integration tests for plan cache isolation between AgentRunner instances."""

from unittest.mock import MagicMock

import pytest

from datahub_integrations.chat.agent import (
    AgentConfig,
    AgentRunner,
    StaticPromptBuilder,
)
from datahub_integrations.chat.planner.models import Constraints, Plan, Step
from datahub_integrations.chat.planner.tools import get_planning_tool_wrappers


class TestPlanCacheIsolation:
    """Tests to ensure plans are isolated between different AgentRunner instances."""

    @pytest.fixture
    def agent_a(self) -> AgentRunner:
        """Create first AgentRunner instance."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test agent"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent = AgentRunner(config=config, client=client)
        agent.session_id = "agent_a"
        return agent

    @pytest.fixture
    def agent_b(self) -> AgentRunner:
        """Create second AgentRunner instance."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test agent"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent = AgentRunner(config=config, client=client)
        agent.session_id = "agent_b"
        return agent

    def test_plans_are_isolated_between_agents(
        self, agent_a: AgentRunner, agent_b: AgentRunner
    ) -> None:
        """Test that plans created in one agent are not visible in another."""
        # Create plan in agent A
        plan_a = Plan(
            plan_id="plan_a",
            version=1,
            title="Agent A Plan",
            goal="Task A",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[Step(id="s0", description="Step A", done_when="Done")],
            expected_deliverable="Result A",
        )
        agent_a._state.set_plan("plan_a", plan_a, internal={})

        # Create plan in agent B
        plan_b = Plan(
            plan_id="plan_b",
            version=1,
            title="Agent B Plan",
            goal="Task B",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[Step(id="s0", description="Step B", done_when="Done")],
            expected_deliverable="Result B",
        )
        agent_b._state.set_plan("plan_b", plan_b, internal={})

        # Verify agent A can only see its own plan
        assert agent_a._state.get_plan("plan_a") is not None
        assert agent_a._state.get_plan("plan_b") is None

        # Verify agent B can only see its own plan
        assert agent_b._state.get_plan("plan_b") is not None
        assert agent_b._state.get_plan("plan_a") is None

    def test_modifying_plan_in_one_agent_does_not_affect_other(
        self, agent_a: AgentRunner, agent_b: AgentRunner
    ) -> None:
        """Test that modifying a plan in one agent doesn't affect the other."""
        # Create same plan_id in both agents
        plan_a = Plan(
            plan_id="shared_plan_id",
            version=1,
            title="Original Title A",
            goal="Task A",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[Step(id="s0", description="Step A", done_when="Done")],
            expected_deliverable="Result A",
        )
        agent_a._state.set_plan("shared_plan_id", plan_a, internal={})

        plan_b = Plan(
            plan_id="shared_plan_id",
            version=1,
            title="Original Title B",
            goal="Task B",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[Step(id="s0", description="Step B", done_when="Done")],
            expected_deliverable="Result B",
        )
        agent_b._state.set_plan("shared_plan_id", plan_b, internal={})

        # Modify plan in agent A
        modified_plan_a = Plan(
            plan_id="shared_plan_id",
            version=2,
            title="Modified Title A",
            goal="Task A Modified",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[Step(id="s0", description="Step A Modified", done_when="Done")],
            expected_deliverable="Result A Modified",
        )
        agent_a._state.set_plan("shared_plan_id", modified_plan_a, internal={})

        # Verify agent B's plan is unaffected
        plan_entry_b = agent_b._state.get_plan("shared_plan_id")
        assert plan_entry_b is not None
        plan_in_b = plan_entry_b["plan"]
        assert plan_in_b.title == "Original Title B"
        assert plan_in_b.goal == "Task B"
        assert plan_in_b.version == 1

    def test_empty_agent_cache_does_not_find_plans_from_other_agent(
        self, agent_a: AgentRunner, agent_b: AgentRunner
    ) -> None:
        """Test that an agent with empty cache cannot access plans from another agent."""
        # Create plan in agent A
        plan_a = Plan(
            plan_id="plan_only_in_a",
            version=1,
            title="Private Plan",
            goal="Private Task",
            assumptions=[],
            constraints=Constraints(tool_allowlist=[], max_llm_turns=5),
            steps=[],
            expected_deliverable="Result",
        )
        agent_a._state.set_plan("plan_only_in_a", plan_a, internal={})

        # Agent B should not be able to access it
        assert agent_b._state.get_plan("plan_only_in_a") is None


class TestPlanningToolWrapperBindsCorrectAgent:
    """Tests that wrapper functions correctly bind and use the agent parameter."""

    @pytest.fixture
    def agent(self) -> AgentRunner:
        """Create an AgentRunner instance."""
        config = AgentConfig(
            model_id="test-model",
            system_prompt_builder=StaticPromptBuilder("Test agent"),
            tools=[],
            plannable_tools=[],
        )
        client = MagicMock()
        agent_instance = AgentRunner(config=config, client=client)
        agent_instance.session_id = "test_agent"
        return agent_instance

    def test_wrapper_functions_are_bound_to_agent(self, agent: AgentRunner) -> None:
        """Test that get_planning_tool_wrappers creates properly bound functions."""
        wrappers = get_planning_tool_wrappers(agent._planning_context)

        assert len(wrappers) == 2
        tool_names = {w.name for w in wrappers}
        assert tool_names == {"create_plan", "revise_plan"}

    def test_wrapper_preserves_function_signatures(self, agent: AgentRunner) -> None:
        """Test that wrapper functions have correct signatures (without agent param)."""
        wrappers = get_planning_tool_wrappers(agent._planning_context)

        create_plan_wrapper = next(w for w in wrappers if w.name == "create_plan")

        # Get the function signature via tool spec
        spec = create_plan_wrapper.to_bedrock_spec()
        params = spec["toolSpec"]["inputSchema"]["json"]["properties"].keys()

        # Should have all parameters except 'agent'
        assert "task" in params
        assert "context" in params
        assert "evidence" in params
        assert "max_steps" in params
        assert "agent" not in params  # Critical: agent should be hidden
