"""
Tests for AgentConfig composition.

These tests verify that AgentConfig properly composes different components.
"""

from datahub_integrations.chat.agent import (
    AgentConfig,
    CallablePromptBuilder,
    StaticPromptBuilder,
)
from datahub_integrations.mcp_integration.tool import ToolWrapper


def test_agent_config_defaults():
    """Test AgentConfig with default values."""
    config = AgentConfig(
        model_id="test-model",
        system_prompt_builder=StaticPromptBuilder("test"),
        tools=[],
        plannable_tools=[],
    )

    assert config.model_id == "test-model"
    assert config.use_prompt_caching is True
    assert config.max_tool_calls == 30
    assert config.temperature == 0.5
    assert config.max_tokens == 4096
    assert config.context_reducers is None


def test_agent_config_custom_values():
    """Test AgentConfig with custom values."""
    config = AgentConfig(
        model_id="custom-model",
        system_prompt_builder=StaticPromptBuilder("custom prompt"),
        tools=[],
        plannable_tools=[],
        use_prompt_caching=False,
        max_tool_calls=10,
        temperature=0.7,
        max_tokens=2048,
        agent_name="Custom Agent",
    )

    assert config.model_id == "custom-model"
    assert config.use_prompt_caching is False
    assert config.max_tool_calls == 10
    assert config.temperature == 0.7
    assert config.max_tokens == 2048
    assert config.agent_name == "Custom Agent"


def test_static_prompt_builder():
    """Test StaticPromptBuilder."""
    prompt = "You are a data engineer assistant"
    builder = StaticPromptBuilder(prompt)

    # Build system messages (client not used for static prompt)
    messages = builder.build_system_messages(None)  # type: ignore

    assert len(messages) == 1
    assert messages[0]["text"] == prompt


def test_callable_prompt_builder():
    """Test CallablePromptBuilder."""

    def build_prompt(client):
        return "Dynamic prompt"

    builder = CallablePromptBuilder(build_prompt)

    # Build system messages
    messages = builder.build_system_messages(None)  # type: ignore

    assert len(messages) == 1
    assert messages[0]["text"] == "Dynamic prompt"


def test_agent_config_with_tools():
    """Test AgentConfig with tools list."""
    tool = ToolWrapper.from_function(
        fn=lambda: "result", name="test_tool", description="Test tool"
    )

    config = AgentConfig(
        model_id="test-model",
        system_prompt_builder=StaticPromptBuilder("test"),
        tools=[tool],
        plannable_tools=[tool],
    )

    assert len(config.tools) == 1
    assert config.tools[0].name == "test_tool"


def test_agent_config_immutability():
    """Test that AgentConfig is a dataclass (can be frozen if needed)."""
    config = AgentConfig(
        model_id="test-model",
        system_prompt_builder=StaticPromptBuilder("test"),
        tools=[],
        plannable_tools=[],
    )

    # Verify it's a dataclass by checking fields
    assert hasattr(config, "__dataclass_fields__")
    assert "model_id" in config.__dataclass_fields__
    assert "system_prompt_builder" in config.__dataclass_fields__
    assert "tools" in config.__dataclass_fields__
