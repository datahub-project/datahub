"""Tests for Phase 2 observability instrumentation."""


def test_slack_command_instrumentation():
    """Test that Slack commands are instrumented."""
    from datahub_integrations.slack.command.search import search

    # Verify the function has the decorator
    assert hasattr(search, "__wrapped__")


def test_slack_get_command_instrumentation():
    """Test that Slack get command is instrumented."""
    from datahub_integrations.slack.command.get import handle_get_command

    # Verify the function has the decorator
    assert hasattr(handle_get_command, "__wrapped__")


def test_slack_ask_command_instrumentation():
    """Test that Slack ask command is instrumented."""
    from datahub_integrations.slack.command.ask import handle_ask_command

    # Verify the function has the decorator
    assert hasattr(handle_ask_command, "__wrapped__")


def test_analytics_schema_instrumentation():
    """Test that analytics schema query is instrumented."""
    from datahub_integrations.analytics.router import schema

    # Verify the function has the decorator
    assert hasattr(schema, "__wrapped__")


def test_analytics_preview_instrumentation():
    """Test that analytics preview query is instrumented."""
    from datahub_integrations.analytics.router import preview

    # Verify the function has the decorator
    assert hasattr(preview, "__wrapped__")


def test_analytics_query_instrumentation():
    """Test that analytics query execution is instrumented."""
    from datahub_integrations.analytics.router import query

    # Verify the function has the decorator
    assert hasattr(query, "__wrapped__")


def test_cost_tracker_initialization():
    """Test that cost tracker can be initialized."""
    from datahub_integrations.observability.cost import TokenUsage, get_cost_tracker
    from datahub_integrations.observability.metrics_constants import AIModule

    tracker = get_cost_tracker()
    assert tracker is not None

    # Test recording usage
    usage = TokenUsage(
        prompt_tokens=100,
        completion_tokens=50,
        total_tokens=150,
    )

    cost = tracker.record_llm_call(
        provider="anthropic",
        model="claude-3-5-sonnet",
        usage=usage,
        ai_module=AIModule.CHAT,
        success=True,
    )

    # Cost should be calculated for known model
    assert cost is not None
    assert cost.total_cost > 0


def test_cost_calculator_pricing():
    """Test that cost calculator has pricing data."""
    from datahub_integrations.observability.cost import CostCalculator, TokenUsage

    calculator = CostCalculator()

    # Test known models
    pricing = calculator.get_model_pricing("anthropic", "claude-3-5-sonnet")
    assert pricing is not None
    assert "prompt" in pricing
    assert "completion" in pricing
    assert "cache_read" in pricing
    assert "cache_write" in pricing

    # Test cost calculation
    usage = TokenUsage(
        prompt_tokens=1_000_000,  # 1M tokens
        completion_tokens=1_000_000,  # 1M tokens
        total_tokens=2_000_000,
    )

    cost = calculator.calculate_cost("anthropic", "claude-3-5-sonnet", usage)
    assert cost is not None
    # Claude 3.5 Sonnet: $3/1M prompt, $15/1M completion
    assert cost.prompt_cost == 3.0
    assert cost.completion_cost == 15.0
    assert cost.total_cost == 18.0


def test_cost_calculator_cache_tokens():
    """Test that cost calculator correctly prices cache tokens."""
    from datahub_integrations.observability.cost import CostCalculator, TokenUsage

    calculator = CostCalculator()

    # Test with cache tokens (80% cache hit rate scenario)
    usage = TokenUsage(
        prompt_tokens=200_000,  # 200K regular prompt tokens
        completion_tokens=100_000,  # 100K completion tokens
        total_tokens=1_100_000,  # Total includes cache tokens
        cache_read_tokens=800_000,  # 800K cache read tokens
        cache_write_tokens=0,  # No cache write for this call
    )

    cost = calculator.calculate_cost("anthropic", "claude-3-5-sonnet", usage)
    assert cost is not None

    # Claude 3.5 Sonnet pricing:
    # - Regular prompt: $3.00/1M = 200K × $3.00/1M = $0.60
    # - Completion: $15.00/1M = 100K × $15.00/1M = $1.50
    # - Cache read: $0.30/1M = 800K × $0.30/1M = $0.24
    # - Total: $0.60 + $1.50 + $0.24 = $2.34

    assert round(cost.prompt_cost, 2) == 0.60
    assert round(cost.completion_cost, 2) == 1.50
    assert round(cost.cache_read_cost, 2) == 0.24
    assert round(cost.cache_write_cost, 2) == 0.0
    assert round(cost.total_cost, 2) == 2.34

    # Verify cache read is 10% of prompt cost (90% savings)
    pricing = calculator.get_model_pricing("anthropic", "claude-3-5-sonnet")
    assert pricing is not None
    assert round(pricing["cache_read"], 2) == round(pricing["prompt"] * 0.1, 2)


def test_actions_manager_instrumentation():
    """Test that actions manager methods are instrumented."""
    from datahub_integrations.actions.actions_manager import ActionsManager

    # Check that start_pipeline has the decorator
    assert hasattr(ActionsManager.start_pipeline, "__wrapped__")

    # Check that _execute_pipeline_stage has decorators
    assert hasattr(ActionsManager._execute_pipeline_stage, "__wrapped__")
