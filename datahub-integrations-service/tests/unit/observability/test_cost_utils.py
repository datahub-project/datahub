"""Tests for cost tracking utility functions."""

from datahub_integrations.observability.cost_utils import (
    detect_provider_and_normalize_model,
)


def test_bedrock_model_detection():
    """Test detection of Bedrock models."""
    provider, model = detect_provider_and_normalize_model(
        "bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0"
    )
    assert provider == "bedrock"
    assert model == "anthropic.claude-3-5-sonnet-20241022-v2:0"


def test_anthropic_direct_model_detection():
    """Test detection of direct Anthropic API models."""
    provider, model = detect_provider_and_normalize_model("anthropic.claude-3-opus")
    assert provider == "anthropic"
    assert model == "anthropic.claude-3-opus"


def test_openai_model_detection():
    """Test detection of OpenAI models."""
    provider, model = detect_provider_and_normalize_model("gpt-4-turbo")
    assert provider == "openai"
    assert model == "gpt-4-turbo"


def test_no_prefix_stripping_when_no_slash():
    """Test that model name is unchanged when no slash present."""
    provider, model = detect_provider_and_normalize_model("claude-3-sonnet")
    assert provider == "bedrock"  # Default
    assert model == "claude-3-sonnet"


def test_openai_model_with_prefix():
    """Test OpenAI model with provider prefix."""
    provider, model = detect_provider_and_normalize_model("openai/gpt-4o")
    assert provider == "openai"
    assert model == "gpt-4o"


def test_case_insensitive_detection():
    """Test that provider detection is case-insensitive."""
    # Test uppercase Anthropic
    provider, model = detect_provider_and_normalize_model("ANTHROPIC.claude-3-opus")
    assert provider == "anthropic"
    assert model == "ANTHROPIC.claude-3-opus"

    # Test uppercase GPT
    provider, model = detect_provider_and_normalize_model("GPT-4-turbo")
    assert provider == "openai"
    assert model == "GPT-4-turbo"
