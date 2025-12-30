"""Tests for cost calculation improvements (2025 pricing, custom pricing, Bedrock premiums)."""

import json
import os
from unittest import mock

import pytest

from datahub_integrations.observability.cost import (
    CostCalculator,
    TokenUsage,
    _apply_bedrock_premiums,
    _detect_bedrock_optimized_latency,
    _detect_bedrock_regional_endpoint,
    _load_custom_pricing,
)


def test_claude_35_haiku_pricing():
    """Test that Claude 3.5 Haiku has correct 2025 pricing ($0.80/$4.00)."""
    calculator = CostCalculator()

    # Test Anthropic direct
    pricing = calculator.get_model_pricing("anthropic", "claude-3-5-haiku")
    assert pricing is not None
    assert pricing["prompt"] == 0.80
    assert pricing["completion"] == 4.0
    assert pricing["cache_read"] == 0.08
    assert pricing["cache_write"] == 0.80

    # Test Bedrock
    pricing = calculator.get_model_pricing("bedrock", "us.anthropic.claude-3-5-haiku")
    assert pricing is not None
    # Base pricing should be same (premiums are applied separately)
    assert pricing["prompt"] >= 0.80  # May have premiums applied


def test_custom_pricing_via_file_json(tmp_path):
    """Test loading custom pricing from JSON file."""
    custom_pricing = {
        "custom_provider": {
            "my-model": {
                "prompt": 5.0,
                "completion": 10.0,
                "cache_read": 0.5,
                "cache_write": 5.0,
            }
        }
    }

    pricing_file = tmp_path / "pricing.json"
    pricing_file.write_text(json.dumps(custom_pricing))

    with mock.patch.dict(os.environ, {"GENAI_MODEL_PRICING_FILE": str(pricing_file)}):
        pricing_dict = _load_custom_pricing()

        # Custom provider should be present
        assert "custom_provider" in pricing_dict
        assert "my-model" in pricing_dict["custom_provider"]
        assert pricing_dict["custom_provider"]["my-model"]["prompt"] == 5.0
        assert pricing_dict["custom_provider"]["my-model"]["completion"] == 10.0

        # Base pricing should still be present (per-model merge)
        assert "anthropic" in pricing_dict
        assert "claude-3-5-sonnet" in pricing_dict["anthropic"]


def test_custom_pricing_via_file_yaml(tmp_path):
    """Test loading custom pricing from YAML file."""
    pricing_yaml = """
anthropic:
  claude-custom-test:
    prompt: 999.0
    completion: 999.0
    cache_read: 99.9
    cache_write: 999.0
"""

    pricing_file = tmp_path / "pricing.yaml"
    pricing_file.write_text(pricing_yaml)

    with mock.patch.dict(os.environ, {"GENAI_MODEL_PRICING_FILE": str(pricing_file)}):
        calculator = CostCalculator()
        pricing = calculator.get_model_pricing("anthropic", "claude-custom-test")

        # Should use custom pricing
        assert pricing["prompt"] == 999.0
        assert pricing["completion"] == 999.0


def test_custom_pricing_per_model_merge(tmp_path):
    """Test that per-model merging preserves unmodified models."""
    custom_pricing = {
        "anthropic": {
            "claude-3-5-haiku": {
                "prompt": 1.5,  # Override just haiku
                "completion": 5.0,
                "cache_read": 0.15,
                "cache_write": 1.5,
            }
        }
    }

    pricing_file = tmp_path / "pricing.json"
    pricing_file.write_text(json.dumps(custom_pricing))

    with mock.patch.dict(os.environ, {"GENAI_MODEL_PRICING_FILE": str(pricing_file)}):
        pricing_dict = _load_custom_pricing()

        # Haiku should have custom pricing
        assert pricing_dict["anthropic"]["claude-3-5-haiku"]["prompt"] == 1.5

        # Sonnet should still have default pricing
        assert pricing_dict["anthropic"]["claude-3-5-sonnet"]["prompt"] == 3.0


def test_custom_pricing_file_not_found():
    """Test that missing pricing file falls back to defaults."""
    with mock.patch.dict(
        os.environ, {"GENAI_MODEL_PRICING_FILE": "/nonexistent/pricing.yaml"}
    ):
        pricing_dict = _load_custom_pricing()

        # Should fall back to base pricing
        assert "anthropic" in pricing_dict
        assert "claude-3-5-sonnet" in pricing_dict["anthropic"]


def test_custom_pricing_invalid_yaml(tmp_path):
    """Test that invalid YAML is handled gracefully."""
    pricing_file = tmp_path / "pricing.yaml"
    pricing_file.write_text("invalid: yaml: [content")

    with mock.patch.dict(os.environ, {"GENAI_MODEL_PRICING_FILE": str(pricing_file)}):
        pricing_dict = _load_custom_pricing()

        # Should fall back to base pricing
        assert "anthropic" in pricing_dict
        assert "claude-3-5-sonnet" in pricing_dict["anthropic"]


def test_detect_bedrock_optimized_latency():
    """Test detection of Bedrock optimized latency setting."""
    # Test default (false)
    with mock.patch.dict(os.environ, {}, clear=True):
        assert not _detect_bedrock_optimized_latency()

    # Test enabled
    with mock.patch.dict(os.environ, {"ENABLE_BEDROCK_OPTIMIZED_LATENCY": "true"}):
        assert _detect_bedrock_optimized_latency()

    # Test other truthy values
    for value in ("1", "yes", "True", "TRUE"):
        with mock.patch.dict(os.environ, {"ENABLE_BEDROCK_OPTIMIZED_LATENCY": value}):
            assert _detect_bedrock_optimized_latency()

    # Test false
    with mock.patch.dict(os.environ, {"ENABLE_BEDROCK_OPTIMIZED_LATENCY": "false"}):
        assert not _detect_bedrock_optimized_latency()


def test_detect_bedrock_regional_endpoint():
    """Test detection of regional Bedrock endpoints."""
    # Test default (us - regional)
    with mock.patch.dict(os.environ, {}, clear=True):
        assert _detect_bedrock_regional_endpoint()

    # Test regional prefixes
    for prefix in ("us", "eu", "apac"):
        with mock.patch.dict(
            os.environ, {"ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": prefix}
        ):
            assert _detect_bedrock_regional_endpoint()

    # Test global endpoint (no premium)
    with mock.patch.dict(os.environ, {"ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": ""}):
        assert not _detect_bedrock_regional_endpoint()


def test_apply_bedrock_premiums_no_premium():
    """Test that non-Bedrock providers don't get premiums."""
    base_pricing = {"prompt": 3.0, "completion": 15.0}

    # Anthropic direct shouldn't get Bedrock premiums
    result = _apply_bedrock_premiums(base_pricing, "anthropic")
    assert result == base_pricing


def test_apply_bedrock_premiums_regional():
    """Test regional endpoint premium (+10%)."""
    base_pricing = {"prompt": 1.0, "completion": 2.0}

    with mock.patch.dict(
        os.environ,
        {
            "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "us",
            "ENABLE_BEDROCK_OPTIMIZED_LATENCY": "false",
        },
    ):
        result = _apply_bedrock_premiums(base_pricing, "bedrock")

        # Should have 10% premium
        assert result["prompt"] == pytest.approx(1.10)
        assert result["completion"] == pytest.approx(2.20)


def test_apply_bedrock_premiums_optimized():
    """Test optimized latency premium (+25%)."""
    base_pricing = {"prompt": 1.0, "completion": 2.0}

    with mock.patch.dict(
        os.environ,
        {
            "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "",
            "ENABLE_BEDROCK_OPTIMIZED_LATENCY": "true",
        },
    ):
        result = _apply_bedrock_premiums(base_pricing, "bedrock")

        # Should have 25% premium
        assert result["prompt"] == pytest.approx(1.25)
        assert result["completion"] == pytest.approx(2.50)


def test_apply_bedrock_premiums_both():
    """Test combined regional + optimized premiums (+10% +25% = +37.5%)."""
    base_pricing = {"prompt": 1.0, "completion": 2.0}

    with mock.patch.dict(
        os.environ,
        {
            "ANTHROPIC_CROSS_REGION_INFERENCE_PREFIX": "us",
            "ENABLE_BEDROCK_OPTIMIZED_LATENCY": "true",
        },
    ):
        result = _apply_bedrock_premiums(base_pricing, "bedrock")

        # Should have 1.10 * 1.25 = 1.375x multiplier
        assert result["prompt"] == pytest.approx(1.375)
        assert result["completion"] == pytest.approx(2.75)


def test_cost_calculation_with_haiku_35():
    """Test cost calculation for Claude 3.5 Haiku."""
    calculator = CostCalculator()

    usage = TokenUsage(
        prompt_tokens=1_000_000,  # 1M tokens
        completion_tokens=1_000_000,  # 1M tokens
        total_tokens=2_000_000,
    )

    cost = calculator.calculate_cost("anthropic", "claude-3-5-haiku", usage)
    assert cost is not None

    # Claude 3.5 Haiku: $0.80/1M prompt, $4.00/1M completion
    assert cost.prompt_cost == pytest.approx(0.80)
    assert cost.completion_cost == pytest.approx(4.0)
    assert cost.total_cost == pytest.approx(4.80)


def test_missing_pricing_returns_none():
    """Test that missing pricing returns None without crashing."""
    calculator = CostCalculator()

    # Unknown provider
    pricing = calculator.get_model_pricing("unknown_provider", "some-model")
    assert pricing is None

    # Known provider, unknown model
    pricing = calculator.get_model_pricing("anthropic", "claude-99-ultra")
    assert pricing is None


def test_prefix_matching_still_works():
    """Test that prefix matching for model names still works."""
    calculator = CostCalculator()

    # "gpt-4-0613" should match "gpt-4"
    pricing = calculator.get_model_pricing("openai", "gpt-4-0613")
    assert pricing is not None
    assert pricing["prompt"] == 30.0
