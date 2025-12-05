"""Comprehensive tests for transform_plugins module."""

from typing import Dict
from unittest.mock import Mock, patch

import jpype
import pytest

from datahub.ingestion.source.kafka_connect.transform_plugins import (
    ComplexTransformPlugin,
    RegexRouterPlugin,
    ReplaceFieldPlugin,
    TransformConfig,
    TransformPipeline,
    TransformPluginRegistry,
    get_transform_pipeline,
)


class TestTransformConfig:
    """Test TransformConfig dataclass."""

    def test_transform_config_creation(self) -> None:
        """Test creating a TransformConfig."""
        config = TransformConfig(
            name="MyTransform",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": ".*", "replacement": "new-topic"},
        )

        assert config.name == "MyTransform"
        assert config.type == "org.apache.kafka.connect.transforms.RegexRouter"
        assert config.config["regex"] == ".*"


class TestRegexRouterPlugin:
    """Test RegexRouterPlugin implementation."""

    def test_supports_transform_type_apache(self) -> None:
        """Test Apache Kafka RegexRouter is supported."""
        plugin = RegexRouterPlugin()
        assert plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.RegexRouter"
        )

    def test_supports_transform_type_confluent(self) -> None:
        """Test Confluent Cloud RegexRouter is supported."""
        plugin = RegexRouterPlugin()
        assert plugin.supports_transform_type(
            "io.confluent.connect.cloud.transforms.TopicRegexRouter"
        )

    def test_supports_transform_type_other(self) -> None:
        """Test other transform types are not supported."""
        plugin = RegexRouterPlugin()
        assert not plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.InsertField"
        )

    def test_should_apply_automatically(self) -> None:
        """Test RegexRouter should be applied automatically."""
        assert RegexRouterPlugin.should_apply_automatically()

    def test_apply_forward_simple_pattern(self) -> None:
        """Test applying a simple regex pattern."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "user-(.*)", "replacement": "customer_$1"},
        )

        result = plugin.apply_forward(["user-events", "user-data"], config)

        assert result == ["customer_events", "customer_data"]

    def test_apply_forward_wildcard_pattern(self) -> None:
        """Test applying a wildcard regex pattern."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": ".*", "replacement": "fixed_name"},
        )

        result = plugin.apply_forward(["any-topic"], config)

        assert result == ["fixed_name"]

    def test_apply_forward_empty_replacement(self) -> None:
        """Test applying regex with empty replacement."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "prefix-(.*)", "replacement": ""},
        )

        result = plugin.apply_forward(["prefix-suffix"], config)

        assert result == [""]

    def test_apply_forward_missing_regex(self) -> None:
        """Test handling missing regex pattern."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"replacement": "new-topic"},
        )

        result = plugin.apply_forward(["test-topic"], config)

        # Should return unchanged
        assert result == ["test-topic"]

    def test_apply_forward_missing_replacement(self) -> None:
        """Test handling missing replacement pattern."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": ".*"},
        )

        result = plugin.apply_forward(["test-topic"], config)

        # With missing replacement, Java's replaceFirst uses empty string
        assert result == [""]

    def test_apply_forward_invalid_regex(self) -> None:
        """Test handling invalid regex pattern."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "[invalid(", "replacement": "new-topic"},
        )

        # Should raise a Java PatternSyntaxException (wrapped by JPype)
        with pytest.raises(jpype.JException):
            plugin.apply_forward(["test-topic"], config)

    def test_apply_forward_no_match(self) -> None:
        """Test pattern that doesn't match."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "user-(.*)", "replacement": "customer_$1"},
        )

        # Topic doesn't match pattern
        result = plugin.apply_forward(["order-events"], config)

        # Should return unchanged (replaceFirst doesn't modify if no match)
        assert result == ["order-events"]

    def test_apply_reverse(self) -> None:
        """Test reverse transformation (limited support)."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": "user-(.*)", "replacement": "customer_$1"},
        )

        # Reverse is not fully implemented, should return topics unchanged
        result = plugin.apply_reverse(["customer_events"], config)

        assert result == ["customer_events"]

    @patch(
        "datahub.ingestion.source.kafka_connect.transform_plugins.JAVA_REGEX_AVAILABLE",
        False,
    )
    def test_apply_forward_jpype_not_available(self) -> None:
        """Test error when JPype is not available."""
        plugin = RegexRouterPlugin()
        config = TransformConfig(
            name="Router",
            type="org.apache.kafka.connect.transforms.RegexRouter",
            config={"regex": ".*", "replacement": "new-topic"},
        )

        with pytest.raises(ValueError, match="RegexRouter transform requires JPype"):
            plugin.apply_forward(["test-topic"], config)


class TestComplexTransformPlugin:
    """Test ComplexTransformPlugin implementation."""

    def test_supports_event_router(self) -> None:
        """Test Debezium EventRouter is supported."""
        plugin = ComplexTransformPlugin()
        assert plugin.supports_transform_type(
            "io.debezium.transforms.outbox.EventRouter"
        )

    def test_supports_extract_field(self) -> None:
        """Test ExtractField is supported."""
        plugin = ComplexTransformPlugin()
        assert plugin.supports_transform_type(
            "io.confluent.connect.transforms.ExtractField"
        )

    def test_supports_timestamp_converter(self) -> None:
        """Test TimestampConverter is supported."""
        plugin = ComplexTransformPlugin()
        assert plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.TimestampConverter"
        )

    def test_supports_transform_type_other(self) -> None:
        """Test other transform types are not supported."""
        plugin = ComplexTransformPlugin()
        assert not plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.RegexRouter"
        )

    def test_should_apply_automatically(self) -> None:
        """Test complex transforms should NOT be applied automatically."""
        assert not ComplexTransformPlugin.should_apply_automatically()

    def test_apply_forward_returns_unchanged(self) -> None:
        """Test complex transforms return topics unchanged."""
        plugin = ComplexTransformPlugin()
        config = TransformConfig(
            name="Outbox",
            type="io.debezium.transforms.outbox.EventRouter",
            config={},
        )

        result = plugin.apply_forward(["test-topic"], config)

        # Should return unchanged with warning
        assert result == ["test-topic"]


class TestReplaceFieldPlugin:
    """Test ReplaceFieldPlugin implementation."""

    def test_supports_replace_field_value(self) -> None:
        """Test ReplaceField$Value is supported."""
        plugin = ReplaceFieldPlugin()
        assert plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.ReplaceField$Value"
        )

    def test_supports_replace_field_key(self) -> None:
        """Test ReplaceField$Key is supported."""
        plugin = ReplaceFieldPlugin()
        assert plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.ReplaceField$Key"
        )

    def test_supports_transform_type_other(self) -> None:
        """Test other transform types are not supported."""
        plugin = ReplaceFieldPlugin()
        assert not plugin.supports_transform_type(
            "org.apache.kafka.connect.transforms.RegexRouter"
        )

    def test_should_apply_automatically(self) -> None:
        """Test ReplaceField should be applied automatically (no-op for topics)."""
        assert ReplaceFieldPlugin.should_apply_automatically()

    def test_apply_forward_returns_unchanged(self) -> None:
        """Test ReplaceField doesn't affect topic names."""
        plugin = ReplaceFieldPlugin()
        config = TransformConfig(
            name="ReplaceField",
            type="org.apache.kafka.connect.transforms.ReplaceField$Value",
            config={"include": "field1,field2"},
        )

        result = plugin.apply_forward(["test-topic"], config)

        # Topic names unchanged - ReplaceField only affects field names
        assert result == ["test-topic"]

    def test_apply_reverse_returns_unchanged(self) -> None:
        """Test ReplaceField reverse doesn't affect topic names."""
        plugin = ReplaceFieldPlugin()
        config = TransformConfig(
            name="ReplaceField",
            type="org.apache.kafka.connect.transforms.ReplaceField$Value",
            config={},
        )

        result = plugin.apply_reverse(["test-topic"], config)

        assert result == ["test-topic"]


class TestTransformPluginRegistry:
    """Test TransformPluginRegistry implementation."""

    def test_default_plugins_registered(self) -> None:
        """Test default plugins are registered."""
        registry = TransformPluginRegistry()

        # Check RegexRouter plugin
        plugin = registry.get_plugin("org.apache.kafka.connect.transforms.RegexRouter")
        assert plugin is not None
        assert isinstance(plugin, RegexRouterPlugin)

        # Check ComplexTransform plugin
        plugin = registry.get_plugin("io.debezium.transforms.outbox.EventRouter")
        assert plugin is not None
        assert isinstance(plugin, ComplexTransformPlugin)

        # Check ReplaceField plugin
        plugin = registry.get_plugin(
            "org.apache.kafka.connect.transforms.ReplaceField$Value"
        )
        assert plugin is not None
        assert isinstance(plugin, ReplaceFieldPlugin)

    def test_get_plugin_unknown_type(self) -> None:
        """Test getting plugin for unknown transform type."""
        registry = TransformPluginRegistry()

        plugin = registry.get_plugin("com.example.UnknownTransform")

        assert plugin is None

    def test_should_apply_automatically_regex_router(self) -> None:
        """Test should_apply_automatically for RegexRouter."""
        registry = TransformPluginRegistry()

        should_apply = registry.should_apply_automatically(
            "org.apache.kafka.connect.transforms.RegexRouter"
        )

        assert should_apply is True

    def test_should_apply_automatically_complex_transform(self) -> None:
        """Test should_apply_automatically for complex transforms."""
        registry = TransformPluginRegistry()

        should_apply = registry.should_apply_automatically(
            "io.debezium.transforms.outbox.EventRouter"
        )

        assert should_apply is False

    def test_should_apply_automatically_unknown_type(self) -> None:
        """Test should_apply_automatically for unknown types."""
        registry = TransformPluginRegistry()

        should_apply = registry.should_apply_automatically(
            "com.example.UnknownTransform"
        )

        # Unknown transforms should not be applied automatically
        assert should_apply is False

    def test_register_custom_plugin(self) -> None:
        """Test registering a custom plugin."""
        registry = TransformPluginRegistry()

        # Create a mock plugin
        custom_plugin = Mock()
        custom_plugin.supports_transform_type.return_value = True

        registry.register(custom_plugin)

        # Custom plugin should be found
        plugin = registry.get_plugin("custom.type")
        assert plugin == custom_plugin


class TestTransformPipeline:
    """Test TransformPipeline implementation."""

    def test_parse_transforms_no_transforms(self) -> None:
        """Test parsing when no transforms are configured."""
        pipeline = TransformPipeline()
        config: Dict[str, str] = {}

        transforms = pipeline.parse_transforms(config)

        assert transforms == []

    def test_parse_transforms_empty_string(self) -> None:
        """Test parsing with empty transforms parameter."""
        pipeline = TransformPipeline()
        config = {"transforms": ""}

        transforms = pipeline.parse_transforms(config)

        assert transforms == []

    def test_parse_transforms_single_transform(self) -> None:
        """Test parsing a single transform."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Router",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Router.regex": ".*",
            "transforms.Router.replacement": "new-topic",
        }

        transforms = pipeline.parse_transforms(config)

        assert len(transforms) == 1
        assert transforms[0].name == "Router"
        assert transforms[0].type == "org.apache.kafka.connect.transforms.RegexRouter"
        assert transforms[0].config["regex"] == ".*"
        assert transforms[0].config["replacement"] == "new-topic"

    def test_parse_transforms_multiple_transforms(self) -> None:
        """Test parsing multiple transforms."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "First,Second",
            "transforms.First.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.First.regex": "pattern1",
            "transforms.Second.type": "org.apache.kafka.connect.transforms.InsertField",
            "transforms.Second.field": "timestamp",
        }

        transforms = pipeline.parse_transforms(config)

        assert len(transforms) == 2
        assert transforms[0].name == "First"
        assert transforms[1].name == "Second"

    def test_parse_transforms_whitespace_handling(self) -> None:
        """Test parsing with whitespace in transform list."""
        pipeline = TransformPipeline()
        config = {
            "transforms": " First , Second ",
            "transforms.First.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Second.type": "org.apache.kafka.connect.transforms.InsertField",
        }

        transforms = pipeline.parse_transforms(config)

        assert len(transforms) == 2

    def test_parse_transforms_without_type(self) -> None:
        """Test parsing transform without type field."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "NoType",
            "transforms.NoType.field": "value",
        }

        transforms = pipeline.parse_transforms(config)

        # Transform without type should be skipped
        assert transforms == []

    def test_apply_forward_no_transforms(self) -> None:
        """Test applying transforms when none are configured."""
        pipeline = TransformPipeline()
        config: Dict[str, str] = {}

        result = pipeline.apply_forward(["test-topic"], config)

        assert result.topics == ["test-topic"]
        assert result.successful is True
        assert result.fallback_used is False
        assert result.warnings == []

    def test_apply_forward_single_regex_router(self) -> None:
        """Test applying a single RegexRouter transform."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Router",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Router.regex": "user-(.*)",
            "transforms.Router.replacement": "customer_$1",
        }

        result = pipeline.apply_forward(["user-events"], config)

        assert result.topics == ["customer_events"]
        assert result.successful is True
        assert result.fallback_used is False

    def test_apply_forward_multiple_transforms(self) -> None:
        """Test applying multiple transforms in sequence."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "First,Second",
            "transforms.First.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.First.regex": "user-(.*)",
            "transforms.First.replacement": "customer_$1",
            "transforms.Second.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Second.regex": "customer_(.*)",
            "transforms.Second.replacement": "final_$1",
        }

        result = pipeline.apply_forward(["user-events"], config)

        assert result.topics == ["final_events"]
        assert result.successful is True

    def test_apply_forward_unknown_transform(self) -> None:
        """Test applying unknown transform type."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Unknown",
            "transforms.Unknown.type": "com.example.UnknownTransform",
        }

        result = pipeline.apply_forward(["test-topic"], config)

        assert result.topics == ["test-topic"]
        assert len(result.warnings) > 0
        assert "Unknown transform type" in result.warnings[0]

    def test_apply_forward_complex_transform(self) -> None:
        """Test applying complex transform that requires explicit config."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Outbox",
            "transforms.Outbox.type": "io.debezium.transforms.outbox.EventRouter",
        }

        result = pipeline.apply_forward(["test-topic"], config)

        assert result.topics == ["test-topic"]
        assert result.fallback_used is True
        assert len(result.warnings) > 0
        assert "Complex transforms detected" in result.warnings[0]

    def test_apply_forward_mixed_transforms(self) -> None:
        """Test applying mix of predictable and complex transforms."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Router,Outbox",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Router.regex": "user-(.*)",
            "transforms.Router.replacement": "customer_$1",
            "transforms.Outbox.type": "io.debezium.transforms.outbox.EventRouter",
        }

        result = pipeline.apply_forward(["user-events"], config)

        # RegexRouter should be applied, Outbox skipped
        assert result.topics == ["customer_events"]
        assert result.fallback_used is True
        assert len(result.warnings) > 0

    def test_apply_forward_replace_field_transform(self) -> None:
        """Test applying ReplaceField transform (no-op for topics)."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "ReplaceField",
            "transforms.ReplaceField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
            "transforms.ReplaceField.include": "field1,field2",
        }

        result = pipeline.apply_forward(["test-topic"], config)

        # Topic unchanged - ReplaceField only affects fields
        assert result.topics == ["test-topic"]
        assert result.successful is True
        assert result.fallback_used is False

    def test_apply_forward_transform_failure(self) -> None:
        """Test handling transform execution failure."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "BadRouter",
            "transforms.BadRouter.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.BadRouter.regex": "[invalid(",
            "transforms.BadRouter.replacement": "new-topic",
        }

        result = pipeline.apply_forward(["test-topic"], config)

        # Should capture the error in warnings
        assert result.topics == ["test-topic"]
        assert len(result.warnings) > 0
        assert "failed" in result.warnings[0]

    def test_has_complex_transforms_none(self) -> None:
        """Test has_complex_transforms with no transforms."""
        pipeline = TransformPipeline()
        config: Dict[str, str] = {}

        assert pipeline.has_complex_transforms(config) is False

    def test_has_complex_transforms_only_simple(self) -> None:
        """Test has_complex_transforms with only simple transforms."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Router",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
        }

        assert pipeline.has_complex_transforms(config) is False

    def test_has_complex_transforms_with_complex(self) -> None:
        """Test has_complex_transforms with complex transforms."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Outbox",
            "transforms.Outbox.type": "io.debezium.transforms.outbox.EventRouter",
        }

        assert pipeline.has_complex_transforms(config) is True

    def test_has_complex_transforms_mixed(self) -> None:
        """Test has_complex_transforms with mixed transforms."""
        pipeline = TransformPipeline()
        config = {
            "transforms": "Router,Outbox",
            "transforms.Router.type": "org.apache.kafka.connect.transforms.RegexRouter",
            "transforms.Outbox.type": "io.debezium.transforms.outbox.EventRouter",
        }

        assert pipeline.has_complex_transforms(config) is True


class TestGetTransformPipeline:
    """Test get_transform_pipeline() caching."""

    def test_returns_transform_pipeline(self) -> None:
        """Test function returns TransformPipeline instance."""
        pipeline = get_transform_pipeline()

        assert isinstance(pipeline, TransformPipeline)

    def test_returns_cached_instance(self) -> None:
        """Test function returns same cached instance."""
        pipeline1 = get_transform_pipeline()
        pipeline2 = get_transform_pipeline()

        # Should be the same instance (cached)
        assert pipeline1 is pipeline2
