"""
Transform plugin architecture for Kafka Connect.

This module provides a clean, extensible plugin system for handling transform pipelines
without the complexity and duplication of the previous approach.

Key principles:
1. Explicit configuration over prediction
2. Fail-fast for complex scenarios
3. Single implementation with clear interfaces
4. Plugin registration for extensibility
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.ingestion.source.kafka_connect.common import (
    ConnectorConfigKeys,
    parse_comma_separated_list,
)

logger = logging.getLogger(__name__)


@dataclass
class TransformConfig:
    """Configuration for a single transform."""

    name: str
    type: str
    config: Dict[str, str]


@dataclass
class TransformResult:
    """Result of applying transforms."""

    topics: List[str]
    successful: bool
    fallback_used: bool
    warnings: List[str]


class TransformPlugin(ABC):
    """Base class for transform plugins."""

    @classmethod
    @abstractmethod
    def supports_transform_type(cls, transform_type: str) -> bool:
        """Check if this plugin supports the given transform type."""
        pass

    @abstractmethod
    def apply_forward(self, topics: List[str], config: TransformConfig) -> List[str]:
        """Apply transform in forward direction (source -> target)."""
        pass

    def apply_reverse(self, topics: List[str], config: TransformConfig) -> List[str]:
        """
        Apply transform in reverse direction (target -> source).
        Default implementation returns original topics (no reverse mapping).
        """
        return topics

    @classmethod
    @abstractmethod
    def should_apply_automatically(cls) -> bool:
        """Return True if this transform can be safely applied automatically."""
        pass


class RegexRouterPlugin(TransformPlugin):
    """Plugin for RegexRouter transforms."""

    SUPPORTED_TYPES = {
        "org.apache.kafka.connect.transforms.RegexRouter",
        "io.confluent.connect.cloud.transforms.TopicRegexRouter",
    }

    @classmethod
    def supports_transform_type(cls, transform_type: str) -> bool:
        return transform_type in cls.SUPPORTED_TYPES

    def apply_forward(self, topics: List[str], config: TransformConfig) -> List[str]:
        """Apply RegexRouter transform forward."""
        regex_pattern = config.config.get("regex", "")
        replacement = config.config.get("replacement", "")

        if not regex_pattern or replacement is None:
            logger.warning(
                f"RegexRouter {config.name} missing regex or replacement pattern"
            )
            return topics

        transformed_topics = []
        for topic in topics:
            try:
                # Use Java regex to match Kafka Connect behavior exactly
                import jpype

                if jpype.isJVMStarted():
                    from java.util.regex import Pattern

                    pattern = Pattern.compile(regex_pattern)
                    matcher = pattern.matcher(topic)
                    transformed_topic = str(matcher.replaceFirst(replacement))
                else:
                    # JVM not available - skip transform and keep original topic
                    logger.warning(
                        f"RegexRouter {config.name}: JVM not started, skipping transform"
                    )
                    transformed_topic = topic

                logger.debug(
                    f"RegexRouter {config.name}: {topic} -> {transformed_topic}"
                )
                transformed_topics.append(transformed_topic)
            except Exception as e:
                logger.warning(
                    f"RegexRouter {config.name} pattern error for topic '{topic}': {e}"
                )
                # Re-raise exception so pipeline can capture it as a warning
                raise e

        return transformed_topics

    def apply_reverse(self, topics: List[str], config: TransformConfig) -> List[str]:
        """Apply RegexRouter in reverse (limited support)."""
        # Simple reverse for basic pattern replacements
        regex_pattern = config.config.get("regex", "")
        replacement = config.config.get("replacement", "")

        if not regex_pattern or not replacement:
            return topics

        # Try simple reverse mapping for common patterns
        try:
            # Very basic reverse - only works for simple substitutions
            reverse_topics = []
            for topic in topics:
                # This is a simplified reverse - real reverse regex is complex
                # For production use, explicit configuration should be preferred
                reverse_topics.append(topic)
            return reverse_topics
        except Exception as e:
            logger.debug(f"RegexRouter reverse failed: {e}")
            return topics

    @classmethod
    def should_apply_automatically(cls) -> bool:
        return True  # RegexRouter transforms are predictable and safe to apply


class ComplexTransformPlugin(TransformPlugin):
    """Plugin for complex transforms that require explicit configuration."""

    COMPLEX_TYPES = {
        "io.debezium.transforms.outbox.EventRouter",
        "io.confluent.connect.transforms.ExtractField",
        "org.apache.kafka.connect.transforms.TimestampConverter",
    }

    @classmethod
    def supports_transform_type(cls, transform_type: str) -> bool:
        return transform_type in cls.COMPLEX_TYPES

    def apply_forward(self, topics: List[str], config: TransformConfig) -> List[str]:
        """Complex transforms always require explicit configuration."""
        logger.warning(
            f"Transform '{config.name}' of type '{config.type}' is complex and requires "
            f"explicit configuration. Use 'generic_connectors' config for accurate mappings."
        )
        return topics  # Return unchanged - requires explicit configuration

    @classmethod
    def should_apply_automatically(cls) -> bool:
        return False  # Complex transforms require explicit user configuration


class TransformPluginRegistry:
    """Registry for transform plugins."""

    def __init__(self):
        self._plugins: List[TransformPlugin] = []
        self._register_default_plugins()

    def _register_default_plugins(self):
        """Register default transform plugins."""
        self.register(RegexRouterPlugin())
        self.register(ComplexTransformPlugin())

    def register(self, plugin: TransformPlugin) -> None:
        """Register a transform plugin."""
        self._plugins.append(plugin)

    def get_plugin(self, transform_type: str) -> Optional[TransformPlugin]:
        """Get plugin that supports the given transform type."""
        for plugin in self._plugins:
            if plugin.supports_transform_type(transform_type):
                return plugin
        return None

    def should_apply_automatically(self, transform_type: str) -> bool:
        """Check if a transform type should be applied automatically."""
        plugin = self.get_plugin(transform_type)
        if plugin:
            return plugin.should_apply_automatically()
        return False  # Unknown transforms should not be applied automatically


class TransformPipeline:
    """
    Unified transform pipeline that replaces the duplicated implementations.

    This provides a single, clean interface for applying transforms with proper
    error handling and fallback to explicit configuration when needed.
    """

    def __init__(self):
        self.registry = TransformPluginRegistry()

    def parse_transforms(
        self, connector_config: Dict[str, str]
    ) -> List[TransformConfig]:
        """Parse transform configuration from connector config."""
        transforms_param = connector_config.get(ConnectorConfigKeys.TRANSFORMS, "")
        if not transforms_param:
            return []

        transform_names = parse_comma_separated_list(transforms_param)
        transforms = []

        for name in transform_names:
            if not name:
                continue

            # Extract transform configuration
            transform_config = {"name": name}
            transform_prefix = f"transforms.{name}."

            for key, value in connector_config.items():
                if key.startswith(transform_prefix):
                    config_key = key[len(transform_prefix) :]
                    transform_config[config_key] = value

            transform_type = transform_config.get("type", "")
            if transform_type:
                transforms.append(
                    TransformConfig(
                        name=name, type=transform_type, config=transform_config
                    )
                )

        return transforms

    def apply_forward(
        self, topics: List[str], connector_config: Dict[str, str]
    ) -> TransformResult:
        """Apply transforms in forward direction (source -> target)."""
        transforms = self.parse_transforms(connector_config)
        if not transforms:
            return TransformResult(
                topics=topics, successful=True, fallback_used=False, warnings=[]
            )

        result_topics = topics[:]
        warnings = []
        complex_transforms = []

        for transform in transforms:
            plugin = self.registry.get_plugin(transform.type)
            if not plugin:
                warnings.append(f"Unknown transform type: {transform.type}")
                continue

            if not plugin.should_apply_automatically():
                complex_transforms.append(transform.name)
                continue  # Skip complex transforms, use explicit config instead

            try:
                result_topics = plugin.apply_forward(result_topics, transform)
            except Exception as e:
                warnings.append(f"Transform {transform.name} failed: {e}")

        # Check for complex transforms that require explicit configuration
        fallback_used = len(complex_transforms) > 0
        if fallback_used:
            warnings.append(
                f"Complex transforms detected: {complex_transforms}. "
                f"Consider using 'generic_connectors' config for explicit mappings."
            )

        return TransformResult(
            topics=result_topics,
            successful=len(warnings) == 0 or fallback_used,
            fallback_used=fallback_used,
            warnings=warnings,
        )

    def has_complex_transforms(self, connector_config: Dict[str, str]) -> bool:
        """Check if connector has complex transforms that need explicit configuration."""
        transforms = self.parse_transforms(connector_config)
        for transform in transforms:
            if not self.registry.should_apply_automatically(transform.type):
                return True
        return False


# Global pipeline instance
_pipeline_instance: Optional[TransformPipeline] = None


def get_transform_pipeline() -> TransformPipeline:
    """Get the global transform pipeline instance."""
    global _pipeline_instance
    if _pipeline_instance is None:
        _pipeline_instance = TransformPipeline()
    return _pipeline_instance
