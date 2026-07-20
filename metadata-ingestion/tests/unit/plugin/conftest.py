"""Shared fixtures and helpers for plugin tests."""

from datahub.plugin.plugin_config import (
    DiscoveredPlugin,
    PluginCapabilityType,
    PluginManifest,
)


def make_discovered_plugin(
    plugin_id: str = "test-source",
    plugin_type: PluginCapabilityType = PluginCapabilityType.SOURCE,
    entry_point: str = "test_module.source:TestSource",
    package_name: str = "datahub-test-source",
    version: str = "1.0.0",
) -> DiscoveredPlugin:
    return DiscoveredPlugin(
        manifest=PluginManifest(
            id=plugin_id,
            name="Test Source",
            type=plugin_type,
            entry_point=entry_point,
        ),
        package_name=package_name,
        version=version,
    )
