"""Bridge between importlib.metadata plugin discovery and PluginRegistry.

Scans installed packages for ``datahub-plugin.yaml`` manifests and
imports the declared entry-point classes.  Plugins are installed
directly into the current Python environment, so no ``sys.path``
manipulation is needed.
"""

import logging
import types
from typing import Dict, Mapping, Optional, Type

from datahub.plugin.plugin_config import DiscoveredPlugin, PluginCapabilityType

logger = logging.getLogger(__name__)

# Module-level singleton
_loader: Optional["PluginLoader"] = None


def get_plugin_loader() -> "PluginLoader":
    """Return the module-level PluginLoader singleton (lazy init)."""
    global _loader
    if _loader is None:
        _loader = PluginLoader()
    return _loader


class PluginLoader:
    """Loads plugin classes discovered via importlib.metadata.

    On first access, scans all installed packages for
    ``datahub-plugin.yaml`` manifests and caches the result.
    When asked to load a class, it imports the manifest's
    entry-point class directly (packages are in the current
    environment).
    """

    def __init__(self) -> None:
        self._plugins: Optional[Dict[str, DiscoveredPlugin]] = None

    @property
    def plugins(self) -> Mapping[str, DiscoveredPlugin]:
        if self._plugins is None:
            from datahub.plugin.plugin_manager import discover_plugins

            self._plugins = discover_plugins()
        return types.MappingProxyType(self._plugins)

    def reload(self) -> None:
        """Force re-scan of installed packages (e.g. after an install)."""
        self._plugins = None

    def can_load(self, key: str, registry_type: PluginCapabilityType) -> bool:
        """Check if a plugin provides *key* for *registry_type*."""
        return self._find_plugin(key, registry_type) is not None

    def try_load(self, key: str, registry_type: PluginCapabilityType) -> Optional[Type]:
        """Attempt to load the class for *key*.

        Returns ``None`` if no plugin provides this key, or raises on
        import errors so the caller can decide how to handle them.
        """
        plugin = self._find_plugin(key, registry_type)
        if plugin is None:
            return None

        from datahub.ingestion.api.registry import import_path

        logger.debug(
            "Loading plugin class %s for '%s' from package %s",
            plugin.manifest.entry_point,
            plugin.manifest.id,
            plugin.package_name,
        )
        return import_path(plugin.manifest.entry_point)

    def get_all_capabilities(
        self, registry_type: PluginCapabilityType
    ) -> Dict[str, str]:
        """Return ``{key: import_path}`` for every plugin of *registry_type*."""
        return {
            plugin.manifest.id: plugin.manifest.entry_point
            for plugin in self.plugins.values()
            if plugin.manifest.type == registry_type
        }

    def _find_plugin(
        self, key: str, registry_type: PluginCapabilityType
    ) -> Optional[DiscoveredPlugin]:
        plugin = self.plugins.get(key)
        if plugin is not None and plugin.manifest.type == registry_type:
            return plugin
        return None
