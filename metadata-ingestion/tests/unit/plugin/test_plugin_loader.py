"""Tests for the plugin loader."""

from unittest.mock import MagicMock, patch

from datahub.plugin.plugin_config import PluginCapabilityType
from datahub.plugin.plugin_loader import PluginLoader
from tests.unit.plugin.conftest import make_discovered_plugin


class TestPluginLoader:
    def test_can_load_returns_true_for_known_plugin(self) -> None:
        loader = PluginLoader()
        loader._plugins = {"test-source": make_discovered_plugin()}

        assert loader.can_load("test-source", PluginCapabilityType.SOURCE)

    def test_can_load_returns_false_for_unknown(self) -> None:
        loader = PluginLoader()
        loader._plugins = {}

        assert not loader.can_load("nonexistent", PluginCapabilityType.SOURCE)

    def test_can_load_respects_registry_type(self) -> None:
        loader = PluginLoader()
        loader._plugins = {
            "test-source": make_discovered_plugin(
                plugin_type=PluginCapabilityType.SOURCE,
            )
        }

        assert loader.can_load("test-source", PluginCapabilityType.SOURCE)
        assert not loader.can_load("test-source", PluginCapabilityType.SINK)

    def test_try_load_returns_none_for_unknown(self) -> None:
        loader = PluginLoader()
        loader._plugins = {}

        assert loader.try_load("nonexistent", PluginCapabilityType.SOURCE) is None

    @patch("datahub.ingestion.api.registry.import_path")
    def test_try_load_success(self, mock_import_path: MagicMock) -> None:
        """Verify the success path returns the imported class."""

        class FakeSource:
            pass

        mock_import_path.return_value = FakeSource

        loader = PluginLoader()
        loader._plugins = {
            "test-source": make_discovered_plugin(
                entry_point="test_module.source:TestSource",
            )
        }

        result = loader.try_load("test-source", PluginCapabilityType.SOURCE)
        assert result is FakeSource
        mock_import_path.assert_called_once_with("test_module.source:TestSource")

    def test_get_all_capabilities(self) -> None:
        loader = PluginLoader()
        loader._plugins = {
            "src-a": make_discovered_plugin(
                plugin_id="src-a",
                plugin_type=PluginCapabilityType.SOURCE,
                entry_point="a:A",
            ),
            "src-b": make_discovered_plugin(
                plugin_id="src-b",
                plugin_type=PluginCapabilityType.SOURCE,
                entry_point="b:B",
            ),
            "my-sink": make_discovered_plugin(
                plugin_id="my-sink",
                plugin_type=PluginCapabilityType.SINK,
                entry_point="s:S",
            ),
        }

        sources = loader.get_all_capabilities(PluginCapabilityType.SOURCE)
        assert len(sources) == 2
        assert "src-a" in sources
        assert "src-b" in sources

        sinks = loader.get_all_capabilities(PluginCapabilityType.SINK)
        assert len(sinks) == 1
        assert "my-sink" in sinks

    def test_reload_clears_cache(self) -> None:
        loader = PluginLoader()
        loader._plugins = {}

        loader.reload()
        assert loader._plugins is None

    @patch("datahub.plugin.plugin_manager.discover_plugins")
    def test_lazy_discovery(self, mock_discover: MagicMock) -> None:
        """Verify plugins property triggers discovery on first access."""
        mock_discover.return_value = {"test-source": make_discovered_plugin()}

        loader = PluginLoader()
        assert loader._plugins is None

        plugins = loader.plugins
        assert len(plugins) == 1
        assert "test-source" in plugins
