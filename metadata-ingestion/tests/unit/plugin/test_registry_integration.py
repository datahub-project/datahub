"""Tests for PluginRegistry integration with the plugin loader."""

import unittest.mock
from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.registry import PluginRegistry


class FakeBase:
    """Base class for testing registry type checks."""

    pass


class FakePlugin(FakeBase):
    """A concrete plugin class."""

    pass


class FakeRegistry(PluginRegistry[FakeBase]):
    """Typed subclass so typing_inspect can resolve the generic arg."""

    pass


class TestRegistryPluginLoaderFallback:
    def test_no_loader_raises_key_error(self) -> None:
        """Without a plugin loader, missing keys raise KeyError as before."""
        registry = FakeRegistry()
        with pytest.raises(KeyError, match="Did not find"):
            registry.get("nonexistent")

    def test_loader_provides_class(self) -> None:
        """When a plugin loader returns a class that passes type check, it's cached."""
        loader = MagicMock()
        loader.try_load.return_value = FakePlugin

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )

        # Mock _check_cls to succeed (typing_inspect can't resolve generic
        # args for FakeRegistry in tests, but works for real registries)
        with unittest.mock.patch.object(registry, "_check_cls"):
            result = registry.get("my-plugin")
            assert result is FakePlugin
            loader.try_load.assert_called_once_with("my-plugin", "source")

            # Second call should use the cached registration
            loader.reset_mock()
            result2 = registry.get("my-plugin")
            assert result2 is FakePlugin
            loader.try_load.assert_not_called()

    def test_loader_class_fails_type_check_not_cached(self) -> None:
        """When type check fails, class is returned but NOT cached."""
        loader = MagicMock()
        loader.try_load.return_value = FakePlugin

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )

        # _check_cls fails naturally in test environment (typing_inspect
        # can't resolve FakeRegistry[FakeBase] generic args).
        result = registry.get("my-plugin")
        assert result is FakePlugin
        loader.try_load.assert_called_once_with("my-plugin", "source")

        # Second call goes back to loader since it wasn't cached
        loader.reset_mock()
        result2 = registry.get("my-plugin")
        assert result2 is FakePlugin
        loader.try_load.assert_called_once()

    def test_loader_returns_none_raises_key_error(self) -> None:
        """When the loader returns None, the registry raises KeyError."""
        loader = MagicMock()
        loader.try_load.return_value = None

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )

        with pytest.raises(KeyError, match="Did not find"):
            registry.get("unknown")

    def test_loader_exception_raises_configuration_error(self) -> None:
        """If the loader raises, we surface a ConfigurationError with context."""
        loader = MagicMock()
        loader.try_load.side_effect = ImportError("broken import")

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )

        with pytest.raises(ConfigurationError, match="failed to load"):
            registry.get("broken-plugin")

    def test_summary_includes_external_plugins(self) -> None:
        """The summary method should list external plugin capabilities."""
        loader = MagicMock()
        loader.get_all_capabilities.return_value = {
            "ext-source": "ext_module:ExtSource",
        }

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )

        summary = registry.summary(verbose=False)
        assert "ext-source" in summary
        assert "(external plugin)" in summary

    def test_builtin_takes_precedence_over_external(self) -> None:
        """Built-in registrations should not be duplicated in summary."""
        loader = MagicMock()
        loader.get_all_capabilities.return_value = {
            "builtin-key": "mod:Cls",
        }

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type="source",
        )
        # Use _register to bypass type check (typing_inspect can't resolve
        # generic args in test subclasses the way it can for PluginRegistry[Source]())
        registry._register(key="builtin-key", tp=FakePlugin)

        summary = registry.summary(verbose=False)
        # Should appear once (built-in), not twice
        assert summary.count("builtin-key") == 1
        assert "(external plugin)" not in summary
