"""Tests for PluginRegistry integration with the plugin loader."""

import abc
import unittest.mock
from unittest.mock import MagicMock

import pytest

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.api.registry import PluginRegistry
from datahub.plugin.plugin_config import PluginCapabilityType


class FakeBase:
    """Base class for testing registry type checks."""

    pass


class FakePlugin(FakeBase):
    """A concrete plugin class."""

    pass


class AbstractPlugin(FakeBase, abc.ABC):
    """An abstract plugin class (should never be accepted)."""

    @abc.abstractmethod
    def run(self) -> None: ...


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
            registry_type=PluginCapabilityType.SOURCE,
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

    def test_loader_class_type_check_tolerated_and_cached(self) -> None:
        """A type-check failure we can't attribute to a wrong type (here the base
        type isn't resolvable, mirroring the import-identity case) is tolerated
        and cached, so a valid-but-differently-imported class still loads."""
        loader = MagicMock()
        loader.try_load.return_value = FakePlugin

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type=PluginCapabilityType.SOURCE,
        )

        # _check_cls fails naturally (FakeRegistry's generic arg isn't resolvable
        # by typing_inspect), and the base type can't be resolved either.
        result = registry.get("my-plugin")
        assert result is FakePlugin
        loader.try_load.assert_called_once_with("my-plugin", "source")

        # Second call is served from the cache — the loader is not re-consulted.
        loader.reset_mock()
        result2 = registry.get("my-plugin")
        assert result2 is FakePlugin
        loader.try_load.assert_not_called()

    def test_loader_wrong_type_raises_configuration_error(self) -> None:
        """An external class that is genuinely not the registry's base type is
        rejected up front with a ConfigurationError, not returned."""
        loader = MagicMock()
        loader.try_load.return_value = FakePlugin

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type=PluginCapabilityType.SOURCE,
        )

        class Unrelated:
            pass

        with (
            unittest.mock.patch.object(
                registry, "_check_cls", side_effect=ValueError("not derived")
            ),
            unittest.mock.patch.object(
                registry, "_get_registered_type", return_value=Unrelated
            ),
        ):
            with pytest.raises(ConfigurationError, match="not a valid"):
                registry.get("my-plugin")

    def test_loader_abstract_class_raises_configuration_error(self) -> None:
        """An abstract external class is rejected even if it shares the base's
        name in its MRO (the import-identity waiver never covers abstract)."""
        loader = MagicMock()
        loader.try_load.return_value = AbstractPlugin

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type=PluginCapabilityType.SOURCE,
        )

        with (
            unittest.mock.patch.object(
                registry, "_check_cls", side_effect=ValueError("abstract")
            ),
            unittest.mock.patch.object(
                registry, "_get_registered_type", return_value=FakeBase
            ),
        ):
            with pytest.raises(ConfigurationError):
                registry.get("abstract-plugin")

    def test_loader_returns_none_raises_key_error(self) -> None:
        """When the loader returns None, the registry raises KeyError."""
        loader = MagicMock()
        loader.try_load.return_value = None

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type=PluginCapabilityType.SOURCE,
        )

        with pytest.raises(KeyError, match="Did not find"):
            registry.get("unknown")

    def test_loader_exception_raises_configuration_error(self) -> None:
        """If the loader raises, we surface a ConfigurationError with context."""
        loader = MagicMock()
        loader.try_load.side_effect = ImportError("broken import")

        registry = FakeRegistry(
            plugin_loader=loader,
            registry_type=PluginCapabilityType.SOURCE,
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
            registry_type=PluginCapabilityType.SOURCE,
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
            registry_type=PluginCapabilityType.SOURCE,
        )
        # Use _register to bypass type check (typing_inspect can't resolve
        # generic args in test subclasses the way it can for PluginRegistry[Source]())
        registry._register(key="builtin-key", tp=FakePlugin)

        summary = registry.summary(verbose=False)
        # Should appear once (built-in), not twice
        assert summary.count("builtin-key") == 1
        assert "(external plugin)" not in summary
