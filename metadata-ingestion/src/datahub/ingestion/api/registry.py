from __future__ import annotations

import importlib
import inspect
import logging
import sys
import threading
import types
import unittest.mock
from importlib.metadata import entry_points
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    Optional,
    Tuple,
    Type,
    TypeVar,
    Union,
)

if TYPE_CHECKING:
    from datahub.plugin.plugin_config import PluginCapabilityType
    from datahub.plugin.plugin_loader import PluginLoader

import typing_inspect

from datahub._version import __package_name__
from datahub.configuration.common import ConfigurationError

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _is_importable(path: str) -> bool:
    return "." in path or ":" in path


def import_path(path: str) -> Any:
    """
    Import an item from a package, as specified by the import path.

    The path is formatted as 'package.module.submodule.ClassName'
    or 'package.module.submodule:ClassName.classmethod'. The dot-based format assumes that the bit
    after the last dot is the item to be fetched. In cases where the item to be imported is embedded
    within another type, the colon-based syntax can be used to disambiguate.

    This method also adds the current working directory to the path so that we can import local
    modules. We add it to the end of the path so that global modules take precedence.
    """
    assert _is_importable(path), "path must be in the appropriate format"

    if ":" in path:
        module_name, object_name = path.rsplit(":", 1)
    else:
        module_name, object_name = path.rsplit(".", 1)

    # Add the current working directory to the path so that we can import local modules.
    with unittest.mock.patch("sys.path", [*sys.path, ""]):
        item = importlib.import_module(module_name)

    for attr in object_name.split("."):
        item = getattr(item, attr)
    return item


class PluginRegistry(Generic[T]):
    def __init__(
        self,
        extra_cls_check: Optional[Callable[[Type[T]], None]] = None,
        plugin_loader: Optional[PluginLoader] = None,
        registry_type: Optional[PluginCapabilityType] = None,
    ) -> None:
        if (plugin_loader is None) != (registry_type is None):
            raise ValueError(
                "plugin_loader and registry_type must be provided together or not at all"
            )
        self._entrypoints: List[str] = []
        self._mapping: Dict[str, Union[str, Type[T], Exception]] = {}
        self._aliases: Dict[str, Tuple[str, Callable[[], None]]] = {}
        self._extra_cls_check = extra_cls_check
        self._lock = threading.Lock()
        self._plugin_loader = plugin_loader
        self._registry_type = registry_type

    def _get_registered_type(self) -> Type[T]:
        cls = typing_inspect.get_generic_type(self)
        tp = typing_inspect.get_args(cls)[0]
        assert tp
        return tp

    def _check_cls(self, cls: Type[T]) -> None:
        if inspect.isabstract(cls):
            raise ValueError(
                f"cannot register an abstract type in the registry; got {cls}"
            )
        super_cls = self._get_registered_type()
        if not issubclass(cls, super_cls):
            raise ValueError(f"must be derived from {super_cls}; got {cls}")
        if self._extra_cls_check is not None:
            self._extra_cls_check(cls)

    def _register(
        self, key: str, tp: Union[str, Type[T], Exception], override: bool = False
    ) -> None:
        if not override and key in self._mapping:
            raise KeyError(f"key already in use - {key}")
        if _is_importable(key):
            raise KeyError(f"key cannot contain special characters; got {key}")
        self._mapping[key] = tp

    def register(self, key: str, cls: Type[T], override: bool = False) -> None:
        self._check_cls(cls)
        self._register(key, cls, override=override)

    def register_lazy(self, key: str, import_path: str) -> None:
        self._register(key, import_path)

    def register_disabled(
        self, key: str, reason: Exception, override: bool = False
    ) -> None:
        self._register(key, reason, override=override)

    def register_alias(
        self, alias: str, real_key: str, fn: Callable[[], None] = lambda: None
    ) -> None:
        self._aliases[alias] = (real_key, fn)

    def _ensure_not_lazy(self, key: str) -> Union[Type[T], Exception]:
        self._materialize_entrypoints()

        path = self._mapping[key]
        if not isinstance(path, str):
            return path
        try:
            plugin_class = import_path(path)
            self.register(key, plugin_class, override=True)
            return plugin_class
        except Exception as e:
            self.register_disabled(key, e, override=True)
            return e

    def is_enabled(self, key: str) -> bool:
        self._materialize_entrypoints()

        tp = self._mapping[key]
        return not isinstance(tp, Exception)

    def register_from_entrypoint(self, entry_point_key: str) -> None:
        self._entrypoints.append(entry_point_key)

    def _load_entrypoint(self, entry_point_key: str) -> None:
        for entry_point in entry_points(group=entry_point_key):
            self.register_lazy(entry_point.name, entry_point.value)

    def _materialize_entrypoints(self) -> None:
        with self._lock:
            for entry_point_key in self._entrypoints:
                self._load_entrypoint(entry_point_key)
            self._entrypoints = []

    @property
    def mapping(self) -> Mapping[str, Union[str, Type[T], Exception]]:
        self._materialize_entrypoints()
        return types.MappingProxyType(self._mapping)

    def _try_load_external(self, key: str) -> Optional[Type[T]]:
        """Attempt to load *key* from an external plugin via the PluginLoader.

        This is a deliberate *fallback*, reached from ``get()`` only when *key*
        is not already registered — so entry-point-based plugins (the primary
        mechanism, materialized first) always take precedence. The manifest
        scan exists for plugins that ship a ``datahub-plugin.yaml`` without also
        declaring a packaging entry point; it is not redundant with the
        entry-point path.

        Returns the loaded class or ``None`` if no external plugin provides it.
        Raises ``ConfigurationError`` when a plugin is found but fails to import.
        """
        if self._plugin_loader is None or self._registry_type is None:
            return None
        try:
            cls = self._plugin_loader.try_load(key, self._registry_type)
        except Exception as e:
            logger.warning(
                "Plugin '%s' was found but failed to load: %s",
                key,
                e,
                exc_info=True,
            )
            raise ConfigurationError(
                f"Plugin '{key}' is installed but failed to load: {e}. "
                f"Check that all plugin dependencies are installed."
            ) from e
        if cls is None:
            return None
        try:
            self._check_cls(cls)
        except Exception as check_err:
            # The one defensible reason _check_cls fails for an otherwise-valid
            # plugin is import identity: the plugin subclasses a differently
            # imported copy of the base class, so issubclass() is False even
            # though the class is the right shape. Tolerate only that case; an
            # abstract or genuinely-wrong class is rejected loudly here rather
            # than crashing later at a more confusing call site.
            try:
                base_name: Optional[str] = self._get_registered_type().__name__
            except Exception:
                # A registry that can't report its base type (unusual) can't be
                # validated strictly — fall back to accepting the class.
                base_name = None
            mro_names = {c.__name__ for c in getattr(cls, "__mro__", [])}
            identity_ok = base_name is not None and base_name in mro_names
            if not inspect.isabstract(cls) and (identity_ok or base_name is None):
                logger.warning(
                    "External plugin '%s' class %s did not pass strict type "
                    "validation (%s); proceeding.",
                    key,
                    cls,
                    check_err,
                )
                # Cache it so we don't re-import on every lookup.
                self._register(key, cls, override=True)
                return cls
            raise ConfigurationError(
                f"Plugin '{key}' declares class {cls} which is not a valid "
                f"{base_name or 'plugin'}: {check_err}."
            ) from check_err
        self._register(key, cls, override=True)
        return cls

    def get(self, key: str) -> Type[T]:
        self._materialize_entrypoints()

        if _is_importable(key):
            # If the key contains a dot or colon, we treat it as a import path and attempt
            # to load it dynamically.
            MyClass = import_path(key)
            self._check_cls(MyClass)
            return MyClass

        if key in self._aliases:
            real_key, fn = self._aliases[key]
            fn()
            return self.get(real_key)

        if key not in self._mapping:
            ext_cls = self._try_load_external(key)
            if ext_cls is not None:
                return ext_cls
            msg = (
                f"Did not find a registered class for {key}.\n"
                f"  Built-in: pip install '{__package_name__}[{key}]'"
            )
            # Only registries wired to the external plugin loader (source/sink/
            # transformer) can be satisfied by a community plugin — don't point
            # classifier/token-provider/etc. users at `datahub plugin search`.
            if self._plugin_loader is not None:
                msg += f"\n  Community plugin: datahub plugin search {key}"
            raise KeyError(msg)

        tp = self._ensure_not_lazy(key)
        if isinstance(tp, ModuleNotFoundError):
            # TODO: Once we're on Python 3.11 (with PEP 678), we can use .add_note()
            # to enrich the error instead of wrapping it.
            raise ConfigurationError(
                f"{key} is disabled due to a missing dependency: {tp.name}; try running `pip install '{__package_name__}[{key}]'`"
            ) from tp
        elif isinstance(tp, Exception):
            raise ConfigurationError(
                f"{key} is disabled due to an error in initialization"
            ) from tp
        else:
            # If it's not an exception, then it's a registered type.
            return tp

    def get_optional(self, key: str) -> Optional[Type[T]]:
        try:
            return self.get(key)
        except KeyError:
            return None
        except Exception:
            logger.warning("Failed to load optional plugin '%s'", key, exc_info=True)
            return None

    def summary(
        self, verbose: bool = True, col_width: int = 15, verbose_col_width: int = 20
    ) -> str:
        self._materialize_entrypoints()

        lines = []
        for key in sorted(self._mapping.keys()):
            # We want to attempt to load all plugins before printing a summary.
            self._ensure_not_lazy(key)

            line = f"{key}"
            if not self.is_enabled(key):
                # Plugin is disabled.
                line += " " * max(1, col_width - len(key))

                details = "(disabled)"
                if verbose:
                    details += " " * max(1, verbose_col_width - len(details))
                    details += repr(self._mapping[key])
                line += details
            elif verbose:
                # Plugin is enabled.
                line += " " * max(1, col_width - len(key))
                line += self.get(key).__name__

            lines.append(line)

        # Append external plugin capabilities
        if self._plugin_loader is not None and self._registry_type is not None:
            ext = self._plugin_loader.get_all_capabilities(self._registry_type)
            for ext_key in sorted(ext.keys()):
                if ext_key not in self._mapping:
                    line = f"{ext_key}"
                    line += " " * max(1, col_width - len(ext_key))
                    line += "(external plugin)"
                    lines.append(line)

        return "\n".join(lines)
