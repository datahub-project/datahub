import importlib
import inspect
from typing import Any, Callable, Dict, Generic, Optional, Tuple, Type, TypeVar, Union

import entrypoints
import typing_inspect

from datahub import __package_name__
from datahub.configuration.common import ConfigurationError

T = TypeVar("T")


def _is_importable(path: str) -> bool:
    return "." in path or ":" in path


def import_path(path: str) -> Any:
    """
    Import an item from a package, where the path is formatted as 'package.module.submodule.ClassName'
    or 'package.module.submodule:ClassName.classmethod'. The dot-based format assumes that the bit
    after the last dot is the item to be fetched. In cases where the item to be imported is embedded
    within another type, the colon-based syntax can be used to disambiguate.
    """
    assert _is_importable(path), "path must be in the appropriate format"

    if ":" in path:
        module_name, object_name = path.rsplit(":", 1)
    else:
        module_name, object_name = path.rsplit(".", 1)

    item = importlib.import_module(module_name)
    for attr in object_name.split("."):
        item = getattr(item, attr)
    return item


class PluginRegistry(Generic[T]):
    _mapping: Dict[str, Union[str, Type[T], Exception]]
    _aliases: Dict[str, Tuple[str, Callable[[], None]]]

    def __init__(
        self, extra_cls_check: Optional[Callable[[Type[T]], None]] = None
    ) -> None:
        self._mapping = {}
        self._aliases = {}
        self._extra_cls_check = extra_cls_check

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
        path = self._mapping[key]
        if not isinstance(path, str):
            return path
        try:
            plugin_class = import_path(path)
            self.register(key, plugin_class, override=True)
            return plugin_class
        except (AssertionError, ImportError) as e:
            self.register_disabled(key, e, override=True)
            return e

    def is_enabled(self, key: str) -> bool:
        tp = self._mapping[key]
        return not isinstance(tp, Exception)

    def register_from_entrypoint(self, entry_point_key: str, lazy: bool = True) -> None:
        entry_point: entrypoints.EntryPoint
        for entry_point in entrypoints.get_group_all(entry_point_key):
            name = entry_point.name

            if entry_point.object_name is None:
                path = entry_point.module_name
            else:
                path = f"{entry_point.module_name}:{entry_point.object_name}"

            self.register_lazy(name, path)
            if not lazy:
                self._ensure_not_lazy(name)

    @property
    def mapping(self) -> Dict[str, Union[str, Type[T], Exception]]:
        return self._mapping

    def get(self, key: str) -> Type[T]:
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
            raise KeyError(f"Did not find a registered class for {key}")

        tp = self._ensure_not_lazy(key)
        if isinstance(tp, ModuleNotFoundError):
            raise ConfigurationError(
                f"{key} is disabled; try running: pip install '{__package_name__}[{key}]'"
            ) from tp
        elif isinstance(tp, Exception):
            raise ConfigurationError(
                f"{key} is disabled due to an error in initialization"
            ) from tp
        else:
            # If it's not an exception, then it's a registered type.
            return tp

    def summary(
        self, verbose: bool = True, col_width: int = 15, verbose_col_width: int = 20
    ) -> str:
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

        return "\n".join(lines)
