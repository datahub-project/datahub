import importlib
from typing import Dict, Generic, Type, TypeVar

import inspect

T = TypeVar("T")


class Registry(Generic[T]):
    def __init__(self):
        self._mapping: Dict[str, Type[T]] = {}

    def register(self, key: str, cls: Type[T]) -> None:
        if key in self._mapping:
            raise KeyError(f"key already in use - {key}")
        if key.find(".") >= 0:
            raise KeyError(f"key cannot contain '.' - {key}")
        if inspect.isabstract(cls):
            raise ValueError("cannot register an abstract type in the registry")
        self._mapping[key] = cls

    def get(self, key: str) -> Type[T]:
        if key.find(".") >= 0:
            # If the key contains a dot, we treat it as a import path and attempt
            # to load it dynamically.
            module_name, class_name = key.rsplit(".", 1)
            MyClass = getattr(importlib.import_module(module_name), class_name)
            return MyClass

        if key not in self._mapping:
            raise KeyError(f"Did not find a registered class for {key}")
        return self._mapping[key]
