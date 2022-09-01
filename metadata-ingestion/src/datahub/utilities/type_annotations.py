import inspect
from typing import Optional, Type, TypeVar

import typing_inspect

TargetClass = TypeVar("TargetClass")


def get_class_from_annotation(
    derived_cls: Type, super_class: Type, target_class: Type[TargetClass]
) -> Optional[Type[TargetClass]]:
    """
    Attempts to find an instance of target_class in the type annotations of derived_class.
    We assume that super_class inherits from typing.Generic and that derived_class inherits from super_class.
    """

    # Modified from https://stackoverflow.com/q/69085037/5004662.
    for base in inspect.getmro(derived_cls):
        for generic_base in getattr(base, "__orig_bases__", []):
            generic_origin = typing_inspect.get_origin(generic_base)
            if generic_origin and issubclass(generic_origin, super_class):
                for arg in typing_inspect.get_args(generic_base):
                    if issubclass(arg, target_class):
                        return arg

    return None
