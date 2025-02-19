from typing import List, Type

from typing_extensions import LiteralString


def get_enum_options(class_: Type[object]) -> List[LiteralString]:
    """Get the valid values for an enum in the datahub.metadata.schema_classes module."""

    return [
        value
        for name, value in vars(class_).items()
        if not callable(value) and not name.startswith("_")
    ]
