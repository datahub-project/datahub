from typing import List, Type


def get_enum_options(_class: Type[object]) -> List[str]:
    """Get the valid values for an enum in the datahub.metadata.schema_classes module."""

    return [
        value
        for name, value in vars(_class).items()
        if not callable(value) and not name.startswith("_")
    ]
