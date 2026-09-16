"""Deprecated dataset-only ownership transformers.

The canonical implementations now live in ``add_ownership.py``.  This
module re-exports everything for backward compatibility and provides the
deprecated ``*Dataset*`` wrappers that pin ``entity_types`` to the legacy
set (everything except container).
"""

import warnings
from typing import List

from datahub.errors import DataHubDeprecationWarning
from datahub.ingestion.transformer.add_ownership import (  # noqa: F401 — re-exports
    AddOwnership,
    AddOwnershipConfig as AddDatasetOwnershipConfig,
    OwnershipBaseConfig as DatasetOwnershipBaseConfig,
    PatternAddOwnership,
    PatternOwnershipConfig as PatternDatasetOwnershipConfig,
    SimpleAddOwnership,
    SimpleOwnershipConfig as SimpleDatasetOwnershipConfig,
)

# The legacy *_dataset_* ownership transformers already processed these
# types (everything except container) despite the "dataset" name.
_LEGACY_OWNERSHIP_ENTITY_TYPES: List[str] = [
    "dataset",
    "dataJob",
    "dataFlow",
    "chart",
    "dashboard",
]

_DEPRECATION_MSG = (
    "{cls} is deprecated — use {replacement} instead. "
    "The *Dataset* variants will be removed in a future release."
)


def _emit_deprecation(cls_name: str, replacement: str) -> None:
    warnings.warn(
        _DEPRECATION_MSG.format(cls=cls_name, replacement=replacement),
        DataHubDeprecationWarning,
        stacklevel=3,
    )


class AddDatasetOwnership(AddOwnership):
    """Deprecated: use AddOwnership instead. Legacy wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("AddDatasetOwnership", "AddOwnership")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _LEGACY_OWNERSHIP_ENTITY_TYPES


class SimpleAddDatasetOwnership(SimpleAddOwnership):
    """Deprecated: use SimpleAddOwnership instead. Legacy wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("SimpleAddDatasetOwnership", "SimpleAddOwnership")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _LEGACY_OWNERSHIP_ENTITY_TYPES


class PatternAddDatasetOwnership(PatternAddOwnership):
    """Deprecated: use PatternAddOwnership instead. Legacy wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("PatternAddDatasetOwnership", "PatternAddOwnership")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _LEGACY_OWNERSHIP_ENTITY_TYPES
