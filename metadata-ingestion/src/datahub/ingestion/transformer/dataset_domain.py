"""Deprecated dataset-only domain transformers.

The canonical implementations now live in ``add_domain.py``.  This module
re-exports everything for backward compatibility and provides the
deprecated ``*Dataset*`` wrappers that pin ``entity_types`` to
dataset-only.
"""

import warnings
from typing import List

from datahub.errors import DataHubDeprecationWarning
from datahub.ingestion.transformer.add_domain import (  # noqa: F401 — re-exports
    AddDomain,
    AddDomainConfig as AddDatasetDomainSemanticsConfig,
    PatternAddDomain,
    PatternDomainConfig as PatternDatasetDomainSemanticsConfig,
    SimpleAddDomain,
    SimpleDomainConfig as SimpleDatasetDomainSemanticsConfig,
    TransformerOnConflict,
)

_DATASET_ONLY_ENTITY_TYPES: List[str] = ["dataset"]

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


class AddDatasetDomain(AddDomain):
    """Deprecated: use AddDomain instead. Dataset-only wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("AddDatasetDomain", "AddDomain")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _DATASET_ONLY_ENTITY_TYPES


class SimpleAddDatasetDomain(SimpleAddDomain):
    """Deprecated: use SimpleAddDomain instead. Dataset-only wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("SimpleAddDatasetDomain", "SimpleAddDomain")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _DATASET_ONLY_ENTITY_TYPES


class PatternAddDatasetDomain(PatternAddDomain):
    """Deprecated: use PatternAddDomain instead. Dataset-only wrapper."""

    def __init__(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        _emit_deprecation("PatternAddDatasetDomain", "PatternAddDomain")
        super().__init__(*args, **kwargs)

    def entity_types(self) -> List[str]:
        return _DATASET_ONLY_ENTITY_TYPES
