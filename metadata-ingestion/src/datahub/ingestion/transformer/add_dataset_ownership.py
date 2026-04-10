"""Deprecated dataset-only ownership transformers.

The canonical implementations now live in ``add_ownership.py``.  This
module re-exports everything for backward compatibility and provides the
deprecated ``*Dataset*`` wrappers that pin ``entity_types`` to the legacy
set (everything except container).
"""

from typing import List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.add_ownership import (  # noqa: F401 — re-exports
    AddOwnership,
    AddOwnershipConfig as AddDatasetOwnershipConfig,
    OwnershipBaseConfig as DatasetOwnershipBaseConfig,
    PatternAddOwnership,
    PatternOwnershipConfig as PatternDatasetOwnershipConfig,
    SimpleAddOwnership,
    SimpleOwnershipConfig as SimpleDatasetOwnershipConfig,
)

# Re-export AddOwnership under its legacy name so that existing code
# importing ``AddDatasetOwnership`` from this module keeps working.
AddDatasetOwnership = AddOwnership

# The legacy *_dataset_* ownership transformers already processed these
# types (everything except container) despite the "dataset" name.
_LEGACY_OWNERSHIP_ENTITY_TYPES: List[str] = [
    "dataset",
    "dataJob",
    "dataFlow",
    "chart",
    "dashboard",
]


# --- Deprecated dataset-only variants ---
# These delegate to the universal transformers with entity_types pinned
# to the legacy set for backward compatibility.


class SimpleAddDatasetOwnership(SimpleAddOwnership):
    """Deprecated: use SimpleAddOwnership instead. Legacy wrapper."""

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetOwnership":
        config_dict = {
            **config_dict,
            "entity_types": config_dict.get(
                "entity_types", _LEGACY_OWNERSHIP_ENTITY_TYPES
            ),
        }
        config = SimpleDatasetOwnershipConfig.model_validate(config_dict)
        return cls(config, ctx)


class PatternAddDatasetOwnership(PatternAddOwnership):
    """Deprecated: use PatternAddOwnership instead. Legacy wrapper."""

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetOwnership":
        config_dict = {
            **config_dict,
            "entity_types": config_dict.get(
                "entity_types", _LEGACY_OWNERSHIP_ENTITY_TYPES
            ),
        }
        config = PatternDatasetOwnershipConfig.model_validate(config_dict)
        return cls(config, ctx)
