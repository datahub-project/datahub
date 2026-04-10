"""Deprecated dataset-only domain transformers.

The canonical implementations now live in ``add_domain.py``.  This module
re-exports everything for backward compatibility and provides the
deprecated ``*Dataset*`` wrappers that pin ``entity_types`` to
dataset-only.
"""

from typing import List

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.add_domain import (  # noqa: F401 — re-exports
    AddDomain,
    AddDomainConfig as AddDatasetDomainSemanticsConfig,
    PatternAddDomain,
    PatternDomainConfig as PatternDatasetDomainSemanticsConfig,
    SimpleAddDomain,
    SimpleDomainConfig as SimpleDatasetDomainSemanticsConfig,
    TransformerOnConflict,
)

# Re-export AddDomain under its legacy name so that existing code
# importing ``AddDatasetDomain`` from this module keeps working.
AddDatasetDomain = AddDomain

_DATASET_ONLY_DOMAIN_ENTITY_TYPES: List[str] = [
    "dataset",
]


# --- Deprecated dataset-only variants ---
# These delegate to the universal transformers with entity_types pinned
# to dataset-only for backward compatibility.


class SimpleAddDatasetDomain(SimpleAddDomain):
    """Deprecated: use SimpleAddDomain instead. Dataset-only wrapper."""

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetDomain":
        config_dict = {
            **config_dict,
            "entity_types": config_dict.get(
                "entity_types", _DATASET_ONLY_DOMAIN_ENTITY_TYPES
            ),
        }
        config = SimpleDatasetDomainSemanticsConfig.model_validate(config_dict)
        return cls(config, ctx)


class PatternAddDatasetDomain(PatternAddDomain):
    """Deprecated: use PatternAddDomain instead. Dataset-only wrapper."""

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetDomain":
        config_dict = {
            **config_dict,
            "entity_types": config_dict.get(
                "entity_types", _DATASET_ONLY_DOMAIN_ENTITY_TYPES
            ),
        }
        config = PatternDatasetDomainSemanticsConfig.model_validate(config_dict)
        return cls(config, ctx)
