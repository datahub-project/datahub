"""Parse the SAP Datasphere Analytic Model business layer (businessLayerDefinitions).

Analytic Models are star schemas: a fact source + dimension sources (each a
possibly cross-space dataEntity), plus measures, attributes (dimensions) and
variables. This block is separate from the CSN `query`/`elements`, so the
generic CSN walker misses it. This module extracts the pieces the emit path
needs for star-schema lineage and measure/dimension classification.
"""

from dataclasses import dataclass
from typing import List

from datahub.ingestion.source.sap_datasphere.lineage import _dedup_preserving_order


@dataclass(frozen=True)
class BusinessLayer:
    fact_source_keys: List[str]
    dimension_source_keys: List[str]
    measure_names: List[str]
    attribute_names: List[str]
    variable_names: List[str]

    @property
    def upstream_keys(self) -> List[str]:
        """Fact + dimension source keys, deduped and order-preserving (facts
        first). Derived rather than stored so it can never drift from its
        inputs."""
        return _dedup_preserving_order(
            self.fact_source_keys + self.dimension_source_keys
        )


def _source_keys(sources: object) -> List[str]:
    keys: List[str] = []
    if isinstance(sources, dict):
        for v in sources.values():
            if isinstance(v, dict):
                k = (v.get("dataEntity") or {}).get("key")
                if isinstance(k, str) and k:
                    keys.append(k)
    return keys


def _names(block: object) -> List[str]:
    return list(block.keys()) if isinstance(block, dict) else []


def parse_business_layer(bld: dict, name: str) -> BusinessLayer:
    model = (bld or {}).get(name) or {}
    sm = model.get("sourceModel") or {}
    fact = _source_keys(sm.get("factSources"))
    dims = _source_keys(sm.get("dimensionSources"))
    return BusinessLayer(
        fact_source_keys=_dedup_preserving_order(fact),
        dimension_source_keys=_dedup_preserving_order(dims),
        measure_names=_names(model.get("measures")),
        attribute_names=_names(model.get("attributes")),
        variable_names=_names(model.get("variables")),
    )
