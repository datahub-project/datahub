from typing import List

from datahub.ingestion.source.sap_datasphere.constants import (
    BLD_ATTRIBUTES,
    BLD_DATA_ENTITY,
    BLD_DIMENSION_SOURCES,
    BLD_FACT_SOURCES,
    BLD_KEY,
    BLD_MEASURES,
    BLD_SOURCE_MODEL,
    BLD_VARIABLES,
)
from datahub.ingestion.source.sap_datasphere.models import (
    BusinessLayer,
    dedup_preserving_order,
)


def _source_keys(sources: object) -> List[str]:
    keys: List[str] = []
    if isinstance(sources, dict):
        for v in sources.values():
            if isinstance(v, dict):
                k = (v.get(BLD_DATA_ENTITY) or {}).get(BLD_KEY)
                if isinstance(k, str) and k:
                    keys.append(k)
    return keys


def _names(block: object) -> List[str]:
    return list(block.keys()) if isinstance(block, dict) else []


def parse_business_layer(bld: dict, name: str) -> BusinessLayer:
    """Extract the star-schema pieces (fact/dimension sources, measures,
    attributes, variables) from an analytic model's ``businessLayerDefinitions``.

    This block is separate from the CSN ``query``/``elements`` the generic CSN
    walker reads, so the pieces here are what the emit path needs for
    star-schema lineage and measure/dimension classification.
    """
    model = (bld or {}).get(name) or {}
    sm = model.get(BLD_SOURCE_MODEL) or {}
    fact = _source_keys(sm.get(BLD_FACT_SOURCES))
    dims = _source_keys(sm.get(BLD_DIMENSION_SOURCES))
    return BusinessLayer(
        fact_source_keys=dedup_preserving_order(fact),
        dimension_source_keys=dedup_preserving_order(dims),
        measure_names=_names(model.get(BLD_MEASURES)),
        attribute_names=_names(model.get(BLD_ATTRIBUTES)),
        variable_names=_names(model.get(BLD_VARIABLES)),
    )
