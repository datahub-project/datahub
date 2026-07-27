from typing import Dict, List

from datahub.ingestion.source.sap_datasphere.constants import (
    BLD_ATTRIBUTES,
    BLD_DATA_ENTITY,
    BLD_DIMENSION_SOURCES,
    BLD_FACT_SOURCES,
    BLD_KEY,
    BLD_MEASURES,
    BLD_SOURCE_MODEL,
    BLD_VARIABLES,
    CSN_AS,
    CSN_COLUMNS,
    CSN_KEY_QUERY,
    CSN_REF,
    CSN_SELECT,
    PROJECTION_ALIAS,
)
from datahub.ingestion.source.sap_datasphere.models import (
    BusinessLayer,
    SourceColumnRef,
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
    """Extract the star-schema pieces from an analytic model's
    ``businessLayerDefinitions`` — separate from the CSN ``query``/``elements``
    the generic walker reads, and what the emit path needs for star-schema
    lineage and measure/dimension classification.
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


def extract_projection_source_columns(csn_def: dict) -> Dict[str, SourceColumnRef]:
    # Map each projected element (keyed by its `as` alias, else the source column
    # name) to the source object + column it reads from, via query.SELECT.columns.
    # Analytic-model elements carry no inline type, so this is how we find the
    # source column whose type to copy. Calculated columns (an expression, or a
    # `$projection` self-reference) have no single source and are skipped.
    query = csn_def.get(CSN_KEY_QUERY)
    if not isinstance(query, dict):
        return {}
    select = query.get(CSN_SELECT)
    if not isinstance(select, dict):
        return {}
    columns = select.get(CSN_COLUMNS)
    if not isinstance(columns, list):
        return {}
    out: Dict[str, SourceColumnRef] = {}
    for col in columns:
        if not isinstance(col, dict):
            continue
        ref = col.get(CSN_REF)
        if not (isinstance(ref, list) and len(ref) >= 2):
            continue
        source_object, column = ref[0], ref[-1]
        if not (isinstance(source_object, str) and isinstance(column, str)):
            continue
        if source_object == PROJECTION_ALIAS:
            continue
        alias = col.get(CSN_AS)
        element = alias if isinstance(alias, str) and alias else column
        out[element] = SourceColumnRef(source_object=source_object, column=column)
    return out
