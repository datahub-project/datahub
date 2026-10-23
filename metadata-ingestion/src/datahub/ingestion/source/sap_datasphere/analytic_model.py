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
    CSN_ARGS,
    CSN_AS,
    CSN_ASSOC_TARGET,
    CSN_COLUMNS,
    CSN_FROM,
    CSN_JOIN,
    CSN_KEY_QUERY,
    CSN_MIXIN,
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


def _from_sources(node: object) -> Dict[str, str]:
    # Maps each FROM source's name, `as` alias and implicit alias (the last
    # dotted segment) to the source object name.
    if not isinstance(node, dict):
        return {}
    ref = node.get(CSN_REF)
    if isinstance(ref, list) and ref and isinstance(ref[0], str):
        name = ref[0]
        sources = {name: name}
        alias = node.get(CSN_AS)
        if isinstance(alias, str) and alias:
            sources[alias] = name
        else:
            sources.setdefault(name.rsplit(".", 1)[-1], name)
        return sources
    sources = {}
    args = node.get(CSN_ARGS)
    if node.get(CSN_JOIN) and isinstance(args, list):
        for arg in args:
            for key, name in _from_sources(arg).items():
                sources.setdefault(key, name)
    return sources


def _mixin_targets(mixin: object) -> Dict[str, str]:
    if not isinstance(mixin, dict):
        return {}
    return {
        name: assoc[CSN_ASSOC_TARGET]
        for name, assoc in mixin.items()
        if isinstance(assoc, dict) and isinstance(assoc.get(CSN_ASSOC_TARGET), str)
    }


def extract_projection_source_columns(csn_def: dict) -> Dict[str, SourceColumnRef]:
    # Analytic-model elements omit types, so map each output to its source column.
    # Calculated columns (an expression, or a `$projection` self-reference) have
    # no single source and are skipped.
    query = csn_def.get(CSN_KEY_QUERY)
    if not isinstance(query, dict):
        return {}
    select = query.get(CSN_SELECT)
    if not isinstance(select, dict):
        return {}
    columns = select.get(CSN_COLUMNS)
    if not isinstance(columns, list):
        return {}
    from_sources = _from_sources(select.get(CSN_FROM))
    distinct_sources = set(from_sources.values())
    single_source = next(iter(distinct_sources)) if len(distinct_sources) == 1 else None
    # A mixin association qualifies refs to its target, but unqualified refs
    # still read from FROM only.
    sources = {**_mixin_targets(select.get(CSN_MIXIN)), **from_sources}
    out: Dict[str, SourceColumnRef] = {}
    for col in columns:
        if not isinstance(col, dict):
            continue
        ref = col.get(CSN_REF)
        if not (isinstance(ref, list) and ref):
            continue
        if len(ref) == 1:
            projected_source = single_source
            column = ref[0]
        elif len(ref) == 2:
            qualifier, column = ref[0], ref[-1]
            if not isinstance(qualifier, str) or qualifier == PROJECTION_ALIAS:
                continue
            # An unknown qualifier is not a fetchable object; with no parseable
            # FROM, keep the old behaviour of treating it as the object name.
            projected_source = sources.get(qualifier) if sources else qualifier
        else:
            # Three or more segments navigate an association; CSN dotted names
            # keep `SPACE.view` in one segment, so these are never object refs.
            continue
        if not (isinstance(projected_source, str) and isinstance(column, str)):
            continue
        alias = col.get(CSN_AS)
        element = alias if isinstance(alias, str) and alias else column
        out[element] = SourceColumnRef(source_object=projected_source, column=column)
    return out
