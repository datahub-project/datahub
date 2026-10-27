from typing import Dict, List, Set

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
    CSN_ID,
    CSN_JOIN,
    CSN_KEY_QUERY,
    CSN_MIXIN,
    CSN_REF,
    CSN_SELECT,
    PROJECTION_ALIAS,
)
from datahub.ingestion.source.sap_datasphere.models import (
    BusinessLayer,
    FromSources,
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


def parse_from_sources(node: object) -> FromSources:
    if not isinstance(node, dict):
        return FromSources(complete=False)
    ref = node.get(CSN_REF)
    if isinstance(ref, list):
        # More than one segment navigates an association from the source.
        if len(ref) != 1:
            return FromSources(complete=False)
        # A parameterized source is `{"id": name, "args": ...}`.
        name = ref[0].get(CSN_ID) if isinstance(ref[0], dict) else ref[0]
        if not isinstance(name, str):
            return FromSources(complete=False)
        alias = node.get(CSN_AS)
        if isinstance(alias, str) and alias:
            # Once aliased, the source is only addressable by its alias.
            keys = {alias: name}
        else:
            keys = {name: name, name.rsplit(".", 1)[-1]: name}
        return FromSources(by_name=keys, objects=frozenset({name}))
    args = node.get(CSN_ARGS)
    if not (node.get(CSN_JOIN) and isinstance(args, list)):
        return FromSources(complete=False)
    by_name: Dict[str, str] = {}
    ambiguous: Set[str] = set()
    objects: Set[str] = set()
    complete = True
    for arg in args:
        part = parse_from_sources(arg)
        complete = complete and part.complete
        objects.update(part.objects)
        for key, name in part.by_name.items():
            if by_name.setdefault(key, name) != name:
                ambiguous.add(key)
    for key in ambiguous:
        del by_name[key]
    return FromSources(by_name=by_name, objects=frozenset(objects), complete=complete)


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
    from_sources = parse_from_sources(select.get(CSN_FROM))
    single_source = from_sources.single_source
    # A mixin association qualifies refs to its target, but unqualified refs
    # still read from FROM only.
    mixins = _mixin_targets(select.get(CSN_MIXIN))
    sources = {**mixins, **from_sources.by_name}
    out: Dict[str, SourceColumnRef] = {}
    for col in columns:
        if not isinstance(col, dict):
            continue
        ref = col.get(CSN_REF)
        # A segment can be a dict, e.g. a filtered association step.
        if not (isinstance(ref, list) and ref and all(isinstance(s, str) for s in ref)):
            continue
        if len(ref) == 1:
            # A bare mixin name publishes the association, not a column.
            if ref[0] in mixins:
                continue
            source_object, column = single_source, ref[0]
        elif len(ref) == 2 and ref[0] != PROJECTION_ALIAS:
            source_object, column = sources.get(ref[0]), ref[1]
        else:
            # Three or more segments navigate an association; CSN dotted names
            # keep `SPACE.view` in one segment, so these are never object refs.
            continue
        if source_object is None:
            continue
        alias = col.get(CSN_AS)
        element = alias if isinstance(alias, str) and alias else column
        out[element] = SourceColumnRef(source_object=source_object, column=column)
    return out
