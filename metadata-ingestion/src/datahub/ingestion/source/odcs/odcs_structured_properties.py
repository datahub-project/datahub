"""ODCS v3.2 descriptive attributes as `odcs.*` structured properties (opt-in)."""

from dataclasses import dataclass, field
from typing import AbstractSet, Dict, Iterable, List, Tuple, Union

import yaml
from pydantic import JsonValue

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.odcs.odcs_models import (
    ODCSProperty,
    ODCSSchemaObject,
    ODCSSynonym,
)
from datahub.metadata.schema_classes import (
    MetadataChangeProposalClass,
    PropertyValueClass,
    StructuredPropertyDefinitionClass,
)
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)
from datahub.utilities.urns.structured_properties_urn import (
    make_structured_property_urn,
)

DATASET_ENTITY_TYPE = "urn:li:entityType:datahub.dataset"
SCHEMA_FIELD_ENTITY_TYPE = "urn:li:entityType:datahub.schemaField"

PropertyValue = Union[str, float]


@dataclass(frozen=True)
class _PropertySpec:
    qualified_name: str
    display_name: str
    value_type: str  # DataHub data type id suffix: "string" or "number"
    multiple: bool
    entity_types: Tuple[str, ...]
    allowed_values: Tuple[str, ...] = ()


_FIELD = (SCHEMA_FIELD_ENTITY_TYPE,)

# Released shapes are permanent: never change a spec's value_type or multiple,
# or remove an allowed value. Only add specs or append allowed values.
SPECS: Tuple[_PropertySpec, ...] = (
    _PropertySpec(
        "odcs.semanticType",
        "ODCS Semantic Type",
        "string",
        False,
        _FIELD,
        ("column", "measure", "dimension"),
    ),
    _PropertySpec(
        "odcs.transformLogic", "ODCS Transform Logic", "string", False, _FIELD
    ),
    _PropertySpec(
        "odcs.synonyms",
        "ODCS Synonyms",
        "string",
        True,
        (DATASET_ENTITY_TYPE, SCHEMA_FIELD_ENTITY_TYPE),
    ),
    _PropertySpec("odcs.enumValues", "ODCS Enum Values", "string", True, _FIELD),
    _PropertySpec(
        "odcs.vector.dimensions", "ODCS Vector Dimensions", "number", False, _FIELD
    ),
    _PropertySpec(
        "odcs.vector.elementType",
        "ODCS Vector Element Type",
        "string",
        False,
        _FIELD,
        ("bfloat16", "binary", "float16", "float32", "float64", "int8", "uint8"),
    ),
    _PropertySpec(
        "odcs.vector.distanceMetric",
        "ODCS Vector Distance Metric",
        "string",
        False,
        _FIELD,
        ("cosine", "dotProduct", "euclidean", "hamming", "manhattan"),
    ),
    # Structured properties have no boolean type.
    _PropertySpec(
        "odcs.vector.normalized",
        "ODCS Vector Normalized",
        "string",
        False,
        _FIELD,
        ("true", "false"),
    ),
    _PropertySpec(
        "odcs.vector.embeddingModel", "ODCS Embedding Model", "string", False, _FIELD
    ),
    _PropertySpec(
        "odcs.vector.embeddingModelVersion",
        "ODCS Embedding Model Version",
        "string",
        False,
        _FIELD,
    ),
    # Contract- and schema-level `context` (RFC 0038), serialized as YAML so the
    # instructions / verifiedStatements / constraints structure survives.
    _PropertySpec(
        "odcs.context", "ODCS AI Context", "string", False, (DATASET_ENTITY_TYPE,)
    ),
)
_SPECS_BY_NAME = {spec.qualified_name: spec for spec in SPECS}


@dataclass
class PropertyValues:
    values: Dict[str, List[PropertyValue]] = field(default_factory=dict)
    # "<qualified name>=<raw value>" for values outside the definition.
    rejected: List[str] = field(default_factory=list)


def structured_property_definition_mcps() -> List[MetadataChangeProposalWrapper]:
    return [
        MetadataChangeProposalWrapper(
            entityUrn=make_structured_property_urn(spec.qualified_name),
            aspect=StructuredPropertyDefinitionClass(
                qualifiedName=spec.qualified_name,
                displayName=spec.display_name,
                valueType=f"urn:li:dataType:datahub.{spec.value_type}",
                cardinality="MULTIPLE" if spec.multiple else "SINGLE",
                entityTypes=list(spec.entity_types),
                allowedValues=[PropertyValueClass(value=v) for v in spec.allowed_values]
                or None,
            ),
        )
        for spec in SPECS
    ]


def _clean_strings(items: Iterable[JsonValue]) -> List[str]:
    """Stringify scalars, drop empties/None/collections, de-duplicate in order."""
    out: List[str] = []
    for item in items:
        if isinstance(item, bool):
            text = "true" if item else "false"
        elif isinstance(item, (str, int, float)):
            text = str(item).strip()
        else:
            continue
        if text and text not in out:
            out.append(text)
    return out


def _put(result: PropertyValues, name: str, values: List[str]) -> None:
    spec = _SPECS_BY_NAME[name]
    if spec.allowed_values:
        canonical = {v.lower(): v for v in spec.allowed_values}
        kept: List[str] = []
        for value in values:
            match = canonical.get(value.lower())
            if match is None:
                result.rejected.append(f"{name}={value}")
            elif match not in kept:
                kept.append(match)
        values = kept
    if values:
        result.values[name] = list(values if spec.multiple else values[:1])


def _synonym_terms(
    synonyms: Union[None, JsonValue, List[Union[str, ODCSSynonym]]],
) -> List[JsonValue]:
    if not isinstance(synonyms, list):
        return []
    terms: List[JsonValue] = []
    for s in synonyms:
        term: JsonValue
        status: JsonValue
        if isinstance(s, ODCSSynonym):
            term, status = s.synonym, s.status
        elif isinstance(s, dict):
            term, status = s.get("synonym"), s.get("status")
        else:
            term, status = s, None
        # RFC 0041: a deprecated synonym is a former name, not a current one.
        if str(status or "").strip().lower() != "deprecated":
            terms.append(term)
    return terms


class _ContextDumper(yaml.SafeDumper):
    """Writes multi-line strings as `|` blocks so agents and the UI read prose."""


def _represent_str(dumper: yaml.SafeDumper, value: str) -> yaml.ScalarNode:
    style = "|" if "\n" in value else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", value, style=style)


_ContextDumper.add_representer(str, _represent_str)


def dataset_property_values(
    schema_entry: ODCSSchemaObject, contract_context: JsonValue = None
) -> PropertyValues:
    result = PropertyValues()
    _put(result, "odcs.synonyms", _clean_strings(_synonym_terms(schema_entry.synonyms)))
    context = {
        level: value
        for level, value in (
            ("contract", contract_context),
            ("schema", schema_entry.context),
        )
        if not (value is None or value in ([], {}) or str(value).strip() == "")
    }
    if context:
        result.values["odcs.context"] = [
            yaml.dump(
                context, Dumper=_ContextDumper, sort_keys=False, allow_unicode=True
            )
        ]
    return result


def field_property_values(prop: ODCSProperty) -> PropertyValues:
    result = PropertyValues()
    _put(result, "odcs.semanticType", _clean_strings([prop.semanticType]))
    _put(result, "odcs.transformLogic", _clean_strings([prop.transformLogic]))
    _put(result, "odcs.synonyms", _clean_strings(_synonym_terms(prop.synonyms)))
    enum_values = [
        e.get("value") if isinstance(e, dict) else e for e in prop.enum or []
    ]
    _put(result, "odcs.enumValues", _clean_strings(enum_values))

    options = prop.logicalTypeOptions
    if (prop.logicalType or "").strip().lower() != "vector" or not isinstance(
        options, dict
    ):
        return result
    dimensions = options.get("dimensions")
    if dimensions is not None:
        if (
            isinstance(dimensions, (int, float))
            and not isinstance(dimensions, bool)
            and dimensions > 0
            and float(dimensions).is_integer()
        ):
            result.values["odcs.vector.dimensions"] = [float(dimensions)]
        else:
            result.rejected.append(f"odcs.vector.dimensions={dimensions}")
    for key in (
        "elementType",
        "distanceMetric",
        "normalized",
        "embeddingModel",
        "embeddingModelVersion",
    ):
        _put(result, f"odcs.vector.{key}", _clean_strings([options.get(key)]))
    return result


class _StructuredPropertiesPatch(HasStructuredPropertiesPatch):
    """Entity-agnostic patch builder; only structured-property operations are used."""


def structured_property_patch_mcps(
    urn: str,
    entity_type: str,
    values: Dict[str, List[PropertyValue]],
    unavailable: AbstractSet[str] = frozenset(),
) -> List[MetadataChangeProposalClass]:
    """Set each applicable `odcs.*` property present in `values`, remove the rest.

    The `odcs.*` namespace is owned by this source, so values follow the contract;
    removing a property the entity does not have is a no-op in GMS. Properties
    outside the namespace are never touched. Properties in `unavailable` (no
    usable definition) are only removed: GMS rejects the whole patch if its result
    holds a value for a missing or soft-deleted definition.
    """
    patch = _StructuredPropertiesPatch(urn)
    for spec in SPECS:
        if entity_type not in spec.entity_types:
            continue
        if spec.qualified_name in values and spec.qualified_name not in unavailable:
            patch.set_structured_property(
                spec.qualified_name, values[spec.qualified_name]
            )
        else:
            patch.remove_structured_property(spec.qualified_name)
    return patch.build()
