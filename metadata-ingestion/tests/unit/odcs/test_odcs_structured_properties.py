import json
from typing import Any, Dict, List

from datahub.ingestion.source.odcs.odcs_models import ODCSProperty, ODCSSchemaObject
from datahub.ingestion.source.odcs.odcs_structured_properties import (
    DATASET_ENTITY_TYPE,
    SCHEMA_FIELD_ENTITY_TYPE,
    dataset_property_values,
    field_property_values,
    structured_property_definition_mcps,
    structured_property_patch_mcps,
)
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass

_FIELD_URN = (
    "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD),col_a)"
)


def _prop(**kwargs: Any) -> ODCSProperty:
    return ODCSProperty.model_validate({"name": "col_a", **kwargs})


def _definitions() -> Dict[str, StructuredPropertyDefinitionClass]:
    out = {}
    for mcp in structured_property_definition_mcps():
        assert isinstance(mcp.aspect, StructuredPropertyDefinitionClass)
        out[mcp.aspect.qualifiedName] = mcp.aspect
    return out


def test_definitions_match_the_fixed_table() -> None:
    defs = _definitions()
    assert set(defs) == {
        "odcs.semanticType",
        "odcs.transformLogic",
        "odcs.synonyms",
        "odcs.enumValues",
        "odcs.vector.dimensions",
        "odcs.vector.elementType",
        "odcs.vector.distanceMetric",
        "odcs.vector.normalized",
        "odcs.vector.embeddingModel",
        "odcs.vector.embeddingModelVersion",
    }
    assert defs["odcs.vector.dimensions"].valueType == "urn:li:dataType:datahub.number"
    assert defs["odcs.synonyms"].cardinality == "MULTIPLE"
    assert defs["odcs.semanticType"].cardinality == "SINGLE"
    assert defs["odcs.synonyms"].entityTypes == [
        DATASET_ENTITY_TYPE,
        SCHEMA_FIELD_ENTITY_TYPE,
    ]
    assert [v.value for v in defs["odcs.semanticType"].allowedValues or []] == [
        "column",
        "measure",
        "dimension",
    ]
    assert defs["odcs.transformLogic"].allowedValues is None


def test_field_values_map_every_attribute() -> None:
    result = field_property_values(
        _prop(
            logicalType="vector",
            semanticType="Measure",  # case variant → canonical
            transformLogic="SUM(col_a)",
            synonyms=[{"synonym": " amount "}, "amount", "total", "  "],
            enum=[{"value": "x"}, 2, True, None, {"label": "no value"}],
            logicalTypeOptions={
                "dimensions": 3,
                "elementType": "float32",
                "distanceMetric": "DotProduct",
                "normalized": True,
                "embeddingModel": "model-a",
                "embeddingModelVersion": "1",
            },
        )
    )
    assert result.rejected == []
    assert result.values == {
        "odcs.semanticType": ["measure"],
        "odcs.transformLogic": ["SUM(col_a)"],
        "odcs.synonyms": ["amount", "total"],
        "odcs.enumValues": ["x", "2", "true"],
        "odcs.vector.dimensions": [3.0],
        "odcs.vector.elementType": ["float32"],
        "odcs.vector.distanceMetric": ["dotProduct"],
        "odcs.vector.normalized": ["true"],
        "odcs.vector.embeddingModel": ["model-a"],
        "odcs.vector.embeddingModelVersion": ["1"],
    }


def test_invalid_values_are_dropped_and_reported() -> None:
    result = field_property_values(
        _prop(
            logicalType="vector",
            semanticType="kpi",
            logicalTypeOptions={"dimensions": "3", "elementType": "float128"},
        )
    )
    assert result.values == {}
    assert sorted(result.rejected) == [
        "odcs.semanticType=kpi",
        "odcs.vector.dimensions=3",
        "odcs.vector.elementType=float128",
    ]


def test_vector_options_ignored_unless_vector_and_dict() -> None:
    assert (
        field_property_values(
            _prop(logicalType="array", logicalTypeOptions={"dimensions": 3})
        ).values
        == {}
    )
    assert (
        field_property_values(
            _prop(logicalType="vector", logicalTypeOptions="oops")
        ).values
        == {}
    )


def test_dataset_values_only_synonyms() -> None:
    schema = ODCSSchemaObject.model_validate(
        {"name": "t", "synonyms": [{"synonym": "orders"}, "purchases", "orders"]}
    )
    assert dataset_property_values(schema).values == {
        "odcs.synonyms": ["orders", "purchases"]
    }


def _ops(mcps: List[Any]) -> List[Dict[str, Any]]:
    ops: List[Dict[str, Any]] = []
    for mcp in mcps:
        assert mcp.changeType == "PATCH"
        assert mcp.aspectName == "structuredProperties"
        payload = json.loads(mcp.aspect.value)
        ops.extend(payload["patch"] if isinstance(payload, dict) else payload)
    return ops


def test_patch_sets_present_and_removes_absent_for_entity_type() -> None:
    ops = _ops(
        structured_property_patch_mcps(
            _FIELD_URN, SCHEMA_FIELD_ENTITY_TYPE, {"odcs.semanticType": ["measure"]}
        )
    )
    adds = [o for o in ops if o["op"] == "add"]
    removes = [o for o in ops if o["op"] == "remove"]
    assert len(adds) == 1 and "odcs.semanticType" in adds[0]["path"]
    assert len(removes) == 9  # every other schemaField property


def test_dataset_patch_touches_only_dataset_properties() -> None:
    urn = "urn:li:dataset:(urn:li:dataPlatform:odcs,c1.t,PROD)"
    ops = _ops(structured_property_patch_mcps(urn, DATASET_ENTITY_TYPE, {}))
    assert [o["op"] for o in ops] == ["remove"]
    assert "odcs.synonyms" in ops[0]["path"]


def test_patch_only_removes_properties_without_a_usable_definition() -> None:
    # Setting a value whose definition is missing or soft-deleted fails the whole
    # patch; removing it is always accepted and clears a leftover value.
    ops = _ops(
        structured_property_patch_mcps(
            _FIELD_URN,
            SCHEMA_FIELD_ENTITY_TYPE,
            {"odcs.semanticType": ["measure"]},
            unavailable={"odcs.semanticType"},
        )
    )
    assert [o["op"] for o in ops] == ["remove"] * 10


def test_off_spec_attribute_shapes_yield_no_values() -> None:
    result = field_property_values(_prop(semanticType=1, transformLogic={"a": 1}))
    assert result.values == {}
    assert result.rejected == ["odcs.semanticType=1"]
    schema = ODCSSchemaObject.model_validate({"name": "t", "synonyms": "orders"})
    assert dataset_property_values(schema).values == {}
