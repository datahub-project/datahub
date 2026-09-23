from datahub.ingestion.source.unity.assertion import (
    StdAssertion,
    build_custom_assertion_info,
    make_urn,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceTypeClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionScopeClass,
)

DATASET = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.my_table,PROD)"
)


def test_make_urn_is_deterministic_and_surface_scoped():
    key = {
        "surface": "governance",
        "platform": "databricks",
        "dataset": DATASET,
        "rule_id": "R1",
    }
    assert make_urn(key) == make_urn(dict(key))
    assert make_urn(key).startswith("urn:li:assertion:")
    # different surface with same remaining key -> different urn (no cross-surface collision)
    other = dict(key)
    other["surface"] = "monitor"
    assert make_urn(key) != make_urn(other)


def test_build_custom_assertion_info_multi_column_and_structured():
    field_a = f"urn:li:schemaField:({DATASET},col_a)"
    field_b = f"urn:li:schemaField:({DATASET},col_b)"
    mcp = build_custom_assertion_info(
        assertion_urn="urn:li:assertion:abc",
        entity_urn=DATASET,
        category="Databricks Governance DQ",
        display_name="pk uniqueness",
        native_type="uniqueness",
        std=StdAssertion(
            # DatasetAssertionScopeClass has no bare "DATASET" value in this schema
            # version (only DATASET_COLUMN/_ROWS/_STORAGE_SIZE/_SCHEMA/UNKNOWN); the
            # brief's literal value doesn't exist, so use the column-scoped one that
            # fits this multi-column test.
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass._NATIVE_,
        ),
        field_urns=[field_a, field_b],
        logic="unique(col_a,col_b)",
        native_parameters={"columns": "col_a,col_b"},
    )
    info = mcp.aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.description == "pk uniqueness"
    assert info.customAssertion.entity == DATASET
    assert info.customAssertion.fields == [field_a, field_b]
    assert info.customAssertion.field == field_a  # first, for single-field UI compat
    assert info.customAssertion.nativeType == "uniqueness"
    assert info.source.type == AssertionSourceTypeClass.EXTERNAL
