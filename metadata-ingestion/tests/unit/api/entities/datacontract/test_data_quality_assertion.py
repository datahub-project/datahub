from datahub.api.entities.datacontract.data_quality_assertion import (
    DataQualityAssertion,
)
from datahub.emitter.mce_builder import SYSTEM_ACTOR
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionSourceTypeClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    AssertionValueChangeTypeClass,
    SqlAssertionInfoClass,
    SqlAssertionTypeClass,
)


def test_parse_sql_assertion():
    assertion_urn = "urn:li:assertion:a"
    entity_urn = "urn:li:dataset:d"
    statement = "SELECT COUNT(*) FROM my_table WHERE value IS NOT NULL"

    d = {
        "type": "custom_sql",
        "sql": statement,
        "operator": {"type": "between", "min": 5, "max": 10},
    }

    mcps = DataQualityAssertion.model_validate(d).generate_mcp(
        assertion_urn, entity_urn
    )
    assert len(mcps) == 1
    mcp = mcps[0]
    assert mcp.entityUrn == assertion_urn

    aspect = mcp.aspect
    assert isinstance(aspect, AssertionInfoClass)
    assert aspect.type == AssertionTypeClass.SQL
    assert aspect.sqlAssertion == SqlAssertionInfoClass(
        type=SqlAssertionTypeClass.METRIC,
        changeType=AssertionValueChangeTypeClass.ABSOLUTE,
        entity=entity_urn,
        statement="SELECT COUNT(*) FROM my_table WHERE value IS NOT NULL",
        operator=AssertionStdOperatorClass.BETWEEN,
        parameters=AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                value="5",
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
            maxValue=AssertionStdParameterClass(
                value="10",
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
        ),
    )
    assert aspect.source is not None
    assert aspect.source.type == AssertionSourceTypeClass.EXTERNAL
    assert aspect.source.created is not None
    assert aspect.source.created.actor == SYSTEM_ACTOR
