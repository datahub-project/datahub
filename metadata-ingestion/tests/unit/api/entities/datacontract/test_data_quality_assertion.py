from datahub.api.entities.datacontract.data_quality_assertion import (
    DataQualityAssertion,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
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

    assert DataQualityAssertion.parse_obj(d).generate_mcp(
        assertion_urn, entity_urn
    ) == [
        MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=AssertionInfoClass(
                type=AssertionTypeClass.SQL,
                sqlAssertion=SqlAssertionInfoClass(
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
                ),
            ),
        )
    ]
