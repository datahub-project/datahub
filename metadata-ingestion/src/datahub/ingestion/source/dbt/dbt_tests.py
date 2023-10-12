import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Union

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    DatasetAssertionInfoClass,
    DatasetAssertionScopeClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.dbt.dbt_common import DBTNode


@dataclass
class DBTTest:
    qualified_test_name: str
    column_name: Optional[str]
    kw_args: dict


@dataclass
class DBTTestResult:
    invocation_id: str

    status: str
    execution_time: datetime

    native_results: Dict[str, str]


def _get_name_for_relationship_test(kw_args: Dict[str, str]) -> Optional[str]:
    """
    Try to produce a useful string for the name of a relationship constraint.
    Return None if we fail to
    """
    destination_ref = kw_args.get("to")
    source_ref = kw_args.get("model")
    column_name = kw_args.get("column_name")
    dest_field_name = kw_args.get("field")
    if not destination_ref or not source_ref or not column_name or not dest_field_name:
        # base assertions are violated, bail early
        return None
    m = re.match(r"^ref\(\'(.*)\'\)$", destination_ref)
    if m:
        destination_table = m.group(1)
    else:
        destination_table = destination_ref
    m = re.search(r"ref\(\'(.*)\'\)", source_ref)
    if m:
        source_table = m.group(1)
    else:
        source_table = source_ref
    return f"{source_table}.{column_name} referential integrity to {destination_table}.{dest_field_name}"


@dataclass
class AssertionParams:
    scope: Union[DatasetAssertionScopeClass, str]
    operator: Union[AssertionStdOperatorClass, str]
    aggregation: Union[AssertionStdAggregationClass, str]
    parameters: Optional[Callable[[Dict[str, str]], AssertionStdParametersClass]] = None
    logic_fn: Optional[Callable[[Dict[str, str]], Optional[str]]] = None


_DBT_TEST_NAME_TO_ASSERTION_MAP: Dict[str, AssertionParams] = {
    "not_null": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "unique": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.EQUAL_TO,
        aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
        parameters=lambda _: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value="1.0",
                type=AssertionStdParameterTypeClass.NUMBER,
            )
        ),
    ),
    "accepted_values": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("values")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
    ),
    "relationships": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass._NATIVE_,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("values")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
        logic_fn=_get_name_for_relationship_test,
    ),
    "dbt_expectations.expect_column_values_to_not_be_null": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.NOT_NULL,
        aggregation=AssertionStdAggregationClass.IDENTITY,
    ),
    "dbt_expectations.expect_column_values_to_be_between": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda x: AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                value=str(x.get("min_value", "unknown")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
            maxValue=AssertionStdParameterClass(
                value=str(x.get("max_value", "unknown")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
        ),
    ),
    "dbt_expectations.expect_column_values_to_be_in_set": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda kw_args: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(kw_args.get("value_set")),
                type=AssertionStdParameterTypeClass.SET,
            ),
        ),
    ),
}


def _string_map(input_map: Dict[str, Any]) -> Dict[str, str]:
    return {k: str(v) for k, v in input_map.items()}


def make_assertion_from_test(
    extra_custom_props: Dict[str, str],
    node: "DBTNode",
    assertion_urn: str,
    upstream_urn: str,
) -> MetadataWorkUnit:
    assert node.test_info
    qualified_test_name = node.test_info.qualified_test_name
    column_name = node.test_info.column_name
    kw_args = node.test_info.kw_args

    if qualified_test_name in _DBT_TEST_NAME_TO_ASSERTION_MAP:
        assertion_params = _DBT_TEST_NAME_TO_ASSERTION_MAP[qualified_test_name]
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            customProperties=extra_custom_props,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=upstream_urn,
                scope=assertion_params.scope,
                operator=assertion_params.operator,
                fields=[mce_builder.make_schema_field_urn(upstream_urn, column_name)]
                if (
                    assertion_params.scope == DatasetAssertionScopeClass.DATASET_COLUMN
                    and column_name
                )
                else [],
                nativeType=node.name,
                aggregation=assertion_params.aggregation,
                parameters=assertion_params.parameters(kw_args)
                if assertion_params.parameters
                else None,
                logic=assertion_params.logic_fn(kw_args)
                if assertion_params.logic_fn
                else None,
                nativeParameters=_string_map(kw_args),
            ),
        )
    elif column_name:
        # no match with known test types, column-level test
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            customProperties=extra_custom_props,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=upstream_urn,
                scope=DatasetAssertionScopeClass.DATASET_COLUMN,
                operator=AssertionStdOperatorClass._NATIVE_,
                fields=[mce_builder.make_schema_field_urn(upstream_urn, column_name)],
                nativeType=node.name,
                logic=node.compiled_code or node.raw_code,
                aggregation=AssertionStdAggregationClass._NATIVE_,
                nativeParameters=_string_map(kw_args),
            ),
        )
    else:
        # no match with known test types, default to row-level test
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            customProperties=extra_custom_props,
            datasetAssertion=DatasetAssertionInfoClass(
                dataset=upstream_urn,
                scope=DatasetAssertionScopeClass.DATASET_ROWS,
                operator=AssertionStdOperatorClass._NATIVE_,
                logic=node.compiled_code or node.raw_code,
                nativeType=node.name,
                aggregation=AssertionStdAggregationClass._NATIVE_,
                nativeParameters=_string_map(kw_args),
            ),
        )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_info,
    ).as_workunit()


def make_assertion_result_from_test(
    node: "DBTNode",
    assertion_urn: str,
    upstream_urn: str,
    test_warnings_are_errors: bool,
) -> MetadataWorkUnit:
    assert node.test_result
    test_result = node.test_result

    assertionResult = AssertionRunEventClass(
        timestampMillis=int(test_result.execution_time.timestamp() * 1000.0),
        assertionUrn=assertion_urn,
        asserteeUrn=upstream_urn,
        runId=test_result.invocation_id,
        result=AssertionResultClass(
            type=AssertionResultTypeClass.SUCCESS
            if test_result.status == "pass"
            or (not test_warnings_are_errors and test_result.status == "warn")
            else AssertionResultTypeClass.FAILURE,
            nativeResults=test_result.native_results,
        ),
        status=AssertionRunStatusClass.COMPLETE,
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertionResult,
    ).as_workunit()
