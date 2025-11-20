import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Callable, Dict, List, Optional, Union

from datahub.emitter import mce_builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.dataform.dataform_api import DataformAssertion
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

# Type alias for test parameters - can contain various types of values
TestParameters = Dict[str, Union[str, int, float, bool, List[Union[str, int, float]]]]


@dataclass
class DataformTest:
    """Represents a Dataform test/assertion."""

    qualified_test_name: str
    column_name: Optional[str]
    test_type: str
    parameters: TestParameters


@dataclass
class DataformTestResult:
    """Represents the result of a Dataform test execution."""

    invocation_id: str
    status: str
    execution_time: datetime
    native_results: Dict[str, str]

    def has_success_status(self) -> bool:
        return self.status in ("pass", "success", "passed")


def _get_name_for_uniqueness_test(parameters: TestParameters) -> Optional[str]:
    """Generate a descriptive name for uniqueness tests."""
    column_name = parameters.get("column_name")
    if column_name:
        return f"{column_name} uniqueness constraint"
    return "uniqueness constraint"


def _get_name_for_not_null_test(parameters: TestParameters) -> Optional[str]:
    """Generate a descriptive name for not null tests."""
    column_name = parameters.get("column_name")
    if column_name:
        return f"{column_name} not null constraint"
    return "not null constraint"


def _get_name_for_accepted_values_test(parameters: TestParameters) -> Optional[str]:
    """Generate a descriptive name for accepted values tests."""
    column_name = parameters.get("column_name")
    values = parameters.get("values", [])
    if column_name and values:
        # Handle case where values might not be a list
        if isinstance(values, list):
            values_str = ", ".join(str(v) for v in values[:3])
            if len(values) > 3:
                values_str += "..."
        else:
            values_str = str(values)
        return f"{column_name} accepted values ({values_str})"
    return "accepted values constraint"


def _get_name_for_range_test(parameters: TestParameters) -> Optional[str]:
    """Generate a descriptive name for range tests."""
    column_name = parameters.get("column_name")
    min_value = parameters.get("min_value")
    max_value = parameters.get("max_value")

    if column_name:
        if min_value is not None and max_value is not None:
            return f"{column_name} range constraint ({min_value} to {max_value})"
        elif min_value is not None:
            return f"{column_name} minimum value constraint (>= {min_value})"
        elif max_value is not None:
            return f"{column_name} maximum value constraint (<= {max_value})"

    return "range constraint"


@dataclass
class AssertionParams:
    """Parameters for creating DataHub assertions from Dataform tests."""

    scope: Union[DatasetAssertionScopeClass, str]
    operator: Union[AssertionStdOperatorClass, str]
    aggregation: Union[AssertionStdAggregationClass, str]
    parameters: Optional[Callable[[TestParameters], AssertionStdParametersClass]] = None
    logic_fn: Optional[Callable[[TestParameters], Optional[str]]] = None


# Mapping from Dataform test types to assertion parameters
_DATAFORM_TEST_NAME_TO_ASSERTION_MAP: Dict[str, AssertionParams] = {
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
        parameters=lambda params: AssertionStdParametersClass(
            value=AssertionStdParameterClass(
                value=json.dumps(params.get("values", [])),
                type=AssertionStdParameterTypeClass.SET,
            )
        ),
    ),
    "relationships": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.IN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        logic_fn=_get_name_for_range_test,  # Reusing for relationships
    ),
    "range": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_COLUMN,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.IDENTITY,
        parameters=lambda params: AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                value=str(params.get("min_value", "")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
            maxValue=AssertionStdParameterClass(
                value=str(params.get("max_value", "")),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
        ),
        logic_fn=_get_name_for_range_test,
    ),
    "row_count": AssertionParams(
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass.BETWEEN,
        aggregation=AssertionStdAggregationClass.ROW_COUNT,
        parameters=lambda params: AssertionStdParametersClass(
            minValue=AssertionStdParameterClass(
                value=str(params.get("min_value", 0)),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
            maxValue=AssertionStdParameterClass(
                value=str(params.get("max_value", float("inf"))),
                type=AssertionStdParameterTypeClass.NUMBER,
            ),
        ),
    ),
}


def make_assertion_from_dataform_test(
    dataform_assertion: "DataformAssertion",
    dataset_urn: str,
    test_type: str,
    test_parameters: TestParameters,
) -> Optional[MetadataChangeProposalWrapper]:
    """Create a DataHub assertion from a Dataform test."""

    assertion_params = _DATAFORM_TEST_NAME_TO_ASSERTION_MAP.get(test_type)
    if not assertion_params:
        # Create a generic assertion for unknown test types
        return _make_generic_assertion(
            dataform_assertion, dataset_urn, test_type, test_parameters
        )

    try:
        # Generate assertion URN
        assertion_urn = mce_builder.make_assertion_urn(
            f"dataform-{dataform_assertion.name}-{test_type}"
        )

        # Build assertion logic description
        logic_description = dataform_assertion.description
        if assertion_params.logic_fn:
            custom_logic = assertion_params.logic_fn(test_parameters)
            if custom_logic:
                logic_description = custom_logic

        # Build parameters
        std_parameters = None
        if assertion_params.parameters:
            std_parameters = assertion_params.parameters(test_parameters)

        # Create dataset assertion info
        dataset_assertion_info = DatasetAssertionInfoClass(
            dataset=dataset_urn,
            scope=assertion_params.scope,
            operator=assertion_params.operator,
            aggregation=assertion_params.aggregation,
            parameters=std_parameters,
            nativeType=test_type,
            logic=logic_description or dataform_assertion.sql_query,
        )

        # Create assertion info
        assertion_info = AssertionInfoClass(
            type=AssertionTypeClass.DATASET,
            datasetAssertion=dataset_assertion_info,
            description=logic_description,
            customProperties={
                "dataform_test_type": test_type,
                "dataform_assertion_name": dataform_assertion.name,
                "dataform_file_path": dataform_assertion.file_path or "",
                **{f"param_{k}": str(v) for k, v in test_parameters.items()},
            },
        )

        # Create metadata change proposal
        return MetadataChangeProposalWrapper(
            entityUrn=assertion_urn,
            aspect=assertion_info,
        )

    except Exception as e:
        # Log error and return None

        logger = logging.getLogger(__name__)
        logger.warning(f"Failed to create assertion from Dataform test: {e}")
        return None


def _make_generic_assertion(
    dataform_assertion: "DataformAssertion",
    dataset_urn: str,
    test_type: str,
    test_parameters: TestParameters,
) -> MetadataChangeProposalWrapper:
    """Create a generic assertion for unknown test types."""

    assertion_urn = mce_builder.make_assertion_urn(
        f"dataform-{dataform_assertion.name}-{test_type}"
    )

    # Create a generic dataset assertion
    dataset_assertion_info = DatasetAssertionInfoClass(
        dataset=dataset_urn,
        scope=DatasetAssertionScopeClass.DATASET_ROWS,
        operator=AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        nativeType=test_type,
        logic=dataform_assertion.sql_query,
    )

    assertion_info = AssertionInfoClass(
        type=AssertionTypeClass.DATASET,
        datasetAssertion=dataset_assertion_info,
        description=dataform_assertion.description or f"Custom {test_type} test",
        customProperties={
            "dataform_test_type": test_type,
            "dataform_assertion_name": dataform_assertion.name,
            "dataform_file_path": dataform_assertion.file_path or "",
            "sql_query": dataform_assertion.sql_query or "",
            **{f"param_{k}": str(v) for k, v in test_parameters.items()},
        },
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_info,
    )


def make_assertion_result_from_dataform_test(
    dataform_assertion: "DataformAssertion",
    dataset_urn: str,
    test_result: DataformTestResult,
    test_type: str,
) -> MetadataChangeProposalWrapper:
    """Create an assertion result from a Dataform test result."""

    assertion_urn = mce_builder.make_assertion_urn(
        f"dataform-{dataform_assertion.name}-{test_type}"
    )

    # Determine result type
    if test_result.has_success_status():
        result_type = AssertionResultTypeClass.SUCCESS
    else:
        result_type = AssertionResultTypeClass.FAILURE

    # Create assertion result
    assertion_result = AssertionResultClass(
        type=result_type,
        nativeResults=test_result.native_results,
    )

    # Create assertion run event
    assertion_run_event = AssertionRunEventClass(
        timestampMillis=int(test_result.execution_time.timestamp() * 1000),
        runId=test_result.invocation_id,
        asserteeUrn=dataset_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=assertion_result,
    )

    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_run_event,
    )


def extract_dataform_test_info(
    assertion: "DataformAssertion",
) -> Optional[DataformTest]:
    """Extract test information from a Dataform assertion."""

    # This is a simplified implementation
    # In practice, you would parse the SQL query to extract test parameters

    # Try to infer test type from SQL query patterns
    sql_query = (assertion.sql_query or "").lower()

    test_type = "custom"
    parameters: TestParameters = {}
    column_name = None

    # Pattern matching for common test types
    if "is null" in sql_query:
        test_type = "not_null"
    elif "count(distinct" in sql_query and "count(*)" in sql_query:
        test_type = "unique"
    elif "not in" in sql_query or "in (" in sql_query:
        test_type = "accepted_values"
    elif "between" in sql_query or ("<" in sql_query and ">" in sql_query):
        test_type = "range"
    elif "count(*)" in sql_query:
        test_type = "row_count"

    # Try to extract column name from SQL
    column_match = re.search(r"\b(\w+)\s+is\s+null", sql_query)
    if column_match:
        column_name = column_match.group(1)

    return DataformTest(
        qualified_test_name=assertion.name,
        column_name=column_name,
        test_type=test_type,
        parameters=parameters,
    )


def parse_dataform_test_parameters(sql_query: str, test_type: str) -> TestParameters:
    """Parse test parameters from a Dataform SQL query."""

    parameters: TestParameters = {}

    if test_type == "accepted_values":
        # Extract values from IN clause
        in_match = re.search(r"in\s*\(\s*([^)]+)\s*\)", sql_query, re.IGNORECASE)
        if in_match:
            values_str = in_match.group(1)
            # Simple parsing - split by comma and clean up
            values = [v.strip().strip("'\"") for v in values_str.split(",")]
            parameters["values"] = values

    elif test_type == "range":
        # Extract min/max values from BETWEEN or comparison operators
        between_match = re.search(
            r"between\s+(\d+(?:\.\d+)?)\s+and\s+(\d+(?:\.\d+)?)",
            sql_query,
            re.IGNORECASE,
        )
        if between_match:
            parameters["min_value"] = float(between_match.group(1))
            parameters["max_value"] = float(between_match.group(2))
        else:
            # Try to find >= and <= patterns
            min_match = re.search(r">=\s*(\d+(?:\.\d+)?)", sql_query)
            max_match = re.search(r"<=\s*(\d+(?:\.\d+)?)", sql_query)
            if min_match:
                parameters["min_value"] = float(min_match.group(1))
            if max_match:
                parameters["max_value"] = float(max_match.group(1))

    elif test_type == "row_count":
        # Extract row count thresholds
        count_match = re.search(
            r"count\(\*\)\s*([<>=]+)\s*(\d+)", sql_query, re.IGNORECASE
        )
        if count_match:
            operator = count_match.group(1)
            value = int(count_match.group(2))
            if ">=" in operator:
                parameters["min_value"] = value
            elif "<=" in operator:
                parameters["max_value"] = value

    return parameters
