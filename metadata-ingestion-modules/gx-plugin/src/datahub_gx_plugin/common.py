import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, List, Optional, Union

from sqlalchemy.engine.url import make_url

import datahub.emitter.mce_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter, EmitMode
from datahub.emitter.serialization_helper import pre_json_transform
from datahub.ingestion.source.sql.sqlalchemy_uri_mapper import (
    get_platform_from_sqlalchemy_uri,
)
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionResult,
    AssertionResultSeverity,
    AssertionResultType,
    AssertionRunEvent,
    AssertionRunStatus,
    AssertionSource,
    AssertionSourceType,
    AssertionStdAggregation,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    AssertionType,
    CustomAssertionInfo,
    DatasetAssertionScope,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import DataPlatformInstance

logger = logging.getLogger(__name__)

GE_PLATFORM_NAME = "great-expectations"


@dataclass
class DataHubStdAssertion:
    scope: Union[str, DatasetAssertionScope]
    operator: Union[str, AssertionStdOperator]
    aggregation: Union[str, AssertionStdAggregation]
    parameters: Optional[AssertionStdParameters] = None


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super().default(o)


def convert_to_string(var: Any) -> str:
    try:
        tmp = (
            str(var)
            if isinstance(var, (str, int, float))
            else json.dumps(var, cls=DecimalEncoder)
        )
    except TypeError as e:
        logger.debug(e)
        tmp = str(var)
    return tmp


def warn(msg: str) -> None:
    logger.warning(msg)


def parse_int_or_default(value, default_value=None):
    if value is None:
        return default_value
    else:
        return int(value)


def coerce_emit_mode(emit_mode: Union[str, EmitMode]) -> EmitMode:
    if isinstance(emit_mode, str):
        try:
            return EmitMode(emit_mode.upper())
        except ValueError:
            valid = ", ".join(m.name for m in EmitMode)
            raise ValueError(
                f"Invalid emit_mode '{emit_mode}'. Valid values are: {valid}"
            ) from None
    return emit_mode


def make_dataset_urn_from_sqlalchemy_uri(
    sqlalchemy_uri,
    schema_name,
    table_name,
    env,
    platform_instance=None,
    exclude_dbname=None,
    platform_alias=None,
    convert_urns_to_lowercase=False,
):
    data_platform = get_platform_from_sqlalchemy_uri(str(sqlalchemy_uri))
    url_instance = make_url(sqlalchemy_uri)

    if schema_name is None and "." in table_name:
        schema_name, table_name = table_name.split(".")[-2:]

    if data_platform in ["redshift", "postgres"]:
        schema_name = schema_name or "public"
        if url_instance.database is None:
            warn(
                f"DataHubValidationAction failed to locate database name for {data_platform}."
            )
            return None
        schema_name = (
            schema_name if exclude_dbname else f"{url_instance.database}.{schema_name}"
        )
    elif data_platform == "mssql":
        schema_name = schema_name or "dbo"
        if url_instance.database is None:
            warn(
                f"DataHubValidationAction failed to locate database name for {data_platform}."
            )
            return None
        schema_name = (
            schema_name if exclude_dbname else f"{url_instance.database}.{schema_name}"
        )
    elif data_platform in ["trino", "snowflake"]:
        if schema_name is None or url_instance.database is None:
            warn(
                "DataHubValidationAction failed to locate schema name and/or database name for {data_platform}.".format(
                    data_platform=data_platform
                )
            )
            return None
        # If data platform is snowflake, we artificially lowercase the Database name.
        # This is because DataHub also does this during ingestion.
        # Ref: https://github.com/datahub-project/datahub/blob/master/metadata-ingestion/src/datahub/ingestion/source/snowflake/snowflake_utils.py#L155
        database_name = (
            url_instance.database.lower()
            if data_platform == "snowflake"
            else url_instance.database
        )
        if database_name.endswith(f"/{schema_name}"):
            database_name = database_name[: -len(f"/{schema_name}")]
        schema_name = (
            schema_name if exclude_dbname else f"{database_name}.{schema_name}"
        )

    elif data_platform == "bigquery":
        if url_instance.host is None or url_instance.database is None:
            warn(
                "DataHubValidationAction failed to locate host and/or database name for {data_platform}. ".format(
                    data_platform=data_platform
                )
            )
            return None
        schema_name = f"{url_instance.host}.{url_instance.database}"

    schema_name = schema_name or url_instance.database
    if schema_name is None:
        warn(
            f"DataHubValidationAction failed to locate schema name for {data_platform}."
        )
        return None

    dataset_name = f"{schema_name}.{table_name}"

    if convert_urns_to_lowercase:
        dataset_name = dataset_name.lower()

    dataset_urn = builder.make_dataset_urn_with_platform_instance(
        platform=data_platform if platform_alias is None else platform_alias,
        name=dataset_name,
        platform_instance=platform_instance,
        env=env,
    )

    return dataset_urn


def map_gx_severity_to_datahub(
    severity: Optional[Any],
) -> Optional[str]:
    """Map GX FailureSeverity (critical/warning/info) to DataHub AssertionResultSeverity."""
    if severity is None:
        return None
    value = getattr(severity, "value", severity)
    if value is None:
        return None
    normalized = str(value).strip().lower()
    if normalized == "critical":
        return AssertionResultSeverity.HIGH
    if normalized == "warning":
        return AssertionResultSeverity.MEDIUM
    if normalized == "info":
        return AssertionResultSeverity.LOW
    return None


def build_assertion_info(
    expectation_type: str,
    kwargs: Dict[str, Any],
    dataset: str,
    fields: Optional[List[str]],
    expectation_suite_name: Optional[str],
    description: Optional[str] = None,
    extra_custom_properties: Optional[Dict[str, str]] = None,
) -> AssertionInfo:
    def get_min_max(kwargs, type=AssertionStdParameterType.UNKNOWN):
        return AssertionStdParameters(
            minValue=AssertionStdParameter(
                value=convert_to_string(kwargs.get("min_value")),
                type=type,
            ),
            maxValue=AssertionStdParameter(
                value=convert_to_string(kwargs.get("max_value")),
                type=type,
            ),
        )

    known_expectations: Dict[str, DataHubStdAssertion] = {
        "expect_column_min_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.MIN,
            parameters=get_min_max(kwargs),
        ),
        "expect_column_max_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.MAX,
            parameters=get_min_max(kwargs),
        ),
        "expect_column_median_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.MEDIAN,
            parameters=get_min_max(kwargs),
        ),
        "expect_column_stdev_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.STDDEV,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_column_mean_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.MEAN,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_column_unique_value_count_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.UNIQUE_COUNT,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_column_proportion_of_unique_values_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.UNIQUE_PROPOTION,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_column_sum_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.SUM,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_column_quantile_values_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation._NATIVE_,
        ),
        "expect_column_values_to_not_be_null": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.NOT_NULL,
            aggregation=AssertionStdAggregation.IDENTITY,
        ),
        "expect_column_values_to_be_in_set": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.IN,
            aggregation=AssertionStdAggregation.IDENTITY,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("value_set")),
                    type=AssertionStdParameterType.SET,
                )
            ),
        ),
        "expect_column_values_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.IDENTITY,
            parameters=get_min_max(kwargs),
        ),
        "expect_column_values_to_match_regex": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.REGEX_MATCH,
            aggregation=AssertionStdAggregation.IDENTITY,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("regex")),
                    type=AssertionStdParameterType.STRING,
                )
            ),
        ),
        "expect_column_values_to_match_regex_list": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_COLUMN,
            operator=AssertionStdOperator.REGEX_MATCH,
            aggregation=AssertionStdAggregation.IDENTITY,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("regex_list")),
                    type=AssertionStdParameterType.LIST,
                )
            ),
        ),
        "expect_table_columns_to_match_ordered_list": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_SCHEMA,
            operator=AssertionStdOperator.EQUAL_TO,
            aggregation=AssertionStdAggregation.COLUMNS,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("column_list")),
                    type=AssertionStdParameterType.LIST,
                )
            ),
        ),
        "expect_table_columns_to_match_set": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_SCHEMA,
            operator=AssertionStdOperator.EQUAL_TO,
            aggregation=AssertionStdAggregation.COLUMNS,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("column_set")),
                    type=AssertionStdParameterType.SET,
                )
            ),
        ),
        "expect_table_column_count_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_SCHEMA,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.COLUMN_COUNT,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
        "expect_table_column_count_to_equal": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_SCHEMA,
            operator=AssertionStdOperator.EQUAL_TO,
            aggregation=AssertionStdAggregation.COLUMN_COUNT,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("value")),
                    type=AssertionStdParameterType.NUMBER,
                )
            ),
        ),
        "expect_column_to_exist": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_SCHEMA,
            operator=AssertionStdOperator._NATIVE_,
            aggregation=AssertionStdAggregation._NATIVE_,
        ),
        "expect_table_row_count_to_equal": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_ROWS,
            operator=AssertionStdOperator.EQUAL_TO,
            aggregation=AssertionStdAggregation.ROW_COUNT,
            parameters=AssertionStdParameters(
                value=AssertionStdParameter(
                    value=convert_to_string(kwargs.get("value")),
                    type=AssertionStdParameterType.NUMBER,
                )
            ),
        ),
        "expect_table_row_count_to_be_between": DataHubStdAssertion(
            scope=DatasetAssertionScope.DATASET_ROWS,
            operator=AssertionStdOperator.BETWEEN,
            aggregation=AssertionStdAggregation.ROW_COUNT,
            parameters=get_min_max(kwargs, AssertionStdParameterType.NUMBER),
        ),
    }

    scope: Union[str, DatasetAssertionScope] = DatasetAssertionScope.DATASET_ROWS
    aggregation: Union[str, AssertionStdAggregation] = AssertionStdAggregation._NATIVE_
    operator: Union[str, AssertionStdOperator] = AssertionStdOperator._NATIVE_
    parameters: Optional[AssertionStdParameters] = None

    if expectation_type in known_expectations.keys():
        assertion = known_expectations[expectation_type]
        scope = assertion.scope
        aggregation = assertion.aggregation
        operator = assertion.operator
        parameters = assertion.parameters
    elif "column" in kwargs and expectation_type.startswith("expect_column_value"):
        scope = DatasetAssertionScope.DATASET_COLUMN
        aggregation = AssertionStdAggregation.IDENTITY
    elif "column" in kwargs:
        scope = DatasetAssertionScope.DATASET_COLUMN
        aggregation = AssertionStdAggregation._NATIVE_

    customAssertionInfo = CustomAssertionInfo(
        type="greatExpectations",
        entity=dataset,
        field=fields[0] if fields else None,
        fields=fields or None,
        scope=scope,
        aggregation=aggregation,
        operator=operator,
        parameters=parameters,
        nativeType=expectation_type,
        nativeParameters={k: convert_to_string(v) for k, v in kwargs.items()},
    )

    custom_properties: Dict[str, str] = {
        "expectation_suite_name": expectation_suite_name or "",
    }
    if extra_custom_properties:
        custom_properties.update(
            {k: v for k, v in extra_custom_properties.items() if k and v is not None}
        )

    return AssertionInfo(
        type=AssertionType.CUSTOM,
        customAssertion=customAssertionInfo,
        customProperties=custom_properties,
        description=description,
        source=AssertionSource(type=AssertionSourceType.EXTERNAL),
    )


def docs_link_from_legacy_payload(payload: Optional[Any]) -> Optional[str]:
    if not payload:
        return None
    docs_link = None
    for action_names in payload.keys():
        if payload[action_names]["class"] == "UpdateDataDocsAction":
            data_docs_pages = payload[action_names]
            for docs_link_key, docs_link_val in data_docs_pages.items():
                if "file://" not in docs_link_val and docs_link_key != "class":
                    docs_link = docs_link_val
    return docs_link


def get_expectation_kwargs_and_type(
    expectation_config: Dict[str, Any],
) -> tuple[Optional[str], Dict[str, Any]]:
    # GX 0.x uses expectation_type; GX 1.x uses type.
    expectation_type = expectation_config.get(
        "expectation_type"
    ) or expectation_config.get("type")
    kwargs = expectation_config.get("kwargs") or {}
    kwargs = {k: v for k, v in kwargs.items() if k != "batch_id"}
    return expectation_type, kwargs


def _normalize_expectation_result(result: Any) -> Dict[str, Any]:
    if isinstance(result, dict):
        return result
    # Read attributes rather than to_json_dict(): the latter coerces Decimal
    # observed values to float (changing emitted nativeResults).
    return {
        "success": getattr(result, "success", None),
        "expectation_config": getattr(result, "expectation_config", {}),
        "result": getattr(result, "result", {}) or {},
    }


def _suite_parameters(validation_result_suite: Any) -> Optional[Dict[str, Any]]:
    params = getattr(validation_result_suite, "suite_parameters", None)
    if params is None:
        params = getattr(validation_result_suite, "evaluation_parameters", None)
    return params or None


def _run_time_from_run_id(run_id: Any) -> datetime:
    if run_id is None:
        raise ValueError("run_id is required to build assertion results")
    if hasattr(run_id, "run_time"):
        return run_id.run_time.astimezone(timezone.utc)
    if isinstance(run_id, dict) and "run_time" in run_id:
        from dateutil import parser as date_parser

        return date_parser.isoparse(str(run_id["run_time"])).astimezone(timezone.utc)
    if isinstance(run_id, str):
        try:
            parsed = json.loads(run_id)
            if isinstance(parsed, dict) and "run_time" in parsed:
                from dateutil import parser as date_parser

                return date_parser.isoparse(str(parsed["run_time"])).astimezone(
                    timezone.utc
                )
        except Exception:
            pass
        from dateutil import parser as date_parser

        return date_parser.isoparse(run_id).astimezone(timezone.utc)
    raise ValueError(f"Unsupported run_id type: {type(run_id)}")


def _normalize_expectation_config(expectation_config: Any) -> Dict[str, Any]:
    if hasattr(expectation_config, "to_json_dict"):
        return expectation_config.to_json_dict()
    if isinstance(expectation_config, dict):
        return expectation_config
    return {
        "type": getattr(expectation_config, "type", None)
        or getattr(expectation_config, "expectation_type", None),
        "kwargs": getattr(expectation_config, "kwargs", {}) or {},
        "id": getattr(expectation_config, "id", None),
        "description": getattr(expectation_config, "description", None),
        "severity": getattr(expectation_config, "severity", None),
        "meta": getattr(expectation_config, "meta", None) or {},
    }


def _external_url_for_result(
    validation_result_suite: Any,
    docs_link: Optional[str],
) -> Optional[str]:
    # result_url is a field on ExpectationSuiteValidationResult (suite-level),
    # not on individual ExpectationValidationResult objects.
    for candidate in (
        getattr(validation_result_suite, "result_url", None),
        docs_link,
    ):
        if isinstance(candidate, str) and candidate and "file://" not in candidate:
            return candidate
    return None


def build_assertions_with_results(
    validation_result_suite: Any,
    expectation_suite_name: Optional[str],
    run_id: Any,
    datasets: List[Dict[str, Any]],
    docs_link: Optional[str] = None,
    context_properties: Optional[Dict[str, str]] = None,
) -> List[Dict[str, Any]]:
    dataPlatformInstance = DataPlatformInstance(
        platform=builder.make_data_platform_urn(GE_PLATFORM_NAME)
    )

    assertions_with_results = []
    for raw_result in validation_result_suite.results:
        result = _normalize_expectation_result(raw_result)
        expectation_config = _normalize_expectation_config(
            result.get("expectation_config")
        )
        expectation_type, kwargs = get_expectation_kwargs_and_type(expectation_config)
        if not expectation_type:
            warn("Skipping expectation result without expectation type")
            continue
        success = bool(result["success"])

        result_data = result.get("result") or {}
        if not isinstance(result_data, dict):
            result_data = dict(result_data) if result_data else {}
        assertion_datasets = [d["dataset_urn"] for d in datasets]
        if len(datasets) == 1 and "column" in kwargs:
            assertion_fields = [
                builder.make_schema_field_urn(
                    datasets[0]["dataset_urn"], kwargs["column"]
                )
            ]
        else:
            assertion_fields = None  # type:ignore

        # Be careful what fields to consider for creating assertion urn.
        # Any change in fields below would lead to a new assertion
        # FIXME - Currently, when using evaluation parameters, new assertion is
        # created when runtime resolved kwargs are different,
        # possibly for each validation run
        #
        # GX expectation `id` is intentionally NOT used for the URN: it is stable
        # within a GX store, but regenerates when a suite is recreated, which would
        # orphan historical assertion run history.
        assertionUrn = builder.make_assertion_urn(
            builder.datahub_guid(
                pre_json_transform(
                    {
                        "platform": GE_PLATFORM_NAME,
                        "nativeType": expectation_type,
                        "nativeParameters": kwargs,
                        "dataset": assertion_datasets[0],
                        "fields": assertion_fields,
                    }
                )
            )
        )
        logger.debug(
            "GE expectation_suite_name - {name}, expectation_type - {type}, Assertion URN - {urn}".format(
                name=expectation_suite_name, type=expectation_type, urn=assertionUrn
            )
        )

        extra_custom_properties: Dict[str, str] = {}
        if context_properties:
            extra_custom_properties.update(context_properties)
        expectation_id = expectation_config.get("id")
        if expectation_id:
            extra_custom_properties["expectation_id"] = str(expectation_id)

        assertionInfo: AssertionInfo = build_assertion_info(
            expectation_type,
            kwargs,
            assertion_datasets[0],
            assertion_fields,
            expectation_suite_name,
            description=expectation_config.get("description") or None,
            extra_custom_properties=extra_custom_properties or None,
        )

        run_time = _run_time_from_run_id(run_id)
        suite_params = _suite_parameters(validation_result_suite)
        evaluation_parameters = (
            {k: convert_to_string(v) for k, v in suite_params.items() if k and v}
            if suite_params
            else None
        )

        nativeResults = {
            k: convert_to_string(v)
            for k, v in result_data.items()
            if (
                k
                in [
                    "observed_value",
                    "partial_unexpected_list",
                    "partial_unexpected_counts",
                    "details",
                ]
                and v
            )
        }

        actualAggValue = (
            result_data.get("observed_value")
            if isinstance(result_data.get("observed_value"), (int, float))
            else None
        )

        result_type = (
            AssertionResultType.SUCCESS if success else AssertionResultType.FAILURE
        )
        severity = None
        if result_type == AssertionResultType.FAILURE:
            severity = map_gx_severity_to_datahub(
                expectation_config.get("severity")
                or result.get("severity")
                or getattr(raw_result, "severity", None)
            )

        ds = datasets[0]
        # https://docs.greatexpectations.io/docs/reference/expectations/result_format/
        assertionResult = AssertionRunEvent(
            timestampMillis=int(round(time.time() * 1000)),
            assertionUrn=assertionUrn,
            asserteeUrn=ds["dataset_urn"],
            runId=run_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            result=AssertionResult(
                type=result_type,
                severity=severity,
                rowCount=parse_int_or_default(result_data.get("element_count")),
                missingCount=parse_int_or_default(result_data.get("missing_count")),
                unexpectedCount=parse_int_or_default(
                    result_data.get("unexpected_count")
                ),
                actualAggValue=actualAggValue,
                externalUrl=_external_url_for_result(
                    validation_result_suite, docs_link
                ),
                nativeResults=nativeResults,
            ),
            batchSpec=ds["batchSpec"],
            status=AssertionRunStatus.COMPLETE,
            runtimeContext=evaluation_parameters,
        )
        if ds.get("partitionSpec") is not None:
            assertionResult.partitionSpec = ds.get("partitionSpec")
        assertionResults = [assertionResult]
        assertions_with_results.append(
            {
                "assertionUrn": assertionUrn,
                "assertionInfo": assertionInfo,
                "assertionPlatform": dataPlatformInstance,
                "assertionResults": assertionResults,
            }
        )
    return assertions_with_results


def build_assertion_info_mcp(
    assertion_urn: str,
    assertion_info: AssertionInfo,
) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn,
        aspect=assertion_info,
    )


def emit_assertion_results(
    emitter: DatahubRestEmitter,
    assertions: List[Dict[str, Any]],
) -> None:
    for assertion in assertions:
        logger.info("Assertion URN - {urn}".format(urn=assertion["assertionUrn"]))

        assertion_info_mcp = build_assertion_info_mcp(
            assertion["assertionUrn"], assertion["assertionInfo"]
        )
        emitter.emit_mcp(assertion_info_mcp)

        assertion_platform_mcp = MetadataChangeProposalWrapper(
            entityUrn=assertion["assertionUrn"],
            aspect=assertion["assertionPlatform"],
        )
        emitter.emit_mcp(assertion_platform_mcp)

        for assertionResult in assertion["assertionResults"]:
            dataset_assertionResult_mcp = MetadataChangeProposalWrapper(
                entityUrn=assertionResult.assertionUrn,
                aspect=assertionResult,
            )
            emitter.emit_mcp(dataset_assertionResult_mcp)
