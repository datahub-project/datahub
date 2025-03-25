import json
import logging
import time
from typing import Dict, Optional

from datahub.metadata.schema_classes import (
    AssertionDryRunEventClass,
    AssertionDryRunResultClass,
    AssertionEvaluationParametersClass,
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultErrorClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
)

from datahub_executor.common.types import (
    Assertion,
    AssertionEvaluationContext,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultType,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionType,
    RawAspect,
)

logger = logging.getLogger(__name__)


def build_assertion_run_event(
    assertion: Assertion,
    assertion_evaluation_result: AssertionEvaluationResult,
    context: Optional[AssertionEvaluationContext] = None,
) -> AssertionRunEventClass:
    event_result = build_assertion_result(
        assertion, assertion_evaluation_result, context
    )

    now_ms = int(time.time() * 1000)
    return AssertionRunEventClass(
        timestampMillis=now_ms,
        runId=f"{assertion.urn}-{str(now_ms)}",
        asserteeUrn=assertion.entity.urn,
        status=AssertionRunStatusClass.COMPLETE,
        assertionUrn=assertion.urn,
        result=event_result,
    )


def build_assertion_result(
    assertion: Assertion,
    result: AssertionEvaluationResult,
    context: Optional[AssertionEvaluationContext],
) -> AssertionResultClass:
    logger.debug(
        f"Attempting to produce Assertion Run Event for assertion run with urn {assertion.urn}. Result {result.type}"
    )

    parameters = result.parameters
    error = None
    native_results = None
    row_count = None
    if parameters is not None:
        if assertion.type == AssertionType.FRESHNESS:
            native_results = _freshness_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.VOLUME:
            native_results = _volume_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.SQL:
            native_results = _sql_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.FIELD:
            native_results = _field_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.DATA_SCHEMA:
            native_results = _schema_parameters_to_native_results(parameters)

        if "row_count" in parameters:
            row_count = parameters["row_count"]

    if result.error is not None:
        error = build_assertion_result_error(result.error)

    current_assertion_info = get_assertion_info(assertion.raw_info_aspect)
    if current_assertion_info is None:
        logger.error(
            "Failed to save assertion info in assertion result,"
            f" raw_info_aspect={assertion.raw_info_aspect}"
        )

    return AssertionResultClass(
        type=(
            AssertionResultTypeClass.SUCCESS
            if result.type == AssertionResultType.SUCCESS
            else (
                AssertionResultTypeClass.FAILURE
                if result.type == AssertionResultType.FAILURE
                else (
                    AssertionResultTypeClass.INIT
                    if result.type == AssertionResultType.INIT
                    else AssertionResultTypeClass.ERROR
                )
            )
        ),
        rowCount=row_count,
        nativeResults=native_results,
        error=error,
        assertion=current_assertion_info,
        baseAssertion=(
            get_assertion_info(context.base_assertion_info) if context else None
        ),
        parameters=get_parameters_from_context(context),
    )


def get_assertion_info(
    raw_info_aspect: Optional[RawAspect],
) -> Optional[AssertionInfoClass]:
    if raw_info_aspect:
        aspect_str = raw_info_aspect.payload
        try:
            info = AssertionInfoClass.from_obj(json.loads(aspect_str))
            return info
        except Exception as e:
            logger.error(f"Failed to map assertion info due to error {e}")

    return None


def get_assertion_std_parameters(
    params: AssertionStdParameters,
) -> AssertionStdParametersClass:
    return AssertionStdParametersClass(
        value=get_assertion_std_parameter(params.value),
        maxValue=get_assertion_std_parameter(params.max_value),
        minValue=get_assertion_std_parameter(params.min_value),
    )


def get_assertion_std_parameter(
    param: Optional[AssertionStdParameter],
) -> Optional[AssertionStdParameterClass]:
    return (
        AssertionStdParameterClass(value=param.value, type=param.type.value)
        if param
        else None
    )


def to_raw_aspect(info_aspect: AssertionInfoClass) -> RawAspect:
    return RawAspect(
        aspectName="assertionInfo", payload=json.dumps(info_aspect.to_obj())
    )


def get_parameters_from_context(
    context: Optional[AssertionEvaluationContext],
) -> Optional[AssertionEvaluationParametersClass]:
    if context and context.evaluation_spec and context.evaluation_spec.raw_parameters:
        try:
            return AssertionEvaluationParametersClass.from_obj(
                json.loads(context.evaluation_spec.raw_parameters)
            )
        except Exception as e:
            logger.error(
                f"Unable to save assertion parameters from context due to error {e}"
            )

    return None


def build_assertion_dry_run_event(
    assertion: Assertion,
    assertion_evaluation_result: AssertionEvaluationResult,
) -> AssertionDryRunEventClass:
    event_result = build_assertion_dry_run_result(
        assertion, assertion_evaluation_result
    )

    now_ms = int(time.time() * 1000)
    return AssertionDryRunEventClass(
        timestampMillis=now_ms,
        asserteeUrn=assertion.entity.urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=event_result,
    )


def build_assertion_dry_run_result(
    assertion: Assertion, result: AssertionEvaluationResult
) -> AssertionDryRunResultClass:
    logger.info(
        f"Attempting to produce Assertion Dry Run Result for assertion run with urn {assertion.urn}. Result {result.type}"
    )

    parameters = result.parameters
    error = None
    native_results = None
    row_count = None
    if parameters is not None:
        if assertion.type == AssertionType.FRESHNESS:
            native_results = _freshness_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.VOLUME:
            native_results = _volume_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.SQL:
            native_results = _sql_parameters_to_native_results(parameters)
        elif assertion.type == AssertionType.FIELD:
            native_results = _field_parameters_to_native_results(parameters)

        if "row_count" in parameters:
            row_count = parameters["row_count"]

    if result.error is not None:
        error = build_assertion_result_error(result.error)

    return AssertionDryRunResultClass(
        type=(
            AssertionResultTypeClass.SUCCESS
            if result.type == AssertionResultType.SUCCESS
            else (
                AssertionResultTypeClass.FAILURE
                if result.type == AssertionResultType.FAILURE
                else (
                    AssertionResultTypeClass.INIT
                    if result.type == AssertionResultType.INIT
                    else AssertionResultTypeClass.ERROR
                )
            )
        ),
        rowCount=row_count,
        nativeResults=native_results,
        error=error,
    )


def build_assertion_result_error(
    error: AssertionEvaluationResultError,
) -> AssertionResultErrorClass:
    return AssertionResultErrorClass(type=error.type.value, properties=error.properties)


def _freshness_parameters_to_native_results(
    parameters: dict,
) -> Optional[Dict[str, str]]:
    results = {}
    if "events" in parameters and parameters["events"] is not None:
        # Serialize the first event.
        events = parameters["events"]
        if len(events) > 0:
            # We found a matching events(s). Lets serialize them!
            events_objs = [
                ({"type": event.event_type.name, "time": event.event_time})
                for event in events
            ]
            events_str = json.dumps(events_objs, separators=(",", ":"))
            results["events"] = events_str
    if "prev_row_count" in parameters and parameters["prev_row_count"] is not None:
        results["Previous Row Count"] = parameters["prev_row_count"]
    if (
        "window_start_time" in parameters
        and parameters["window_start_time"] is not None
    ):
        results["Window Start Time"] = str(parameters["window_start_time"])
    if "window_end_time" in parameters and parameters["window_end_time"] is not None:
        results["Window End Time"] = str(parameters["window_end_time"])
    return results


def _volume_parameters_to_native_results(parameters: dict) -> Optional[Dict[str, str]]:
    results = {}
    if "prev_row_count" in parameters and parameters["prev_row_count"] is not None:
        results["Previous Row Count"] = parameters["prev_row_count"]
    return results


def _sql_parameters_to_native_results(parameters: dict) -> Optional[Dict[str, str]]:
    results = {}
    if "metric_value" in parameters and parameters["metric_value"] is not None:
        results["Value"] = parameters["metric_value"]
    if (
        "prev_metric_value" in parameters
        and parameters["prev_metric_value"] is not None
    ):
        results["Previous Value"] = parameters["prev_metric_value"]
    return results


def _field_parameters_to_native_results(parameters: dict) -> Optional[Dict[str, str]]:
    results = {}
    if "values_count" in parameters and parameters["values_count"] is not None:
        results["Invalid Rows"] = parameters["values_count"]
    if "threshold_value" in parameters and parameters["threshold_value"] is not None:
        results["Threshold Value"] = parameters["threshold_value"]
    if "metric_value" in parameters and parameters["metric_value"] is not None:
        results["Metric Value"] = parameters["metric_value"]
    if "value" in parameters and parameters["value"] is not None:
        results["Compared Value"] = parameters["value"]
    if "min_value" in parameters and parameters["min_value"] is not None:
        results["Compared Min Value"] = parameters["min_value"]
    if "max_value" in parameters and parameters["max_value"] is not None:
        results["Compared Max Value"] = parameters["max_value"]
    return results


def _schema_parameters_to_native_results(parameters: dict) -> Optional[Dict[str, str]]:
    results = {}
    if (
        "mismatched_type_fields" in parameters
        and parameters["mismatched_type_fields"] is not None
    ):
        results["Mismatched Type Fields"] = parameters["mismatched_type_fields"]
    if (
        "extra_fields_in_expected" in parameters
        and parameters["extra_fields_in_expected"] is not None
    ):
        results["Extra Fields in Expected"] = parameters["extra_fields_in_expected"]
    if (
        "extra_fields_in_actual" in parameters
        and parameters["extra_fields_in_actual"] is not None
    ):
        results["Extra Fields in Actual"] = parameters["extra_fields_in_actual"]
    return results
