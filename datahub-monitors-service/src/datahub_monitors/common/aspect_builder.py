import json
import logging
import time
from typing import Dict, Optional

from datahub.metadata.schema_classes import (
    AssertionResultClass,
    AssertionResultErrorClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
)

from datahub_monitors.types import (
    Assertion,
    AssertionEvaluationResult,
    AssertionEvaluationResultError,
    AssertionResultType,
    AssertionType,
)

logger = logging.getLogger(__name__)


def build_assertion_run_event(
    assertion: Assertion,
    assertion_evaluation_result: AssertionEvaluationResult,
) -> AssertionRunEventClass:
    event_result = build_assertion_result(assertion, assertion_evaluation_result)

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
    assertion: Assertion, result: AssertionEvaluationResult
) -> AssertionResultClass:
    logger.info(
        f"Attempting to produce Assertion Run Event for assertion run with urn {assertion.urn}. Result {result.type}"
    )  # TODO - debug

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

    return AssertionResultClass(
        type=AssertionResultTypeClass.SUCCESS
        if result.type == AssertionResultType.SUCCESS
        else AssertionResultTypeClass.FAILURE
        if result.type == AssertionResultType.FAILURE
        else AssertionResultTypeClass.INIT
        if result.type == AssertionResultType.INIT
        else AssertionResultTypeClass.ERROR,
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
    return results
