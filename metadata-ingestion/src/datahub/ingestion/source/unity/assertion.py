from dataclasses import dataclass
from typing import Dict, List, Optional

from datahub.emitter.mce_builder import (
    datahub_guid,
    make_assertion_source,
    make_assertion_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParametersClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
)

_RESULT_TYPE = {
    "SUCCESS": AssertionResultTypeClass.SUCCESS,
    "FAILURE": AssertionResultTypeClass.FAILURE,
    "ERROR": AssertionResultTypeClass.ERROR,
    "INIT": AssertionResultTypeClass.INIT,
}
_SEVERITY = {
    "LOW": AssertionResultSeverityClass.LOW,
    "MEDIUM": AssertionResultSeverityClass.MEDIUM,
    "HIGH": AssertionResultSeverityClass.HIGH,
}


@dataclass
class StdAssertion:
    # Same shape as dbt AssertionParams / GX DataHubStdAssertion (reused, not new vocabulary).
    scope: str
    operator: str = AssertionStdOperatorClass._NATIVE_
    aggregation: str = AssertionStdAggregationClass._NATIVE_
    parameters: Optional[AssertionStdParametersClass] = None


def make_urn(key: Dict[str, str]) -> str:
    # Key includes a "surface" tag so GUIDs cannot collide across surfaces, plus the
    # dataset URN (which already encodes platform_instance/metastore/env).
    return make_assertion_urn(datahub_guid(key))


def build_custom_assertion_info(
    *,
    assertion_urn: str,
    entity_urn: str,
    category: str,
    display_name: str,
    native_type: str,
    std: StdAssertion,
    field_urns: Optional[List[str]] = None,
    logic: Optional[str] = None,
    native_parameters: Optional[Dict[str, str]] = None,
    external_url: Optional[str] = None,
    custom_properties: Optional[Dict[str, str]] = None,
) -> MetadataChangeProposalWrapper:
    fields = field_urns or []
    info = AssertionInfoClass(
        type=AssertionTypeClass.CUSTOM,
        description=display_name,  # the assertions list renders the name from description
        externalUrl=external_url,
        customProperties=custom_properties or None,
        source=make_assertion_source(),  # EXTERNAL source (same call the existing PRs use)
        customAssertion=CustomAssertionInfoClass(
            type=category,
            entity=entity_urn,
            field=fields[0] if fields else None,
            fields=fields or None,
            scope=std.scope,
            aggregation=std.aggregation,
            operator=std.operator,
            parameters=std.parameters,
            nativeType=native_type,
            nativeParameters=native_parameters,
            logic=logic,
        ),
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=info)


def build_assertion_run_event(
    *,
    assertion_urn: str,
    dataset_urn: str,
    run_id: str,
    timestamp_millis: int,
    status: str,
    warning: bool = False,
    severity: Optional[str] = None,
    actual_value: Optional[float] = None,
    row_count: Optional[int] = None,
    missing_count: Optional[int] = None,
    unexpected_count: Optional[int] = None,
    external_url: Optional[str] = None,
    error_type: Optional[str] = None,
    error_message: Optional[str] = None,
    native_results: Optional[Dict[str, str]] = None,
) -> MetadataChangeProposalWrapper:
    result_type = _RESULT_TYPE[status]
    native: Dict[str, str] = dict(native_results or {})
    # Warning policy in ONE place: a non-blocking warning stays SUCCESS + a flag.
    if warning and result_type == AssertionResultTypeClass.SUCCESS:
        native["warning"] = "true"
    if result_type == AssertionResultTypeClass.ERROR:
        # Structured AssertionResultError shape varies by version; native_results is
        # always safe and renders in the UI. Upgrade to result.error if desired later.
        if error_type:
            native["error_type"] = error_type
        if error_message:
            native["error_message"] = error_message
    run_severity = (
        _SEVERITY.get(severity)
        if (severity and result_type == AssertionResultTypeClass.FAILURE)
        else None
    )
    result = AssertionResultClass(
        type=result_type,
        severity=run_severity,
        actualAggValue=actual_value,
        rowCount=row_count,
        missingCount=missing_count,
        unexpectedCount=unexpected_count,
        externalUrl=external_url,
        nativeResults=native or None,
    )
    run_event = AssertionRunEventClass(
        timestampMillis=timestamp_millis,
        runId=run_id,
        asserteeUrn=dataset_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=result,
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event)
