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
    AssertionResultErrorClass,
    AssertionResultErrorTypeClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
    AssertionTypeClass,
    CustomAssertionInfoClass,
    StatusClass,
)

RESULT_TYPES: Dict[str, str] = {
    "SUCCESS": AssertionResultTypeClass.SUCCESS,
    "FAILURE": AssertionResultTypeClass.FAILURE,
    "ERROR": AssertionResultTypeClass.ERROR,
    "INIT": AssertionResultTypeClass.INIT,
}
SEVERITIES: Dict[str, str] = {
    "LOW": AssertionResultSeverityClass.LOW,
    "MEDIUM": AssertionResultSeverityClass.MEDIUM,
    "HIGH": AssertionResultSeverityClass.HIGH,
}


@dataclass(frozen=True)
class StdAssertion:
    # Same shape as dbt's AssertionParams / GX's DataHubStdAssertion.
    scope: str
    operator: str = AssertionStdOperatorClass._NATIVE_
    aggregation: str = AssertionStdAggregationClass._NATIVE_
    parameters: Optional[AssertionStdParametersClass] = None


def make_external_assertion_urn(key: Dict[str, str]) -> str:
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
        # The assertions list renders its name from `description`.
        description=display_name,
        externalUrl=external_url,
        customProperties=custom_properties or {},
        source=make_assertion_source(),
        customAssertion=CustomAssertionInfoClass(
            type=category,
            entity=entity_urn,
            # `field` is deprecated; only mirror it when it is unambiguous.
            field=fields[0] if len(fields) == 1 else None,
            fields=fields or None,
            scope=std.scope,
            aggregation=std.aggregation,
            operator=std.operator,
            parameters=std.parameters,
            nativeType=native_type,
            nativeParameters=native_parameters or None,
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
    result_type = RESULT_TYPES[status]
    native: Dict[str, str] = dict(native_results or {})
    # A non-blocking warning must never fail health: keep SUCCESS and flag it.
    if warning and result_type == AssertionResultTypeClass.SUCCESS:
        native["warning"] = "true"
    error: Optional[AssertionResultErrorClass] = None
    if result_type == AssertionResultTypeClass.ERROR:
        properties = {
            k: v
            for k, v in (("error_type", error_type), ("error_message", error_message))
            if v is not None
        }
        # OSS GraphQL does not map result.error, so mirror the reason into
        # nativeResults to keep it visible there; DataHub Cloud reads result.error.
        native.update(properties)
        error = AssertionResultErrorClass(
            type=AssertionResultErrorTypeClass.UNKNOWN_ERROR,
            properties=properties or None,
        )
    result = AssertionResultClass(
        type=result_type,
        severity=(
            SEVERITIES.get(severity)
            if severity and result_type == AssertionResultTypeClass.FAILURE
            else None
        ),
        actualAggValue=actual_value,
        rowCount=row_count,
        missingCount=missing_count,
        unexpectedCount=unexpected_count,
        externalUrl=external_url,
        nativeResults=native or None,
        error=error,
    )
    run_event = AssertionRunEventClass(
        timestampMillis=timestamp_millis,
        runId=run_id,
        asserteeUrn=dataset_urn,
        assertionUrn=assertion_urn,
        status=AssertionRunStatusClass.COMPLETE,
        result=result,
        # Part of the timeseries document id in GMS (with timestamp and urn), so
        # two results for one rule in the same millisecond stay distinct, while a
        # re-emitted result overwrites its own document instead of duplicating it.
        messageId=run_id,
    )
    return MetadataChangeProposalWrapper(entityUrn=assertion_urn, aspect=run_event)


def build_status(assertion_urn: str, active: bool) -> MetadataChangeProposalWrapper:
    return MetadataChangeProposalWrapper(
        entityUrn=assertion_urn, aspect=StatusClass(removed=not active)
    )


def _num(value: float) -> AssertionStdParameterClass:
    return AssertionStdParameterClass(
        value=str(value), type=AssertionStdParameterTypeClass.NUMBER
    )


def map_operator(
    native_op: str,
    min_v: Optional[float],
    max_v: Optional[float],
    value: Optional[float],
    *,
    scope: str,
) -> StdAssertion:
    # Structured where the operator maps cleanly (mirrors dbt's test mapping);
    # everything else stays native and is described by nativeType/logic.
    op = (native_op or "").strip().upper()
    if op == "NOT_NULL":
        return StdAssertion(
            scope=scope,
            operator=AssertionStdOperatorClass.NOT_NULL,
            aggregation=AssertionStdAggregationClass.IDENTITY,
        )
    if op == "UNIQUE":
        return StdAssertion(
            scope=scope,
            operator=AssertionStdOperatorClass.EQUAL_TO,
            # UNIQUE_PROPOTION: intentional typo in the enum name, matching the
            # GraphQL schema.
            aggregation=AssertionStdAggregationClass.UNIQUE_PROPOTION,
            parameters=AssertionStdParametersClass(value=_num(1.0)),
        )
    if op == "BETWEEN" and min_v is not None and max_v is not None:
        return StdAssertion(
            scope=scope,
            operator=AssertionStdOperatorClass.BETWEEN,
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=AssertionStdParametersClass(
                minValue=_num(min_v), maxValue=_num(max_v)
            ),
        )
    if op in ("GREATER_THAN", "LESS_THAN", "EQUAL_TO") and value is not None:
        return StdAssertion(
            scope=scope,
            operator=getattr(AssertionStdOperatorClass, op),
            aggregation=AssertionStdAggregationClass.IDENTITY,
            parameters=AssertionStdParametersClass(value=_num(value)),
        )
    return StdAssertion(scope=scope)
