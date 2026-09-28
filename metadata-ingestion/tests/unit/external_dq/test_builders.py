from datahub.ingestion.source.external_dq.builders import (
    StdAssertion,
    build_assertion_run_event,
    build_custom_assertion_info,
    build_status,
    make_external_assertion_urn,
    map_operator,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultErrorTypeClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionScopeClass,
    StatusClass,
)

DATASET = "urn:li:dataset:(urn:li:dataPlatform:databricks,main.sales.orders,PROD)"
ASSERTION = make_external_assertion_urn({"rule_id": "r1"})


def _run_event(**kwargs: object) -> AssertionRunEventClass:
    defaults: dict = dict(
        assertion_urn=ASSERTION,
        dataset_urn=DATASET,
        run_id="run-1",
        timestamp_millis=1_700_000_000_000,
        status="SUCCESS",
    )
    defaults.update(kwargs)
    event = build_assertion_run_event(**defaults).aspect
    assert isinstance(event, AssertionRunEventClass)
    return event


def test_urn_is_deterministic_and_key_order_independent() -> None:
    assert make_external_assertion_urn({"a": "1", "b": "2"}) == make_external_assertion_urn(
        {"b": "2", "a": "1"}
    )
    assert ASSERTION.startswith("urn:li:assertion:")


def test_multi_column_rule_uses_fields_not_deprecated_field() -> None:
    fields = [f"urn:li:schemaField:({DATASET},a)", f"urn:li:schemaField:({DATASET},b)"]
    info = build_custom_assertion_info(
        assertion_urn=ASSERTION,
        entity_urn=DATASET,
        category="External Data Quality",
        display_name="a and b are unique together",
        native_type="uniqueness",
        std=StdAssertion(scope=DatasetAssertionScopeClass.DATASET_COLUMN),
        field_urns=fields,
    ).aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.description == "a and b are unique together"
    assert info.customAssertion is not None
    assert info.customAssertion.fields == fields
    assert info.customAssertion.field is None


def test_single_column_rule_also_sets_field() -> None:
    fields = [f"urn:li:schemaField:({DATASET},a)"]
    info = build_custom_assertion_info(
        assertion_urn=ASSERTION,
        entity_urn=DATASET,
        category="External Data Quality",
        display_name="a not null",
        native_type="completeness",
        std=StdAssertion(scope=DatasetAssertionScopeClass.DATASET_COLUMN),
        field_urns=fields,
    ).aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.customAssertion is not None
    assert info.customAssertion.field == fields[0]


def test_warning_stays_success_with_flag_and_no_severity() -> None:
    event = _run_event(status="SUCCESS", warning=True, severity="HIGH")
    assert event.result is not None
    assert event.result.type == AssertionResultTypeClass.SUCCESS
    assert event.result.nativeResults == {"warning": "true"}
    assert event.result.severity is None


def test_severity_applies_on_failure() -> None:
    event = _run_event(status="FAILURE", severity="MEDIUM")
    assert event.result is not None
    assert event.result.severity == AssertionResultSeverityClass.MEDIUM


def test_error_uses_structured_result_error() -> None:
    event = _run_event(status="ERROR", error_type="timeout", error_message="query timed out")
    assert event.result is not None
    assert event.result.type == AssertionResultTypeClass.ERROR
    assert event.result.error is not None
    assert event.result.error.type == AssertionResultErrorTypeClass.UNKNOWN_ERROR
    assert event.result.error.properties == {
        "error_type": "timeout",
        "error_message": "query timed out",
    }


def test_status_retires_and_reactivates() -> None:
    retired = build_status(ASSERTION, active=False).aspect
    active = build_status(ASSERTION, active=True).aspect
    assert isinstance(retired, StatusClass) and retired.removed is True
    assert isinstance(active, StatusClass) and active.removed is False


def test_map_operator_structured_and_native_fallback() -> None:
    scope = DatasetAssertionScopeClass.DATASET_ROWS
    between = map_operator("between", 1.0, 5.0, None, scope=scope)
    assert between.operator == AssertionStdOperatorClass.BETWEEN
    assert between.parameters is not None and between.parameters.minValue is not None
    assert between.parameters.minValue.value == "1.0"
    # BETWEEN without both bounds cannot be structured.
    assert map_operator("BETWEEN", 1.0, None, None, scope=scope).operator == (
        AssertionStdOperatorClass._NATIVE_
    )
    assert map_operator("regex_match", None, None, None, scope=scope).operator == (
        AssertionStdOperatorClass._NATIVE_
    )
