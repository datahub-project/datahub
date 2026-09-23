from datahub.ingestion.source.unity.assertion import (
    StdAssertion,
    build_assertion_run_event,
    build_custom_assertion_info,
    make_urn,
)
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultSeverityClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    AssertionRunStatusClass,
    AssertionSourceTypeClass,
    AssertionStdAggregationClass,
    AssertionStdOperatorClass,
    AssertionTypeClass,
    DatasetAssertionScopeClass,
)

DATASET = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.my_table,PROD)"
)


def test_make_urn_is_deterministic_and_surface_scoped():
    key = {
        "surface": "governance",
        "platform": "databricks",
        "dataset": DATASET,
        "rule_id": "R1",
    }
    assert make_urn(key) == make_urn(dict(key))
    assert make_urn(key).startswith("urn:li:assertion:")
    # different surface with same remaining key -> different urn (no cross-surface collision)
    other = dict(key)
    other["surface"] = "monitor"
    assert make_urn(key) != make_urn(other)


def test_build_custom_assertion_info_multi_column_and_structured():
    field_a = f"urn:li:schemaField:({DATASET},col_a)"
    field_b = f"urn:li:schemaField:({DATASET},col_b)"
    mcp = build_custom_assertion_info(
        assertion_urn="urn:li:assertion:abc",
        entity_urn=DATASET,
        category="Databricks Governance DQ",
        display_name="pk uniqueness",
        native_type="uniqueness",
        std=StdAssertion(
            # DatasetAssertionScopeClass has no bare "DATASET" value in this schema
            # version (only DATASET_COLUMN/_ROWS/_STORAGE_SIZE/_SCHEMA/UNKNOWN); the
            # brief's literal value doesn't exist, so use the column-scoped one that
            # fits this multi-column test.
            scope=DatasetAssertionScopeClass.DATASET_COLUMN,
            operator=AssertionStdOperatorClass._NATIVE_,
            aggregation=AssertionStdAggregationClass._NATIVE_,
        ),
        field_urns=[field_a, field_b],
        logic="unique(col_a,col_b)",
        native_parameters={"columns": "col_a,col_b"},
    )
    info = mcp.aspect
    assert isinstance(info, AssertionInfoClass)
    assert info.type == AssertionTypeClass.CUSTOM
    assert info.description == "pk uniqueness"
    assert info.customAssertion.entity == DATASET
    assert info.customAssertion.fields == [field_a, field_b]
    assert info.customAssertion.field == field_a  # first, for single-field UI compat
    assert info.customAssertion.nativeType == "uniqueness"
    assert info.source.type == AssertionSourceTypeClass.EXTERNAL


def test_run_event_failure_carries_severity_and_counts():
    mcp = build_assertion_run_event(
        assertion_urn="urn:li:assertion:abc",
        dataset_urn=DATASET,
        run_id="run-1",
        timestamp_millis=1000,
        status="FAILURE",
        severity="HIGH",
        actual_value=99.5,
        row_count=1000,
        unexpected_count=5,
    )
    ev = mcp.aspect
    assert isinstance(ev, AssertionRunEventClass)
    assert ev.status == AssertionRunStatusClass.COMPLETE
    assert ev.runId == "run-1" and ev.asserteeUrn == DATASET
    assert ev.result.type == AssertionResultTypeClass.FAILURE
    assert ev.result.severity == AssertionResultSeverityClass.HIGH
    assert ev.result.rowCount == 1000 and ev.result.unexpectedCount == 5
    assert ev.result.actualAggValue == 99.5


def test_run_event_warning_stays_success_with_flag():
    mcp = build_assertion_run_event(
        assertion_urn="urn:li:assertion:abc",
        dataset_urn=DATASET,
        run_id="r",
        timestamp_millis=1,
        status="SUCCESS",
        warning=True,
    )
    assert mcp.aspect.result.type == AssertionResultTypeClass.SUCCESS
    assert mcp.aspect.result.nativeResults["warning"] == "true"
    assert mcp.aspect.result.severity is None  # severity only on FAILURE


def test_severity_ignored_on_non_failure():
    mcp = build_assertion_run_event(
        assertion_urn="urn:li:assertion:abc",
        dataset_urn=DATASET,
        run_id="r",
        timestamp_millis=1,
        status="SUCCESS",
        severity="HIGH",
    )
    # Severity passed but status != FAILURE -> gate drops it, not just an unset default.
    assert mcp.aspect.result.severity is None


def test_run_event_init_status():
    mcp = build_assertion_run_event(
        assertion_urn="urn:li:assertion:abc",
        dataset_urn=DATASET,
        run_id="r",
        timestamp_millis=1,
        status="INIT",
    )
    assert mcp.aspect.result.type == AssertionResultTypeClass.INIT


def test_run_event_error_records_type_and_message():
    mcp = build_assertion_run_event(
        assertion_urn="urn:li:assertion:abc",
        dataset_urn=DATASET,
        run_id="r",
        timestamp_millis=1,
        status="ERROR",
        error_type="QUERY_TIMEOUT",
        error_message="engine exceeded 30s",
    )
    res = mcp.aspect.result
    assert res.type == AssertionResultTypeClass.ERROR
    assert res.nativeResults["error_type"] == "QUERY_TIMEOUT"
    assert res.nativeResults["error_message"] == "engine exceeded 30s"
