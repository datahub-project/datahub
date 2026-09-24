from datahub.ingestion.source.unity.governance_dq import rows_to_workunits
from datahub.metadata.schema_classes import (
    AssertionInfoClass,
    AssertionResultTypeClass,
    AssertionRunEventClass,
    StatusClass,
)

DATASET = (
    "urn:li:dataset:(urn:li:dataPlatform:databricks,my_catalog.my_schema.my_table,PROD)"
)


def _resolve_ds(cat, sch, tbl):
    return DATASET


def _resolve_field(ds, col):
    return f"urn:li:schemaField:({ds},{col})"


def _mcps(rules, results):
    return list(rows_to_workunits(rules, results, _resolve_ds, _resolve_field))


def test_active_rule_emits_definition_and_run_no_status_removed():
    rules = [
        {
            "rule_id": "R1",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "customer_id",
            "rule_name": "cid not null",
            "rule_type": "completeness",
            "operator": "NOT_NULL",
            "severity": "MEDIUM",
            "active": True,
            "rule_version": 1,
            "rule_description": "cid populated",
            "dataset_name": "src",
            "source_format": "custom_engine",
        }
    ]
    results = [
        {
            "run_id": "run-1",
            "rule_id": "R1",
            "status": "SUCCESS",
            "warning": False,
            "actual_value": 100.0,
            "evaluated_row_count": 1000,
            "failed_row_count": 0,
            "executed_at_millis": 1000,
        }
    ]
    aspects = [m.aspect for m in _mcps(rules, results)]
    assert any(isinstance(a, AssertionInfoClass) for a in aspects)
    assert any(isinstance(a, AssertionRunEventClass) for a in aspects)
    assert not any(isinstance(a, StatusClass) and a.removed for a in aspects)


def test_retired_rule_emits_status_removed():
    rules = [
        {
            "rule_id": "R5",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "",
            "rule_name": "retired",
            "rule_type": "freshness",
            "operator": "LESS_THAN",
            "threshold_value": 24,
            "severity": "LOW",
            "active": False,
            "rule_version": 2,
            "rule_description": "retired",
            "dataset_name": "src",
            "source_format": "custom_engine",
        }
    ]
    aspects = [m.aspect for m in _mcps(rules, [])]
    assert any(isinstance(a, StatusClass) and a.removed for a in aspects)


def test_multi_column_single_assertion_with_fields():
    rules = [
        {
            "rule_id": "R2",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "col_a,col_b",
            "rule_name": "uniq",
            "rule_type": "uniqueness",
            "operator": "UNIQUE",
            "severity": "HIGH",
            "active": True,
            "rule_version": 1,
            "rule_description": "unique",
            "dataset_name": "src",
            "source_format": "custom_engine",
        }
    ]
    info = next(
        a
        for a in (m.aspect for m in _mcps(rules, []))
        if isinstance(a, AssertionInfoClass)
    )
    assert len(info.customAssertion.fields) == 2


def test_idempotent_dedup_on_run_id():
    rules = [
        {
            "rule_id": "R1",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "customer_id",
            "rule_name": "n",
            "rule_type": "completeness",
            "operator": "NOT_NULL",
            "severity": "LOW",
            "active": True,
            "rule_version": 1,
            "rule_description": "d",
            "dataset_name": "s",
            "source_format": "custom_engine",
        }
    ]
    dup = {
        "run_id": "run-1",
        "rule_id": "R1",
        "status": "SUCCESS",
        "warning": False,
        "actual_value": 1.0,
        "evaluated_row_count": 1,
        "failed_row_count": 0,
        "executed_at_millis": 1,
    }
    runs = [
        m.aspect
        for m in _mcps(rules, [dup, dict(dup)])
        if isinstance(m.aspect, AssertionRunEventClass)
    ]
    assert len(runs) == 1


def test_error_status_maps_to_error():
    rules = [
        {
            "rule_id": "R4",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "id",
            "rule_name": "n",
            "rule_type": "completeness",
            "operator": "NOT_NULL",
            "severity": "MEDIUM",
            "active": True,
            "rule_version": 1,
            "rule_description": "d",
            "dataset_name": "s",
            "source_format": "custom_engine",
        }
    ]
    results = [
        {
            "run_id": "r",
            "rule_id": "R4",
            "status": "ERROR",
            "warning": False,
            "error_type": "QUERY_TIMEOUT",
            "error_message": "timeout",
            "executed_at_millis": 1,
        }
    ]
    run = next(
        a
        for a in (m.aspect for m in _mcps(rules, results))
        if isinstance(a, AssertionRunEventClass)
    )
    assert run.result.type == AssertionResultTypeClass.ERROR


def test_result_snapshot_fields_land_in_native_results():
    rules = [
        {
            "rule_id": "R1",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "customer_id",
            "rule_name": "cid not null",
            "rule_type": "completeness",
            "operator": "NOT_NULL",
            "severity": "MEDIUM",
            "active": True,
            "rule_version": 1,
            "rule_description": "cid populated",
            "dataset_name": "src",
            "source_format": "custom_engine",
        }
    ]
    results = [
        {
            "run_id": "run-1",
            "rule_id": "R1",
            "status": "SUCCESS",
            "warning": False,
            "actual_value": 100.0,
            "evaluated_row_count": 1000,
            "failed_row_count": 0,
            "executed_at_millis": 1000,
            "operator_snapshot": "NOT_NULL",
            "threshold_min_snapshot": 1,
            "threshold_max_snapshot": 2,
            "threshold_value_snapshot": 3,
            "rule_version_snapshot": 1,
        }
    ]
    run = next(
        a
        for a in (m.aspect for m in _mcps(rules, results))
        if isinstance(a, AssertionRunEventClass)
    )
    native = run.result.nativeResults
    assert native["operator_snapshot"] == "NOT_NULL"
    assert native["threshold_min_snapshot"] == "1"
    assert native["threshold_max_snapshot"] == "2"
    assert native["threshold_value_snapshot"] == "3"
    assert native["rule_version_snapshot"] == "1"
