from unittest.mock import patch

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.governance_dq import rows_to_workunits
from datahub.ingestion.source.unity.proxy_types import TableReference
from datahub.ingestion.source.unity.source import UnityCatalogSource
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


def _mcps(rules, results, on_warning=None):
    return list(
        rows_to_workunits(
            rules, results, _resolve_ds, _resolve_field, on_warning=on_warning
        )
    )


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


def test_active_rule_emits_status_removed_false():
    # Spec §8 reactivation: a rule that flips retired->active must clear `removed`, not
    # just skip emitting Status. Since the pure core is stateless, this is the same as
    # asserting every active rule emits Status(removed=False) unconditionally.
    rules = [
        {
            "rule_id": "R6",
            "catalog": "my_catalog",
            "schema": "my_schema",
            "table": "my_table",
            "columns": "",
            "rule_name": "reactivated",
            "rule_type": "freshness",
            "operator": "NOT_NULL",
            "severity": "LOW",
            "active": True,
            "rule_version": 3,
            "rule_description": "reactivated",
            "dataset_name": "src",
            "source_format": "custom_engine",
        }
    ]
    statuses = [
        a for a in (m.aspect for m in _mcps(rules, [])) if isinstance(a, StatusClass)
    ]
    assert any(not s.removed for s in statuses)


def test_rule_row_missing_required_field_skipped_others_still_emit():
    rules = [
        {"rule_id": "BAD"},  # missing catalog/schema/table/rule_type
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
        },
    ]
    warnings: list = []
    infos = [
        a
        for a in (m.aspect for m in _mcps(rules, [], on_warning=warnings.append))
        if isinstance(a, AssertionInfoClass)
    ]
    assert len(infos) == 1
    assert len(warnings) == 1


def test_result_row_unknown_status_skipped_others_still_emit():
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
    results = [
        {
            "run_id": "run-bad",
            "rule_id": "R1",
            "status": "passed",  # not a recognized AssertionResultTypeClass value
            "executed_at_millis": 1,
        },
        {
            "run_id": "run-good",
            "rule_id": "R1",
            "status": "SUCCESS",
            "warning": False,
            "actual_value": 1.0,
            "evaluated_row_count": 1,
            "failed_row_count": 0,
            "executed_at_millis": 2,
        },
    ]
    warnings: list = []
    runs = [
        a
        for a in (m.aspect for m in _mcps(rules, results, on_warning=warnings.append))
        if isinstance(a, AssertionRunEventClass)
    ]
    assert len(runs) == 1
    assert runs[0].runId == "run-good"
    assert len(warnings) == 1


def _make_uc_source(*, include_metastore: bool) -> UnityCatalogSource:
    with patch("datahub.ingestion.source.unity.source.create_workspace_client"):
        cfg = UnityCatalogSourceConfig.model_validate(
            {
                "workspace_url": "https://x.cloud.databricks.com",
                "token": "t",
                "include_metastore": include_metastore,
            }
        )
        return UnityCatalogSource(ctx=PipelineContext(run_id="t"), config=cfg)


def test_resolve_governance_dataset_urn_includes_metastore_prefix_when_enabled():
    # Finding 1: the injected resolve_dataset_urn callable must match gen_dataset_urn's
    # own output exactly, including the metastore-id prefix, or governance assertions
    # attach to a dataset URN the connector never emits.
    src = _make_uc_source(include_metastore=True)
    src.metastore_id = "test_metastore"
    urn = src._resolve_governance_dataset_urn("my_catalog", "my_schema", "my_table")
    assert urn == src.gen_dataset_urn(
        TableReference(
            metastore="test_metastore",
            catalog="my_catalog",
            schema="my_schema",
            table="my_table",
        )
    )
    assert urn is not None and "test_metastore.my_catalog.my_schema.my_table" in urn


def test_resolve_governance_dataset_urn_omits_metastore_prefix_by_default():
    src = _make_uc_source(include_metastore=False)
    urn = src._resolve_governance_dataset_urn("my_catalog", "my_schema", "my_table")
    assert urn == src.gen_dataset_urn(
        TableReference(
            metastore=None,
            catalog="my_catalog",
            schema="my_schema",
            table="my_table",
        )
    )
    assert urn is not None and urn.endswith("my_catalog.my_schema.my_table,PROD)")
