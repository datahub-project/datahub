from datetime import datetime, timezone
from decimal import Decimal
from types import SimpleNamespace

from datahub.emitter.rest_emitter import EmitMode
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionResultSeverity,
    AssertionResultType,
    AssertionStdOperator,
    AssertionType,
    DatasetAssertionScope,
)
from datahub_gx_plugin.common import (
    build_assertion_info,
    build_assertions_with_results,
    coerce_emit_mode,
    convert_to_string,
    docs_link_from_legacy_payload,
    make_dataset_urn_from_sqlalchemy_uri,
    map_gx_severity_to_datahub,
)


def test_coerce_emit_mode_string_and_enum():
    assert coerce_emit_mode("async") == EmitMode.ASYNC
    assert coerce_emit_mode(EmitMode.SYNC_PRIMARY) == EmitMode.SYNC_PRIMARY


def test_coerce_emit_mode_invalid():
    import pytest

    with pytest.raises(ValueError, match="Invalid emit_mode"):
        coerce_emit_mode("not_a_mode")


def test_convert_to_string_primitives_and_list():
    assert convert_to_string("abc") == "abc"
    assert convert_to_string(12) == "12"
    assert convert_to_string([1, 2]) == "[1, 2]"


def test_docs_link_from_legacy_payload():
    payload = {
        "update_data_docs": {
            "class": "UpdateDataDocsAction",
            "local_site": "file:///tmp/docs",
            "s3_site": "https://docs.example.com/index.html",
        }
    }
    assert (
        docs_link_from_legacy_payload(payload) == "https://docs.example.com/index.html"
    )
    assert docs_link_from_legacy_payload(None) is None


def test_make_dataset_urn_from_sqlalchemy_uri_postgres():
    urn = make_dataset_urn_from_sqlalchemy_uri(
        "postgresql://localhost:5432/testdb",
        "public",
        "foo",
        "PROD",
        platform_instance="postgres1",
        convert_urns_to_lowercase=True,
    )
    assert urn is not None
    assert "testdb.public.foo" in urn
    assert "postgres" in urn


def test_map_gx_severity_to_datahub():
    assert map_gx_severity_to_datahub("critical") == AssertionResultSeverity.HIGH
    assert map_gx_severity_to_datahub("warning") == AssertionResultSeverity.MEDIUM
    assert map_gx_severity_to_datahub("info") == AssertionResultSeverity.LOW
    assert map_gx_severity_to_datahub(SimpleNamespace(value="CRITICAL")) == (
        AssertionResultSeverity.HIGH
    )
    assert map_gx_severity_to_datahub(None) is None
    assert map_gx_severity_to_datahub("unknown") is None


def test_build_assertion_info_known_expectation():
    info = build_assertion_info(
        "expect_column_values_to_not_be_null",
        {"column": "id"},
        "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)",
        [
            "urn:li:schemaField:(urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD),id)"
        ],
        "suite",
        description="id must be present",
        extra_custom_properties={
            "checkpoint_name": "cp",
            "expectation_id": "exp-1",
        },
    )
    assert info.type == AssertionType.CUSTOM
    assert info.customAssertion is not None
    assert info.datasetAssertion is None
    assert info.customAssertion.scope == DatasetAssertionScope.DATASET_COLUMN
    assert info.customAssertion.operator == AssertionStdOperator.NOT_NULL
    assert info.customAssertion.nativeType == "expect_column_values_to_not_be_null"
    assert info.description == "id must be present"
    assert info.customProperties["checkpoint_name"] == "cp"
    assert info.customProperties["expectation_id"] == "exp-1"


def test_build_assertions_with_results_gx1_type_key():
    run_id = SimpleNamespace(
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    validation_result = SimpleNamespace(
        evaluation_parameters=None,
        result_url="https://docs.example.com/result",
        results=[
            {
                "success": True,
                "expectation_config": {
                    "type": "expect_table_row_count_to_equal",
                    "kwargs": {"value": 10},
                    "id": "exp-row-count",
                    "description": "row count is 10",
                },
                "result": {"element_count": 10, "observed_value": 10},
            }
        ],
    )
    datasets = [
        {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)",
            "partitionSpec": None,
            "batchSpec": None,
        }
    ]
    assertions = build_assertions_with_results(
        validation_result,
        "suite",
        run_id,
        datasets,
        docs_link="https://docs.example.com/fallback",
        context_properties={"checkpoint_name": "orders_checkpoint"},
    )
    assert len(assertions) == 1
    assert assertions[0]["assertionUrn"].startswith("urn:li:assertion:")
    assert assertions[0]["assertionResults"][0].result.actualAggValue == 10
    assert (
        assertions[0]["assertionResults"][0].result.externalUrl
        == "https://docs.example.com/result"
    )
    assert assertions[0]["assertionInfo"].description == "row count is 10"
    assert (
        assertions[0]["assertionInfo"].customProperties["checkpoint_name"]
        == "orders_checkpoint"
    )
    assert (
        assertions[0]["assertionInfo"].customProperties["expectation_id"]
        == "exp-row-count"
    )


def test_build_assertions_with_results_maps_severity_on_failure():
    run_id = SimpleNamespace(
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    validation_result = SimpleNamespace(
        evaluation_parameters=None,
        results=[
            {
                "success": False,
                "expectation_config": {
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": "id"},
                    "severity": "critical",
                },
                "result": {"element_count": 5, "unexpected_count": 1},
            }
        ],
    )
    datasets = [
        {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:pandas,orders,PROD)",
            "partitionSpec": None,
            "batchSpec": None,
        }
    ]
    assertions = build_assertions_with_results(
        validation_result, "suite", run_id, datasets
    )
    result = assertions[0]["assertionResults"][0].result
    assert result.type == AssertionResultType.FAILURE
    assert result.severity == AssertionResultSeverity.HIGH


def test_build_assertions_with_results_gx1_objects_and_suite_parameters():
    """GX 1.x returns ExpectationValidationResult objects and suite_parameters."""
    run_id = SimpleNamespace(
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    expectation_config = SimpleNamespace(
        type="expect_column_values_to_not_be_null",
        kwargs={"column": "id"},
        to_json_dict=lambda: {
            "type": "expect_column_values_to_not_be_null",
            "kwargs": {"column": "id"},
        },
    )
    raw_result = SimpleNamespace(
        success=True,
        expectation_config=expectation_config,
        result={"element_count": 5, "unexpected_count": 0},
        to_json_dict=lambda: {
            "success": True,
            "expectation_config": {
                "type": "expect_column_values_to_not_be_null",
                "kwargs": {"column": "id"},
            },
            "result": {"element_count": 5, "unexpected_count": 0},
        },
    )
    validation_result = SimpleNamespace(
        suite_parameters={"param_a": "1"},
        evaluation_parameters=None,
        results=[raw_result],
    )
    datasets = [
        {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:pandas,orders,PROD)",
            "partitionSpec": None,
            "batchSpec": None,
        }
    ]
    assertions = build_assertions_with_results(
        validation_result, "suite_v1", run_id, datasets, docs_link=None
    )
    assert len(assertions) == 1
    run_event = assertions[0]["assertionResults"][0]
    assert run_event.runtimeContext == {"param_a": "1"}
    assert run_event.result.rowCount == 5
    assert run_event.result.unexpectedCount == 0
    assert run_event.result.severity is None


def test_build_assertions_with_results_preserves_decimal_observed_values():
    """SQL backends return Decimal observed values.

    GX's to_json_dict() coerces those to float, so we read the result
    attributes directly instead. Suite-level result_url (not per-expectation)
    feeds externalUrl.
    """
    run_id = SimpleNamespace(
        run_time=datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    )
    expectation_config = SimpleNamespace(
        to_json_dict=lambda: {
            "type": "expect_column_values_to_be_between",
            "kwargs": {"column": "score", "min_value": 0, "max_value": 8},
        },
    )
    raw_result = SimpleNamespace(
        success=False,
        expectation_config=expectation_config,
        result={
            "element_count": 5,
            "unexpected_count": 2,
            "partial_unexpected_list": [Decimal("10"), Decimal("9")],
        },
        # Mirrors GX: floats. Must not be used.
        to_json_dict=lambda: {
            "success": False,
            "expectation_config": {
                "type": "expect_column_values_to_be_between",
                "kwargs": {"column": "score", "min_value": 0, "max_value": 8},
            },
            "result": {
                "element_count": 5,
                "unexpected_count": 2,
                "partial_unexpected_list": [10.0, 9.0],
            },
        },
    )
    validation_result = SimpleNamespace(
        evaluation_parameters=None,
        result_url="https://app.example.com/validations/abc",
        results=[raw_result],
    )
    datasets = [
        {
            "dataset_urn": "urn:li:dataset:(urn:li:dataPlatform:postgres,db.public.t,PROD)",
            "partitionSpec": None,
            "batchSpec": None,
        }
    ]
    assertions = build_assertions_with_results(
        validation_result, "suite", run_id, datasets
    )
    result = assertions[0]["assertionResults"][0].result
    assert result.nativeResults["partial_unexpected_list"] == '["10", "9"]'
    assert result.externalUrl == "https://app.example.com/validations/abc"
