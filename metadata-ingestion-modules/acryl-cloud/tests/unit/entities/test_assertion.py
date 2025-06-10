import pathlib
from datetime import datetime
from typing import Any, Dict
from unittest.mock import MagicMock

import pytest

from acryl_datahub_cloud.sdk.entities.assertion import Assertion
from acryl_datahub_cloud.sdk.errors import SDKNotYetSupportedError
from datahub.errors import SdkUsageError
from datahub.metadata import schema_classes as models
from datahub.metadata.urns import (
    AssertionUrn,
    DataPlatformInstanceUrn,
    DataPlatformUrn,
    DatasetUrn,
    TagUrn,
)
from datahub.testing.sdk_v2_helpers import assert_entity_golden

_GOLDEN_DIR = pathlib.Path(__file__).parent / "assertion_golden"

_any_assertion_urn = "urn:li:assertion:1234"
_any_dataset_urn = (
    "urn:li:dataset:(urn:li:dataPlatform:bigquery,my_bq_project.my_dataset,DEV)"
)
_any_dataset_assertion_info = models.DatasetAssertionInfoClass(
    dataset=_any_dataset_urn,
    scope=models.DatasetAssertionScopeClass.DATASET_ROWS,
    operator=models.AssertionStdOperatorClass.GREATER_THAN,
)
_any_additional_args_for_complex_assertion: Dict[str, Any] = dict(
    description="This is a test assertion",
    external_url="https://example.com/assertion/1234",
    custom_properties={"key": "value"},
    platform="bigquery",
    platform_instance="my_instance",
    tags=[TagUrn("tag1"), TagUrn("tag2")],
    last_updated=(1234567890, "urn:li:corpuser:test_user"),
    source=models.AssertionSourceTypeClass.NATIVE,
    on_success=[models.AssertionActionTypeClass.RESOLVE_INCIDENT],
    on_failure=[models.AssertionActionTypeClass.RAISE_INCIDENT],
)


def test_assertion_basic() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
    )

    assert assertion.urn == AssertionUrn(_any_assertion_urn)
    assert isinstance(assertion.info, models.DatasetAssertionInfoClass)
    assert assertion.info.dataset == _any_dataset_urn
    assert assertion.info.scope == models.DatasetAssertionScopeClass.DATASET_ROWS
    assert assertion.info.operator == models.AssertionStdOperatorClass.GREATER_THAN
    assert assertion.description is None
    assert assertion.external_url is None
    assert assertion.custom_properties == {}
    assert assertion.platform is None
    assert assertion.platform_instance is None
    assert assertion.tags is None
    assert assertion.on_success == []
    assert assertion.on_failure == []
    assert assertion.dataset == DatasetUrn.from_string(_any_dataset_urn)

    assert_entity_golden(assertion, _GOLDEN_DIR / "test_assertion_basic_golden.json")


def test_assertion_basic_with_assertion_urn() -> None:
    assertion = Assertion(
        id=AssertionUrn(_any_assertion_urn),
        info=_any_dataset_assertion_info,
    )

    assert assertion.urn == AssertionUrn(_any_assertion_urn)

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_assertion_urn_golden.json"
    )


def test_assertion_basic_no_id() -> None:
    assertion = Assertion(
        info=_any_dataset_assertion_info,
    )

    assert assertion.urn is not None
    assert isinstance(assertion.urn, AssertionUrn)


def test_assertion_basic_with_description() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        description="This is a test assertion",
    )

    assert assertion.description == "This is a test assertion"

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_description_golden.json"
    )


def test_assertion_basic_with_external_url() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        external_url="https://example.com/assertion/1234",
    )

    assert assertion.external_url == "https://example.com/assertion/1234"

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_external_url_golden.json"
    )


def test_assertion_basic_with_custom_props() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        custom_properties={"key": "value"},
    )

    assert assertion.custom_properties == {"key": "value"}

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_custom_props_golden.json"
    )


def test_assertion_basic_with_platform() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        platform="bigquery",
    )

    assert assertion.platform == DataPlatformUrn("urn:li:dataPlatform:bigquery")
    assert assertion.platform_instance is None

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_platform_golden.json"
    )


def test_assertion_basic_with_platform_instance() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        platform="bigquery",
        platform_instance="my_instance",
    )

    assert assertion.platform == DataPlatformUrn("urn:li:dataPlatform:bigquery")
    assert assertion.platform_instance == DataPlatformInstanceUrn.from_string(
        "urn:li:dataPlatformInstance:(urn:li:dataPlatform:bigquery,my_instance)"
    )

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_basic_with_platform_instance_golden.json",
    )


def test_assertion_basic_with_tags() -> None:
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        tags=[TagUrn("tag1"), TagUrn("tag2")],
    )

    assert assertion.tags is not None and len(assertion.tags) == 2
    assert [TagUrn(t.tag).name for t in assertion.tags] == ["tag1", "tag2"]

    # Test tag add/remove flows.
    # For each loop - the second iteration should be a no-op.

    for _ in range(2):
        assertion.add_tag(TagUrn("tag3"))
        assert [TagUrn(t.tag).name for t in assertion.tags] == ["tag1", "tag2", "tag3"]
    for _ in range(2):
        assertion.remove_tag(TagUrn("tag1"))
        assert [TagUrn(t.tag).name for t in assertion.tags] == ["tag2", "tag3"]

    assert [TagUrn(t.tag).name for t in assertion.tags] == ["tag2", "tag3"]

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_basic_with_tags_golden.json",
    )


def test_assertion_basic_with_source() -> None:
    source_type = "any_source_type"
    source_type_from_enum = models.AssertionSourceTypeClass.NATIVE
    source = models.AssertionSourceClass(
        type=source_type_from_enum,
        created=models.AuditStampClass(
            time=1234567890,
            actor="urn:li:corpuser:test_user",
        ),
    )
    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        source=source_type,
    )
    assert assertion.source is not None
    assert assertion.source == models.AssertionSourceClass(
        type=source_type,
    )

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        source=source_type_from_enum,
    )
    assert assertion.source is not None
    assert assertion.source == models.AssertionSourceClass(
        type=source_type_from_enum,
    )

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        source=source,
    )
    assert assertion.source is not None
    assert assertion.source == source

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_source_golden.json"
    )


def test_assertion_basic_with_last_updated() -> None:
    dt = datetime.fromisoformat("2025-05-15 00:00:00+00:00")
    tm = int(dt.timestamp() * 1000)
    actor = "urn:li:corpuser:test_user"
    audit_stamp = models.AuditStampClass(
        time=tm,
        actor=actor,
        message="This is a test message",
        impersonator="urn:li:corpuser:impersonator_user",
    )

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        last_updated=(tm, actor),
    )
    assert assertion.last_updated is not None
    assert assertion.last_updated.time == tm
    assert assertion.last_updated.actor == actor
    assert assertion.last_updated.message is None
    assert assertion.last_updated.impersonator is None

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        last_updated=(dt, actor),
    )
    assert assertion.last_updated is not None
    assert assertion.last_updated.time == tm
    assert assertion.last_updated.actor == actor
    assert assertion.last_updated.message is None
    assert assertion.last_updated.impersonator is None

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        last_updated=audit_stamp,
    )
    assert assertion.last_updated is not None
    assert assertion.last_updated.time == tm
    assert assertion.last_updated.actor == actor
    assert assertion.last_updated.message == "This is a test message"
    assert assertion.last_updated.impersonator == "urn:li:corpuser:impersonator_user"

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_last_updated_golden.json"
    )

    with pytest.raises(SdkUsageError):
        Assertion(
            id=_any_assertion_urn,
            info=_any_dataset_assertion_info,
            last_updated=(tm, "urn:li:this-is-not-a-valid-actor-urn"),
        )

    with pytest.raises(SdkUsageError):
        Assertion(
            id=_any_assertion_urn,
            info=_any_dataset_assertion_info,
            last_updated=models.AuditStampClass(
                time=tm,
                actor="urn:li:this-is-not-a-valid-actor-urn",
            ),
        )


def test_assertion_basic_with_on_success_actions() -> None:
    action_type = "RESOLVE_INCIDENT"
    action_type_from_enum = models.AssertionActionTypeClass.RESOLVE_INCIDENT
    action = models.AssertionActionClass(
        type=action_type_from_enum,
    )

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_success=[action_type],
    )
    assert assertion.on_success is not None
    assert len(assertion.on_success) == 1
    assert assertion.on_success[0] == action

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_success=[action_type_from_enum],
    )
    assert assertion.on_success is not None
    assert len(assertion.on_success) == 1
    assert assertion.on_success[0] == action

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_success=[action],
    )
    assert assertion.on_success is not None
    assert len(assertion.on_success) == 1
    assert assertion.on_success[0] == action

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_on_success_actions.json"
    )

    with pytest.raises(SdkUsageError):
        Assertion(
            id=_any_assertion_urn,
            info=_any_dataset_assertion_info,
            on_success=["this-is-not-a-valid-action"],
        )


def test_assertion_basic_with_on_failure_actions() -> None:
    action_type = "RAISE_INCIDENT"
    action_type_from_enum = models.AssertionActionTypeClass.RAISE_INCIDENT
    action = models.AssertionActionClass(
        type=action_type_from_enum,
    )

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_failure=[action_type],
    )
    assert assertion.on_failure is not None
    assert len(assertion.on_failure) == 1
    assert assertion.on_failure[0] == action

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_failure=[action_type_from_enum],
    )
    assert assertion.on_failure is not None
    assert len(assertion.on_failure) == 1
    assert assertion.on_failure[0] == action

    assertion = Assertion(
        id=_any_assertion_urn,
        info=_any_dataset_assertion_info,
        on_failure=[action],
    )
    assert assertion.on_failure is not None
    assert len(assertion.on_failure) == 1
    assert assertion.on_failure[0] == action

    assert_entity_golden(
        assertion, _GOLDEN_DIR / "test_assertion_basic_with_on_failure_actions.json"
    )

    with pytest.raises(SdkUsageError):
        Assertion(
            id=_any_assertion_urn,
            info=_any_dataset_assertion_info,
            on_failure=["this-is-not-a-valid-action"],
        )


def test_assertion_complex_dataset_assertion() -> None:
    dataset_assertion = models.DatasetAssertionInfoClass(
        scope=models.DatasetAssertionScopeClass.DATASET_ROWS,
        dataset=_any_dataset_urn,
        operator=models.AssertionStdOperatorClass.BETWEEN,
        nativeType="expect_table_row_count_to_be_between",
        aggregation=models.AssertionStdAggregationClass.ROW_COUNT,
        parameters=models.AssertionStdParametersClass(
            maxValue=models.AssertionStdParameterClass(
                value="100", type=models.AssertionStdParameterTypeClass.NUMBER
            ),
            minValue=models.AssertionStdParameterClass(
                value="10000", type=models.AssertionStdParameterTypeClass.NUMBER
            ),
        ),
        nativeParameters={
            "max_value": "10000",
            "min_value": "10000",
        },
        logic="assertion_logic",
    )
    assertion = Assertion(
        id=_any_assertion_urn,
        info=dataset_assertion,
        **_any_additional_args_for_complex_assertion,
    )

    assert assertion.dataset == DatasetUrn.from_string(_any_dataset_urn)

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_complex_dataset_assertion_golden.json",
    )


def test_assertion_complex_freshness_assertion() -> None:
    freshness_assertion = models.FreshnessAssertionInfoClass(
        type=models.FreshnessAssertionTypeClass.DATASET_CHANGE,
        entity=_any_dataset_urn,
        schedule=models.FreshnessAssertionScheduleClass(
            type=models.FreshnessAssertionScheduleTypeClass.FIXED_INTERVAL,
            fixedInterval=models.FixedIntervalScheduleClass(
                unit=models.CalendarIntervalClass.HOUR,
                multiple=6,
            ),
        ),
        filter=models.DatasetFilterClass(
            type=models.DatasetFilterTypeClass.SQL,
            sql="SELECT * FROM my_table WHERE my_column = 'my_value'",
        ),
    )
    assertion = Assertion(
        id=_any_assertion_urn,
        info=freshness_assertion,
        **_any_additional_args_for_complex_assertion,
    )

    assert assertion.dataset == DatasetUrn.from_string(_any_dataset_urn)

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_complex_freshness_assertion_golden.json",
    )


def test_assertion_complex_volume_assertion() -> None:
    volume_assertion = models.VolumeAssertionInfoClass(
        type=models.VolumeAssertionTypeClass.ROW_COUNT_CHANGE,
        entity=_any_dataset_urn,
        rowCountChange=models.RowCountChangeClass(
            type=models.AssertionValueChangeTypeClass.ABSOLUTE,
            operator=models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            parameters=models.AssertionStdParametersClass(
                value=models.AssertionStdParameterClass(
                    value="100", type=models.AssertionStdParameterTypeClass.NUMBER
                ),
            ),
        ),
        filter=models.DatasetFilterClass(
            type=models.DatasetFilterTypeClass.SQL,
            sql="SELECT * FROM my_table WHERE my_column = 'my_value'",
        ),
    )
    assertion = Assertion(
        id=_any_assertion_urn,
        info=volume_assertion,
        **_any_additional_args_for_complex_assertion,
    )

    assert assertion.dataset == DatasetUrn.from_string(_any_dataset_urn)

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_complex_volume_assertion_golden.json",
    )


def test_assertiion_complex_sql_assertion() -> None:
    sql_assertion = models.SqlAssertionInfoClass(
        type=models.SqlAssertionTypeClass.METRIC,
        entity=_any_dataset_urn,
        statement="SELECT COUNT(*) FROM my_table",
        operator=models.AssertionStdOperatorClass.GREATER_THAN,
        parameters=models.AssertionStdParametersClass(
            value=models.AssertionStdParameterClass(
                value="100", type=models.AssertionStdParameterTypeClass.NUMBER
            ),
        ),
        changeType=models.AssertionValueChangeTypeClass.ABSOLUTE,
    )
    assertion = Assertion(
        id=_any_assertion_urn,
        info=sql_assertion,
        **_any_additional_args_for_complex_assertion,
    )

    assert assertion.dataset == DatasetUrn.from_string(_any_dataset_urn)

    assert_entity_golden(
        assertion,
        _GOLDEN_DIR / "test_assertion_complex_sql_assertion_golden.json",
    )


@pytest.mark.parametrize(
    "input_assertion,expected,expected_exception",
    [
        pytest.param(
            models.AssertionInfoClass(
                type=models.AssertionTypeClass.DATASET,
                datasetAssertion=MagicMock(spec=models.DatasetAssertionInfoClass),
            ),
            models.DatasetAssertionInfoClass,
            None,
            id="dataset_assertion",
        ),
        pytest.param(
            models.AssertionInfoClass(
                type=models.AssertionTypeClass.FRESHNESS,
                freshnessAssertion=MagicMock(spec=models.FreshnessAssertionInfoClass),
            ),
            models.FreshnessAssertionInfoClass,
            None,
            id="freshness_assertion",
        ),
        pytest.param(
            models.AssertionInfoClass(
                type=models.AssertionTypeClass.VOLUME,
                volumeAssertion=MagicMock(spec=models.VolumeAssertionInfoClass),
            ),
            models.VolumeAssertionInfoClass,
            None,
            id="volume_assertion",
        ),
        pytest.param(
            models.AssertionInfoClass(
                type=models.AssertionTypeClass.SQL,
                sqlAssertion=MagicMock(spec=models.SqlAssertionInfoClass),
            ),
            models.SqlAssertionInfoClass,
            None,
            id="sql_assertion",
        ),
        pytest.param(
            models.AssertionInfoClass(type="UNSUPPORTED_TYPE"),
            None,
            SDKNotYetSupportedError,
            id="unsupported_type",
        ),
    ],
)
def test_switch_assertion_info(
    input_assertion: models.AssertionInfoClass,
    expected: type,
    expected_exception: type,
) -> None:
    if expected_exception:
        with pytest.raises(expected_exception):
            Assertion._switch_assertion_info(input_assertion)
    else:
        result = Assertion._switch_assertion_info(input_assertion)
        assert isinstance(result, expected)
