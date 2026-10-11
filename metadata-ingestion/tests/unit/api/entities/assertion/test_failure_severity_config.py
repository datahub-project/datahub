from typing import Union

import pytest
from pydantic import ValidationError

import datahub.metadata.schema_classes as models
from datahub.api.entities.assertion.assertion import (
    AssertionFailureSeverity,
    AssertionFailureSeverityConfig,
    AssertionFailureSeverityDefaultConfig,
    AssertionFailureSeverityOperator,
    AssertionFailureSeverityRule,
)
from datahub.api.entities.assertion.assertion_operator import GreaterThanOperator
from datahub.api.entities.assertion.field_assertion import (
    FieldMetricAssertion,
    FieldValuesAssertion,
)
from datahub.api.entities.assertion.field_metric import FieldMetric
from datahub.api.entities.assertion.freshness_assertion import (
    FixedIntervalFreshnessAssertion,
)
from datahub.api.entities.assertion.sql_assertion import (
    SqlMetricAssertion,
    SqlMetricChangeAssertion,
)
from datahub.api.entities.assertion.volume_assertion import (
    RowCountChangeVolumeAssertion,
    RowCountTotalVolumeAssertion,
)

DATASET_URN = "urn:li:dataset:(urn:li:dataPlatform:snowflake,test.dataset,PROD)"
OperatorInput = Union[
    str,
    AssertionFailureSeverityOperator,
    models.AssertionStdOperatorClass,
]


def _severity_config() -> AssertionFailureSeverityConfig:
    return AssertionFailureSeverityConfig(
        default_severity=AssertionFailureSeverity.MEDIUM,
        rules=[
            AssertionFailureSeverityRule(
                severity=AssertionFailureSeverity.HIGH,
                operator=AssertionFailureSeverityOperator.GREATER_THAN_OR_EQUAL_TO,
                value=10,
            )
        ],
    )


def _default_severity_config() -> AssertionFailureSeverityDefaultConfig:
    return AssertionFailureSeverityDefaultConfig(
        default_severity=AssertionFailureSeverity.MEDIUM,
    )


def _assert_default_only_severity_config(
    config: models.AssertionFailureSeverityConfigClass,
) -> None:
    assert config.defaultSeverity == models.AssertionResultSeverityClass.MEDIUM
    assert len(config.rules or []) == 0


def _require_severity_config(
    config: object,
) -> models.AssertionFailureSeverityConfigClass:
    assert isinstance(config, models.AssertionFailureSeverityConfigClass)
    return config


def _assert_severity_config(
    config: models.AssertionFailureSeverityConfigClass,
) -> None:
    assert config.defaultSeverity == models.AssertionResultSeverityClass.MEDIUM
    rule = config.rules[0]
    assert rule.parameters.value is not None
    assert rule.severity == models.AssertionResultSeverityClass.HIGH
    assert rule.operator == models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO
    assert rule.parameters.value.value == "10"
    assert rule.parameters.value.type == models.AssertionStdParameterTypeClass.NUMBER


@pytest.mark.parametrize(
    ("operator", "expected_operator"),
    [
        (
            AssertionFailureSeverityOperator.GREATER_THAN,
            models.AssertionStdOperatorClass.GREATER_THAN,
        ),
        ("GREATER_THAN", models.AssertionStdOperatorClass.GREATER_THAN),
        (
            AssertionFailureSeverityOperator.GREATER_THAN_OR_EQUAL_TO,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        ),
        (
            "GREATER_THAN_OR_EQUAL_TO",
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        ),
        (
            AssertionFailureSeverityOperator.LESS_THAN,
            models.AssertionStdOperatorClass.LESS_THAN,
        ),
        ("LESS_THAN", models.AssertionStdOperatorClass.LESS_THAN),
        (
            AssertionFailureSeverityOperator.LESS_THAN_OR_EQUAL_TO,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
        ),
        (
            "LESS_THAN_OR_EQUAL_TO",
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
        ),
        (
            models.AssertionStdOperatorClass.GREATER_THAN,
            models.AssertionStdOperatorClass.GREATER_THAN,
        ),
        (
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
            models.AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO,
        ),
        (
            models.AssertionStdOperatorClass.LESS_THAN,
            models.AssertionStdOperatorClass.LESS_THAN,
        ),
        (
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
            models.AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO,
        ),
    ],
)
def test_failure_severity_rule_accepts_canonical_operator_inputs(
    operator: OperatorInput,
    expected_operator: models.AssertionStdOperatorClass,
) -> None:
    rule = AssertionFailureSeverityRule(
        severity=AssertionFailureSeverity.HIGH,
        operator=operator,
        value=10,
    )
    assert rule.to_model().operator == expected_operator

    config = AssertionFailureSeverityConfig.model_validate(
        {
            "default_severity": AssertionFailureSeverity.MEDIUM,
            "rules": [
                {
                    "severity": AssertionFailureSeverity.HIGH,
                    "operator": operator,
                    "value": 10,
                }
            ],
        }
    )
    assert config.to_model().rules[0].operator == expected_operator


def test_freshness_assertion_emits_failure_severity_config() -> None:
    assertion = FixedIntervalFreshnessAssertion(
        type="freshness",
        entity=DATASET_URN,
        lookback_interval="1 hour",
        last_modified_field="updated_at",
        failure_severity_config=_default_severity_config(),
    )

    info = assertion.get_assertion_info_aspect()

    assert info.freshnessAssertion is not None
    _assert_default_only_severity_config(
        _require_severity_config(info.freshnessAssertion.failureSeverityConfig)
    )


def test_freshness_assertion_rejects_failure_severity_rules() -> None:
    with pytest.raises(ValidationError):
        FixedIntervalFreshnessAssertion(
            type="freshness",
            entity=DATASET_URN,
            lookback_interval="1 hour",
            last_modified_field="updated_at",
            failure_severity_config={
                "default_severity": AssertionFailureSeverity.MEDIUM,
                "rules": [
                    {
                        "severity": AssertionFailureSeverity.HIGH,
                        "operator": AssertionFailureSeverityOperator.GREATER_THAN_OR_EQUAL_TO,
                        "value": 10,
                    }
                ],
            },
        )


def test_volume_assertions_emit_failure_severity_config() -> None:
    row_count_total = RowCountTotalVolumeAssertion(
        type="volume",
        entity=DATASET_URN,
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()
    row_count_change = RowCountChangeVolumeAssertion(
        type="volume",
        entity=DATASET_URN,
        change_type="absolute",
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()

    assert row_count_total.volumeAssertion is not None
    assert row_count_total.volumeAssertion.rowCountTotal is not None
    _assert_severity_config(
        _require_severity_config(
            row_count_total.volumeAssertion.rowCountTotal.failureSeverityConfig
        )
    )

    assert row_count_change.volumeAssertion is not None
    assert row_count_change.volumeAssertion.rowCountChange is not None
    _assert_severity_config(
        _require_severity_config(
            row_count_change.volumeAssertion.rowCountChange.failureSeverityConfig
        )
    )


def test_sql_assertions_emit_failure_severity_config() -> None:
    sql_metric = SqlMetricAssertion(
        type="sql",
        entity=DATASET_URN,
        statement="SELECT COUNT(*) FROM table",
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()
    sql_change = SqlMetricChangeAssertion(
        type="sql",
        entity=DATASET_URN,
        statement="SELECT COUNT(*) FROM table",
        change_type="absolute",
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()

    assert sql_metric.sqlAssertion is not None
    _assert_severity_config(
        _require_severity_config(sql_metric.sqlAssertion.failureSeverityConfig)
    )

    assert sql_change.sqlAssertion is not None
    _assert_severity_config(
        _require_severity_config(sql_change.sqlAssertion.failureSeverityConfig)
    )


def test_field_assertions_emit_failure_severity_config() -> None:
    field_metric = FieldMetricAssertion(
        type="field",
        entity=DATASET_URN,
        field="count",
        metric=FieldMetric.NULL_COUNT,
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()
    field_values = FieldValuesAssertion(
        type="field",
        entity=DATASET_URN,
        field="count",
        condition=GreaterThanOperator(type="greater_than", value=100),
        failure_severity_config=_severity_config(),
    ).get_assertion_info_aspect()

    assert field_metric.fieldAssertion is not None
    assert field_metric.fieldAssertion.fieldMetricAssertion is not None
    _assert_severity_config(
        _require_severity_config(
            field_metric.fieldAssertion.fieldMetricAssertion.failureSeverityConfig
        )
    )

    assert field_values.fieldAssertion is not None
    assert field_values.fieldAssertion.fieldValuesAssertion is not None
    _assert_severity_config(
        _require_severity_config(
            field_values.fieldAssertion.fieldValuesAssertion.failureSeverityConfig
        )
    )
