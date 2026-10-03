import pytest

from datahub.api.entities.assertion.assertion_operator import (
    BetweenOperator,
    EqualToOperator,
    IsNullOperator,
)


@pytest.mark.parametrize("value", [0, 0.0])
def test_equal_to_zero(value: float) -> None:
    parameters = EqualToOperator(type="equal_to", value=value).generate_parameters()
    assert parameters.to_obj() == {"value": {"value": str(value), "type": "NUMBER"}}


@pytest.mark.parametrize("lower, upper", [(0, 10), (-10, 0), (0.0, 10.5), (-10.5, 0.0)])
def test_between_with_zero_bound(lower: float, upper: float) -> None:
    parameters = BetweenOperator(
        type="between", min=lower, max=upper
    ).generate_parameters()
    assert parameters.to_obj() == {
        "minValue": {"value": str(lower), "type": "NUMBER"},
        "maxValue": {"value": str(upper), "type": "NUMBER"},
    }


def test_absent_parameters_remain_absent() -> None:
    assert IsNullOperator(type="is_null").generate_parameters().to_obj() == {}
