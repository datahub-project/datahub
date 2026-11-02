import pytest

from datahub.api.entities.assertion.assertion_operator import (
    BetweenOperator,
    ContainsOperator,
    EndsWithOperator,
    EqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    InOperator,
    IsNullOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    MatchesRegexOperator,
    NotEqualToOperator,
    NotInOperator,
    Operators,
    StartsWithOperator,
)


@pytest.mark.parametrize("value", [0, 0.0])
def test_equal_to_zero(value: float) -> None:
    parameters = EqualToOperator(type="equal_to", value=value).generate_parameters()
    assert parameters.to_obj() == {"value": {"value": str(value), "type": "NUMBER"}}


@pytest.mark.parametrize(
    "operator",
    [
        NotEqualToOperator(type="not_equal_to", value=0),
        LessThanOperator(type="less_than", value=0),
        LessThanOrEqualToOperator(type="less_than_or_equal_to", value=0),
        GreaterThanOperator(type="greater_than", value=0),
        GreaterThanOrEqualToOperator(type="greater_than_or_equal_to", value=0),
    ],
)
def test_comparison_with_zero(operator: Operators) -> None:
    assert operator.generate_parameters().to_obj() == {
        "value": {"value": "0", "type": "NUMBER"}
    }


@pytest.mark.parametrize(
    "operator",
    [
        EqualToOperator(type="equal_to", value=""),
        NotEqualToOperator(type="not_equal_to", value=""),
        ContainsOperator(type="contains", value=""),
        EndsWithOperator(type="ends_with", value=""),
        StartsWithOperator(type="starts_with", value=""),
        MatchesRegexOperator(type="matches_regex", value=""),
    ],
)
def test_empty_string_is_preserved(operator: Operators) -> None:
    assert operator.generate_parameters().to_obj() == {
        "value": {"value": "", "type": "STRING"}
    }


@pytest.mark.parametrize(
    "operator, serialized_value",
    [
        (InOperator(type="in", value=[]), "[]"),
        (NotInOperator(type="not_in", value=[]), "[]"),
        (InOperator(type="in", value=[0, 0.0, ""]), '[0, 0.0, ""]'),
        (NotInOperator(type="not_in", value=[0, 0.0, ""]), '[0, 0.0, ""]'),
    ],
)
def test_list_preserves_falsey_values(
    operator: Operators, serialized_value: str
) -> None:
    assert operator.generate_parameters().to_obj() == {
        "value": {"value": serialized_value, "type": "LIST"}
    }


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
