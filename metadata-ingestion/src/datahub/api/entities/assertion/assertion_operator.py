import json
from typing import List, Optional, Union

from typing_extensions import Literal, Protocol

from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel
from datahub.metadata.schema_classes import (
    AssertionStdOperatorClass,
    AssertionStdParameterClass,
    AssertionStdParametersClass,
    AssertionStdParameterTypeClass,
)


class Operator(Protocol):
    """Specification for an assertion operator.

    This class exists only for documentation (not used in typing checking).
    """

    operator: str

    def id(self) -> str:
        ...

    def generate_parameters(self) -> AssertionStdParametersClass:
        ...


def _generate_assertion_std_parameter(
    value: Union[str, int, float, list]
) -> AssertionStdParameterClass:
    if isinstance(value, str):
        return AssertionStdParameterClass(
            value=value, type=AssertionStdParameterTypeClass.STRING
        )
    elif isinstance(value, (int, float)):
        return AssertionStdParameterClass(
            value=str(value), type=AssertionStdParameterTypeClass.NUMBER
        )
    elif isinstance(value, list):
        return AssertionStdParameterClass(
            value=json.dumps(value), type=AssertionStdParameterTypeClass.LIST
        )
    else:
        raise ValueError(
            f"Unsupported assertion parameter {value} of type {type(value)}"
        )


Param = Union[str, int, float, List[Union[str, float, int]]]


def _generate_assertion_std_parameters(
    value: Optional[Param] = None,
    min_value: Optional[Param] = None,
    max_value: Optional[Param] = None,
) -> AssertionStdParametersClass:
    return AssertionStdParametersClass(
        value=_generate_assertion_std_parameter(value) if value else None,
        minValue=_generate_assertion_std_parameter(min_value) if min_value else None,
        maxValue=_generate_assertion_std_parameter(max_value) if max_value else None,
    )


class EqualToOperator(v1_ConfigModel):
    type: Literal["equal_to"]
    value: Union[str, int, float]

    operator: str = AssertionStdOperatorClass.EQUAL_TO

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class NotEqualToOperator(v1_ConfigModel):
    type: Literal["not_equal_to"]
    value: Union[str, int, float]

    operator: str = AssertionStdOperatorClass.NOT_EQUAL_TO

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class BetweenOperator(v1_ConfigModel):
    type: Literal["between"]
    min: Union[int, float]
    max: Union[int, float]

    operator: str = AssertionStdOperatorClass.BETWEEN

    def id(self) -> str:
        return f"{self.type}-{self.min}-{self.max}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(
            min_value=self.min, max_value=self.max
        )


class LessThanOperator(v1_ConfigModel):
    type: Literal["less_than"]
    value: Union[int, float]

    operator: str = AssertionStdOperatorClass.LESS_THAN

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class GreaterThanOperator(v1_ConfigModel):
    type: Literal["greater_than"]
    value: Union[int, float]

    operator: str = AssertionStdOperatorClass.GREATER_THAN

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class LessThanOrEqualToOperator(v1_ConfigModel):
    type: Literal["less_than_or_equal_to"]
    value: Union[int, float]

    operator: str = AssertionStdOperatorClass.LESS_THAN_OR_EQUAL_TO

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class GreaterThanOrEqualToOperator(v1_ConfigModel):
    type: Literal["greater_than_or_equal_to"]
    value: Union[int, float]

    operator: str = AssertionStdOperatorClass.GREATER_THAN_OR_EQUAL_TO

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class InOperator(v1_ConfigModel):
    type: Literal["in"]
    value: List[Union[str, float, int]]

    operator: str = AssertionStdOperatorClass.IN

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class NotInOperator(v1_ConfigModel):
    type: Literal["not_in"]
    value: List[Union[str, float, int]]

    operator: str = AssertionStdOperatorClass.NOT_IN

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class IsNullOperator(v1_ConfigModel):
    type: Literal["is_null"]

    operator: str = AssertionStdOperatorClass.NULL

    def id(self) -> str:
        return f"{self.type}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters()


class NotNullOperator(v1_ConfigModel):
    type: Literal["is_not_null"]

    operator: str = AssertionStdOperatorClass.NOT_NULL

    def id(self) -> str:
        return f"{self.type}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters()


class IsTrueOperator(v1_ConfigModel):
    type: Literal["is_true"]

    operator: str = AssertionStdOperatorClass.IS_TRUE

    def id(self) -> str:
        return f"{self.type}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters()


class IsFalseOperator(v1_ConfigModel):
    type: Literal["is_false"]

    operator: str = AssertionStdOperatorClass.IS_FALSE

    def id(self) -> str:
        return f"{self.type}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters()


class ContainsOperator(v1_ConfigModel):
    type: Literal["contains"]
    value: str

    operator: str = AssertionStdOperatorClass.CONTAIN

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class EndsWithOperator(v1_ConfigModel):
    type: Literal["ends_with"]
    value: str

    operator: str = AssertionStdOperatorClass.END_WITH

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class StartsWithOperator(v1_ConfigModel):
    type: Literal["starts_with"]
    value: str

    operator: str = AssertionStdOperatorClass.START_WITH

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


class MatchesRegexOperator(v1_ConfigModel):
    type: Literal["matches_regex"]
    value: str

    operator: str = AssertionStdOperatorClass.REGEX_MATCH

    def id(self) -> str:
        return f"{self.type}-{self.value}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters(value=self.value)


Operators = Union[
    InOperator,
    NotInOperator,
    EqualToOperator,
    NotEqualToOperator,
    BetweenOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    IsNullOperator,
    NotNullOperator,
    IsTrueOperator,
    IsFalseOperator,
    ContainsOperator,
    EndsWithOperator,
    StartsWithOperator,
    MatchesRegexOperator,
]
