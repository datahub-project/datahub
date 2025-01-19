from typing import Optional, Union

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

    def id(self) -> str: ...

    def generate_parameters(self) -> AssertionStdParametersClass: ...


def _generate_assertion_std_parameter(
    value: Union[str, int, float],
) -> AssertionStdParameterClass:
    if isinstance(value, str):
        return AssertionStdParameterClass(
            value=value, type=AssertionStdParameterTypeClass.STRING
        )
    elif isinstance(value, (int, float)):
        return AssertionStdParameterClass(
            value=str(value), type=AssertionStdParameterTypeClass.NUMBER
        )
    else:
        raise ValueError(
            f"Unsupported assertion parameter {value} of type {type(value)}"
        )


Param = Union[str, int, float]


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


class NotNullOperator(v1_ConfigModel):
    type: Literal["not_null"]

    operator: str = AssertionStdOperatorClass.NOT_NULL

    def id(self) -> str:
        return f"{self.type}"

    def generate_parameters(self) -> AssertionStdParametersClass:
        return _generate_assertion_std_parameters()


Operators = Union[
    EqualToOperator,
    BetweenOperator,
    LessThanOperator,
    LessThanOrEqualToOperator,
    GreaterThanOperator,
    GreaterThanOrEqualToOperator,
    NotNullOperator,
]
