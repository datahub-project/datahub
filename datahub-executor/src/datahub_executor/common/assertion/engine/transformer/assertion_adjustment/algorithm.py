from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from datahub_executor.common.types import (
    Assertion,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    PermissiveBaseModel,
)


class AdjustmentAlgorithm(PermissiveBaseModel, ABC):
    algorithmName: str

    @abstractmethod
    def adjust_assertion_info(self, base_assertion: Assertion) -> Assertion:
        """
        Implements the adjustment algorithm
        """
        pass


IQR_ADJUSTMENT_ALGORITHM_NAME = "IQR"


class IQRAdjustmentContext(BaseModel):
    K: float = Field(
        description="The multiplier to apply to IQR to compute parameter values allowed range.",
        default=2.5,
    )


class IQRAdjustmentAlgorithm(AdjustmentAlgorithm):
    """
    Applicable for BETWEEN operator only
    Adjusts min and max value by buffer thats computed using IQR (max-min) value and context parameters.
    """

    @classmethod
    def name(cls) -> str:
        return IQR_ADJUSTMENT_ALGORITHM_NAME

    algorithmName: str = IQR_ADJUSTMENT_ALGORITHM_NAME
    context: IQRAdjustmentContext = Field(default=IQRAdjustmentContext())

    def adjust_assertion_info(self, assertion: Assertion) -> Assertion:
        if (
            assertion.volume_assertion
            and assertion.volume_assertion.row_count_total
            and assertion.volume_assertion.row_count_total.operator
            == AssertionStdOperator.BETWEEN
        ):
            assertion.volume_assertion.row_count_total.parameters = (
                self._adjust_parameters(
                    assertion.volume_assertion.row_count_total.parameters
                )
            )

        elif (
            assertion.field_assertion
            and assertion.field_assertion.field_metric_assertion
            and assertion.field_assertion.field_metric_assertion.operator
            == AssertionStdOperator.BETWEEN
        ):
            assertion.field_assertion.field_metric_assertion.parameters = (
                self._adjust_parameters(
                    assertion.field_assertion.field_metric_assertion.parameters
                )
            )

        return assertion

    def _adjust_parameters(
        self, parameters: AssertionStdParameters
    ) -> AssertionStdParameters:
        assert parameters.max_value is not None
        assert parameters.min_value is not None
        max_val = float(parameters.max_value.value)
        min_val = float(parameters.min_value.value)
        iqr = max_val - min_val
        if iqr == 0:
            buffer = 0.1 * self.context.K * max_val
        else:
            buffer = self.context.K * iqr
        return AssertionStdParameters(
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value=str(min_val - buffer)
            ),
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value=str(max_val + buffer)
            ),
        )
