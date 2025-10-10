import os
from abc import ABC, abstractmethod

from pydantic import BaseModel, Field

from datahub_executor.common.types import (
    Assertion,
    AssertionStdOperator,
    AssertionStdParameter,
    AssertionStdParameters,
    AssertionStdParameterType,
    FieldMetricType,
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
POSITIVE_VALUE_FIELD_METRICS = [
    FieldMetricType.EMPTY_COUNT,
    FieldMetricType.EMPTY_PERCENTAGE,
    FieldMetricType.MAX_LENGTH,
    FieldMetricType.MIN_LENGTH,
    FieldMetricType.NEGATIVE_COUNT,
    FieldMetricType.NEGATIVE_PERCENTAGE,
    FieldMetricType.NULL_COUNT,
    FieldMetricType.NULL_PERCENTAGE,
    FieldMetricType.UNIQUE_COUNT,
    FieldMetricType.UNIQUE_PERCENTAGE,
    FieldMetricType.ZERO_COUNT,
    FieldMetricType.ZERO_PERCENTAGE,
]


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
                    assertion.volume_assertion.row_count_total.parameters, floor=0
                )
            )

        elif (
            assertion.field_assertion
            and assertion.field_assertion.field_metric_assertion
            and assertion.field_assertion.field_metric_assertion.operator
            == AssertionStdOperator.BETWEEN
        ):
            floor = (
                0
                if assertion.field_assertion.field_metric_assertion.metric
                in POSITIVE_VALUE_FIELD_METRICS
                else None
            )
            assertion.field_assertion.field_metric_assertion.parameters = (
                self._adjust_parameters(
                    assertion.field_assertion.field_metric_assertion.parameters,
                    floor=floor,
                )
            )

        return assertion

    def _adjust_parameters(
        self, parameters: AssertionStdParameters, floor: float | None
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
        uncapped_min = min_val - buffer
        return AssertionStdParameters(
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER,
                value=str(
                    max(uncapped_min, floor) if floor is not None else uncapped_min
                ),
            ),
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value=str(max_val + buffer)
            ),
        )


STD_DEV_ALGORITHM_NAME = "STDDEV"


class StdDevAdjustmentContext(BaseModel):
    buffer_factor: float = Field(
        default_factory=lambda: float(
            os.getenv("ASSERTION_ADJUSTMENT_STDDEV_BUFFER_FACTOR", 0.25)
        ),
        description="The multiplier to apply to StdDev to compute parameter values allowed range.",
    )


class StdDevAdjustmentAlgorithm(AdjustmentAlgorithm):
    """
    Applicable for BETWEEN operator only
    """

    @classmethod
    def name(cls) -> str:
        return STD_DEV_ALGORITHM_NAME

    algorithmName: str = STD_DEV_ALGORITHM_NAME
    std_dev: float = Field(description="The std deviation itself")
    context: StdDevAdjustmentContext = Field(default=StdDevAdjustmentContext())

    def adjust_assertion_info(self, assertion: Assertion) -> Assertion:
        if (
            assertion.volume_assertion
            and assertion.volume_assertion.row_count_total
            and assertion.volume_assertion.row_count_total.operator
            == AssertionStdOperator.BETWEEN
        ):
            assertion.volume_assertion.row_count_total.parameters = (
                self._adjust_parameters(
                    assertion.volume_assertion.row_count_total.parameters, floor=0
                )
            )

        elif (
            assertion.field_assertion
            and assertion.field_assertion.field_metric_assertion
            and assertion.field_assertion.field_metric_assertion.operator
            == AssertionStdOperator.BETWEEN
        ):
            floor = (
                0
                if assertion.field_assertion.field_metric_assertion.metric
                in POSITIVE_VALUE_FIELD_METRICS
                else None
            )
            assertion.field_assertion.field_metric_assertion.parameters = (
                self._adjust_parameters(
                    assertion.field_assertion.field_metric_assertion.parameters,
                    floor=floor,
                )
            )

        return assertion

    def _adjust_parameters(
        self, parameters: AssertionStdParameters, floor: float | None
    ) -> AssertionStdParameters:
        assert parameters.max_value is not None
        assert parameters.min_value is not None
        max_val = float(parameters.max_value.value)
        min_val = float(parameters.min_value.value)
        max_val_with_buffer = max_val + self.context.buffer_factor * float(self.std_dev)
        min_val_with_buffer = min_val - self.context.buffer_factor * float(self.std_dev)
        return AssertionStdParameters(
            minValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER,
                value=str(
                    max(min_val_with_buffer, floor)
                    if floor is not None
                    else min_val_with_buffer
                ),
            ),
            maxValue=AssertionStdParameter(
                type=AssertionStdParameterType.NUMBER, value=str(max_val_with_buffer)
            ),
        )
