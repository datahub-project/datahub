from enum import Enum
from typing import Optional, Union

from pydantic import Field
from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.field_metric import FieldMetric
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import datahub_guid
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionType,
    FieldAssertionInfo,
    FieldAssertionType,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import SchemaFieldSpec
from datahub.metadata.schema_classes import (
    FieldMetricAssertionClass,
    FieldTransformClass,
    FieldTransformTypeClass,
    FieldValuesAssertionClass,
    FieldValuesFailThresholdClass,
    FieldValuesFailThresholdTypeClass,
)


class FieldValuesFailThreshold(ConfigModel):
    type: Literal["count", "percentage"] = Field(default="count")
    value: int = Field(default=0)

    def to_field_values_failure_threshold(self) -> FieldValuesFailThresholdClass:
        return FieldValuesFailThresholdClass(
            type=(
                FieldValuesFailThresholdTypeClass.COUNT
                if self.type == "count"
                else FieldValuesFailThresholdTypeClass.PERCENTAGE
            ),
            value=self.value,
        )


class FieldTransform(Enum):
    LENGTH = "length"


class FieldValuesAssertion(BaseEntityAssertion):
    type: Literal["field"]
    field: str
    field_transform: Optional[FieldTransform] = Field(default=None)
    operator: Operators = Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = Field(default=None)
    failure_threshold: FieldValuesFailThreshold = Field(
        default=FieldValuesFailThreshold()
    )
    exclude_nulls: bool = Field(default=True)

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.FIELD,
            fieldAssertion=FieldAssertionInfo(
                type=FieldAssertionType.FIELD_VALUES,
                entity=self.entity,
                fieldValuesAssertion=FieldValuesAssertionClass(
                    field=SchemaFieldSpec(
                        path=self.field,
                        type="",  # Not required
                        nativeType="",  # Not required
                    ),
                    operator=self.operator.operator,
                    parameters=self.operator.generate_parameters(),
                    failThreshold=self.failure_threshold.to_field_values_failure_threshold(),
                    excludeNulls=self.exclude_nulls,
                    transform=(
                        FieldTransformClass(type=FieldTransformTypeClass.LENGTH)
                        if self.field_transform == FieldTransform.LENGTH
                        else None
                    ),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "field": self.field,
            "operator": str(self.operator.operator),
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


class FieldMetricAssertion(BaseEntityAssertion):
    type: Literal["field"]
    field: str
    operator: Operators = Field(discriminator="type", alias="condition")
    metric: FieldMetric
    filters: Optional[DatasetFilter] = Field(default=None)

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.FIELD,
            fieldAssertion=FieldAssertionInfo(
                type=FieldAssertionType.FIELD_METRIC,
                entity=self.entity,
                fieldMetricAssertion=FieldMetricAssertionClass(
                    field=SchemaFieldSpec(
                        path=self.field,
                        type="",  # Not required
                        nativeType="",  # Not required
                    ),
                    metric=self.metric.name,
                    operator=self.operator.operator,
                    parameters=self.operator.generate_parameters(),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "field": self.field,
            "metric": self.metric.value,
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


# Pydantic v2 smart union: automatically discriminates based on presence of
# unique fields (eg metric field vs operator+failure_threshold combination)
FieldAssertion = Union[FieldMetricAssertion, FieldValuesAssertion]
