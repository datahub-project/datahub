from enum import Enum
from typing import Optional, Union

from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseAssertionProtocol,
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.field_metric import FieldMetric
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field
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


class FieldValuesFailThreshold(v1_ConfigModel):
    type: Literal["count", "percentage"] = v1_Field(default="count")
    value: int = v1_Field(default=0)

    def to_field_values_failure_threshold(self) -> FieldValuesFailThresholdClass:
        return FieldValuesFailThresholdClass(
            type=(
                FieldValuesFailThresholdTypeClass.COUNT
                if self.type == Literal["count"]
                else FieldValuesFailThresholdTypeClass.PERCENTAGE
            ),
            value=self.value,
        )


class FieldTransform(Enum):
    LENGTH = "length"


class FieldValuesAssertion(BaseEntityAssertion):
    type: Literal["field"]
    field: str
    field_transform: Optional[FieldTransform] = v1_Field(default=None)
    operator: Operators = v1_Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = v1_Field(default=None)
    failure_threshold: FieldValuesFailThreshold = v1_Field(
        default=FieldValuesFailThreshold()
    )
    exclude_nulls: bool = v1_Field(default=True)

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
                        if self.field_transform == Literal["length"]
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


class FieldMetricAssertion(BaseEntityAssertion):
    type: Literal["field"]
    field: str
    operator: Operators = v1_Field(discriminator="type", alias="condition")
    metric: FieldMetric
    filters: Optional[DatasetFilter] = v1_Field(default=None)

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


class FieldAssertion(BaseAssertionProtocol):
    __root__: Union[FieldMetricAssertion, FieldValuesAssertion]

    @property
    def assertion(self):
        return self.__root__

    def get_id(self) -> str:
        return self.__root__.get_id()

    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        return self.__root__.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.__root__.trigger
