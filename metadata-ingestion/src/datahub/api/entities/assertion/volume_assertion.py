from typing import Optional, Union

from pydantic import Field
from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.emitter.mce_builder import datahub_guid
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionType,
    AssertionValueChangeType,
    RowCountChange,
    RowCountTotal,
    VolumeAssertionInfo,
    VolumeAssertionType,
)


class RowCountTotalVolumeAssertion(BaseEntityAssertion):
    type: Literal["volume"]
    metric: Literal["row_count"] = Field(default="row_count")
    operator: Operators = Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = Field(default=None)

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.VOLUME,
            volumeAssertion=VolumeAssertionInfo(
                type=VolumeAssertionType.ROW_COUNT_TOTAL,
                entity=self.entity,
                rowCountTotal=RowCountTotal(
                    operator=self.operator.operator,
                    parameters=self.operator.generate_parameters(),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


class RowCountChangeVolumeAssertion(BaseEntityAssertion):
    type: Literal["volume"]
    metric: Literal["row_count"] = Field(default="row_count")
    change_type: Literal["absolute", "percentage"]
    operator: Operators = Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = Field(default=None)

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.VOLUME,
            volumeAssertion=VolumeAssertionInfo(
                type=VolumeAssertionType.ROW_COUNT_CHANGE,
                entity=self.entity,
                rowCountChange=RowCountChange(
                    type=(
                        AssertionValueChangeType.ABSOLUTE
                        if self.change_type == "absolute"
                        else AssertionValueChangeType.PERCENTAGE
                    ),
                    operator=self.operator.operator,
                    parameters=self.operator.generate_parameters(),
                ),
            ),
        )

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.entity,
            "type": self.type,
            "id_raw": self.id_raw,
        }
        return self.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return self.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.trigger


# Pydantic v2 smart union: automatically discriminates based on presence of
# unique fields (eg absence vs presence of change_type)
VolumeAssertion = Union[RowCountTotalVolumeAssertion, RowCountChangeVolumeAssertion]
