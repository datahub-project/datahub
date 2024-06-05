from typing import Optional, Union

from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseAssertionProtocol,
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.filter import DatasetFilter
from datahub.configuration.pydantic_migration_helpers import v1_Field
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
    metric: Literal["row_count"] = v1_Field(default="row_count")
    operator: Operators = v1_Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = v1_Field(default=None)

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


class RowCountChangeVolumeAssertion(BaseEntityAssertion):
    type: Literal["volume"]
    metric: Literal["row_count"] = v1_Field(default="row_count")
    change_type: Literal["absolute", "percentage"]
    operator: Operators = v1_Field(discriminator="type", alias="condition")
    filters: Optional[DatasetFilter] = v1_Field(default=None)

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
                        if self.change_type == Literal["absolute"]
                        else AssertionValueChangeType.PERCENTAGE
                    ),
                    operator=self.operator.operator,
                    parameters=self.operator.generate_parameters(),
                ),
            ),
        )


class VolumeAssertion(BaseAssertionProtocol):
    __root__: Union[RowCountTotalVolumeAssertion, RowCountChangeVolumeAssertion]

    @property
    def assertion(self):
        return self.__root__

    def get_id(self) -> str:
        guid_dict = {
            "entity": self.__root__.entity,
            "type": self.__root__.type,
            "id_raw": self.__root__.id_raw,
        }
        return self.__root__.id or datahub_guid(guid_dict)

    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        return self.__root__.get_assertion_info()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.__root__.trigger
