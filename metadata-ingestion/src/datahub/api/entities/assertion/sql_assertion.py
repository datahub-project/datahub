from typing import Optional, Union

from pydantic import Field
from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.emitter.mce_builder import datahub_guid
from datahub.metadata.com.linkedin.pegasus2avro.assertion import (
    AssertionInfo,
    AssertionType,
    AssertionValueChangeType,
    SqlAssertionInfo,
    SqlAssertionType,
)


class SqlMetricAssertion(BaseEntityAssertion):
    type: Literal["sql"]
    statement: str
    operator: Operators = Field(discriminator="type", alias="condition")

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.SQL,
            sqlAssertion=SqlAssertionInfo(
                type=SqlAssertionType.METRIC,
                entity=self.entity,
                statement=self.statement,
                operator=self.operator.operator,
                parameters=self.operator.generate_parameters(),
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


class SqlMetricChangeAssertion(BaseEntityAssertion):
    type: Literal["sql"]
    statement: str
    change_type: Literal["absolute", "percentage"]
    operator: Operators = Field(discriminator="type", alias="condition")

    def get_assertion_info(
        self,
    ) -> AssertionInfo:
        return AssertionInfo(
            description=self.description,
            type=AssertionType.SQL,
            sqlAssertion=SqlAssertionInfo(
                type=SqlAssertionType.METRIC_CHANGE,
                entity=self.entity,
                statement=self.statement,
                changeType=(
                    AssertionValueChangeType.ABSOLUTE
                    if self.change_type == "absolute"
                    else AssertionValueChangeType.PERCENTAGE
                ),
                operator=self.operator.operator,
                parameters=self.operator.generate_parameters(),
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
SQLAssertion = Union[SqlMetricAssertion, SqlMetricChangeAssertion]
