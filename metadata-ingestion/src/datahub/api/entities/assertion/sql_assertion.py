from typing import Optional, Union

from typing_extensions import Literal

from datahub.api.entities.assertion.assertion import (
    BaseAssertionProtocol,
    BaseEntityAssertion,
)
from datahub.api.entities.assertion.assertion_operator import Operators
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.configuration.pydantic_migration_helpers import v1_Field
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
    operator: Operators = v1_Field(discriminator="type", alias="condition")

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


class SqlMetricChangeAssertion(BaseEntityAssertion):
    type: Literal["sql"]
    statement: str
    change_type: Literal["absolute", "percentage"]
    operator: Operators = v1_Field(discriminator="type", alias="condition")

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
                    if self.change_type == Literal["absolute"]
                    else AssertionValueChangeType.PERCENTAGE
                ),
                operator=self.operator.operator,
                parameters=self.operator.generate_parameters(),
            ),
        )


class SQLAssertion(BaseAssertionProtocol):
    __root__: Union[SqlMetricAssertion, SqlMetricChangeAssertion] = v1_Field()

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
