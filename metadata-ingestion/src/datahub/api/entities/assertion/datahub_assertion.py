from typing import Optional, Union

from datahub.api.entities.assertion.assertion import BaseAssertionProtocol
from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.api.entities.assertion.field_assertion import FieldAssertion
from datahub.api.entities.assertion.freshness_assertion import FreshnessAssertion
from datahub.api.entities.assertion.sql_assertion import SQLAssertion
from datahub.api.entities.assertion.volume_assertion import VolumeAssertion
from datahub.configuration.pydantic_migration_helpers import v1_Field
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionInfo


class DataHubAssertion(BaseAssertionProtocol):
    __root__: Union[
        FreshnessAssertion,
        VolumeAssertion,
        SQLAssertion,
        FieldAssertion,
        # TODO: Add SchemaAssertion
    ] = v1_Field(discriminator="type")

    @property
    def assertion(self):
        return self.__root__.assertion

    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        return self.__root__.get_assertion_info_aspect()

    def get_id(self) -> str:
        return self.__root__.get_id()

    def get_assertion_trigger(self) -> Optional[AssertionTrigger]:
        return self.__root__.get_assertion_trigger()
