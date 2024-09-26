from abc import abstractmethod
from typing import Optional

from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.configuration.pydantic_migration_helpers import v1_ConfigModel, v1_Field
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionInfo


class BaseAssertionProtocol(v1_ConfigModel):
    @abstractmethod
    def get_id(self) -> str:
        pass

    @abstractmethod
    def get_assertion_info_aspect(
        self,
    ) -> AssertionInfo:
        pass

    @abstractmethod
    def get_assertion_trigger(
        self,
    ) -> Optional[AssertionTrigger]:
        pass


class BaseAssertion(v1_ConfigModel):
    id_raw: Optional[str] = v1_Field(
        default=None,
        description="The raw id of the assertion."
        "If provided, this is used when creating identifier for this assertion"
        "along with assertion type and entity.",
    )

    id: Optional[str] = v1_Field(
        default=None,
        description="The id of the assertion."
        "If provided, this is used as identifier for this assertion."
        "If provided, no other assertion fields are considered to create identifier.",
    )

    description: Optional[str] = None

    # Can contain metadata extracted from datahub. e.g.
    # - entity qualified name
    # - entity schema
    meta: Optional[dict] = None


class BaseEntityAssertion(BaseAssertion):
    entity: str = v1_Field(
        description="The entity urn that the assertion is associated with"
    )

    trigger: Optional[AssertionTrigger] = v1_Field(
        description="The trigger schedule for assertion", alias="schedule"
    )
