from abc import abstractmethod
from typing import Optional

from pydantic import Field

from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import make_assertion_source
from datahub.metadata.com.linkedin.pegasus2avro.assertion import AssertionInfo


class BaseAssertionProtocol(ConfigModel):
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


class BaseAssertion(ConfigModel):
    id_raw: Optional[str] = Field(
        default=None,
        description="The raw id of the assertion."
        "If provided, this is used when creating identifier for this assertion"
        "along with assertion type and entity.",
    )

    id: Optional[str] = Field(
        default=None,
        description="The id of the assertion."
        "If provided, this is used as identifier for this assertion."
        "If provided, no other assertion fields are considered to create identifier.",
    )

    description: Optional[str] = None

    meta: Optional[dict] = None


def _ensure_source_created(info: AssertionInfo) -> AssertionInfo:
    """Ensure AssertionInfo has source.created populated."""
    if info.source is None:
        info.source = make_assertion_source()
    elif info.source.created is None:
        info.source.created = make_assertion_source().created
    return info


class BaseEntityAssertion(BaseAssertion):
    entity: str = Field(
        description="The entity urn that the assertion is associated with"
    )

    trigger: Optional[AssertionTrigger] = Field(
        default=None, description="The trigger schedule for assertion", alias="schedule"
    )

    @abstractmethod
    def get_assertion_info(self) -> AssertionInfo:
        pass

    def get_assertion_info_aspect(self) -> AssertionInfo:
        return _ensure_source_created(self.get_assertion_info())
