# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from abc import abstractmethod
from typing import Optional

from pydantic import Field

from datahub.api.entities.assertion.assertion_trigger import AssertionTrigger
from datahub.configuration.common import ConfigModel
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


class BaseEntityAssertion(BaseAssertion):
    entity: str = Field(
        description="The entity urn that the assertion is associated with"
    )

    trigger: Optional[AssertionTrigger] = Field(
        default=None, description="The trigger schedule for assertion", alias="schedule"
    )
