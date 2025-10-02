import logging
from typing import Optional

from pydantic import BaseModel, Field

from datahub_integrations.actions.action_extended import AutomationActionConfig

logger = logging.getLogger(__name__)


class DescriptionPropagationConfig(AutomationActionConfig):
    """Configuration for description propagation to Snowflake."""

    enabled: bool = Field(
        default=True,
        description="Whether description propagation is enabled",
    )
    table_description_sync_enabled: bool = Field(
        default=True,
        description="Whether to sync table/view descriptions",
    )
    column_description_sync_enabled: bool = Field(
        default=True,
        description="Whether to sync column descriptions",
    )


class DescriptionPropagationDirective(BaseModel):
    """Directive for propagating a description to Snowflake."""

    entity: str = Field(description="The URN of the entity to update")
    description: str = Field(description="The description to apply")
    operation: str = Field(description="The operation to perform (ADD/REMOVE)")
    subtype: Optional[str] = Field(
        default=None, description="The subtype of the entity (TABLE/VIEW)"
    )
    propagate: bool = Field(default=True, description="Whether to propagate")
