import logging
from typing import Optional

from datahub.configuration.common import ConfigModel
from datahub.metadata.urns import ContainerUrn, DatasetUrn, SchemaFieldUrn, Urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DescriptionSyncConfig(ConfigModel):
    """

    Configuration model for description sync.

    Attributes:
    enabled (bool): Indicates whether description sync is enabled or not. Default is True.
    table_description_sync_enabled (bool): Indicates whether table description sync is enabled or not. Default is True.
    column_description_sync_enabled (bool): Indicates whether column description sync is enabled or not. Default is True.
    container_description_sync_enabled (bool): Indicates whether container description sync_enabled is enabled or not. Default is True.

    Note:
    Description sync allows descriptions to be automatically propagated to downstream entities.
    Enabling description sync can help maintain consistent metadata across connected entities.
    The `enabled` attribute controls whether description sync is enabled or disabled.
    The `table_description_sync_enabled` and `column_description_sync_enabled` attributes control whether table and column descriptions are synced, respectively.

    Example:
    config = DescriptionSyncConfig(enabled=True, table_description_sync_enabled=True, column_description_sync_enabled=True)
    """

    enabled: bool = Field(
        default=True,
        description="Indicates whether tag propagation is enabled or not.",
        examples=[True],
    )

    table_description_sync_enabled: bool = Field(
        default=True,
        description="Indicates whether table description sync is enabled or not.",
        examples=[True],
    )

    column_description_sync_enabled: bool = Field(
        default=True,
        description="Indicates whether column description sync is enabled or not.",
        examples=[True],
    )

    container_description_sync_enabled: bool = Field(
        default=True,
        description="Indicates whether container description sync is enabled or not.",
        examples=[True],
    )


class UnityCatalogDescriptionSyncDirective(BaseModel):
    docs: str
    entity: str
    operation: str
    propagate: bool


class DescriptionSyncAction(Action):
    def __init__(self, config: DescriptionSyncConfig, ctx: PipelineContext):
        self.config: DescriptionSyncConfig = config
        self.ctx = ctx

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DescriptionSyncAction":
        config = DescriptionSyncConfig.model_validate(config_dict or {})
        logger.info(f"DescriptionSyncAction configured with {config}")
        return cls(config, ctx)

    def name(self) -> str:
        return "DescriptionSyncAction"

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[UnityCatalogDescriptionSyncDirective]:
        """
        Return a tag urn to propagate or None if no propagation is desired
        """
        if self.config.enabled and event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None
            semantic_event = event.event
            parameters = semantic_event._inner_dict.get("__parameters_json", {})

            docs: Optional[str] = None
            if (
                semantic_event.category == "DOCUMENTATION"
                and "description" in parameters
            ):
                docs = parameters["description"]
            else:
                return None

            if not docs:
                # Description can't be deleted we ignore these changes
                logger.info("No description found. Skipping description sync.")
                return None

            enity_urn = Urn.create_from_string(semantic_event.entityUrn)

            if not self.config.column_description_sync_enabled and isinstance(
                enity_urn, SchemaFieldUrn
            ):
                logger.info(
                    "Schema metadata description sync is disabled. Skipping schema metadata description sync"
                )
                return None

            if not self.config.table_description_sync_enabled and isinstance(
                enity_urn, DatasetUrn
            ):
                logger.info(
                    "Table description sync is disabled. Skipping table description sync."
                )
                return None

            if not self.config.container_description_sync_enabled and isinstance(
                enity_urn, ContainerUrn
            ):
                logger.info(
                    "Container description sync is disabled. Skipping container description sync."
                )
                return None

            if semantic_event.operation in {"ADD", "MODIFY"}:
                return UnityCatalogDescriptionSyncDirective(
                    entity=semantic_event.entityUrn,
                    docs=docs,
                    operation=semantic_event.operation,
                    propagate=True,
                )
            else:
                logger.debug(
                    f"Skipping unknown documentation operation {semantic_event.operation} for {event.event.entityUrn}"
                )

        return None

    def act(self, event: EventEnvelope) -> None:
        tag_propagation_directive = self.should_propagate(event)
        logger.debug(
            f"Propagation is not implemented. Not propagating {tag_propagation_directive} in DataHub"
        )

    def close(self) -> None:
        return super().close()
