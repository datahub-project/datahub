import logging
from typing import Optional

from datahub.configuration.common import ConfigModel
from datahub.metadata.urns import DatasetUrn, SchemaFieldUrn, Urn
from datahub_actions.action.action import Action
from datahub_actions.event.event_envelope import EventEnvelope
from datahub_actions.event.event_registry import EntityChangeEvent
from datahub_actions.pipeline.pipeline_context import PipelineContext
from pydantic import BaseModel, Field

from datahub_integrations.propagation.snowflake.util import (
    SnowflakeTagHelper,
    is_snowflake_urn,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class DescriptionSyncConfig(ConfigModel):
    """
    Configuration model for Snowflake description sync.

    Attributes:
    enabled (bool): Indicates whether description sync is enabled or not. Default is True.
    table_description_sync_enabled (bool): Indicates whether table description sync is enabled or not. Default is True.
    column_description_sync_enabled (bool): Indicates whether column description sync is enabled or not. Default is True.

    Note:
    Description sync allows descriptions to be automatically propagated to Snowflake as comments.
    Enabling description sync can help maintain consistent metadata between DataHub and Snowflake.
    """

    enabled: bool = Field(
        True,
        description="Indicates whether description sync is enabled or not.",
        examples=[True],
    )

    table_description_sync_enabled: bool = Field(
        True,
        description="Indicates whether table description sync is enabled or not.",
        examples=[True],
    )

    column_description_sync_enabled: bool = Field(
        True,
        description="Indicates whether column description sync is enabled or not.",
        examples=[True],
    )


class SnowflakeDescriptionSyncDirective(BaseModel):
    """
    Directive for syncing descriptions to Snowflake.
    """

    entity: str
    docs: str
    operation: str
    propagate: bool


class DescriptionSyncAction(Action):
    """Action to sync DataHub descriptions to Snowflake as table and column comments."""

    def __init__(
        self,
        config: DescriptionSyncConfig,
        ctx: PipelineContext,
        snowflake_helper: SnowflakeTagHelper,
    ):
        super().__init__(ctx)
        self.config = config
        self.ctx = ctx
        self.snowflake_helper = snowflake_helper

    def should_propagate(
        self, event: EventEnvelope
    ) -> Optional[SnowflakeDescriptionSyncDirective]:
        """
        Determines if a description change event should be propagated to Snowflake.
        """
        if self.config.enabled and event.event_type == "EntityChangeEvent_v1":
            assert isinstance(event.event, EntityChangeEvent)
            assert self.ctx.graph is not None

            semantic_event = event.event

            # Only process Snowflake entities
            if not is_snowflake_urn(semantic_event.entityUrn):
                return None

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
                # Description can't be deleted, we ignore these changes
                logger.info("No description found. Skipping description sync.")
                return None

            entity_urn = Urn.create_from_string(semantic_event.entityUrn)

            if not self.config.column_description_sync_enabled and isinstance(
                entity_urn, SchemaFieldUrn
            ):
                logger.info(
                    "Column description sync is disabled. Skipping column description sync"
                )
                return None

            if not self.config.table_description_sync_enabled and isinstance(
                entity_urn, DatasetUrn
            ):
                logger.info(
                    "Table description sync is disabled. Skipping table description sync."
                )
                return None

            if semantic_event.operation in {"ADD", "MODIFY"}:
                return SnowflakeDescriptionSyncDirective(
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

    def process_directive(self, directive: SnowflakeDescriptionSyncDirective) -> None:
        """
        Process a description sync directive by updating the Snowflake object comment.
        """
        try:
            entity_urn = Urn.create_from_string(directive.entity)

            if isinstance(entity_urn, DatasetUrn):
                # Update table comment
                self._update_table_comment(entity_urn, directive.docs)
            elif isinstance(entity_urn, SchemaFieldUrn):
                # Update column comment
                self._update_column_comment(entity_urn, directive.docs)
            else:
                logger.warning(
                    f"Unsupported entity type for description sync: {type(entity_urn)}"
                )

        except Exception as e:
            logger.error(f"Failed to sync description for {directive.entity}: {str(e)}")

    def _update_table_comment(self, dataset_urn: DatasetUrn, description: str) -> None:
        """
        Update a Snowflake table comment.
        """
        try:
            # Extract database.schema.table from the dataset name
            dataset_name = dataset_urn.get_dataset_name()
            parts = dataset_name.split(".")

            if len(parts) >= 3:
                database = parts[0]
                schema = parts[1]
                table = parts[2]

                # Escape single quotes in description
                escaped_description = description.replace("'", "''")

                # Execute ALTER TABLE statement to update comment
                sql = f"ALTER TABLE {database}.{schema}.{table} SET COMMENT = '{escaped_description}'"

                logger.info(f"Updating table comment for {database}.{schema}.{table}")
                self.snowflake_helper._run_query(database, schema, sql)

            else:
                logger.warning(
                    f"Could not parse table name from dataset URN: {dataset_name}"
                )

        except Exception as e:
            logger.error(f"Failed to update table comment: {str(e)}")

    def _update_column_comment(
        self, field_urn: SchemaFieldUrn, description: str
    ) -> None:
        """
        Update a Snowflake column comment.
        """
        try:
            # Get the parent dataset URN
            parent_urn = Urn.create_from_string(field_urn.parent)
            if not isinstance(parent_urn, DatasetUrn):
                logger.warning(
                    f"Parent of schema field is not a dataset: {field_urn.parent}"
                )
                return

            # Parse the dataset URN to get database, schema, and table
            dataset_name = parent_urn.get_dataset_name()
            parts = dataset_name.split(".")

            if len(parts) >= 3:
                database = parts[0]
                schema = parts[1]
                table = parts[2]
                column = field_urn.field_path

                # Escape single quotes in description
                escaped_description = description.replace("'", "''")

                # Execute ALTER TABLE statement to update column comment
                sql = f"ALTER TABLE {database}.{schema}.{table} ALTER COLUMN {column} COMMENT '{escaped_description}'"

                logger.info(
                    f"Updating column comment for {database}.{schema}.{table}.{column}"
                )
                self.snowflake_helper._run_query(database, schema, sql)

            else:
                logger.warning(
                    f"Could not parse table name from dataset URN: {dataset_name}"
                )

        except Exception as e:
            logger.error(f"Failed to update column comment: {str(e)}")

    def act(self, event: EventEnvelope) -> None:
        """
        Process an event and sync descriptions if applicable.
        """
        description_sync_directive = self.should_propagate(event)
        if description_sync_directive and description_sync_directive.propagate:
            logger.info(
                f"Processing description sync for {description_sync_directive.entity}"
            )
            self.process_directive(description_sync_directive)
