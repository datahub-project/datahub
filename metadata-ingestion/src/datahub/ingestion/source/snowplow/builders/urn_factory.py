"""
URN Factory for Snowplow entities.

This module centralizes URN construction for all Snowplow entity types,
providing a single source of truth for URN formats and ensuring consistency
across the connector.
"""

import logging
from typing import Optional

from datahub.emitter.mce_builder import (
    make_container_urn,
    make_dataset_urn,
    make_dataset_urn_with_platform_instance,
)
from datahub.ingestion.source.snowplow.builders.container_keys import (
    SnowplowOrganizationKey,
    SnowplowTrackingScenarioKey,
)
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.ingestion.source.snowplow.types import (
    ContainerURN,
    DatasetURN,
)

logger = logging.getLogger(__name__)


class SnowplowURNFactory:
    """
    Factory for constructing DataHub URNs for Snowplow entities.

    Centralizes URN construction logic to ensure consistency and make it
    easy to change URN formats if needed.

    This factory handles URN construction for:
    - Organization containers
    - Schema datasets
    - Event specification datasets
    - Tracking scenario containers
    - Warehouse table datasets (with destination mapping support)
    """

    def __init__(
        self,
        platform: str,
        config: SnowplowSourceConfig,
    ):
        """
        Initialize URN factory.

        Args:
            platform: Platform name (e.g., "snowplow")
            config: Source configuration containing platform_instance, env, etc.
        """
        self.platform = platform
        self.config = config

    def make_organization_urn(self, org_id: str) -> ContainerURN:
        """
        Generate URN for organization container.

        Args:
            org_id: Organization UUID from Snowplow BDP

        Returns:
            Container URN for organization
        """
        org_key = SnowplowOrganizationKey(
            organization_id=org_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        return ContainerURN(org_key.as_urn())

    def make_schema_dataset_urn(
        self,
        vendor: str,
        name: str,
        version: str,
    ) -> DatasetURN:
        """
        Generate dataset URN for schema.

        By default (include_version_in_urn=False), version is NOT included in URN.
        This creates a single dataset entity per schema, with version tracked in properties.

        When include_version_in_urn=True (legacy), version is included in URN for
        backwards compatibility with existing metadata.

        Args:
            vendor: Schema vendor (e.g., "com.acme")
            name: Schema name (e.g., "checkout_started")
            version: Schema version (e.g., "1-0-0")

        Returns:
            Dataset URN for schema
        """
        if self.config.include_version_in_urn:
            # Legacy behavior: version in URN
            dataset_name = f"{vendor}.{name}.{version}".replace("/", ".")
        else:
            # New behavior: version in properties only
            dataset_name = f"{vendor}.{name}".replace("/", ".")

        return DatasetURN(
            make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=dataset_name,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
        )

    def make_event_spec_dataset_urn(self, event_spec_id: str) -> DatasetURN:
        """
        Generate dataset URN for event specification.

        Args:
            event_spec_id: Event specification UUID from Snowplow BDP

        Returns:
            Dataset URN for event specification
        """
        return DatasetURN(
            make_dataset_urn_with_platform_instance(
                platform=self.platform,
                name=f"event_spec.{event_spec_id}",
                platform_instance=self.config.platform_instance,
                env=self.config.env,
            )
        )

    def make_tracking_scenario_urn(self, scenario_id: str) -> ContainerURN:
        """
        Generate container URN for tracking scenario.

        Args:
            scenario_id: Tracking scenario UUID from Snowplow BDP

        Returns:
            Container URN for tracking scenario
        """
        if not self.config.bdp_connection:
            # Fallback for non-BDP mode (shouldn't happen)
            return ContainerURN(make_container_urn(f"snowplow_scenario_{scenario_id}"))

        org_id = self.config.bdp_connection.organization_id
        scenario_key = SnowplowTrackingScenarioKey(
            organization_id=org_id,
            scenario_id=scenario_id,
            platform=self.platform,
            instance=self.config.platform_instance,
            env=self.config.env,
        )
        return ContainerURN(scenario_key.as_urn())

    def construct_warehouse_urn(
        self,
        query_engine: str,
        table_name: str,
        destination_id: Optional[str] = None,
    ) -> DatasetURN:
        """
        Construct warehouse table URN with optional platform instance.

        Supports destination-specific mappings for multi-warehouse scenarios where
        different Snowplow destinations write to different warehouse instances.

        Args:
            query_engine: Query engine type (snowflake, bigquery, databricks, redshift)
            table_name: Fully qualified table name (e.g., database.schema.table)
            destination_id: Optional Snowplow destination UUID for mapping lookup

        Returns:
            DataHub URN for warehouse table
        """
        # Find mapping for this destination
        platform_instance = None
        env = self.config.warehouse_lineage.env

        if destination_id:
            for mapping in self.config.warehouse_lineage.destination_mappings:
                if mapping.destination_id == destination_id:
                    platform_instance = mapping.platform_instance
                    env = mapping.env
                    logger.debug(
                        f"Using destination mapping for {destination_id}: "
                        f"platform_instance={platform_instance}, env={env}"
                    )
                    break

        # Fallback to global config
        if platform_instance is None:
            platform_instance = self.config.warehouse_lineage.platform_instance

        # Build dataset name with optional platform instance prefix
        dataset_name = table_name
        if platform_instance:
            dataset_name = f"{platform_instance}.{table_name}"

        urn = make_dataset_urn(
            platform=query_engine,
            name=dataset_name,
            env=env,
        )

        logger.debug(
            f"Constructed warehouse URN: {urn} "
            f"(query_engine={query_engine}, table_name={table_name}, "
            f"platform_instance={platform_instance})"
        )

        return DatasetURN(urn)
