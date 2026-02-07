"""
Column-level lineage builder for Snowplow connector.

Handles mapping of Iglu schema fields to Snowflake atomic.events VARIANT columns.
"""

import logging
from typing import TYPE_CHECKING, Iterable, Optional

from datahub.emitter.mce_builder import make_schema_field_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.builders.urn_factory import SnowplowURNFactory
from datahub.ingestion.source.snowplow.snowplow_config import SnowplowSourceConfig
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    FineGrainedLineageClass,
    FineGrainedLineageDownstreamTypeClass,
    FineGrainedLineageUpstreamTypeClass,
    SchemaMetadataClass,
    UpstreamClass,
    UpstreamLineageClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import IngestionState

logger = logging.getLogger(__name__)


class ColumnLineageBuilder:
    """
    Builder for emitting column-level lineage from Iglu schemas to Snowflake VARIANT columns.

    This builder handles the mapping between:
    - Iglu schema fields (fine-grained metadata)
    - Snowflake atomic.events VARIANT columns (coarse storage format)

    Since DataHub's Snowflake connector doesn't parse VARIANT sub-fields, we create
    explicit fine-grained lineage showing which Iglu fields feed into each VARIANT column.
    """

    def __init__(
        self,
        config: SnowplowSourceConfig,
        urn_factory: SnowplowURNFactory,
        state: "IngestionState",
    ):
        """
        Initialize column lineage builder.

        Args:
            config: Source configuration
            urn_factory: URN factory for creating field URNs
            state: Shared mutable state containing warehouse table URN and parsed events URN
        """
        self.config = config
        self.urn_factory = urn_factory
        self.state = state

    def get_warehouse_table_urn(self) -> Optional[str]:
        """
        Get cached warehouse table URN.

        Returns:
            URN of warehouse atomic.events table, or None if not configured
        """
        return self.state.warehouse_table_urn

    def emit_column_lineage(
        self,
        dataset_urn: str,
        vendor: str,
        name: str,
        version: str,
        schema_metadata: SchemaMetadataClass,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Emit column-level lineage from Iglu schema fields to atomic.events Snowflake VARIANT column.

        Maps all fields from an Iglu schema to the single VARIANT column in Snowflake.
        Since DataHub's Snowflake connector doesn't parse VARIANT sub-fields, we create
        FineGrainedLineage showing that all Iglu fields feed into the VARIANT column.

        Args:
            dataset_urn: URN of the Iglu schema dataset
            vendor: Schema vendor
            name: Schema name
            version: Schema version
            schema_metadata: Parsed schema metadata with fields

        Yields:
            MetadataWorkUnit with UpstreamLineage containing FineGrainedLineage
        """
        # Get warehouse table URN (atomic.events from Snowflake)
        warehouse_table_urn = self.get_warehouse_table_urn()

        if not warehouse_table_urn:
            logger.debug(
                f"No warehouse table configured, skipping column lineage for {vendor}/{name}"
            )
            return

        # Check that parsed events dataset exists (intermediate hop)
        if not self.state.parsed_events_urn:
            logger.debug(
                f"Parsed events dataset not created, skipping column lineage for {vendor}/{name}"
            )
            return

        # Extract field paths from schema metadata
        if not schema_metadata.fields:
            logger.debug(f"No fields in schema metadata for {vendor}/{name}")
            return

        # Get the Snowflake VARIANT column name
        snowflake_column = self.map_schema_to_snowflake_column(
            vendor=vendor,
            name=name,
            version=version,
        )

        # Create URNs for all Iglu fields
        iglu_field_urns = [
            make_schema_field_urn(dataset_urn, field.fieldPath)
            for field in schema_metadata.fields
        ]

        # Create URN for Snowflake VARIANT column
        snowflake_field_urn = make_schema_field_urn(
            warehouse_table_urn, snowflake_column
        )

        # Create single FineGrainedLineage mapping all Iglu fields to VARIANT column
        fine_grained_lineage = FineGrainedLineageClass(
            upstreamType=FineGrainedLineageUpstreamTypeClass.FIELD_SET,
            upstreams=iglu_field_urns,  # All Iglu schema fields
            downstreamType=FineGrainedLineageDownstreamTypeClass.FIELD_SET,
            downstreams=[snowflake_field_urn],  # Single Snowflake VARIANT column
        )

        # Create UpstreamLineage with FineGrainedLineage
        # Use parsed events dataset as upstream (not the Iglu schema directly)
        # This creates the correct flow: Schema → Parsed Events → Warehouse
        upstream = UpstreamClass(
            dataset=self.state.parsed_events_urn,  # Parsed events dataset (intermediate)
            type=DatasetLineageTypeClass.TRANSFORMED,
        )

        upstream_lineage = UpstreamLineageClass(
            upstreams=[upstream],
            fineGrainedLineages=[fine_grained_lineage],
        )

        logger.info(
            f"Emitting column-level lineage: {len(iglu_field_urns)} Iglu fields "
            f"from {vendor}/{name} → Snowflake VARIANT column '{snowflake_column}'"
        )

        yield MetadataChangeProposalWrapper(
            entityUrn=warehouse_table_urn,  # Lineage is attached to downstream (warehouse table)
            aspect=upstream_lineage,
        ).as_workunit()

    def map_schema_to_snowflake_column(
        self, vendor: str, name: str, version: str
    ) -> str:
        """
        Map Iglu schema to Snowflake atomic.events VARIANT column name.

        Snowflake stores context/entity schemas as VARIANT columns:
        contexts_{vendor}_{name}_{major_version}

        The VARIANT column contains JSON with all schema fields nested inside.
        Since DataHub's Snowflake connector does NOT parse VARIANT sub-fields,
        all fields from an Iglu schema map to the same VARIANT column.

        Examples:
        - vendor=com.acme, name=checkout_started, version=1-0-0
          → contexts_com_acme_checkout_started_1
        - vendor=com.snowplowanalytics.snowplow, name=web_page, version=1-0-0
          → contexts_com_snowplowanalytics_snowplow_web_page_1

        Args:
            vendor: Schema vendor (e.g., "com.acme")
            name: Schema name (e.g., "checkout_started")
            version: Schema version (e.g., "1-0-0")

        Returns:
            Snowflake VARIANT column name
        """
        # Replace dots and slashes with underscores
        vendor_clean = vendor.replace(".", "_").replace("/", "_")
        name_clean = name.replace(".", "_").replace("/", "_")

        # Version: Keep only major version (1-0-0 → 1)
        major_version = (
            version.split("-")[0] if "-" in version else version.split(".")[0]
        )

        # Build Snowflake VARIANT column name
        return f"contexts_{vendor_clean}_{name_clean}_{major_version}"
