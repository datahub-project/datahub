"""
Warehouse Lineage Processor for Snowplow connector.

Handles extraction of warehouse lineage from Snowplow BDP Data Models API.
"""

import logging
import traceback
from typing import TYPE_CHECKING, Iterable

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import (
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class WarehouseLineageProcessor(EntityProcessor):
    """
    Processor for extracting warehouse lineage metadata from Snowplow BDP.

    Extracts lineage from atomic events table to derived data models
    in the warehouse.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize warehouse lineage processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if warehouse lineage extraction is enabled."""
        return (
            self.config.warehouse_lineage.enabled and self.deps.bdp_client is not None
        )

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage to warehouse destinations via Data Models API.

        Creates lineage from enrichment outputs to warehouse tables by:
        1. Fetching data products and their data models
        2. Extracting destination warehouse info (query engine, table name)
        3. Constructing warehouse table URNs
        4. Creating lineage edges

        This approach uses the DataHub Graph API to validate warehouse URNs
        and avoids needing warehouse credentials in the Snowplow connector.

        Yields:
            MetadataWorkUnit: Warehouse lineage metadata work units
        """
        if not self.config.warehouse_lineage.enabled:
            yield from []  # Empty generator
            return

        if not self.deps.bdp_client:
            logger.warning(
                "Warehouse lineage enabled but BDP client not available. Skipping."
            )
            yield from []  # Empty generator
            return

        logger.info("Extracting warehouse lineage via Data Models API...")

        try:
            # Get all data products
            data_products = self.deps.bdp_client.get_data_products()
            logger.info(
                f"Found {len(data_products)} data products, checking for data models..."
            )

            total_models = 0
            total_lineage_created = 0

            for data_product in data_products:
                # Get data models for this product
                data_models = self.deps.bdp_client.get_data_models(data_product.id)

                if not data_models:
                    logger.debug(
                        f"No data models configured for data product '{data_product.name}'"
                    )
                    continue

                logger.info(
                    f"Data product '{data_product.name}' has {len(data_models)} data models"
                )
                total_models += len(data_models)

                for data_model in data_models:
                    # Skip if model doesn't have destination info
                    if not data_model.destination or not data_model.query_engine:
                        logger.debug(
                            f"Skipping data model '{data_model.name}': missing destination or query_engine"
                        )
                        continue

                    if not data_model.table_name:
                        logger.warning(
                            f"Data model '{data_model.name}' has destination but no table_name, skipping"
                        )
                        continue

                    # Construct warehouse table URN
                    warehouse_urn = self.urn_factory.construct_warehouse_urn(
                        query_engine=data_model.query_engine,
                        table_name=data_model.table_name,
                        destination_id=data_model.destination,
                    )

                    # Optional: Validate URN exists in DataHub
                    if self.config.warehouse_lineage.validate_urns:
                        if self.deps.graph:
                            exists = self.deps.graph.exists(warehouse_urn)
                            if not exists:
                                logger.warning(
                                    f"Warehouse table {warehouse_urn} not found in DataHub. "
                                    f"Lineage will be created but may appear broken until warehouse is ingested. "
                                    f"Set validate_urns=False to skip this check."
                                )
                        else:
                            logger.debug(
                                "Graph client not available, skipping URN validation"
                            )

                    # Get source table URN (atomic.events where raw events land)
                    source_table_urn = self.state.warehouse_table_urn

                    if source_table_urn:
                        # Create lineage: atomic.events → derived table
                        upstream = UpstreamClass(
                            dataset=source_table_urn,
                            type=DatasetLineageTypeClass.TRANSFORMED,
                        )

                        upstream_lineage = UpstreamLineageClass(upstreams=[upstream])

                        yield MetadataChangeProposalWrapper(
                            entityUrn=warehouse_urn,
                            aspect=upstream_lineage,
                        ).as_workunit()

                        logger.info(
                            f"Created warehouse lineage: {source_table_urn} → {warehouse_urn} (via data model '{data_model.name}')"
                        )
                        total_lineage_created += 1
                    else:
                        logger.warning(
                            f"Cannot create lineage for data model '{data_model.name}': "
                            "source warehouse table URN (atomic.events) not available. "
                            "Ensure warehouse destination is configured in BDP."
                        )

            logger.info(
                f"Warehouse lineage extraction complete: "
                f"{total_models} data models processed, "
                f"{total_lineage_created} lineage edges created"
            )

        except Exception as e:
            self.report.warning(
                title="Failed to extract warehouse lineage via data models",
                message="Unable to retrieve data models from BDP API for lineage extraction. This is optional and won't affect other metadata.",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
                exc=e,
            )
            traceback.print_exc()
