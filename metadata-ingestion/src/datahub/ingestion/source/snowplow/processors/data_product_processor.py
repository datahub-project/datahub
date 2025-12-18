"""
Data Product Processor for Snowplow connector.

Handles extraction of data products from Snowplow BDP.
"""

import logging
from typing import TYPE_CHECKING, Iterable

from datahub.emitter.mce_builder import make_container_urn, make_user_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_containers
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowplow.container_keys import SnowplowDataProductKey
from datahub.ingestion.source.snowplow.processors.base import EntityProcessor
from datahub.metadata.schema_classes import (
    ContainerClass,
    OwnerClass,
    OwnershipClass,
    OwnershipSourceClass,
    OwnershipSourceTypeClass,
    OwnershipTypeClass,
)

if TYPE_CHECKING:
    from datahub.ingestion.source.snowplow.dependencies import (
        IngestionState,
        ProcessorDependencies,
    )

logger = logging.getLogger(__name__)


class DataProductProcessor(EntityProcessor):
    """
    Processor for extracting data product metadata from Snowplow BDP.

    Data products represent logical groupings of event specifications
    and tracking scenarios.
    """

    def __init__(self, deps: "ProcessorDependencies", state: "IngestionState"):
        """
        Initialize data product processor.

        Args:
            deps: Explicit dependencies needed by this processor
            state: Shared mutable state populated during extraction
        """
        super().__init__(deps, state)

    def is_enabled(self) -> bool:
        """Check if data product extraction is enabled."""
        return self.config.extract_data_products and self.deps.bdp_client is not None

    def extract(self) -> Iterable[MetadataWorkUnit]:
        """
        Extract data products from BDP Console API.

        Data products are high-level groupings of event specifications with
        ownership, domain, and access information. They help organize tracking
        design at a business domain level.

        Each data product is represented as a Container with subtype "Data Product".
        Event specifications are linked to data products via container relationships.

        Yields:
            MetadataWorkUnit: Data product metadata work units
        """
        if not self.deps.bdp_client:
            return

        try:
            # Fetch data products from API
            data_products = self.deps.bdp_client.get_data_products()
            self.report.num_data_products_found = len(data_products)
            logger.info(f"Found {len(data_products)} data products")

            for product in data_products:
                # Apply filtering
                if not self.config.data_product_pattern.allowed(product.id):
                    logger.debug(f"Skipping filtered data product: {product.id}")
                    self.report.num_data_products_filtered += 1
                    continue

                # Get organization ID (checked at source init)
                org_id = (
                    self.config.bdp_connection.organization_id
                    if self.config.bdp_connection
                    else ""
                )

                # Create data product container
                product_key = SnowplowDataProductKey(
                    organization_id=org_id,
                    product_id=product.id,
                    platform=self.deps.platform,
                )

                # Build custom properties for additional metadata
                custom_properties = {}
                if product.access_instructions:
                    custom_properties["accessInstructions"] = (
                        product.access_instructions
                    )
                if product.source_applications:
                    custom_properties["sourceApplications"] = ", ".join(
                        product.source_applications
                    )
                if product.type:
                    custom_properties["type"] = product.type
                if product.lock_status:
                    custom_properties["lockStatus"] = product.lock_status
                if product.status:
                    custom_properties["status"] = product.status
                if product.created_at:
                    custom_properties["createdAt"] = product.created_at
                if product.updated_at:
                    custom_properties["updatedAt"] = product.updated_at

                # Emit container with properties (SDK V2 pattern via gen_containers)
                yield from gen_containers(
                    container_key=product_key,
                    name=product.name,
                    sub_types=["Data Product"],  # Custom subtype for data products
                    domain_urn=None,  # Domain URL is not available from API
                    description=product.description,
                    owner_urn=None,  # Owner is email, handled separately below
                    external_url=None,
                    tags=None,
                    extra_properties=custom_properties if custom_properties else None,
                )

                # Create container URN
                dataset_urn = str(make_container_urn(guid=product_key.guid()))

                # Add ownership if owner specified
                if product.owner:
                    ownership_aspect = OwnershipClass(
                        owners=[
                            OwnerClass(
                                owner=make_user_urn(product.owner),
                                type=OwnershipTypeClass.DATAOWNER,
                                source=OwnershipSourceClass(
                                    type=OwnershipSourceTypeClass.SERVICE,
                                    url=None,
                                ),
                            )
                        ]
                    )
                    yield MetadataChangeProposalWrapper(
                        entityUrn=dataset_urn,
                        aspect=ownership_aspect,
                    ).as_workunit()

                # Link event specifications to this data product container
                if product.event_specs:
                    product_container_urn = str(
                        make_container_urn(guid=product_key.guid())
                    )

                    for event_spec_ref in product.event_specs:
                        # Extract event spec ID from reference object
                        event_spec_id = event_spec_ref.id

                        # Only link to event specs that were actually emitted (not filtered)
                        if event_spec_id not in self.state.emitted_event_spec_ids:
                            logger.debug(
                                f"Skipping container link for filtered event spec {event_spec_id} in data product {product.name}"
                            )
                            continue

                        # Create event spec dataset URN
                        event_spec_urn = self.urn_factory.make_event_spec_dataset_urn(
                            event_spec_id
                        )

                        # Link event spec to data product container
                        container_aspect = ContainerClass(
                            container=product_container_urn
                        )
                        yield MetadataChangeProposalWrapper(
                            entityUrn=event_spec_urn,
                            aspect=container_aspect,
                        ).as_workunit()

                logger.debug(f"Extracted data product: {product.name} ({product.id})")
                self.report.num_data_products_extracted += 1

        except Exception as e:
            self.deps.error_handler.handle_api_error(
                error=e,
                operation="extract data products",
                context=f"organization_id={self.config.bdp_connection.organization_id if self.config.bdp_connection else 'N/A'}",
            )
