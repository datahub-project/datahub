"""
Data Product MCP Builder

Creates DataHub MCPs for data products.
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.data_product.ast import DataHubDataProduct

logger = logging.getLogger(__name__)


class DataProductMCPBuilder(EntityMCPBuilder[DataHubDataProduct]):
    """
    Creates MCPs for data products.

    Note: Data products require a domain. Products without domains are skipped.
    """

    @property
    def entity_type(self) -> str:
        return "data_product"

    def build_mcps(
        self, product: DataHubDataProduct, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single data product."""
        from datahub.api.entities.dataproduct.dataproduct import DataProduct

        # Convert domain name to domain URN if needed
        domain_urn = product.domain
        if domain_urn and not domain_urn.startswith("urn:li:domain:"):
            domain_urn = f"urn:li:domain:{domain_urn}"

        # DataProduct requires a domain and generate_mcp() fails with empty string
        # Skip data products without a domain
        if not domain_urn:
            logger.warning(
                f"Skipping data product {product.name}: domain is required but not provided"
            )
            return []

        # Convert owner to proper format (supports custom owner types)
        owners = []
        if product.owner:
            # product.owner is already a URN from the converter
            owner_urn = product.owner
            # Get owner type - must be provided (supports custom types)
            owner_type = getattr(product, "owner_type", None)
            if not owner_type:
                # Owner is optional for data products - skip if no type
                logger.warning(
                    f"Data product '{product.name}' has owner {product.owner} but no owner type. "
                    f"Skipping owner assignment. Add dh:hasOwnerType to owner in RDF (supports custom owner types)."
                )
            else:
                owners.append({"id": owner_urn, "type": owner_type})

        # Prepare properties
        properties = product.properties.copy() if hasattr(product, "properties") else {}
        if hasattr(product, "sla") and product.sla:
            properties["sla"] = product.sla
        if hasattr(product, "quality_score") and product.quality_score:
            properties["quality_score"] = str(product.quality_score)

        # Convert all property values to strings
        string_properties = {}
        for key, value in properties.items():
            string_properties[key] = str(value)

        try:
            # Create DataProduct using modern API
            datahub_data_product = DataProduct(
                id=product.name.lower().replace(" ", "_").replace("-", "_"),
                display_name=product.name,
                domain=domain_urn,  # Required - we've already validated it exists
                description=product.description or f"Data Product: {product.name}",
                assets=getattr(product, "assets", []),
                owners=owners,
                properties=string_properties,
            )

            # Generate MCPs
            return list(datahub_data_product.generate_mcp(upsert=False))

        except Exception as e:
            logger.error(f"Failed to create MCP for data product {product.name}: {e}")
            return []

    def build_all_mcps(
        self, products: List[DataHubDataProduct], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for all data products."""
        mcps = []

        for product in products:
            product_mcps = self.build_mcps(product, context)
            mcps.extend(product_mcps)

        logger.info(f"Built {len(mcps)} data product MCPs")
        return mcps
