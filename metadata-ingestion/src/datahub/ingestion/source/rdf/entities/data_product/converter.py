"""
Data Product Converter

Converts RDF data products to DataHub format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.data_product.ast import (
    DataHubDataProduct,
    RDFDataProduct,
)
from datahub.ingestion.source.rdf.entities.data_product.urn_generator import (
    DataProductUrnGenerator,
)
from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
    DatasetUrnGenerator,  # For dataset URNs
)
from datahub.ingestion.source.rdf.entities.domain.urn_generator import (
    DomainUrnGenerator,  # For domain URNs
)

logger = logging.getLogger(__name__)


class DataProductConverter(EntityConverter[RDFDataProduct, DataHubDataProduct]):
    """
    Converts RDF data products to DataHub data products.
    """

    def __init__(self):
        """Initialize the converter with entity-specific generators."""
        # Use entity-specific generators
        self.product_urn_generator = DataProductUrnGenerator()
        self.dataset_urn_generator = DatasetUrnGenerator()
        self.domain_urn_generator = DomainUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "data_product"

    def convert(
        self, rdf_product: RDFDataProduct, context: Dict[str, Any] = None
    ) -> Optional[DataHubDataProduct]:
        """Convert an RDF data product to DataHub format."""
        try:
            environment = context.get("environment", "PROD") if context else "PROD"

            # Generate URN using entity-specific generator
            product_urn = self.product_urn_generator.generate_data_product_urn(
                rdf_product.uri
            )

            # Convert domain
            domain_urn = None
            if rdf_product.domain:
                # Handle both IRI format and path string format
                domain_str = rdf_product.domain
                if "/" in domain_str and not (
                    domain_str.startswith("http://")
                    or domain_str.startswith("https://")
                ):
                    # Path string format (e.g., "TRADING/FIXED_INCOME")
                    domain_path = tuple(domain_str.split("/"))
                else:
                    # IRI format - convert to path segments tuple
                    domain_path = tuple(
                        self.domain_urn_generator.derive_path_from_iri(
                            domain_str, include_last=True
                        )
                    )
                domain_urn = self.domain_urn_generator.generate_domain_urn(domain_path)

            # Convert owner (using base class method available on all generators)
            owner_urn = None
            if rdf_product.owner:
                owner_urn = (
                    self.product_urn_generator.generate_corpgroup_urn_from_owner_iri(
                        rdf_product.owner
                    )
                )

            # Convert assets - platform will default to "logical" if None via URN generator
            asset_urns = []
            for asset in rdf_product.assets:
                asset_urn = self.dataset_urn_generator.generate_dataset_urn(
                    asset.uri, asset.platform, environment
                )
                asset_urns.append(asset_urn)

            return DataHubDataProduct(
                urn=product_urn,
                name=rdf_product.name,
                description=rdf_product.description,
                domain=domain_urn,
                owner=owner_urn,
                owner_type=rdf_product.owner_type,  # Owner type from RDF (supports custom types)
                assets=asset_urns,
                properties=rdf_product.properties or {},
            )

        except Exception as e:
            logger.warning(f"Error converting data product {rdf_product.name}: {e}")
            return None

    def convert_all(
        self, rdf_products: List[RDFDataProduct], context: Dict[str, Any] = None
    ) -> List[DataHubDataProduct]:
        """Convert all RDF data products to DataHub format."""
        datahub_products = []

        for rdf_product in rdf_products:
            datahub_product = self.convert(rdf_product, context)
            if datahub_product:
                datahub_products.append(datahub_product)

        logger.info(f"Converted {len(datahub_products)} data products")
        return datahub_products
