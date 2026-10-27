import logging
import uuid
from typing import Dict, List, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import DataProductPropertiesClass

logger = logging.getLogger(__name__)


def _is_uuid(value: str) -> bool:
    try:
        uuid.UUID(value)
    except ValueError:
        return False
    return True


class DataProductRegistry:
    """Resolve Data Product identifiers (name or id) to URNs using DataHub."""

    def __init__(
        self,
        cached_data_products: Optional[List[str]] = None,
        graph: Optional[DataHubGraph] = None,
    ):
        self.data_product_registry: Dict[str, str] = {}
        if cached_data_products:
            # Isolate identifiers that don't look fully specified (URN or UUID id).
            needing_resolution = [
                d
                for d in cached_data_products
                if (not d.startswith("urn:li:dataProduct") and not _is_uuid(d))
            ]
            if needing_resolution and not graph:
                raise ValueError(
                    f"Following data products need server-side resolution {needing_resolution} "
                    f"but a DataHub server wasn't provided. Either use a fully qualified data "
                    f"product urn (e.g. urn:li:dataProduct:my_product) or a UUID id, or ensure "
                    f"the CLI is connected to DataHub (e.g. via datahub init)."
                )
            for identifier in needing_resolution:
                assert graph
                maybe_urn = f"urn:li:dataProduct:{identifier}"
                maybe_properties = graph.get_aspect(
                    maybe_urn, DataProductPropertiesClass
                )
                if maybe_properties:
                    self.data_product_registry[identifier] = maybe_urn
                else:
                    data_product_urn = graph.get_data_product_urn_by_name(identifier)
                    if data_product_urn:
                        self.data_product_registry[identifier] = data_product_urn
                    else:
                        logger.error(
                            f"Failed to retrieve data product id for {identifier}"
                        )
                        raise ValueError(
                            f"data product {identifier} doesn't seem to be provisioned on "
                            f"DataHub. Either provision it first and re-run, or provide a "
                            f"fully qualified data product urn "
                            f"(e.g. urn:li:dataProduct:my_product) to skip this check."
                        )

    def get_data_product_urn(self, data_product_identifier: str) -> str:
        return (
            self.data_product_registry.get(data_product_identifier)
            or data_product_identifier
        )
