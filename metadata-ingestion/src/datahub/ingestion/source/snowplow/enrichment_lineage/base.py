"""
Base classes for extracting fine-grained lineage from Snowplow enrichments.

This module provides the abstract base class that all enrichment lineage extractors
must implement, along with the FieldLineage dataclass for representing field-level
transformations.

Documentation:
- Snowplow Enrichments Overview: https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/
- Field Mapping Reference: /connectors-accelerator/SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from datahub.ingestion.source.snowplow.snowplow_models import Enrichment


@dataclass
class FieldLineage:
    """
    Represents input → output field mapping for an enrichment.

    This captures which source fields an enrichment reads from and which
    destination fields it writes to, along with the type of transformation.
    """

    upstream_fields: List[str]
    """List of input field URNs (e.g., 'urn:li:schemaField:(...,user_ipaddress)')"""

    downstream_fields: List[str]
    """List of output field URNs (e.g., 'urn:li:schemaField:(...,geo_country)')"""

    transformation_type: str
    """
    Type of transformation performed:
    - DIRECT: Direct mapping with minimal transformation
    - DERIVED: Derived/computed value from input(s)
    - AGGREGATED: Aggregation of multiple inputs
    - IN_PLACE: Input field is modified in-place
    """


class EnrichmentLineageExtractor(ABC):
    """
    Abstract base class for extracting field-level lineage from Snowplow enrichments.

    Each enrichment type has its own extractor implementation that understands:
    1. Which fields the enrichment reads from (inputs)
    2. Which fields the enrichment writes to (outputs)
    3. How the enrichment configuration affects field mappings

    Example implementations:
    - IpLookupLineageExtractor: user_ipaddress → geo_country, geo_region, etc.
    - CampaignAttributionLineageExtractor: page_url[utm_*] → mkt_* fields
    """

    @abstractmethod
    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for this enrichment.

        Args:
            enrichment: The enrichment configuration from Snowplow API
            event_schema_urn: URN of the input event schema (upstream)
            warehouse_table_urn: URN of the output warehouse table (downstream)

        Returns:
            List of FieldLineage objects representing input→output mappings
        """
        pass

    @abstractmethod
    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles the given enrichment schema.

        Args:
            enrichment_schema: Iglu schema URI (e.g., 'iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0')

        Returns:
            True if this extractor can handle this enrichment type
        """
        pass
