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
from typing import List, Optional, Sequence

from datahub.ingestion.source.snowplow.enrichment_lineage.utils import make_field_urn
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment


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


@dataclass
class EnrichmentFieldInfo:
    """
    Field information for an enrichment, used for building descriptions.

    This captures which fields an enrichment reads from (inputs) and which
    fields it produces (outputs), based on the enrichment configuration.
    Unlike FieldLineage which contains URNs, this contains simple field names.
    """

    input_fields: List[str]
    """List of input field names (e.g., ['user_ipaddress'])"""

    output_fields: List[str]
    """List of output field names (e.g., ['geo_country', 'geo_region', 'geo_city'])"""

    transformation_description: Optional[str] = None
    """Optional description of the transformation (e.g., 'IP geolocation lookup')"""


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

    Subclasses should use the helper methods provided:
    - _create_simple_lineages(): For simple one-input-to-many-outputs patterns
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

    def get_field_info(self, enrichment: Enrichment) -> EnrichmentFieldInfo:
        """
        Get field information for this enrichment based on its configuration.

        This method returns the input and output field names that this enrichment
        processes, based on the enrichment's configuration. This is used to build
        descriptions without needing to parse URNs from fine-grained lineages.

        Subclasses should override this method to provide specific field information
        based on the enrichment configuration.

        Args:
            enrichment: The enrichment configuration from Snowplow API

        Returns:
            EnrichmentFieldInfo with input/output field names
        """
        return EnrichmentFieldInfo(input_fields=[], output_fields=[])

    def _create_simple_lineages(
        self,
        input_field: str,
        output_fields: Sequence[str],
        event_schema_urn: str,
        warehouse_table_urn: str,
        transformation_type: str = "DERIVED",
    ) -> List[FieldLineage]:
        """
        Create lineages for a simple one-input-to-many-outputs pattern.

        This helper handles the common case where a single input field is
        transformed into multiple output fields (e.g., page_referrer → refr_medium,
        refr_source, refr_term).

        Args:
            input_field: Name of the input field (e.g., "page_referrer")
            output_fields: List of output field names (e.g., ["refr_medium", "refr_source"])
            event_schema_urn: URN of the input event schema
            warehouse_table_urn: URN of the output warehouse table
            transformation_type: Type of transformation (default: "DERIVED")

        Returns:
            List of FieldLineage objects, one per output field
        """
        upstream_field_urn = make_field_urn(event_schema_urn, input_field)

        return [
            FieldLineage(
                upstream_fields=[upstream_field_urn],
                downstream_fields=[make_field_urn(warehouse_table_urn, output_field)],
                transformation_type=transformation_type,
            )
            for output_field in output_fields
        ]
