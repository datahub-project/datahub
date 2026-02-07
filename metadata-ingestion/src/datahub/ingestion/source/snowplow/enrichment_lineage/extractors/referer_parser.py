"""
Referer Parser enrichment lineage extractor.

Extracts field-level lineage for the Referer Parser enrichment, which extracts marketing
attribution data from referrer URLs (search engines, social media, etc.).

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/referrer-parser-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/referer_parser/jsonschema/2-0-0

Field Mapping Reference:
/connectors-accelerator/SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md#4-referer-parser-enrichment
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentFieldInfo,
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class RefererParserLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for Referer Parser enrichment lineage.

    Input Fields:
    - page_referrer (atomic field, HTTP Referer header)

    Output Fields:
    - refr_medium (search, social, internal, unknown, email)
    - refr_source (Google, Facebook, Twitter, etc.)
    - refr_term (search keywords if source is search engine)

    Configuration Impact:
    None - uses built-in referer database, no configuration affects field mappings.
    """

    # Input field name
    INPUT_FIELD = "page_referrer"

    # Output fields (all atomic, always present)
    OUTPUT_FIELDS = [
        "refr_medium",  # Type of referer
        "refr_source",  # Name of referer if recognized
        "refr_term",  # Keywords if source is search engine
    ]

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles Referer Parser enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for Referer Parser enrichment
        """
        return "referer_parser" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for Referer Parser enrichment.

        Args:
            enrichment: Referer Parser enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of field lineages (page_referrer → refr_* fields)
        """
        if not warehouse_table_urn:
            return []

        lineages = self._create_simple_lineages(
            input_field=self.INPUT_FIELD,
            output_fields=self.OUTPUT_FIELDS,
            event_schema_urn=event_schema_urn,
            warehouse_table_urn=warehouse_table_urn,
        )

        logger.debug(
            f"Referer Parser: Extracted {len(lineages)} field lineages ({self.INPUT_FIELD} → refr_*)"
        )

        return lineages

    def get_field_info(self, enrichment: Enrichment) -> EnrichmentFieldInfo:
        """
        Get field information for Referer Parser enrichment.

        This enrichment always uses the same input/output fields regardless of configuration.
        """
        return EnrichmentFieldInfo(
            input_fields=[self.INPUT_FIELD],
            output_fields=list(self.OUTPUT_FIELDS),
            transformation_description="Parses HTTP referer to extract traffic source",
        )
