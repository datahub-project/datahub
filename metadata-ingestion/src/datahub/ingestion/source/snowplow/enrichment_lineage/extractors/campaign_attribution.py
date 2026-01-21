"""
Campaign Attribution enrichment lineage extractor.

Extracts field-level lineage for the Campaign Attribution enrichment, which populates
marketing campaign fields from URL query string parameters.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/campaign-attribution-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/campaign_attribution/jsonschema/1-0-1

Output Fields:
- mkt_medium: The advertising or marketing medium (e.g., banner, email newsletter)
- mkt_source: The advertiser, site, publication sending traffic
- mkt_term: Keywords/search terms
- mkt_content: Differentiates similar content or links within same ad
- mkt_campaign: Campaign name, slogan, promo code, etc.
- mkt_clickid: Click ID which resulted in the redirect
- mkt_network: Advertising network name
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


class CampaignAttributionLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for Campaign Attribution enrichment lineage.

    Input Fields:
    - page_urlquery (atomic field, URL query string containing campaign parameters)

    Output Fields:
    - mkt_medium (advertising medium)
    - mkt_source (traffic source)
    - mkt_term (keywords)
    - mkt_content (content differentiator)
    - mkt_campaign (campaign name)
    - mkt_clickid (click ID)
    - mkt_network (advertising network)

    Configuration Impact:
    Configuration specifies which query string parameter names map to each mkt_* field.
    Default mappings include utm_* parameters, but can be customized.
    """

    # Input field name
    INPUT_FIELD = "page_urlquery"

    # Output fields (all atomic, populated when relevant query params present)
    OUTPUT_FIELDS = [
        "mkt_medium",
        "mkt_source",
        "mkt_term",
        "mkt_content",
        "mkt_campaign",
        "mkt_clickid",
        "mkt_network",
    ]

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles Campaign Attribution enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for Campaign Attribution enrichment
        """
        return "campaign_attribution" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for Campaign Attribution enrichment.

        Args:
            enrichment: Campaign Attribution enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of field lineages (page_urlquery → mkt_* fields)
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
            f"Campaign Attribution: Extracted {len(lineages)} field lineages ({self.INPUT_FIELD} → mkt_*)"
        )

        return lineages

    def get_field_info(self, enrichment: Enrichment) -> EnrichmentFieldInfo:
        """
        Get field information for Campaign Attribution enrichment.

        This enrichment always uses the same input/output fields regardless of configuration.
        """
        return EnrichmentFieldInfo(
            input_fields=[self.INPUT_FIELD],
            output_fields=list(self.OUTPUT_FIELDS),
            transformation_description="Extracts marketing campaign parameters from URL query string",
        )
