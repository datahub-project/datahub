"""
Event Fingerprint enrichment lineage extractor.

Extracts field-level lineage for the Event Fingerprint enrichment, which generates
a hash of specified event fields for deduplication purposes.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/event-fingerprint-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow.enrichments/event_fingerprint_config/jsonschema/1-0-1

Output Fields:
- event_fingerprint: Hash value for deduplication
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.utils import make_field_urn
from datahub.ingestion.source.snowplow.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class EventFingerprintLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for Event Fingerprint enrichment lineage.

    Input Fields:
    - Configurable list of fields to hash (common: event_id, domain_userid,
      collector_tstamp, event_name, etc.)
    - Default algorithm uses multiple client-set fields

    Output Fields:
    - event_fingerprint (hash for deduplication)

    Configuration Impact:
    The configuration specifies which fields are hashed together. We extract all
    commonly used fields as potential inputs.
    """

    # Common input fields used for fingerprinting
    # The actual fields depend on configuration, these are the most common ones
    COMMON_INPUT_FIELDS = [
        "event_id",
        "event_name",
        "domain_userid",
        "network_userid",
        "user_id",
        "user_ipaddress",
        "domain_sessionid",
        "collector_tstamp",
        "dvce_created_tstamp",
        "event",
        "platform",
        "app_id",
    ]

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles Event Fingerprint enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for Event Fingerprint enrichment
        """
        return "event_fingerprint" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for Event Fingerprint enrichment.

        Args:
            enrichment: Event Fingerprint enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List with single field lineage (multiple input fields → event_fingerprint)
        """
        if not warehouse_table_urn:
            return []

        # Create URNs for all common input fields
        upstream_field_urns = [
            make_field_urn(event_schema_urn, field)
            for field in self.COMMON_INPUT_FIELDS
        ]

        # Single output field
        output_field = "event_fingerprint"
        downstream_field_urn = make_field_urn(warehouse_table_urn, output_field)

        lineage = FieldLineage(
            upstream_fields=upstream_field_urns,
            downstream_fields=[downstream_field_urn],
            transformation_type="DERIVED",
        )

        logger.debug(
            f"Event Fingerprint: Extracted lineage ({len(self.COMMON_INPUT_FIELDS)} fields → event_fingerprint)"
        )

        return [lineage]
