"""
UA Parser enrichment lineage extractor.

Extracts field-level lineage for the UA Parser enrichment, which parses the user agent
string to extract browser, OS, and device information.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/ua-parser-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0

Field Mapping Reference:
/connectors-accelerator/SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md#3-ua-parser-enrichment

Note:
This enrichment outputs to a CONTEXT (not atomic fields). Context lineage tracking
is planned for a future enhancement. For now, this extractor returns empty lineages.

Output Context Fields (not yet tracked):
- useragent_family, useragent_major, useragent_minor, useragent_patch, useragent_version
- os_family, os_major, os_minor, os_patch, os_patch_minor, os_version
- device_family
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class UaParserLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for UA Parser enrichment lineage.

    Input Fields:
    - useragent (atomic field from HTTP User-Agent header)

    Output Fields:
    - Context: com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0
      Contains 12 fields: useragent_family, useragent_major, useragent_minor, etc.

    Current Limitation:
    Context lineage is not yet implemented. This enrichment adds a derived context
    rather than atomic event fields. Future enhancement will track lineage to
    warehouse context tables (e.g., events_contexts_com_snowplowanalytics_snowplow_ua_parser_context_1).
    """

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles UA Parser enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for UA Parser enrichment
        """
        # The enrichment config schema, not the output context schema
        return "ua_parser_config" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for UA Parser enrichment.

        Currently returns empty list because context lineage is not yet implemented.

        Args:
            enrichment: UA Parser enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            Empty list (context lineage not yet supported)
        """
        # TODO: Implement context lineage tracking
        # Would need to:
        # 1. Construct context table URN (e.g., warehouse.events_contexts_com_snowplowanalytics_snowplow_ua_parser_context_1)
        # 2. Map useragent → context fields (useragent_family, os_family, device_family, etc.)
        # 3. Return FieldLineage objects for each field

        logger.debug(
            "UA Parser: Context lineage not yet implemented, returning empty lineages"
        )
        return []
