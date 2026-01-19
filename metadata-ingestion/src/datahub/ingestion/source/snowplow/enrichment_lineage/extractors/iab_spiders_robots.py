"""
IAB Spiders and Robots enrichment lineage extractor.

Extracts field-level lineage for the IAB Spiders and Robots enrichment, which
identifies whether traffic comes from known bots/spiders using the IAB/ABC
International Spiders and Bots List.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/iab-spiders-and-bots-enrichment/

Schema:
iglu:com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0

Note:
This enrichment writes to a derived context (separate table in warehouse), not to
atomic event fields. However, it reads from the atomic event's user agent field.
We model this as reading useragent but not writing to the main atomic table.
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class IabSpidersRobotsLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for IAB Spiders and Robots enrichment lineage.

    Input Fields:
    - useragent (atomic field from HTTP User-Agent header)
    - user_ipaddress (atomic field for IP-based detection)

    Output:
    - Derived context: com.iab.snowplow/spiders_and_robots/jsonschema/1-0-0
    - Written to separate context table in warehouse, not atomic event fields

    Current Implementation:
    Since this enrichment writes to a context table (not atomic fields), we
    return empty lineages. The enrichment will still show as connected to the
    Event dataset through its input/output datasets, but without field-level lineage.
    """

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles IAB Spiders and Robots enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for IAB enrichment
        """
        return "iab_spiders_and_robots" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for IAB Spiders and Robots enrichment.

        Currently returns empty list because this enrichment writes to a derived
        context (separate table), not to atomic event fields.

        Args:
            enrichment: IAB enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            Empty list (context lineage not yet supported)
        """
        # TODO: Implement context table lineage
        # Would need to:
        # 1. Construct context table URN (e.g., warehouse.events_contexts_com_iab_snowplow_spiders_and_robots_1)
        # 2. Map useragent/user_ipaddress → context fields (category, primaryImpact, reason, spiderOrRobot)
        # 3. Return FieldLineage objects for each field

        logger.debug(
            "IAB Spiders & Robots: Context lineage not yet implemented, returning empty lineages"
        )
        return []
