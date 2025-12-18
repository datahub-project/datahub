"""
YAUAA (Yet Another UserAgent Analyzer) enrichment lineage extractor.

Extracts field-level lineage for the YAUAA enrichment, which parses the user agent
string to extract detailed browser, OS, device, and bot detection information.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/yauaa-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/yauaa_context/jsonschema/1-0-4

Output Fields (written to atomic event table):
- br_family, br_name, br_version, br_type
- os_family, os_name, os_version
- device_class, device_name, device_brand
- agent_class, agent_name, agent_version
- layout_engine_class, layout_engine_name, layout_engine_version
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


class YauaaLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for YAUAA enrichment lineage.

    Input Fields:
    - useragent (atomic field from HTTP User-Agent header)

    Output Fields (atomic event fields):
    - br_family (browser family)
    - br_name (browser name)
    - br_version (browser version)
    - br_type (browser type)
    - os_family (OS family)
    - os_name (OS name)
    - os_version (OS version)
    - device_class (device class)
    - device_name (device name)
    - device_brand (device brand)
    - agent_class (agent class)
    - agent_name (agent name)
    - agent_version (agent version)
    - layout_engine_class (layout engine class)
    - layout_engine_name (layout engine name)
    - layout_engine_version (layout engine version)
    """

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles YAUAA enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for YAUAA enrichment
        """
        return "yauaa_enrichment_config" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for YAUAA enrichment.

        Args:
            enrichment: YAUAA enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of FieldLineage objects
        """
        if not warehouse_table_urn:
            return []

        lineages: List[FieldLineage] = []

        # Input field (from event schema)
        useragent_urn = make_field_urn(event_schema_urn, "useragent")

        # Output fields (in warehouse table) - Browser fields
        browser_fields = [
            "br_family",
            "br_name",
            "br_version",
            "br_type",
        ]

        # OS fields
        os_fields = [
            "os_family",
            "os_name",
            "os_version",
        ]

        # Device fields
        device_fields = [
            "device_class",
            "device_name",
            "device_brand",
        ]

        # Agent fields
        agent_fields = [
            "agent_class",
            "agent_name",
            "agent_version",
        ]

        # Layout engine fields
        engine_fields = [
            "layout_engine_class",
            "layout_engine_name",
            "layout_engine_version",
        ]

        all_output_fields = (
            browser_fields + os_fields + device_fields + agent_fields + engine_fields
        )

        # Create lineage for each output field
        for field_name in all_output_fields:
            output_urn = make_field_urn(warehouse_table_urn, field_name)
            lineages.append(
                FieldLineage(
                    upstream_fields=[useragent_urn],
                    downstream_fields=[output_urn],
                    transformation_type="DERIVED",
                )
            )

        logger.debug(
            f"YAUAA: Created {len(lineages)} field lineages (useragent → browser/OS/device fields)"
        )

        return lineages
