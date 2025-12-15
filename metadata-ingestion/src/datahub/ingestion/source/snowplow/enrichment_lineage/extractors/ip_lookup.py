"""
IP Lookup enrichment lineage extractor.

Extracts field-level lineage for the IP Lookup enrichment, which queries MaxMind
databases to enrich events with geographic and ISP information based on IP address.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/ip-lookup-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/ip_lookups/jsonschema/2-0-0

Field Mapping Reference:
/connectors-accelerator/SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md#1-ip-lookup-enrichment
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


class IpLookupLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for IP Lookup enrichment lineage.

    Input Fields:
    - user_ipaddress (atomic field)

    Output Fields (depends on configured databases):
    - geo_country, geo_region, geo_city, geo_zipcode (from GeoLite2-City/GeoIP2-City)
    - geo_latitude, geo_longitude, geo_region_name, geo_timezone (from geo database)
    - ip_isp, ip_organization (from GeoIP2-ISP)
    - ip_domain (from GeoIP2-Domain)
    - ip_netspeed (from GeoIP2-Connection-Type)

    Configuration Impact:
    Only databases configured in the enrichment parameters will add their corresponding fields.
    """

    # Geo database fields (from GeoLite2-City or GeoIP2-City)
    GEO_FIELDS = [
        "geo_country",
        "geo_region",
        "geo_city",
        "geo_zipcode",
        "geo_latitude",
        "geo_longitude",
        "geo_region_name",
        "geo_timezone",
    ]

    # ISP database fields (from GeoIP2-ISP)
    ISP_FIELDS = ["ip_isp", "ip_organization"]

    # Domain database fields (from GeoIP2-Domain)
    DOMAIN_FIELDS = ["ip_domain"]

    # Connection type fields (from GeoIP2-Connection-Type)
    CONNECTION_FIELDS = ["ip_netspeed"]

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles IP Lookup enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for IP Lookup enrichment
        """
        return "ip_lookups" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for IP Lookup enrichment.

        Args:
            enrichment: IP Lookup enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of field lineages (user_ipaddress → geo_*/ip_* fields)
        """
        if not warehouse_table_urn:
            return []

        lineages = []
        # Get enrichment configuration to determine which databases are enabled
        config = enrichment.parameters

        # Input field
        input_field = "user_ipaddress"
        upstream_field_urn = make_field_urn(event_schema_urn, input_field)

        # Determine which databases are configured
        # The config structure has database type as key (e.g., "geo", "isp", "domain")
        has_geo = "geo" in config
        has_isp = "isp" in config
        has_domain = "domain" in config
        has_connection = "connectionType" in config

        # Add geo fields if geo database configured
        if has_geo:
            for field in self.GEO_FIELDS:
                lineages.append(
                    FieldLineage(
                        upstream_fields=[upstream_field_urn],
                        downstream_fields=[make_field_urn(warehouse_table_urn, field)],
                        transformation_type="DERIVED",
                    )
                )

        # Add ISP fields if ISP database configured
        if has_isp:
            for field in self.ISP_FIELDS:
                lineages.append(
                    FieldLineage(
                        upstream_fields=[upstream_field_urn],
                        downstream_fields=[make_field_urn(warehouse_table_urn, field)],
                        transformation_type="DERIVED",
                    )
                )

        # Add domain fields if domain database configured
        if has_domain:
            for field in self.DOMAIN_FIELDS:
                lineages.append(
                    FieldLineage(
                        upstream_fields=[upstream_field_urn],
                        downstream_fields=[make_field_urn(warehouse_table_urn, field)],
                        transformation_type="DERIVED",
                    )
                )

        # Add connection type fields if configured
        if has_connection:
            for field in self.CONNECTION_FIELDS:
                lineages.append(
                    FieldLineage(
                        upstream_fields=[upstream_field_urn],
                        downstream_fields=[make_field_urn(warehouse_table_urn, field)],
                        transformation_type="DERIVED",
                    )
                )

        logger.debug(
            f"IP Lookup: Extracted {len(lineages)} field lineages (geo={has_geo}, isp={has_isp}, domain={has_domain})"
        )

        return lineages
