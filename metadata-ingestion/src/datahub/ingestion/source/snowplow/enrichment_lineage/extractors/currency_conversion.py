"""
Currency Conversion enrichment lineage extractor.

Extracts field-level lineage for the Currency Conversion enrichment, which converts
transaction values to a base currency using Open Exchange Rates API.

Enrichment Documentation:
https://docs.snowplow.io/docs/pipeline/enrichments/available-enrichments/currency-conversion-enrichment/

Schema:
iglu:com.snowplowanalytics.snowplow/currency_conversion_config/jsonschema/1-0-0

Field Mapping Reference:
/connectors-accelerator/SNOWPLOW_ENRICHMENT_FIELD_MAPPING.md#10-currency-conversion-enrichment
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.utils import make_field_urn
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class CurrencyConversionLineageExtractor(EnrichmentLineageExtractor):
    """
    Extractor for Currency Conversion enrichment lineage.

    Input Fields:
    - tr_total (transaction total amount)
    - tr_tax (transaction tax amount)
    - tr_shipping (shipping cost)
    - tr_currency (original currency code)
    - ti_price (item price)
    - ti_currency (item currency code)

    Output Fields:
    - base_currency (ISO 4217 currency code from config)
    - tr_total_base (transaction total in base currency)
    - tr_tax_base (tax amount in base currency)
    - tr_shipping_base (shipping cost in base currency)
    - ti_price_base (item price in base currency)

    Configuration Impact:
    The base_currency output field value is determined by the baseCurrency config parameter,
    but the field mapping itself is always the same.
    """

    # Mapping of input fields to output fields
    # Each conversion uses the corresponding currency field as input too
    FIELD_MAPPINGS = [
        (["tr_total", "tr_currency"], "tr_total_base"),
        (["tr_tax", "tr_currency"], "tr_tax_base"),
        (["tr_shipping", "tr_currency"], "tr_shipping_base"),
        (["ti_price", "ti_currency"], "ti_price_base"),
    ]

    def supports_enrichment(self, enrichment_schema: str) -> bool:
        """
        Check if this extractor handles Currency Conversion enrichments.

        Args:
            enrichment_schema: Iglu schema URI

        Returns:
            True if schema is for Currency Conversion enrichment
        """
        return "currency_conversion" in enrichment_schema

    def extract_lineage(
        self,
        enrichment: Enrichment,
        event_schema_urn: str,
        warehouse_table_urn: Optional[str],
    ) -> List[FieldLineage]:
        """
        Extract field-level lineage for Currency Conversion enrichment.

        Args:
            enrichment: Currency Conversion enrichment configuration
            event_schema_urn: URN of input event schema
            warehouse_table_urn: URN of output warehouse table

        Returns:
            List of field lineages (tr_*/ti_* + currency → *_base fields)
        """
        if not warehouse_table_urn:
            return []

        lineages = []

        # Create lineage for each conversion
        for input_fields, output_field in self.FIELD_MAPPINGS:
            # Each conversion depends on the value field AND the currency field
            upstream_field_urns = [
                make_field_urn(event_schema_urn, field) for field in input_fields
            ]

            lineages.append(
                FieldLineage(
                    upstream_fields=upstream_field_urns,
                    downstream_fields=[
                        make_field_urn(warehouse_table_urn, output_field)
                    ],
                    transformation_type="DERIVED",
                )
            )

        # Note: base_currency field is also added, but it comes from config not from input fields
        # So we don't create lineage for it (it's a constant from configuration)

        logger.debug(
            f"Currency Conversion: Extracted {len(lineages)} field lineages (tr_*/ti_* → *_base)"
        )

        return lineages
