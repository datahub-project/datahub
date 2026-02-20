"""
Enrichment lineage registry factory.

Configures and initializes the enrichment lineage registry with all
available enrichment lineage extractors.
"""

import logging

from datahub.ingestion.source.snowplow.enrichment_lineage import (
    CampaignAttributionLineageExtractor,
    CurrencyConversionLineageExtractor,
    EnrichmentLineageRegistry,
    EventFingerprintLineageExtractor,
    IabSpidersRobotsLineageExtractor,
    IpLookupLineageExtractor,
    PiiPseudonymizationLineageExtractor,
    RefererParserLineageExtractor,
    UaParserLineageExtractor,
    YauaaLineageExtractor,
)

logger = logging.getLogger(__name__)


class EnrichmentRegistryFactory:
    """
    Factory for creating and configuring enrichment lineage registry.

    Registers all available enrichment lineage extractors with the registry.
    """

    @staticmethod
    def create_registry() -> EnrichmentLineageRegistry:
        """
        Create and configure enrichment lineage registry.

        Registers all available enrichment lineage extractors:
        - IP Lookup
        - UA Parser
        - YAUAA
        - IAB Spiders & Robots
        - PII Pseudonymization
        - Referer Parser
        - Currency Conversion
        - Campaign Attribution
        - Event Fingerprint

        Returns:
            Configured EnrichmentLineageRegistry instance
        """
        logger.debug("Initializing enrichment lineage registry")

        registry = EnrichmentLineageRegistry()

        # Register all enrichment lineage extractors
        registry.register(IpLookupLineageExtractor())
        registry.register(UaParserLineageExtractor())
        registry.register(YauaaLineageExtractor())
        registry.register(IabSpidersRobotsLineageExtractor())
        registry.register(PiiPseudonymizationLineageExtractor())
        registry.register(RefererParserLineageExtractor())
        registry.register(CurrencyConversionLineageExtractor())
        registry.register(CampaignAttributionLineageExtractor())
        registry.register(EventFingerprintLineageExtractor())

        logger.debug(
            f"Enrichment lineage registry initialized with {len(registry._extractors)} extractors"
        )

        return registry
