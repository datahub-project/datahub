"""
Enrichment lineage extraction for Snowplow connector.

This package provides infrastructure for extracting fine-grained (column-level) lineage
from Snowplow enrichments. It uses a registry pattern to manage different enrichment
types, with each enrichment having its own extractor implementation.

Architecture:
- EnrichmentLineageExtractor: Abstract base class for all extractors
- EnrichmentLineageRegistry: Registry for managing and looking up extractors
- extractors/: Individual extractor implementations for each enrichment type

Usage:
    from datahub.ingestion.source.snowplow.enrichment_lineage import (
        EnrichmentLineageRegistry,
        IpLookupLineageExtractor,
        RefererParserLineageExtractor,
    )

    registry = EnrichmentLineageRegistry()
    registry.register(IpLookupLineageExtractor())
    registry.register(RefererParserLineageExtractor())

    extractor = registry.get_extractor(enrichment)
    if extractor:
        lineages = extractor.extract_lineage(enrichment, event_urn, warehouse_urn)
"""

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentLineageExtractor,
    FieldLineage,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.extractors import (
    CampaignAttributionLineageExtractor,
    CurrencyConversionLineageExtractor,
    EventFingerprintLineageExtractor,
    IabSpidersRobotsLineageExtractor,
    IpLookupLineageExtractor,
    PiiPseudonymizationLineageExtractor,
    RefererParserLineageExtractor,
    UaParserLineageExtractor,
    YauaaLineageExtractor,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.registry import (
    EnrichmentLineageRegistry,
)

__all__ = [
    "EnrichmentLineageExtractor",
    "FieldLineage",
    "EnrichmentLineageRegistry",
    "IpLookupLineageExtractor",
    "UaParserLineageExtractor",
    "YauaaLineageExtractor",
    "IabSpidersRobotsLineageExtractor",
    "PiiPseudonymizationLineageExtractor",
    "RefererParserLineageExtractor",
    "CurrencyConversionLineageExtractor",
    "CampaignAttributionLineageExtractor",
    "EventFingerprintLineageExtractor",
]
