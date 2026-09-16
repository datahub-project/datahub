"""
Registry for enrichment lineage extractors.

This module implements the registry pattern for managing enrichment lineage extractors.
Extractors can be registered dynamically, and the registry handles lookup based on
enrichment schema URIs.
"""

import logging
from typing import List, Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentFieldInfo,
    EnrichmentLineageExtractor,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class EnrichmentLineageRegistry:
    """
    Registry for enrichment lineage extractors.

    This registry maintains a collection of enrichment lineage extractors and
    provides lookup functionality to find the appropriate extractor for a given
    enrichment based on its schema URI.

    Usage:
        registry = EnrichmentLineageRegistry()
        registry.register(IpLookupLineageExtractor())
        registry.register(CampaignAttributionLineageExtractor())

        extractor = registry.get_extractor(enrichment)
        if extractor:
            lineages = extractor.extract_lineage(...)
    """

    def __init__(self) -> None:
        """Initialize an empty registry."""
        self._extractors: List[EnrichmentLineageExtractor] = []

    def register(self, extractor: EnrichmentLineageExtractor) -> None:
        """
        Register an enrichment lineage extractor.

        Args:
            extractor: The enrichment lineage extractor to register
        """
        self._extractors.append(extractor)
        logger.debug(
            f"Registered enrichment lineage extractor: {extractor.__class__.__name__}"
        )

    def get_extractor(
        self, enrichment: Enrichment
    ) -> Optional[EnrichmentLineageExtractor]:
        """
        Get the appropriate extractor for the given enrichment.

        Looks through registered extractors to find one that supports the
        enrichment's schema URI. Returns the first matching extractor.

        Args:
            enrichment: The enrichment to find an extractor for

        Returns:
            The matching enrichment lineage extractor, or None if no match found
        """
        for extractor in self._extractors:
            if extractor.supports_enrichment(enrichment.schema_ref):
                logger.debug(
                    f"Found extractor {extractor.__class__.__name__} for enrichment {enrichment.filename}"
                )
                return extractor

        logger.debug(
            f"No lineage extractor found for enrichment {enrichment.filename} (schema: {enrichment.schema_ref})"
        )
        return None

    def get_extractor_count(self) -> int:
        """
        Get the number of registered extractors.

        Returns:
            Number of extractors in the registry
        """
        return len(self._extractors)

    def get_field_info(self, enrichment: Enrichment) -> Optional[EnrichmentFieldInfo]:
        """
        Get field information for the given enrichment.

        Finds the appropriate extractor and delegates to its get_field_info() method
        to retrieve input/output field information based on the enrichment configuration.

        Args:
            enrichment: The enrichment to get field info for

        Returns:
            EnrichmentFieldInfo with input/output fields, or None if no extractor found
        """
        extractor = self.get_extractor(enrichment)
        if extractor:
            return extractor.get_field_info(enrichment)
        return None
