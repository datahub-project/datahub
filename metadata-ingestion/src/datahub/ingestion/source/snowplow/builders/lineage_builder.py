"""
Lineage Builder for Snowplow connector.

Handles construction of lineage metadata for enrichments and pipelines.
"""

import logging
from typing import Optional

from datahub.ingestion.source.snowplow.enrichment_lineage.base import (
    EnrichmentFieldInfo,
)
from datahub.ingestion.source.snowplow.enrichment_lineage.registry import (
    EnrichmentLineageRegistry,
)
from datahub.ingestion.source.snowplow.models.snowplow_models import Enrichment

logger = logging.getLogger(__name__)


class LineageBuilder:
    """
    Builder for constructing lineage metadata for Snowplow enrichments.

    Handles:
    - Enrichment descriptions with field information
    """

    def build_enrichment_description(
        self,
        enrichment: Enrichment,
        registry: EnrichmentLineageRegistry,
    ) -> str:
        """
        Build enrichment description with field information from enrichment config.

        Uses the registry to get field information directly from the enrichment
        configuration rather than parsing URNs from lineage objects.

        Args:
            enrichment: The enrichment configuration
            registry: Registry to look up the appropriate extractor

        Returns:
            Enhanced description with field information
        """
        # Get name from nested content.data.name, fallback to filename
        enrichment_name = enrichment.filename
        if enrichment.content and enrichment.content.data:
            enrichment_name = enrichment.content.data.name

        # Get field info from the appropriate extractor
        field_info: Optional[EnrichmentFieldInfo] = registry.get_field_info(enrichment)

        if not field_info or (
            not field_info.input_fields and not field_info.output_fields
        ):
            return f"{enrichment_name} enrichment"

        # Build markdown-formatted description with proper structure
        description_lines = [f"## {enrichment_name} enrichment", ""]

        # Add transformation description if available
        if field_info.transformation_description:
            description_lines.append(f"*{field_info.transformation_description}*")
            description_lines.append("")

        if field_info.output_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            output_list = sorted(set(field_info.output_fields))
            description_lines.append("**Adds fields:**")
            for field in output_list:
                description_lines.append(f"- `{field}`")
            description_lines.append("")

        if field_info.input_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            input_list = sorted(set(field_info.input_fields))
            description_lines.append("**From source fields:**")
            for field in input_list:
                description_lines.append(f"- `{field}`")

        return "\n".join(description_lines)
