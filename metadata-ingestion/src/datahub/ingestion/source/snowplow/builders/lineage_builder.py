"""
Lineage Builder for Snowplow connector.

Handles construction of lineage metadata for enrichments and pipelines.
"""

import logging
from typing import List

from datahub.metadata.schema_classes import FineGrainedLineageClass

logger = logging.getLogger(__name__)


class LineageBuilder:
    """
    Builder for constructing lineage metadata for Snowplow enrichments.

    Handles:
    - Enrichment descriptions with field information
    """

    def build_enrichment_description(
        self,
        enrichment_name: str,
        fine_grained_lineages: List[FineGrainedLineageClass],
    ) -> str:
        """
        Build enrichment description with field lineage information.

        Args:
            enrichment_name: Name of the enrichment
            fine_grained_lineages: List of fine-grained lineage objects

        Returns:
            Enhanced description with field information
        """
        if not fine_grained_lineages:
            return f"{enrichment_name} enrichment"

        # Extract unique upstream and downstream field names
        upstream_fields = set()
        downstream_fields = set()

        for lineage in fine_grained_lineages:
            # Extract field names from URNs
            for upstream_urn in lineage.upstreams or []:
                # URN format: urn:li:schemaField:(urn:li:dataset:...,field_name)
                if "," in upstream_urn:
                    field_name = upstream_urn.split(",")[-1].rstrip(")")
                    upstream_fields.add(field_name)

            for downstream_urn in lineage.downstreams or []:
                if "," in downstream_urn:
                    field_name = downstream_urn.split(",")[-1].rstrip(")")
                    downstream_fields.add(field_name)

        # Build markdown-formatted description with proper structure
        description_lines = [f"## {enrichment_name} enrichment", ""]

        if downstream_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            downstream_list = sorted(downstream_fields)
            description_lines.append("**Adds fields:**")
            for field in downstream_list:
                description_lines.append(f"- `{field}`")
            description_lines.append("")

        if upstream_fields:
            # Sort for consistent output and show ALL fields as individual bullets
            upstream_list = sorted(upstream_fields)
            description_lines.append("**From source fields:**")
            for field in upstream_list:
                description_lines.append(f"- `{field}`")

        return "\n".join(description_lines)
