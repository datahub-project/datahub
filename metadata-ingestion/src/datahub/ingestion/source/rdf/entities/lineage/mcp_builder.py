"""
Lineage MCP Builder

Creates DataHub MCPs for lineage relationships and activities.
"""

import logging
from typing import Any, Dict, List

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.source.rdf.entities.base import EntityMCPBuilder
from datahub.ingestion.source.rdf.entities.lineage.ast import (
    DataHubLineageActivity,
    DataHubLineageRelationship,
)
from datahub.metadata.schema_classes import (
    DataJobInfoClass,
    DataJobInputOutputClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


class LineageMCPBuilder(EntityMCPBuilder[DataHubLineageRelationship]):
    """
    Creates MCPs for lineage relationships.

    Creates:
    - UpstreamLineage MCPs for dataset-to-dataset lineage
    - DataJobInfo MCPs for lineage activities
    """

    @property
    def entity_type(self) -> str:
        return "lineage"

    def build_mcps(
        self, relationship: DataHubLineageRelationship, context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for a single lineage relationship."""
        return []  # Relationships are aggregated

    def build_all_mcps(
        self,
        relationships: List[DataHubLineageRelationship],
        context: Dict[str, Any] = None,
    ) -> List[MetadataChangeProposalWrapper]:
        """
        Build MCPs for all lineage relationships.

        Aggregates relationships by target dataset and creates one MCP per dataset
        with all its upstream dependencies.
        """
        mcps = []

        # Aggregate by target dataset
        upstream_map = {}  # target_urn -> [source_urns]

        for rel in relationships:
            target = str(rel.target_urn)
            source = str(rel.source_urn)

            if target not in upstream_map:
                upstream_map[target] = []
            upstream_map[target].append(source)

        # Create UpstreamLineage MCPs
        for target_urn, source_urns in upstream_map.items():
            try:
                unique_sources = list(set(source_urns))

                upstreams = [
                    UpstreamClass(
                        dataset=source_urn, type=DatasetLineageTypeClass.TRANSFORMED
                    )
                    for source_urn in unique_sources
                ]

                mcp = MetadataChangeProposalWrapper(
                    entityUrn=target_urn,
                    aspect=UpstreamLineageClass(upstreams=upstreams),
                )
                mcps.append(mcp)

                logger.debug(
                    f"Created lineage MCP for {target_urn} with {len(unique_sources)} upstreams"
                )

            except Exception as e:
                logger.error(f"Failed to create lineage MCP for {target_urn}: {e}")

        logger.info(f"Built {len(mcps)} lineage MCPs")
        return mcps

    def build_activity_mcps(
        self, activities: List[DataHubLineageActivity], context: Dict[str, Any] = None
    ) -> List[MetadataChangeProposalWrapper]:
        """Build MCPs for lineage activities (DataJobs)."""
        mcps = []

        for activity in activities:
            try:
                # DataJobInfo MCP
                job_info = DataJobInfoClass(
                    name=activity.name,
                    type="BATCH",  # Default type
                    description=activity.description,
                    customProperties=activity.properties or {},
                )

                mcps.append(
                    MetadataChangeProposalWrapper(
                        entityUrn=str(activity.urn), aspect=job_info
                    )
                )

                # DataJobInputOutput MCP if has inputs/outputs
                if activity.used_entities or activity.generated_entities:
                    input_output = DataJobInputOutputClass(
                        inputDatasets=activity.used_entities,
                        outputDatasets=activity.generated_entities,
                    )

                    mcps.append(
                        MetadataChangeProposalWrapper(
                            entityUrn=str(activity.urn), aspect=input_output
                        )
                    )

            except Exception as e:
                logger.error(f"Failed to create MCP for activity {activity.name}: {e}")

        logger.info(f"Built {len(mcps)} activity MCPs")
        return mcps

    @staticmethod
    def create_datajob_mcp(activity) -> MetadataChangeProposalWrapper:
        """Create MCP for a DataJob (lineage activity) per specification Section 6."""
        # Extract job type from activity properties or use default
        job_type = "BATCH"  # Default type for lineage activities
        if hasattr(activity, "properties") and activity.properties:
            # Check for common type indicators in properties
            if "type" in activity.properties:
                job_type = activity.properties["type"]
            elif "jobType" in activity.properties:
                job_type = activity.properties["jobType"]
            elif "transformationType" in activity.properties:
                job_type = activity.properties["transformationType"]

        job_info = DataJobInfoClass(
            name=activity.name,
            type=job_type,
            description=activity.description or f"Data job: {activity.name}",
            customProperties=activity.properties or {},
        )

        return MetadataChangeProposalWrapper(
            entityUrn=str(activity.urn), aspect=job_info
        )
