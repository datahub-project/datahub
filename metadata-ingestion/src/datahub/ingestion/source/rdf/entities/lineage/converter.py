"""
Lineage Converter

Converts RDF lineage relationships to DataHub format.
"""

import logging
from typing import Any, Dict, List, Optional

from datahub.ingestion.source.rdf.entities.base import EntityConverter
from datahub.ingestion.source.rdf.entities.dataset.urn_generator import (
    DatasetUrnGenerator,  # For dataset URNs
)
from datahub.ingestion.source.rdf.entities.lineage.ast import (
    DataHubLineageActivity,
    DataHubLineageRelationship,
    RDFLineageActivity,
    RDFLineageRelationship,
)
from datahub.ingestion.source.rdf.entities.lineage.urn_generator import (
    LineageUrnGenerator,
)

logger = logging.getLogger(__name__)


class LineageConverter(
    EntityConverter[RDFLineageRelationship, DataHubLineageRelationship]
):
    """
    Converts RDF lineage relationships to DataHub format.

    Handles URN generation for datasets and DataJobs.
    """

    def __init__(self):
        """Initialize the converter with entity-specific generators."""
        # Use entity-specific generators
        self.lineage_urn_generator = LineageUrnGenerator()
        self.dataset_urn_generator = DatasetUrnGenerator()

    @property
    def entity_type(self) -> str:
        return "lineage"

    def convert(
        self, rdf_rel: RDFLineageRelationship, context: Dict[str, Any] = None
    ) -> Optional[DataHubLineageRelationship]:
        """Convert a single lineage relationship to DataHub format."""
        try:
            environment = context.get("environment", "PROD") if context else "PROD"

            # Generate URNs
            source_urn = self.dataset_urn_generator.generate_dataset_urn(
                rdf_rel.source_uri, rdf_rel.source_platform, environment
            )

            target_urn = self.dataset_urn_generator.generate_dataset_urn(
                rdf_rel.target_uri, rdf_rel.target_platform, environment
            )

            # Generate activity URN if present
            activity_urn = None
            if rdf_rel.activity_uri:
                # Skip if no platform - platform is required for DataJob URNs
                if not rdf_rel.activity_platform:
                    logger.debug(
                        f"Skipping activity URN for relationship {rdf_rel.source_uri} -> {rdf_rel.target_uri}: "
                        f"activity {rdf_rel.activity_uri} has no platform"
                    )
                else:
                    # Extract job name from URI
                    job_name = rdf_rel.activity_uri.split("/")[-1].split("#")[-1]
                    activity_urn = self.lineage_urn_generator.generate_data_job_urn(
                        rdf_rel.activity_platform, job_name, environment
                    )

            return DataHubLineageRelationship(
                source_urn=source_urn,
                target_urn=target_urn,
                lineage_type=rdf_rel.lineage_type,
                activity_urn=activity_urn,
                properties=rdf_rel.properties or {},
            )

        except Exception as e:
            logger.warning(f"Error converting lineage relationship: {e}")
            return None

    def convert_all(
        self,
        rdf_relationships: List[RDFLineageRelationship],
        context: Dict[str, Any] = None,
    ) -> List[DataHubLineageRelationship]:
        """Convert all lineage relationships to DataHub format."""
        datahub_relationships = []

        for rdf_rel in rdf_relationships:
            datahub_rel = self.convert(rdf_rel, context)
            if datahub_rel:
                datahub_relationships.append(datahub_rel)

        logger.info(f"Converted {len(datahub_relationships)} lineage relationships")
        return datahub_relationships

    def convert_activity(
        self, rdf_activity: RDFLineageActivity, context: Dict[str, Any] = None
    ) -> Optional[DataHubLineageActivity]:
        """Convert a lineage activity to DataHub format."""
        try:
            # Skip activities without platforms - platform is required for DataJob URNs
            if not rdf_activity.platform:
                logger.debug(
                    f"Skipping lineage activity '{rdf_activity.name}' ({rdf_activity.uri}): "
                    f"no platform found. Activity has no platform and no connected datasets with platforms."
                )
                return None

            environment = context.get("environment", "PROD") if context else "PROD"

            # Extract job name from URI
            job_name = rdf_activity.uri.split("/")[-1].split("#")[-1]
            activity_urn = self.lineage_urn_generator.generate_data_job_urn(
                rdf_activity.platform, job_name, environment
            )

            return DataHubLineageActivity(
                urn=activity_urn,
                name=rdf_activity.name,
                description=rdf_activity.description,
                properties=rdf_activity.properties or {},
            )

        except Exception as e:
            logger.warning(f"Error converting activity {rdf_activity.name}: {e}")
            return None

    def convert_activities(
        self, rdf_activities: List[RDFLineageActivity], context: Dict[str, Any] = None
    ) -> List[DataHubLineageActivity]:
        """Convert all activities to DataHub format."""
        datahub_activities = []

        for rdf_activity in rdf_activities:
            datahub_activity = self.convert_activity(rdf_activity, context)
            if datahub_activity:
                datahub_activities.append(datahub_activity)

        logger.info(f"Converted {len(datahub_activities)} lineage activities")
        return datahub_activities
