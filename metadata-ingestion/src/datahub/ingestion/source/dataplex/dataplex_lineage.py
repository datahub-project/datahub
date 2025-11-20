"""Dataplex lineage extraction module."""

import collections
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, Iterable, Optional, Set

from google.cloud import dataplex_v1

try:
    from google.cloud.datacatalog_lineage_v1 import (
        EntityReference,
        LineageClient,
        SearchLinksRequest,
    )

    LINEAGE_CLIENT_AVAILABLE = True
except ImportError:
    LINEAGE_CLIENT_AVAILABLE = False
    LineageClient = None  # type: ignore
    EntityReference = None  # type: ignore
    SearchLinksRequest = None  # type: ignore

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    make_entity_dataset_urn,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    AuditStampClass,
    DatasetLineageTypeClass,
    UpstreamClass,
    UpstreamLineageClass,
)

logger = logging.getLogger(__name__)


@dataclass(order=True, eq=True, frozen=True)
class LineageEdge:
    """
    Represents a lineage edge between two entities.

    Attributes:
        entity_id: The upstream entity ID in Dataplex format
        audit_stamp: When this lineage was observed
        lineage_type: Type of lineage (TRANSFORMED, COPY, etc.)
    """

    entity_id: str
    audit_stamp: datetime
    lineage_type: str = DatasetLineageTypeClass.TRANSFORMED


class DataplexLineageExtractor:
    """
    Extracts lineage information from Google Dataplex using the Data Lineage API.

    This class queries the Dataplex Lineage API to discover upstream and downstream
    relationships between entities and generates DataHub lineage metadata.
    """

    def __init__(
        self,
        config: DataplexConfig,
        report: DataplexReport,
        lineage_client: Optional[LineageClient] = None,
        dataplex_client: Optional[dataplex_v1.DataplexServiceClient] = None,
    ):
        """
        Initialize the lineage extractor.

        Args:
            config: Dataplex source configuration
            report: Source report for tracking metrics
            lineage_client: Optional pre-configured LineageClient
        """
        self.config = config
        self.report = report
        self.lineage_client = lineage_client
        self.dataplex_client = dataplex_client
        self.platform = "dataplex"

        # Cache for lineage information
        self.lineage_map: Dict[str, Set[LineageEdge]] = collections.defaultdict(set)

    def get_lineage_for_entity(
        self, project_id: str, entity: EntityDataTuple
    ) -> Optional[Dict[str, list]]:
        """
        Get lineage information for a specific Dataplex entity.

        Args:
            project_id: GCP project ID
            entity: EntityDataTuple

        Returns:
            Dictionary with 'upstream' and 'downstream' lists of entity FQNs,
            or None if lineage extraction is disabled or fails
        """
        if not self.config.extract_lineage or not self.lineage_client:
            return None

        try:
            fully_qualified_name = self._construct_fqn(
                entity.source_platform, project_id, entity.dataset_id, entity.entity_id
            )
            lineage_data = {"upstream": [], "downstream": []}
            # We only need multi-region name like US, EU, etc., specific region name like us-central1, eu-central1, etc. does not work
            parent = (
                f"projects/{project_id}/locations/{self.config.location.split('-')[0]}"
            )

            # Get upstream lineage (where this entity is the target)
            upstream_links = self._search_links_by_target(parent, fully_qualified_name)
            for link in upstream_links:
                if link.source and link.source.fully_qualified_name:
                    lineage_data["upstream"].append(link.source.fully_qualified_name)

            # Get downstream lineage (where this entity is the source)
            downstream_links = self._search_links_by_source(
                parent, fully_qualified_name
            )
            for link in downstream_links:
                if link.target and link.target.fully_qualified_name:
                    lineage_data["downstream"].append(link.target.fully_qualified_name)

            if lineage_data["upstream"] or lineage_data["downstream"]:
                logger.debug(
                    f"Found lineage for {entity.entity_id}: "
                    f"{len(lineage_data['upstream'])} upstream, "
                    f"{len(lineage_data['downstream'])} downstream"
                )
                self.report.num_lineage_entries_scanned += 1

            return lineage_data

        except Exception as e:
            logger.warning(f"Failed to get lineage for entity {entity.entity_id}: {e}")
            self.report.num_lineage_entries_failed += 1
            return None

    def _search_links_by_target(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
        """
        Search for lineage links where the entity is a target (to find upstream).

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entity

        Returns:
            Iterator of Link objects
        """
        try:
            target = EntityReference(fully_qualified_name=fully_qualified_name)
            request = SearchLinksRequest(parent=parent, target=target)
            return self.lineage_client.search_links(request=request)
        except Exception as e:
            logger.debug(f"No upstream lineage found for {fully_qualified_name}: {e}")
            return []

    def _search_links_by_source(
        self, parent: str, fully_qualified_name: str
    ) -> Iterable:
        """
        Search for lineage links where the entity is a source (to find downstream).

        Args:
            parent: Parent resource path (projects/{project}/locations/{location})
            fully_qualified_name: FQN of the entity

        Returns:
            Iterator of Link objects
        """
        try:
            source = EntityReference(fully_qualified_name=fully_qualified_name)
            request = SearchLinksRequest(parent=parent, source=source)
            return self.lineage_client.search_links(request=request)
        except Exception as e:
            logger.debug(f"No downstream lineage found for {fully_qualified_name}: {e}")
            return []

    def _construct_fqn(
        self, platform: str, project_id: str, dataset_id: str, entity_id: str
    ) -> str:
        """
        Construct a fully qualified name for an entity.

        The FQN format varies by platform:
        - BigQuery: bigquery:projects/{project}/datasets/{dataset}/tables/{table}
        - GCS: gs://{bucket}/{path}

        For now, we construct a generic format and will enhance based on asset type.

        Args:
            project_id: GCP project ID
            entity_id: Entity ID from Dataplex

        Returns:
            Fully qualified name string
        """
        # This is a simplified implementation
        # In production, we should determine the asset type and construct
        # the appropriate FQN format
        return f"{platform}:{project_id}.{dataset_id}.{entity_id}"

    def build_lineage_map(
        self, project_id: str, entity_data: Iterable[EntityDataTuple]
    ) -> Dict[str, Set[LineageEdge]]:
        """
        Build a map of entity lineage for multiple entities.

        Args:
            project_id: GCP project ID
            entity_data: Iterable of EntityDataTuple objects to process

        Returns:
            Dictionary mapping entity IDs to sets of LineageEdge objects
        """
        lineage_map: Dict[str, Set[LineageEdge]] = collections.defaultdict(set)

        for entity in entity_data:
            lineage_data = self.get_lineage_for_entity(project_id, entity)

            if not lineage_data:
                continue

            # Convert upstream FQNs to LineageEdge objects
            for upstream_fqn in lineage_data.get("upstream", []):
                # Extract entity ID from FQN
                upstream_entity_id = self._extract_entity_id_from_fqn(upstream_fqn)

                if upstream_entity_id:
                    edge = LineageEdge(
                        entity_id=upstream_entity_id,
                        audit_stamp=datetime.now(timezone.utc),
                        lineage_type=DatasetLineageTypeClass.TRANSFORMED,
                    )
                    lineage_map[entity.entity_id].add(edge)

        self.lineage_map = lineage_map
        return lineage_map

    def _extract_entity_id_from_fqn(self, fqn: str) -> Optional[str]:
        """
        Extract entity ID from a fully qualified name.

        Args:
            fqn: Fully qualified name

        Returns:
            Entity ID or None if extraction fails
        """
        # This is a simplified implementation
        # In production, we should parse different FQN formats correctly
        try:
            if ":" in fqn:
                _, entity_part = fqn.split(":", 1)
                return entity_part
            return fqn
        except Exception:
            return None

    def get_lineage_for_table(
        self, entity_id: str, dataset_urn: str
    ) -> Optional[UpstreamLineageClass]:
        """
        Build UpstreamLineageClass for a specific entity.

        Args:
            entity_id: Entity ID
            dataset_urn: DataHub URN for the dataset

        Returns:
            UpstreamLineageClass object or None if no lineage exists
        """
        if entity_id not in self.lineage_map:
            return None

        upstream_list: list[UpstreamClass] = []

        for lineage_edge in self.lineage_map[entity_id]:
            # Generate URN for the upstream entity
            # Extract project_id from entity_id (format: project_id.entity_name)
            if "." in lineage_edge.entity_id:
                project_id, dataset_id, entity_name = lineage_edge.entity_id.split(
                    ".", 2
                )
            else:
                # Fallback if format is different
                project_id = (
                    self.config.project_ids[0] if self.config.project_ids else "unknown"
                )
                dataset_id = "unknown"
                entity_name = lineage_edge.entity_id

            upstream_urn = make_entity_dataset_urn(
                project_id=project_id,
                entity_id=entity_name,
                platform=self.platform,
                env=self.config.env,
                dataset_id=dataset_id,
            )

            # Create table-level lineage
            upstream_class = UpstreamClass(
                dataset=upstream_urn,
                type=lineage_edge.lineage_type,
                auditStamp=AuditStampClass(
                    actor="urn:li:corpuser:datahub",
                    time=int(lineage_edge.audit_stamp.timestamp() * 1000),
                ),
            )
            upstream_list.append(upstream_class)

        if not upstream_list:
            return None

        return UpstreamLineageClass(upstreams=upstream_list)

    def gen_lineage(
        self,
        entity_id: str,
        dataset_urn: str,
        upstream_lineage: Optional[UpstreamLineageClass] = None,
    ) -> Iterable[MetadataWorkUnit]:
        """
        Generate lineage workunits for a dataset.

        Args:
            entity_id: Entity ID
            dataset_urn: DataHub URN for the dataset
            upstream_lineage: Optional pre-built UpstreamLineageClass

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if upstream_lineage is None:
            upstream_lineage = self.get_lineage_for_table(entity_id, dataset_urn)

        if upstream_lineage is None:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=dataset_urn, aspect=upstream_lineage
        ).as_workunit()

    def get_lineage_workunits(
        self, project_id: str, entity_data: Iterable[EntityDataTuple]
    ) -> Iterable[MetadataWorkUnit]:
        """
        Main entry point to get lineage workunits for multiple entities.

        Args:
            project_id: GCP project ID
            entity_ids: Iterable of entity IDs

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.config.extract_lineage:
            logger.info("Lineage extraction is disabled")
            return

        logger.info(f"Extracting lineage for project {project_id}")

        # Build lineage map for all entities
        self.build_lineage_map(project_id, entity_data)

        # Generate workunits for each entity
        for entity in entity_data:
            dataset_urn = make_entity_dataset_urn(
                project_id=project_id,
                entity_id=entity.entity_id,
                platform=self.platform,
                env=self.config.env,
                dataset_id=entity.dataset_id,
            )

            try:
                yield from self.gen_lineage(entity.entity_id, dataset_urn)
            except Exception as e:
                logger.warning(
                    f"Failed to generate lineage for {entity.entity_id}: {e}"
                )
                self.report.num_lineage_entries_failed += 1
