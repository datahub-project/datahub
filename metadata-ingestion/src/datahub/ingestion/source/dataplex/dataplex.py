"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex, including:
- Entries (Universal Catalog) as Datasets with source platform URNs (bigquery, gcs, etc.)
- Entities (discovered tables/filesets from Lakes/Zones) as Datasets with source platform URNs
- BigQuery Projects as Containers (project-level containers)
- BigQuery Datasets as Containers (dataset-level containers, nested under project containers)
- Dataplex hierarchy (lakes, zones, assets, zone types) preserved as custom properties on datasets

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import logging
from threading import Lock
from typing import Iterable, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.cloud.datacatalog.lineage_v1 import LineageClient
from google.oauth2 import service_account

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    CapabilityReport,
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_containers import (
    gen_bigquery_containers,
)
from datahub.ingestion.source.dataplex.dataplex_entities import process_zone_entities
from datahub.ingestion.source.dataplex.dataplex_entries import process_entry
from datahub.ingestion.source.dataplex.dataplex_helpers import EntityDataTuple
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageExtractor
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)


@platform_name("Dataplex", id="dataplex")
@config_class(DataplexConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Enabled by default",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Enabled by default, can be disabled via configuration `include_schema`",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Optionally enabled via configuration `include_lineage`",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default via stateful ingestion",
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Enabled by default",
)
class DataplexSource(StatefulIngestionSourceBase, TestableSource):
    """Source to ingest metadata from Google Dataplex Universal Catalog."""

    platform: str = "dataplex"

    def __init__(self, ctx: PipelineContext, config: DataplexConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: DataplexReport = DataplexReport()

        # Track entity IDs for lineage extraction
        # Key: project_id, Value: set of tuples (entity_id, zone_id, lake_id)
        self.entity_data_by_project: dict[str, set[EntityDataTuple]] = {}

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )

        self.dataplex_client = dataplex_v1.DataplexServiceClient(
            credentials=credentials
        )
        self.metadata_client = dataplex_v1.MetadataServiceClient(
            credentials=credentials
        )
        # Catalog client for Phase 2: Entry Groups and Entries extraction
        self.catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)

        # Initialize redundant lineage run skip handler for stateful lineage ingestion
        redundant_lineage_run_skip_handler: Optional[RedundantLineageRunSkipHandler] = (
            None
        )
        if self.config.enable_stateful_lineage_ingestion:
            redundant_lineage_run_skip_handler = RedundantLineageRunSkipHandler(
                source=self,
                config=self.config,
                pipeline_name=self.ctx.pipeline_name,
                run_id=self.ctx.run_id,
            )

        if self.config.include_lineage:
            self.lineage_client: Optional[LineageClient] = LineageClient(
                credentials=credentials
            )
            self.lineage_extractor: Optional[DataplexLineageExtractor] = (
                DataplexLineageExtractor(
                    config=self.config,
                    report=self.report,
                    lineage_client=self.lineage_client,
                    dataplex_client=self.dataplex_client,
                    redundant_run_skip_handler=redundant_lineage_run_skip_handler,
                )
            )
        else:
            self.lineage_client = None
            self.lineage_extractor = None

        self.asset_metadata: dict[str, tuple[str, str]] = {}
        self.zone_metadata: dict[
            str, str
        ] = {}  # Store zone types for adding to entity custom properties

        # Track BigQuery containers to create (project_id -> set of dataset_ids)
        self.bq_containers: dict[str, set[str]] = {}

        # Thread safety locks for parallel processing
        self._report_lock = Lock()
        self._asset_metadata_lock = Lock()
        self._entity_data_lock = Lock()
        self._zone_metadata_lock = Lock()
        self._bq_containers_lock = Lock()

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Dataplex API."""
        test_report = TestConnectionReport()
        try:
            config = DataplexConfig.model_validate(config_dict)
            creds = config.get_credentials()
            credentials = (
                service_account.Credentials.from_service_account_info(creds)
                if creds
                else None
            )

            # Test connection by attempting to create a client and list one project
            dataplex_client = dataplex_v1.DataplexServiceClient(credentials=credentials)
            if config.project_ids:
                project_id = config.project_ids[0]
                # Try to list lakes to verify access
                parent = f"projects/{project_id}/locations/{config.location}"
                list(dataplex_client.list_lakes(parent=parent))

            test_report.basic_connectivity = CapabilityReport(capable=True)
        except exceptions.GoogleAPICallError as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Failed to connect to Dataplex: {e}"
            )
        except Exception as e:
            test_report.basic_connectivity = CapabilityReport(
                capable=False, failure_reason=f"Unexpected error: {e}"
            )

        return test_report

    def get_report(self) -> DataplexReport:
        """Return the ingestion report."""
        return self.report

    def get_workunit_processors(self) -> list[Optional[MetadataWorkUnitProcessor]]:
        """
        Get workunit processors for stateful ingestion.

        Returns processors for:
        - Stale entity removal (deletion detection)
        """
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor,
        ]

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Main function to fetch and yield workunits for various Dataplex resources."""
        # Iterate over all configured projects
        for project_id in self.config.project_ids:
            logger.info(f"Processing Dataplex resources for project: {project_id}")
            yield from self._process_project(project_id)

    def _emit_final_batch(
        self,
        cached_mcps: list[MetadataChangeProposalWrapper],
        total_emitted: int,
        project_id: str,
        resource_type: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Emit the final batch of MCPs for a resource type.

        Args:
            cached_mcps: List of cached MCPs to emit
            total_emitted: Total count of MCPs emitted so far
            project_id: GCP project ID
            resource_type: Type of resource being emitted ("entities" or "entries")

        Yields:
            MetadataWorkUnit objects
        """
        if cached_mcps:
            yield from auto_workunit(cached_mcps)
            total_emitted += len(cached_mcps)
            logger.info(
                f"Emitted final batch of {len(cached_mcps)} {resource_type} ({total_emitted} total) for project {project_id}"
            )

    def _process_project(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Process all Dataplex resources for a single project.

        This uses a single-pass approach with batched emission:
        1. Collect entities/entries MCPs in batches and track containers simultaneously
        2. Emit batches as they fill up to keep memory bounded
        3. Emit BigQuery containers (so entities can reference them)
        4. Extract lineage

        Processing order: Entities first, then Entries.
        When both are enabled, entries will overwrite entity metadata for the same table,
        making Universal Catalog the source of truth without requiring deduplication tracking.

        IMPORTANT: When both APIs are enabled and discover the same table:
        - Entry metadata (schema, entry custom properties) REPLACES entity metadata
        - Entity custom properties (lake, zone, asset) are LOST
        - This is DataHub's aspect-level replacement behavior (not a bug)
        - Users should choose ONE API, or use both only for non-overlapping datasets
        - See documentation for details: docs/sources/dataplex/dataplex_pre.md

        Memory optimization: Batched emission prevents memory issues in large deployments
        while maintaining the performance benefit of avoiding duplicate schema extraction.
        """
        # Determine batch size (None means no batching)
        batch_size = self.config.batch_size
        should_batch = batch_size is not None

        # Cache MCPs during the first pass
        cached_entities_mcps: list[MetadataChangeProposalWrapper] = []
        cached_entries_mcps: list[MetadataChangeProposalWrapper] = []
        entities_emitted = 0
        entries_emitted = 0

        # Process Entities API FIRST (if enabled) - collect MCPs and track containers
        if self.config.include_entities:
            logger.info(
                f"Processing entities from Dataplex Entities API for project {project_id}"
            )
            for mcp in self._get_entities_mcps(project_id):
                cached_entities_mcps.append(mcp)

                # Emit batch if we've reached the batch size
                if (
                    should_batch
                    and batch_size
                    and len(cached_entities_mcps) >= batch_size
                ):
                    yield from auto_workunit(cached_entities_mcps)
                    entities_emitted += len(cached_entities_mcps)
                    logger.info(
                        f"Emitted batch of {len(cached_entities_mcps)} entities ({entities_emitted} total) for project {project_id}"
                    )
                    cached_entities_mcps.clear()

            # Emit remaining cached entities MCPs
            yield from self._emit_final_batch(
                cached_entities_mcps, entities_emitted, project_id, "entities"
            )

        # Process Entries API SECOND (if enabled) - collect MCPs and track containers
        # Entries will overwrite any duplicate entity metadata
        if self.config.include_entries:
            logger.info(
                f"Processing entries from Universal Catalog for project {project_id}"
            )
            for mcp in self._get_entries_mcps(project_id):
                cached_entries_mcps.append(mcp)

                # Emit batch if we've reached the batch size
                if (
                    should_batch
                    and batch_size
                    and len(cached_entries_mcps) >= batch_size
                ):
                    yield from auto_workunit(cached_entries_mcps)
                    entries_emitted += len(cached_entries_mcps)
                    logger.info(
                        f"Emitted batch of {len(cached_entries_mcps)} entries ({entries_emitted} total) for project {project_id}"
                    )
                    cached_entries_mcps.clear()

            # Emit remaining cached entries MCPs (will overwrite any duplicate entities)
            yield from self._emit_final_batch(
                cached_entries_mcps, entries_emitted, project_id, "entries"
            )

        # Emit BigQuery containers (so entities can reference them)
        yield from gen_bigquery_containers(project_id, self.bq_containers, self.config)

        # Extract lineage for entities (after entities and containers have been processed)
        if self.config.include_lineage and self.lineage_extractor:
            yield from self._get_lineage_workunits(project_id)

    def _construct_mcps(
        self, dataset_urn: str, aspects: list
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Construct MCPs for the given dataset.

        Args:
            dataset_urn: Dataset URN
            aspects: List of aspect objects

        Yields:
            MetadataChangeProposalWrapper objects
        """
        return MetadataChangeProposalWrapper.construct_many(
            entityUrn=dataset_urn,
            aspects=aspects,
        )

    def _process_zone_entities(
        self, project_id: str, lake_id: str, zone_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Process all entities for a single zone (called by parallel workers).

        Args:
            project_id: GCP project ID
            lake_id: Dataplex lake ID
            zone_id: Dataplex zone ID

        Yields:
            MetadataChangeProposalWrapper objects for entities in this zone
        """
        yield from process_zone_entities(
            project_id=project_id,
            lake_id=lake_id,
            zone_id=zone_id,
            config=self.config,
            report=self.report,
            metadata_client=self.metadata_client,
            dataplex_client=self.dataplex_client,
            asset_metadata=self.asset_metadata,
            asset_metadata_lock=self._asset_metadata_lock,
            zone_metadata=self.zone_metadata,
            zone_metadata_lock=self._zone_metadata_lock,
            entity_data_by_project=self.entity_data_by_project,
            entity_data_lock=self._entity_data_lock,
            bq_containers=self.bq_containers,
            bq_containers_lock=self._bq_containers_lock,
            report_lock=self._report_lock,
            construct_mcps_fn=self._construct_mcps,
        )

    def _get_entities_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entities from Dataplex and generate corresponding MCPs as Datasets.

        This method parallelizes entity extraction at the zone level using ThreadedIteratorExecutor,
        following the pattern established by BigQuery V2.
        """
        parent = f"projects/{project_id}/locations/{self.config.location}"

        # Collect all zones to process in parallel
        zones_to_process = []

        try:
            with self.report.dataplex_api_timer as _:
                lakes_request = dataplex_v1.ListLakesRequest(parent=parent)
                lakes = self.dataplex_client.list_lakes(request=lakes_request)

            for lake in lakes:
                lake_id = lake.name.split("/")[-1]

                if not self.config.filter_config.entities.lake_pattern.allowed(lake_id):
                    continue

                zones_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}"
                zones_request = dataplex_v1.ListZonesRequest(parent=zones_parent)

                try:
                    zones = self.dataplex_client.list_zones(request=zones_request)

                    for zone in zones:
                        zone_id = zone.name.split("/")[-1]

                        if not self.config.filter_config.entities.zone_pattern.allowed(
                            zone_id
                        ):
                            continue

                        # Store zone type for later use in entity custom properties
                        zone_key = f"{project_id}.{lake_id}.{zone_id}"
                        with self._zone_metadata_lock:
                            self.zone_metadata[zone_key] = zone.type_.name

                        # Add this zone to the list for parallel processing
                        zones_to_process.append((project_id, lake_id, zone_id))

                except exceptions.GoogleAPICallError as e:
                    self.report.report_failure(
                        title=f"Failed to list zones in lake {lake_id}",
                        message=f"Error listing zones for entity extraction in project {project_id}",
                        exc=e,
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list lakes for entity extraction",
                message=f"Error listing lakes in project {project_id}",
                exc=e,
            )

        # Process zones in parallel using ThreadedIteratorExecutor
        if zones_to_process:
            logger.info(
                f"Processing {len(zones_to_process)} zones in parallel with {self.config.max_workers} workers"
            )
            for wu in ThreadedIteratorExecutor.process(
                worker_func=self._process_zone_entities,
                args_list=zones_to_process,
                max_workers=self.config.max_workers,
            ):
                yield wu

    def _get_entries_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entries from Universal Catalog and generate MCPs.

        This method uses the Entries API to extract metadata from Universal Catalog.
        It processes entry groups and their entries, extracting aspects as custom properties.

        Uses entries_location if specified, otherwise falls back to location.
        For system entry groups (@bigquery, @pubsub), use multi-region locations (us, eu, asia).
        """
        # Use configured entries_location (defaults to "us")
        entries_location = self.config.entries_location
        parent = f"projects/{project_id}/locations/{entries_location}"

        try:
            with self.report.catalog_api_timer as _:
                entry_groups_request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
                entry_groups = self.catalog_client.list_entry_groups(
                    request=entry_groups_request
                )

            for entry_group in entry_groups:
                entry_group_id = entry_group.name.split("/")[-1]
                logger.debug(f"Processing entry group: {entry_group_id}")
                with self._report_lock:
                    self.report.report_entry_group_scanned()
                entries_request = dataplex_v1.ListEntriesRequest(
                    parent=entry_group.name
                )
                entries = self.catalog_client.list_entries(request=entries_request)

                for entry in entries:
                    entry_id = entry.name.split("/")[-1]
                    logger.debug(f"Processing entry: {entry_id}")

                    # Apply dataset_pattern filter to entries
                    if not self.config.filter_config.entries.dataset_pattern.allowed(
                        entry_id
                    ):
                        logger.debug(
                            f"Entry {entry_id} filtered out by entries.dataset_pattern"
                        )
                        with self._report_lock:
                            self.report.report_entry_scanned(entry_id, filtered=True)
                        continue

                    entry_details_request = dataplex_v1.GetEntryRequest(
                        name=entry.name, view=dataplex_v1.EntryView.ALL
                    )
                    entry_details = self.catalog_client.get_entry(
                        request=entry_details_request
                    )

                    with self._report_lock:
                        self.report.report_entry_scanned(entry_id)
                    yield from self._process_entry(
                        project_id, entry_details, entry_group_id
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list entry groups for entity extraction",
                message=f"Error listing entry groups in project {project_id}",
                exc=e,
            )

    def _process_entry(
        self,
        project_id: str,
        entry: dataplex_v1.Entry,
        entry_group_id: str,
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Process a single entry from Universal Catalog.

        Args:
            project_id: GCP project ID
            entry: Entry object from Catalog API
            entry_group_id: Entry group ID

        Yields:
            MetadataChangeProposalWrapper objects for the entry
        """
        yield from process_entry(
            project_id=project_id,
            entry=entry,
            entry_group_id=entry_group_id,
            config=self.config,
            entity_data_by_project=self.entity_data_by_project,
            entity_data_lock=self._entity_data_lock,
            bq_containers=self.bq_containers,
            bq_containers_lock=self._bq_containers_lock,
            construct_mcps_fn=self._construct_mcps,
        )

    def _get_lineage_workunits(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage for entities in a project.

        Args:
            project_id: GCP project ID

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.lineage_extractor:
            return

        # Get entity IDs that were processed for this project
        entity_data = self.entity_data_by_project.get(project_id, set())
        if not entity_data:
            logger.info(
                f"No entities found for lineage extraction in project {project_id}"
            )
            return

        logger.info(
            f"Extracting lineage for {len(entity_data)} entities in project {project_id}"
        )

        try:
            yield from self.lineage_extractor.get_lineage_workunits(
                project_id, entity_data
            )
        except Exception as e:
            logger.warning(f"Failed to extract lineage for project {project_id}: {e}")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
