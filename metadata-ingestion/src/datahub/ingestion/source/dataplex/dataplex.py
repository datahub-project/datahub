"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex Universal Catalog, including:
- Entries (Universal Catalog) as Datasets with source platform URNs (bigquery, gcs, etc.)
- BigQuery Projects as Containers (project-level containers)
- BigQuery Datasets as Containers (dataset-level containers, nested under project containers)

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
from datahub.ingestion.source.dataplex.dataplex_entries import process_entry
from datahub.ingestion.source.dataplex.dataplex_helpers import EntryDataTuple
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

        # Track entry data for lineage extraction
        # Key: project_id, Value: set of EntryDataTuple
        self.entry_data_by_project: dict[str, set[EntryDataTuple]] = {}

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )

        # Catalog client for Entry Groups and Entries extraction
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
                    redundant_run_skip_handler=redundant_lineage_run_skip_handler,
                )
            )
        else:
            self.lineage_client = None
            self.lineage_extractor = None

        # Track BigQuery containers to create (project_id -> set of dataset_ids)
        self.bq_containers: dict[str, set[str]] = {}

        # Thread safety locks for parallel processing
        self._report_lock = Lock()
        self._entry_data_lock = Lock()
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

            # Test connection by attempting to list entry groups
            catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)
            if config.project_ids:
                project_id = config.project_ids[0]
                parent = f"projects/{project_id}/locations/{config.entries_location}"
                entry_groups_request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
                # Just iterate once to verify access
                for _ in catalog_client.list_entry_groups(request=entry_groups_request):
                    break

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
            resource_type: Type of resource being emitted

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
        1. Collect entries MCPs in batches and track containers simultaneously
        2. Emit batches as they fill up to keep memory bounded
        3. Emit BigQuery containers (so entries can reference them)
        4. Extract lineage

        Memory optimization: Batched emission prevents memory issues in large deployments.
        """
        # Determine batch size (None means no batching)
        batch_size = self.config.batch_size
        should_batch = batch_size is not None

        # Cache MCPs during the first pass
        cached_entries_mcps: list[MetadataChangeProposalWrapper] = []
        entries_emitted = 0

        # Process Entries API - collect MCPs and track containers
        logger.info(
            f"Processing entries from Universal Catalog for project {project_id}"
        )
        for mcp in self._get_entries_mcps(project_id):
            cached_entries_mcps.append(mcp)

            # Emit batch if we've reached the batch size
            if should_batch and batch_size and len(cached_entries_mcps) >= batch_size:
                yield from auto_workunit(cached_entries_mcps)
                entries_emitted += len(cached_entries_mcps)
                logger.info(
                    f"Emitted batch of {len(cached_entries_mcps)} entries ({entries_emitted} total) for project {project_id}"
                )
                cached_entries_mcps.clear()

        # Emit remaining cached entries MCPs
        yield from self._emit_final_batch(
            cached_entries_mcps, entries_emitted, project_id, "entries"
        )

        # Emit BigQuery containers (so entries can reference them)
        yield from gen_bigquery_containers(project_id, self.bq_containers, self.config)

        # Extract lineage for entries (after entries and containers have been processed)
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

    def _get_entries_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entries from Universal Catalog and generate MCPs.

        This method uses the Entries API to extract metadata from Universal Catalog.
        It processes entry groups and their entries, extracting aspects as custom properties.

        For system entry groups (@bigquery, @pubsub), use multi-region locations (us, eu, asia).
        """
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
                title="Failed to list entry groups",
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
            entry_data_by_project=self.entry_data_by_project,
            entry_data_lock=self._entry_data_lock,
            bq_containers=self.bq_containers,
            bq_containers_lock=self._bq_containers_lock,
            construct_mcps_fn=self._construct_mcps,
        )

    def _get_lineage_workunits(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """
        Extract lineage for entries in a project.

        Args:
            project_id: GCP project ID

        Yields:
            MetadataWorkUnit objects containing lineage information
        """
        if not self.lineage_extractor:
            return

        # Get entry data that was processed for this project
        entry_data = self.entry_data_by_project.get(project_id, set())
        if not entry_data:
            logger.info(
                f"No entries found for lineage extraction in project {project_id}"
            )
            return

        logger.info(
            f"Extracting lineage for {len(entry_data)} entries in project {project_id}"
        )

        try:
            yield from self.lineage_extractor.get_lineage_workunits(
                project_id, entry_data
            )
        except Exception as e:
            logger.warning(f"Failed to extract lineage for project {project_id}: {e}")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
