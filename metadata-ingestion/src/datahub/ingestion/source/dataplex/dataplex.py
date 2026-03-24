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
from google.cloud.datacatalog_lineage import LineageClient
from google.oauth2 import service_account

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
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
)
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
    SourceCapability.PLATFORM_INSTANCE,
    "Project containers are generated instead",
    supported=False,
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Enabled by default",
)
class DataplexSource(StatefulIngestionSourceBase, TestableSource):
    """Source to ingest metadata from Google Dataplex Universal Catalog.

    To add support for a new Dataplex entry type, first define its identity model
    in ``dataplex_ids.py`` by adding a SchemaKey class at the correct level in the
    existing hierarchy (for example project -> instance -> database -> table). Then
    register the entry type in ``DATAPLEX_ENTRY_TYPE_MAPPINGS`` with required fields:
    DataHub platform, DataHub entity type (Container or Dataset), subtype constant
    from ``subtypes.py``, FQN regex, parent-entry regex (if applicable), and key classes.
    FQN formats and parent hierarchy must be derived from:
    https://cloud.google.com/dataplex/docs/fully-qualified-names

    Decide whether the entry maps to a Container or Dataset based on whether it is
    a logical grouping level (instance/database/dataset) versus a concrete asset
    (table/topic). Use subtype constants from ``datahub.ingestion.source.common.subtypes``.
    Top-level containers may not have ``parent_entry`` and should rely on key hierarchy
    for parent traversal. Also note that Spanner entries are currently fetched through
    a search workaround, which bypasses entry-group filtering and must be controlled
    with entry-level filters.
    """

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

        self._entry_data_lock = Lock()
        self.entries_processor = DataplexEntriesProcessor(
            config=self.config,
            catalog_client=self.catalog_client,
            report=self.report,
            entry_data_by_project=self.entry_data_by_project,
            entry_data_lock=self._entry_data_lock,
            source_report=self.report,
        )

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
                parent = (
                    f"projects/{project_id}/locations/{config.entries_locations[0]}"
                )
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

    def _process_project(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Process all Dataplex resources for a single project."""
        logger.info(
            "Processing entries from Universal Catalog for project %s", project_id
        )
        try:
            yield from auto_workunit(self.entries_processor.process_project(project_id))
        except exceptions.GoogleAPICallError as exc:
            self.report.report_failure(
                title="Failed to process Dataplex entries",
                message="Error while extracting entries from Universal Catalog.",
                context=project_id,
                exc=exc,
            )
            return

        if self.config.include_lineage and self.lineage_extractor:
            yield from self._get_lineage_workunits(project_id)

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
            self.report.report_failure(
                title="Lineage extraction failed",
                message="Failed to extract lineage for project",
                context=project_id,
                exc=e,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
