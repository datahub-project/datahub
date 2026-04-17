"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex Universal Catalog, including:
- Entries (Universal Catalog) as Datasets with source platform URNs (bigquery, gcs, etc.)
- BigQuery Projects as Containers (project-level containers)
- BigQuery Datasets as Containers (dataset-level containers, nested under project containers)

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import logging
from itertools import product
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


@platform_name("Google Cloud Knowledge Catalog (Dataplex)", id="dataplex")
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
    SourceCapability.DESCRIPTIONS,
    "Enabled by default",
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

    Lineage extraction strategy:
    - The source first traverses all entries, then performs lineage lookups for each
      entry across the full configured ``(project_id, location)`` scan matrix.
    - This is required because the Dataplex Lineage API does not provide a bulk
      lineage retrieval endpoint that can return all lineage for a project in one call.
    - For graph construction, upstream lookup is sufficient: we query links with
      ``target=<entry_fqn>`` and build DataHub upstream edges from returned sources.
    - The tradeoff is API call volume: every entry is queried separately against every
      configured project-location pair.

    Parallelization strategy:
    - The entries stage runs in three sequential phases inside
      ``DataplexEntriesProcessor.process_entries``:

      * Phase 1a (sequential): ``list_entry_groups`` + ``list_entries`` across all
        configured project × location pairs.  These listing calls are fast (paginated,
        no detail payload) and accumulate a flat list of entry-name stubs.

      * Phase 1b (parallel): ``get_entry(view=ALL)`` calls are submitted to a
        ``ThreadPoolExecutor`` with ``max_workers_entries`` workers.  This is the
        main bottleneck — one blocking RPC per entry.  Distributing across a single
        flat pool avoids skew when entries are unevenly spread across projects.

      * Phase 1c (sequential): Spanner entries are fetched via the
        ``search_entries`` workaround.  They arrive already fully-fetched so
        there is no detail-fetch RPC to parallelise.

    - The lineage stage submits one task per entry to a ``ThreadPoolExecutor``
      with ``max_workers_lineage`` workers via
      ``DataplexLineageExtractor.get_lineage_workunits``.  Each worker
      queries the Dataplex Lineage API across all configured project/location
      pairs with the configured retry logic.

    - Thread safety: ``DataplexEntriesReport`` and ``DataplexLineageReport``
      protect all mutable fields with an internal ``threading.Lock`` initialised
      in ``__post_init__``.  ``DataplexEntriesProcessor`` uses ``_container_lock``
      for atomic check+add on ``_emitted_project_containers`` and
      ``_entry_data_lock`` for appends to the shared ``entry_data`` list.
      The GCP gRPC clients (``CatalogServiceClient``, ``LineageClient``) are
      thread-safe and shared across all workers.
    """

    platform: str = "dataplex"

    def _resolve_lineage_project_location_pairs(self) -> list[tuple[str, str]]:
        """Resolve and report lineage scan pairs from configured project/location product."""
        lineage_project_location_pairs = list(
            product(self.config.project_ids, self.config.lineage_locations)
        )
        self.report.info(
            title="Lineage extraction project/location pairs",
            message=(
                "Extracting lineage for configured project/location pairs. "
                "Lineage scan scope is derived from the Cartesian product of "
                "project_ids and lineage_locations."
            ),
            context=str(
                dict(
                    lineage_locations=self.config.lineage_locations,
                    lineage_project_location_pairs=lineage_project_location_pairs,
                )
            ),
        )
        return lineage_project_location_pairs

    def __init__(self, ctx: PipelineContext, config: DataplexConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report: DataplexReport = DataplexReport()

        # Track entry data for lineage extraction across all configured projects.
        self.entry_data: list[EntryDataTuple] = []

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )

        # Catalog client for Entry Groups and Entries extraction
        self.catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)

        # Initialize redundant lineage run skip handler for stateful lineage ingestion.
        # TODO: Wire this into DataplexLineageExtractor execution flow so lineage API calls
        # can be short-circuited for redundant runs.
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
                    report=self.report.lineage_report,
                    source_report=self.report,
                    lineage_client=self.lineage_client,
                    redundant_run_skip_handler=redundant_lineage_run_skip_handler,
                )
            )
        else:
            self.lineage_client = None
            self.lineage_extractor = None

        self.entries_processor = DataplexEntriesProcessor(
            config=self.config,
            catalog_client=self.catalog_client,
            report=self.report.entries_report,
            entry_data=self.entry_data,
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
        with self.report.new_stage(
            "Processing entries from Universal Catalog (parallel)"
        ):
            try:
                yield from auto_workunit(
                    self.entries_processor.process_entries(
                        project_ids=self.config.project_ids,
                        max_workers=self.config.max_workers_entries,
                    )
                )
            except exceptions.GoogleAPICallError as exc:
                self.report.warning(
                    title="Failed to process Dataplex entries",
                    message="Error while extracting entries from Universal Catalog.",
                    context=str(self.config.project_ids),
                    exc=exc,
                )

        if self.config.include_lineage and self.lineage_extractor:
            with self.report.new_stage(
                "Extracting Dataplex lineage across configured projects (parallel)"
            ):
                if len(self.entry_data) == 0:
                    logger.info(
                        "No entries found for lineage extraction across configured projects"
                    )
                    return

                logger.info(
                    "Extracting lineage for %s entries across %s configured projects",
                    len(self.entry_data),
                    len(self.config.project_ids),
                )
                lineage_project_location_pairs = (
                    self._resolve_lineage_project_location_pairs()
                )

                try:
                    yield from self.lineage_extractor.get_lineage_workunits(
                        self.entry_data,
                        active_lineage_project_location_pairs=lineage_project_location_pairs,
                        max_workers=self.config.max_workers_lineage,
                    )
                except Exception as e:
                    self.report.warning(
                        title="Lineage extraction failed",
                        message="Failed to extract lineage across configured projects",
                        context="all-configured-projects",
                        exc=e,
                    )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
