"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex Universal Catalog, including:
- Entries (Universal Catalog) as Datasets with source platform URNs (bigquery, gcs, etc.)
- BigQuery Projects as Containers (project-level containers)
- BigQuery Datasets as Containers (dataset-level containers, nested under project containers)
- Business Glossary entities (GlossaryNode, GlossaryTerm) from Dataplex Business Glossary API
- Term-to-asset associations via Dataplex lookupEntryLinks API (opt-in)

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import logging
from functools import cached_property
from itertools import product
from typing import Dict, Iterable, List, Optional

import google.auth
import google.auth.transport.requests
from google.api_core import exceptions
from google.cloud import dataplex_v1, resourcemanager_v3
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
from datahub.ingestion.source.common.gcp_project_filter import resolve_gcp_projects
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_context import DataplexContext
from datahub.ingestion.source.dataplex.dataplex_entries import (
    DataplexEntriesProcessor,
)
from datahub.ingestion.source.dataplex.dataplex_glossary import (
    DataplexGlossaryProcessor,
)
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


def _resolve_project_numbers(
    project_ids: List[str],
    credentials: Optional[service_account.Credentials],
) -> Dict[str, str]:
    """Resolve GCP project IDs to project numbers via the Resource Manager API.

    The Dataplex lookupEntryLinks REST API requires project NUMBER (not project ID)
    inside glossary term entry paths. Called once at startup when
    include_glossaries=True.
    """
    rm_client = resourcemanager_v3.ProjectsClient(credentials=credentials)
    return {
        pid: rm_client.get_project(name=f"projects/{pid}").name.split("/")[-1]
        for pid in project_ids
    }


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
@capability(
    SourceCapability.GLOSSARY_TERMS,
    "Optionally enabled via configuration `include_glossaries`",
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

    - The glossary stage lists all glossaries across configured glossary_locations,
      then fetches categories and terms per glossary in parallel using
      ``max_workers_glossary`` workers.

    - Thread safety: ``DataplexEntriesReport`` and ``DataplexLineageReport``
      protect all mutable fields with an internal ``threading.Lock`` initialised
      in ``__post_init__``.  ``DataplexEntriesProcessor`` uses ``_container_lock``
      for atomic check+add on ``_emitted_project_containers`` and
      ``_entry_data_lock`` for appends to the shared ``DataplexContext.entry_data`` list.
      The GCP gRPC clients (``CatalogServiceClient``, ``LineageClient``) are
      thread-safe and shared across all workers.
    """

    platform: str = "dataplex"

    @cached_property
    def _project_ids(self) -> List[str]:
        """Effective list of GCP project ids, resolved lazily on first access.

        Honors `project_ids` (explicit override), then `project_labels`, then
        `project_id_pattern` via the shared GCP project filter helper. When
        `project_ids` is empty, projects are discovered via the Cloud Resource
        Manager API on first use.
        """
        projects_client = (
            resourcemanager_v3.ProjectsClient(credentials=self._credentials)
            if not self.config.project_ids
            else None
        )
        resolved = resolve_gcp_projects(
            self.config, self.report, projects_client=projects_client
        )
        return [p.id for p in resolved]

    def _resolve_lineage_project_location_pairs(self) -> list[tuple[str, str]]:
        """Resolve and report lineage scan pairs from configured project/location product."""
        lineage_project_location_pairs = list(
            product(self._project_ids, self.config.lineage_locations)
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

        creds = self.config.get_credentials()
        credentials = (
            service_account.Credentials.from_service_account_info(creds)
            if creds
            else None
        )
        # Stored for the lazy `_project_ids` cached_property to use on first access.
        self._credentials: Optional[service_account.Credentials] = credentials

        # Shared context — all processors read/write to this single object.
        self.ctx_data = DataplexContext(config=self.config, credentials=credentials)

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
            source_report=self.report,
            ctx=self.ctx_data,
        )

        # Glossary processor — only instantiated when glossary ingestion is enabled.
        self.glossary_processor: Optional[DataplexGlossaryProcessor] = None
        if self.config.include_glossaries:
            glossary_client = dataplex_v1.BusinessGlossaryServiceClient(
                credentials=credentials
            )

            if self.config.include_glossary_term_associations:
                # Project numbers are required for the lookupEntryLinks term entry path.
                # Requires roles/resourcemanager.projectViewer on all configured projects.
                try:
                    self.ctx_data.project_numbers = _resolve_project_numbers(
                        self._project_ids, credentials
                    )
                except exceptions.GoogleAPICallError as exc:
                    self.report.failure(
                        title="Failed to resolve GCP project numbers",
                        message=(
                            "Could not resolve project numbers via the Resource Manager API. "
                            "Ensure the service account has roles/resourcemanager.projectViewer "
                            "on all configured projects."
                        ),
                        context=str(self._project_ids),
                        exc=exc,
                    )
                    raise
                raw_creds = (
                    credentials
                    if credentials is not None
                    else google.auth.default(
                        scopes=["https://www.googleapis.com/auth/cloud-platform"]
                    )[0]
                )
                self.ctx_data.authed_session = (
                    google.auth.transport.requests.AuthorizedSession(raw_creds)
                )

            self.glossary_processor = DataplexGlossaryProcessor(
                ctx=self.ctx_data,
                glossary_client=glossary_client,
                report=self.report.glossary_report,
                source_report=self.report,
            )

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        """Test connection to Dataplex API.

        When `project_ids` is set explicitly we list entry groups on the first
        configured project. When the user relies on `project_id_pattern` or
        `project_labels`, we first resolve at least one project via the Cloud
        Resource Manager API (which validates those credentials/permissions)
        and then list entry groups on the first resolved project.
        """
        test_report = TestConnectionReport()
        try:
            config = DataplexConfig.model_validate(config_dict)
            creds = config.get_credentials()
            credentials = (
                service_account.Credentials.from_service_account_info(creds)
                if creds
                else None
            )

            if config.project_ids:
                project_id: Optional[str] = config.project_ids[0]
            else:
                # Auto-discovery mode — exercise the Resource Manager API to
                # verify credentials and roles.
                projects_client = resourcemanager_v3.ProjectsClient(
                    credentials=credentials
                )
                probe_report = DataplexReport()
                resolved = resolve_gcp_projects(
                    config, probe_report, projects_client=projects_client
                )
                if not resolved:
                    failure_reason = (
                        "No GCP projects matched the configured "
                        "project_id_pattern / project_labels, or the service "
                        "account lacks Cloud Resource Manager access."
                    )
                    test_report.basic_connectivity = CapabilityReport(
                        capable=False, failure_reason=failure_reason
                    )
                    return test_report
                project_id = resolved[0].id

            catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)
            parent = f"projects/{project_id}/locations/{config.entries_locations[0]}"
            entry_groups_request = dataplex_v1.ListEntryGroupsRequest(parent=parent)
            # Iterate once to verify Dataplex API access.
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
                        project_ids=self._project_ids,
                        max_workers=self.config.max_workers_entries,
                    )
                )
            except exceptions.GoogleAPICallError as exc:
                self.report.warning(
                    title="Failed to process Dataplex entries",
                    message="Error while extracting entries from Universal Catalog.",
                    context=str(self._project_ids),
                    exc=exc,
                )

        if self.config.include_glossaries and self.glossary_processor:
            with self.report.new_stage(
                "Processing Dataplex Business Glossaries (parallel)"
            ):
                try:
                    yield from self.glossary_processor.process_glossaries(
                        project_ids=self._project_ids,
                        max_workers=self.config.max_workers_glossary,
                    )
                except exceptions.GoogleAPICallError as exc:
                    self.report.warning(
                        title="Failed to process Dataplex glossaries",
                        message="Error while extracting Business Glossary entities.",
                        context=str(self._project_ids),
                        exc=exc,
                    )

            if self.config.include_glossary_term_associations:
                with self.report.new_stage(
                    "Extracting Dataplex term-asset associations (parallel)"
                ):
                    try:
                        yield from self.glossary_processor.process_term_associations(
                            project_ids=self.config.project_ids,
                            max_workers=self.config.max_workers_glossary,
                        )
                    except Exception as exc:
                        self.report.warning(
                            title="Failed to extract term-asset associations",
                            message="Error while calling lookupEntryLinks.",
                            context=str(self.config.project_ids),
                            exc=exc,
                        )

        if self.config.include_lineage and self.lineage_extractor:
            with self.report.new_stage(
                "Extracting Dataplex lineage across configured projects (parallel)"
            ):
                if len(self.ctx_data.entry_data) == 0:
                    logger.info(
                        "No entries found for lineage extraction across configured projects"
                    )
                    return

                logger.info(
                    "Extracting lineage for %s entries across %s configured projects",
                    len(self.ctx_data.entry_data),
                    len(self.config.project_ids),
                )
                lineage_project_location_pairs = (
                    self._resolve_lineage_project_location_pairs()
                )

                try:
                    yield from self.lineage_extractor.get_lineage_workunits(
                        self.ctx_data.entry_data,
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
