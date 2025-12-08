"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex, including:
- Projects as Containers
- Lakes as Containers (sub-containers of Projects)
- Zones as Containers (sub-containers of Lakes)
- Assets as Data Products (linked to Zone containers)
- Entities (discovered tables/filesets) as Datasets (linked to Zone containers)

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import logging
from threading import Lock
from typing import Iterable, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.cloud.datacatalog.lineage_v1 import LineageClient
from google.oauth2 import service_account

from datahub.emitter import mcp_builder as builder
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import ContainerKey, ProjectIdKey
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    extract_entity_metadata,
    make_audit_stamp,
    make_bigquery_dataset_container_key,
    make_entity_dataset_urn,
    map_dataplex_field_to_datahub,
)
from datahub.ingestion.source.dataplex.dataplex_lineage import DataplexLineageExtractor
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.ingestion.source.sql.sql_utils import (
    gen_database_container,
    gen_schema_container,
)
from datahub.ingestion.source.state.redundant_run_skip_handler import (
    RedundantLineageRunSkipHandler,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    NumberTypeClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    StringTypeClass,
    SubTypesClass,
    TimeTypeClass,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)


@platform_name("Dataplex", id="dataplex")
@config_class(DataplexConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Links BigQuery entities to BigQuery dataset containers. Dataplex hierarchy (lakes, zones, assets) preserved as custom properties.",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Extract schema information from discovered entities",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Extract lineage from Dataplex Lineage API",
)
@capability(
    SourceCapability.DELETION_DETECTION,
    "Enabled by default when stateful ingestion is configured",
)
@capability(
    SourceCapability.TEST_CONNECTION,
    "Verifies connectivity to Dataplex API",
)
class DataplexSource(StatefulIngestionSourceBase, TestableSource):
    """Source to ingest metadata from Google Dataplex."""

    platform: str = "dataplex"

    def __init__(self, ctx: PipelineContext, config: DataplexConfig):
        super().__init__(config, ctx)
        self.config = config
        self.report = DataplexReport()

        # Track entity IDs for lineage extraction
        # Key: project_id, Value: set of tuples (entity_id, zone_id, lake_id)
        self.entity_data_by_project: dict[str, set[EntityDataTuple]] = {}

        # Track which entities were found in Entries API to avoid duplicate processing
        self.entries_found: set[str] = set()

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
            self.lineage_client = LineageClient(credentials=credentials)
            self.lineage_extractor = DataplexLineageExtractor(
                config=self.config,
                report=self.report,
                lineage_client=self.lineage_client,
                dataplex_client=self.dataplex_client,
                redundant_run_skip_handler=redundant_lineage_run_skip_handler,
            )
        else:
            self.lineage_client = None
            self.lineage_extractor = None

        self.asset_metadata = {}
        self.zone_metadata = {}  # Store zone types for adding to entity custom properties

        # Track BigQuery containers to create (project_id -> set of dataset_ids)
        self.bq_containers: dict[str, set[str]] = {}

        # Thread safety locks for parallel processing
        self._report_lock = Lock()
        self._asset_metadata_lock = Lock()
        self._entity_data_lock = Lock()
        self._zone_metadata_lock = Lock()
        self._bq_containers_lock = Lock()
        self._entries_found_lock = Lock()

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
                list(dataplex_client.list_lakes(parent=parent, page_size=1))

            test_report.basic_connectivity = TestConnectionReport.Capability(
                capable=True
            )
        except exceptions.GoogleAPICallError as e:
            test_report.basic_connectivity = TestConnectionReport.Capability(
                capable=False, failure_reason=f"Failed to connect to Dataplex: {e}"
            )
        except Exception as e:
            test_report.basic_connectivity = TestConnectionReport.Capability(
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
        """Process all Dataplex resources for a single project.

        This uses a multi-pass approach:
        1. If include_entries is enabled, process Entries API first (discovers containers)
        2. If include_entities is enabled:
           - First pass: Stream through entities to discover containers (tracking only)
           - Skip entities already found in Entries API
        3. Emit containers (lightweight)
        4. If include_entries is enabled, re-stream entries to emit full metadata
        5. If include_entities is enabled, re-stream entities to emit full metadata

        The container tracking (self.bq_containers) is populated during the first pass.

        Memory optimization: The entries_found set is cleared after the first pass to
        free memory before emitting metadata.
        """
        # Process Entries API first (if enabled)
        if self.config.include_entries:
            logger.info(
                f"Processing entries from Universal Catalog for project {project_id}"
            )
            # First pass: Stream entries to discover containers (consume but don't yield)
            for _ in self._get_entries_mcps(project_id):
                pass  # Container tracking happens in _process_entry

        # Process Entities API (if enabled)
        if self.config.include_entities:
            logger.info(
                f"Processing entities from Dataplex Entities API for project {project_id}"
            )
            # First pass: Stream entities to discover containers (consume but don't yield)
            for _ in self._get_entities_mcps(project_id):
                pass  # Container tracking happens in _process_zone_entities

        # Clear entries_found set to free memory after deduplication
        # We only need it during the first pass to detect duplicates
        if self.config.include_entries and self.config.include_entities:
            entries_count = len(self.entries_found)
            with self._entries_found_lock:
                self.entries_found.clear()
            logger.info(
                f"Cleared {entries_count} entries from deduplication tracking to free memory"
            )

        # Emit BigQuery containers BEFORE entities (so entities can reference them)
        yield from self._gen_bigquery_containers(project_id)

        # Second pass: Re-stream to emit full metadata
        if self.config.include_entries:
            logger.info(f"Emitting entries metadata for project {project_id}")
            yield from auto_workunit(self._get_entries_mcps(project_id))

        if self.config.include_entities:
            logger.info(f"Emitting entities metadata for project {project_id}")
            yield from auto_workunit(self._get_entities_mcps(project_id))

        # Extract lineage for entities (after entities and containers have been processed)
        if self.config.include_lineage and self.lineage_extractor:
            yield from self._get_lineage_workunits(project_id)

    def _extract_aspects_to_custom_properties(
        self, aspects: dict, custom_properties: dict[str, str]
    ) -> None:
        """Extract aspects as custom properties.

        Args:
            aspects: Dictionary of aspects from entry or entity
            custom_properties: Dictionary to update with aspect properties
        """
        for aspect_key, aspect_value in aspects.items():
            aspect_type = aspect_key.split("/")[-1]
            custom_properties[f"dataplex_aspect_{aspect_type}"] = aspect_type

            if hasattr(aspect_value, "data") and aspect_value.data:
                for field_key, field_value in aspect_value.data.items():
                    property_key = f"dataplex_{aspect_type}_{field_key}"
                    custom_properties[property_key] = str(field_value)

    def _track_bigquery_container(
        self, project_id: str, dataset_id: str
    ) -> Optional[str]:
        """Track BigQuery dataset for container creation and return container URN.

        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID (format: project.dataset)

        Returns:
            Container URN if BigQuery, None otherwise
        """
        with self._bq_containers_lock:
            if project_id not in self.bq_containers:
                self.bq_containers[project_id] = set()
            self.bq_containers[project_id].add(dataset_id)

        bq_dataset_container_key = make_bigquery_dataset_container_key(
            project_id=project_id,
            dataset_id=dataset_id,
            platform="bigquery",
            env=self.config.env,
        )
        return bq_dataset_container_key.as_urn()

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

    def _extract_entry_custom_properties(
        self, entry: dataplex_v1.Entry, entry_id: str, entry_group_id: str
    ) -> dict[str, str]:
        """Extract custom properties from a Dataplex entry.

        Args:
            entry: Entry object from Catalog API
            entry_id: Entry ID
            entry_group_id: Entry group ID

        Returns:
            Dictionary of custom properties
        """
        custom_properties = {
            "dataplex_ingested": "true",
            "dataplex_entry_id": entry_id,
            "dataplex_entry_group": entry_group_id,
            "dataplex_fully_qualified_name": entry.fully_qualified_name,
        }

        if entry.entry_type:
            custom_properties["dataplex_entry_type"] = entry.entry_type

        if hasattr(entry, "parent_entry") and entry.parent_entry:
            custom_properties["dataplex_parent_entry"] = entry.parent_entry

        # Extract entry source metadata
        if entry.entry_source:
            if hasattr(entry.entry_source, "resource") and entry.entry_source.resource:
                custom_properties["dataplex_source_resource"] = (
                    entry.entry_source.resource
                )
            if hasattr(entry.entry_source, "system") and entry.entry_source.system:
                custom_properties["dataplex_source_system"] = entry.entry_source.system
            if hasattr(entry.entry_source, "platform") and entry.entry_source.platform:
                custom_properties["dataplex_source_platform"] = (
                    entry.entry_source.platform
                )

        # Extract aspects as custom properties
        if entry.aspects:
            self._extract_aspects_to_custom_properties(entry.aspects, custom_properties)

        return custom_properties

    def _extract_entity_custom_properties(
        self,
        entity_full: dataplex_v1.Entity,
        project_id: str,
        lake_id: str,
        zone_id: str,
        entity_id: str,
    ) -> dict[str, str]:
        """Extract custom properties from a Dataplex entity.

        Args:
            entity_full: Full entity object from Dataplex
            project_id: GCP project ID
            lake_id: Dataplex lake ID
            zone_id: Dataplex zone ID
            entity_id: Entity ID

        Returns:
            Dictionary of custom properties
        """
        custom_properties = {
            "dataplex_ingested": "true",
            "dataplex_lake": lake_id,
            "dataplex_zone": zone_id,
            "dataplex_entity_id": entity_id,
        }

        # Add zone type from metadata
        zone_key = f"{project_id}.{lake_id}.{zone_id}"
        with self._zone_metadata_lock:
            if zone_key in self.zone_metadata:
                custom_properties["dataplex_zone_type"] = self.zone_metadata[zone_key]

        if entity_full.data_path:
            custom_properties["data_path"] = entity_full.data_path

        if entity_full.system:
            custom_properties["system"] = entity_full.system.name

        if entity_full.format:
            custom_properties["format"] = entity_full.format.format_.name

        # Extract additional metadata fields
        if entity_full.asset:
            custom_properties["asset"] = entity_full.asset

        if hasattr(entity_full, "catalog_entry") and entity_full.catalog_entry:
            custom_properties["catalog_entry"] = entity_full.catalog_entry

        if hasattr(entity_full, "compatibility") and entity_full.compatibility:
            custom_properties["compatibility"] = str(entity_full.compatibility)

        # Extract aspects as custom properties
        if hasattr(entity_full, "aspects") and entity_full.aspects:
            self._extract_aspects_to_custom_properties(
                entity_full.aspects, custom_properties
            )

        return custom_properties

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
        entities_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}/zones/{zone_id}"
        entities_request = dataplex_v1.ListEntitiesRequest(parent=entities_parent)

        try:
            entities = self.metadata_client.list_entities(request=entities_request)

            for entity in entities:
                entity_id = entity.id
                logger.debug(
                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                )

                if not self.config.filter_config.entity_pattern.allowed(entity_id):
                    logger.debug(f"Entity {entity_id} filtered out by pattern")
                    with self._report_lock:
                        self.report.report_entity_scanned(entity_id, filtered=True)
                    continue

                with self._report_lock:
                    self.report.report_entity_scanned(entity_id)
                logger.debug(
                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                )

                # Determine source platform and dataset id from asset (bigquery, gcs, etc.)
                with self._asset_metadata_lock:
                    if entity.asset in self.asset_metadata:
                        source_platform, dataset_id = self.asset_metadata[entity.asset]
                    else:
                        source_platform, dataset_id = extract_entity_metadata(
                            project_id,
                            lake_id,
                            zone_id,
                            entity_id,
                            entity.asset,
                            self.config.location,
                            self.dataplex_client,
                        )

                # Check if this entity was already processed via Entries API
                # Construct FQN to match entries format
                if source_platform == "bigquery":
                    fqn = f"{source_platform}:{dataset_id}.{entity_id}"
                elif source_platform == "gcs":
                    fqn = f"{source_platform}:{dataset_id}/{entity_id}"
                else:
                    fqn = f"{source_platform}:{dataset_id}"

                with self._entries_found_lock:
                    if fqn in self.entries_found:
                        logger.debug(
                            f"Skipping entity {entity_id} - already processed via Entries API with FQN {fqn}"
                        )
                        continue

                # Track entity ID for lineage extraction
                with self._entity_data_lock:
                    if project_id not in self.entity_data_by_project:
                        self.entity_data_by_project[project_id] = set[EntityDataTuple]()
                    self.entity_data_by_project[project_id].add(
                        EntityDataTuple(
                            lake_id=lake_id,
                            zone_id=zone_id,
                            entity_id=entity_id,
                            asset_id=entity.asset,
                            source_platform=source_platform,
                            dataset_id=dataset_id,
                        )
                    )

                # Fetch full entity details including schema
                try:
                    get_entity_request = dataplex_v1.GetEntityRequest(
                        name=entity.name,
                        view=dataplex_v1.GetEntityRequest.EntityView.FULL,
                    )
                    entity_full = self.metadata_client.get_entity(
                        request=get_entity_request
                    )
                except exceptions.GoogleAPICallError as e:
                    logger.warning(
                        f"Could not fetch full entity details for {entity_id}: {e}"
                    )
                    entity_full = entity

                # Generate dataset URN with source platform (bigquery, gcs, etc.)
                dataset_urn = make_entity_dataset_urn(
                    entity_id,
                    project_id,
                    self.config.env,
                    dataset_id=dataset_id,
                    platform=source_platform,
                )

                # Extract schema metadata
                schema_metadata = self._extract_schema_metadata(
                    entity_full, dataset_urn, source_platform
                )

                # Extract custom properties using helper method
                custom_properties = self._extract_entity_custom_properties(
                    entity_full, project_id, lake_id, zone_id, entity_id
                )

                # Build aspects list
                aspects = [
                    DatasetPropertiesClass(
                        name=entity_id,
                        description=entity_full.description or "",
                        customProperties=custom_properties,
                        created=make_audit_stamp(entity_full.create_time),
                        lastModified=make_audit_stamp(entity_full.update_time),
                    ),
                    DataPlatformInstanceClass(
                        platform=str(DataPlatformUrn(source_platform))
                    ),
                    SubTypesClass(
                        typeNames=[
                            entity_full.type_.name,
                        ]
                    ),
                ]

                # Add schema metadata if available
                if schema_metadata:
                    aspects.append(schema_metadata)

                # Link to source platform container (only for BigQuery)
                if source_platform == "bigquery":
                    container_urn = self._track_bigquery_container(
                        project_id, dataset_id
                    )
                    aspects.append(ContainerClass(container=container_urn))

                # Construct MCPs
                yield from self._construct_mcps(dataset_urn, aspects)

        except exceptions.GoogleAPICallError as e:
            with self._report_lock:
                self.report.report_failure(
                    title=f"Failed to list entities in zone {zone_id}",
                    message=f"Error listing entities in project {project_id}, lake {lake_id}, zone {zone_id}",
                    exc=e,
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
            with self.report.dataplex_api_timer:
                lakes_request = dataplex_v1.ListLakesRequest(parent=parent)
                lakes = self.dataplex_client.list_lakes(request=lakes_request)

            for lake in lakes:
                lake_id = lake.name.split("/")[-1]

                if not self.config.filter_config.lake_pattern.allowed(lake_id):
                    continue

                zones_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}"
                zones_request = dataplex_v1.ListZonesRequest(parent=zones_parent)

                try:
                    zones = self.dataplex_client.list_zones(request=zones_request)

                    for zone in zones:
                        zone_id = zone.name.split("/")[-1]

                        if not self.config.filter_config.zone_pattern.allowed(zone_id):
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
        """
        parent = f"projects/{project_id}/locations/{self.config.location}"

        try:
            with self.report.catalog_api_timer:
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

                    if not self.config.filter_config.entry_pattern.allowed(entry_id):
                        logger.debug(f"Entry {entry_id} filtered out by pattern")
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
        entry_id = entry.name.split("/")[-1]

        if not entry.fully_qualified_name:
            logger.debug(f"Entry {entry_id} has no fully_qualified_name, skipping")
            return

        fqn = entry.fully_qualified_name
        logger.debug(f"Processing entry with FQN: {fqn}")

        # Track that we found this entry
        with self._entries_found_lock:
            self.entries_found.add(fqn)

        # Parse the FQN to determine platform and dataset_id
        source_platform, dataset_id = self._parse_entry_fqn(fqn)
        if not source_platform or not dataset_id:
            logger.warning(f"Could not parse FQN {fqn} for entry {entry_id}, skipping")
            return

        # Track entry for lineage extraction (entries don't have lake/zone/asset info,
        # but lineage API only needs FQN which we can construct from entry metadata)
        with self._entity_data_lock:
            if project_id not in self.entity_data_by_project:
                self.entity_data_by_project[project_id] = set[EntityDataTuple]()
            self.entity_data_by_project[project_id].add(
                EntityDataTuple(
                    lake_id="",  # Not available in Entry objects
                    zone_id="",  # Not available in Entry objects
                    entity_id=entry_id,
                    asset_id="",  # Not available in Entry objects
                    source_platform=source_platform,
                    dataset_id=dataset_id,
                    is_entry=True,  # Flag that this is from Entries API
                )
            )

        # Generate dataset URN
        # For entries, use just the entry_id as the dataset name (e.g., "adoption-human_profiles")
        # rather than the full hierarchy (e.g., "project.dataset.table")
        dataset_urn = builder.make_dataset_urn_with_platform_instance(
            platform=source_platform,
            name=entry_id,
            platform_instance=None,
            env=self.config.env,
        )
        logger.debug(f"Created dataset URN for entry {entry_id}: {dataset_urn}")

        # Extract custom properties using helper method
        custom_properties = self._extract_entry_custom_properties(
            entry, entry_id, entry_group_id
        )

        # Try to extract schema from entry aspects
        schema_metadata = self._extract_schema_from_entry_aspects(
            entry, entry_id, source_platform
        )

        # Build aspects list
        aspects = [
            DatasetPropertiesClass(
                name=entry_id,
                description=entry.entry_source.description or "",
                customProperties=custom_properties,
                created=(
                    make_audit_stamp(entry.entry_source.create_time)
                    if entry.entry_source.create_time
                    else None
                ),
                lastModified=(
                    make_audit_stamp(entry.entry_source.update_time)
                    if entry.entry_source.update_time
                    else None
                ),
            ),
            DataPlatformInstanceClass(platform=str(DataPlatformUrn(source_platform))),
        ]

        # Add schema metadata if available
        if schema_metadata:
            aspects.append(schema_metadata)
            logger.debug(
                f"Added schema metadata for entry {entry_id} with {len(schema_metadata.fields)} fields"
            )

        # Link to source platform container (only for BigQuery)
        if source_platform == "bigquery":
            container_urn = self._track_bigquery_container(project_id, dataset_id)
            aspects.append(ContainerClass(container=container_urn))

        # Construct MCPs
        yield from self._construct_mcps(dataset_urn, aspects)

    def _parse_entry_fqn(self, fqn: str) -> tuple[str, str]:
        """Parse fully qualified name to extract platform and dataset_id.

        Args:
            fqn: Fully qualified name (e.g., 'bigquery:project.dataset.table')

        Returns:
            Tuple of (platform, dataset_id)
        """
        if ":" not in fqn:
            return "", ""

        platform, resource_path = fqn.split(":", 1)

        if platform == "bigquery":
            parts = resource_path.split(".")
            if len(parts) >= 2:
                dataset_id = f"{parts[1]}"
                return platform, dataset_id
        elif platform == "gcs":
            parts = resource_path.split("/")
            if parts:
                dataset_id = parts[0]
                return platform, dataset_id

        return platform, resource_path

    def _extract_schema_metadata(
        self, entity: dataplex_v1.Entity, dataset_urn: str, platform: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema metadata from Dataplex entity."""
        if not entity.schema or not entity.schema.fields:
            return None

        fields = []
        for field in entity.schema.fields:
            field_path = field.name

            field_type = map_dataplex_field_to_datahub(field)

            schema_field = SchemaFieldClass(
                fieldPath=field_path,
                type=field_type,
                nativeDataType=dataplex_v1.types.Schema.Type(field.type_).name,
                description=field.description or "",
                nullable=True,  # Dataplex doesn't explicitly track nullability
                recursive=False,
            )

            # Handle nested fields
            if field.fields:
                schema_field.type = SchemaFieldDataTypeClass(type=RecordTypeClass())
                # Add nested fields
                for nested_field in field.fields:
                    nested_field_path = f"{field_path}.{nested_field.name}"
                    nested_type = map_dataplex_field_to_datahub(nested_field)
                    nested_schema_field = SchemaFieldClass(
                        fieldPath=nested_field_path,
                        type=nested_type,
                        nativeDataType=dataplex_v1.types.Schema.Type(
                            nested_field.type_
                        ).name,
                        description=nested_field.description or "",
                        nullable=True,
                        recursive=False,
                    )
                    fields.append(nested_schema_field)

            fields.append(schema_field)

        return SchemaMetadataClass(
            schemaName=entity.id,
            platform=str(DataPlatformUrn(platform)),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    def _extract_field_value(
        self, field_data: any, field_key: str, default: str = ""
    ) -> str:
        """Extract a field value from protobuf field data (dict or object).

        Args:
            field_data: Field data (dict or protobuf object)
            field_key: Primary key to look for
            default: Default value if not found

        Returns:
            Extracted value as string
        """
        if isinstance(field_data, dict):
            val = field_data.get(field_key)
            if val is None:
                return default
            return (
                val.string_value
                if hasattr(val, "string_value")
                else str(val)
                if val
                else default
            )
        else:
            val = getattr(field_data, field_key, None)
            return str(val) if val else default

    def _process_schema_field_item(
        self, field_value: any, entry_id: str
    ) -> Optional[any]:
        """Process a single schema field item from protobuf data.

        Args:
            field_value: Field value from schema fields list
            entry_id: Entry ID for logging

        Returns:
            Field data object or None
        """
        if hasattr(field_value, "struct_value"):
            # Protobuf Value with struct_value
            return dict(field_value.struct_value.fields)
        elif hasattr(field_value, "__getitem__") or hasattr(field_value, "__dict__"):
            # Direct object or dict-like (proto.marshal objects)
            try:
                return (
                    dict(field_value) if hasattr(field_value, "items") else field_value
                )
            except (TypeError, AttributeError):
                return field_value
        return None

    def _extract_schema_from_entry_aspects(
        self, entry: dataplex_v1.Entry, entry_id: str, platform: str
    ) -> Optional[SchemaMetadataClass]:
        """Extract schema metadata from Entry aspects.

        Looks for schema-type aspects in the entry and extracts column/field information.
        The schema aspect is typically stored at 'dataplex-types.global.schema'.

        Args:
            entry: Entry object from Catalog API
            entry_id: Entry ID for naming the schema
            platform: Platform name (bigquery, gcs, etc.)

        Returns:
            SchemaMetadataClass if schema aspect found, None otherwise
        """
        if not entry.aspects:
            logger.debug(f"Entry {entry_id} has no aspects")
            return None

        # Log all available aspect types for debugging
        aspect_keys = list(entry.aspects.keys())
        logger.debug(f"Entry {entry_id} has aspects: {aspect_keys}")

        # Look for the standard Dataplex schema aspect type
        # According to Dataplex docs, schema aspects are at:
        # "dataplex-types.global.schema" or similar paths
        schema_aspect = None
        schema_aspect_key = None

        # First, try the standard Dataplex schema aspect type
        for aspect_key in entry.aspects:
            # Check for the global schema aspect type (most common)
            if "dataplex-types.global.schema" in aspect_key or aspect_key.endswith(
                "/schema"
            ):
                schema_aspect = entry.aspects[aspect_key]
                schema_aspect_key = aspect_key
                logger.debug(
                    f"Found schema aspect for entry {entry_id} at key: {aspect_key}"
                )
                break

        # Fallback: Look for any aspect with "schema" in the name
        if not schema_aspect:
            for aspect_key, aspect_value in entry.aspects.items():
                aspect_type = aspect_key.split("/")[-1]
                if "schema" in aspect_type.lower():
                    schema_aspect = aspect_value
                    schema_aspect_key = aspect_key
                    logger.debug(
                        f"Found schema-like aspect for entry {entry_id} at key: {aspect_key}"
                    )
                    break

        if not schema_aspect:
            logger.debug(
                f"No schema aspect found for entry {entry_id}. Available aspects: {aspect_keys}"
            )
            return None

        if not hasattr(schema_aspect, "data") or not schema_aspect.data:
            logger.debug(
                f"Schema aspect {schema_aspect_key} for entry {entry_id} has no data"
            )
            return None

        # Extract schema fields from aspect data
        fields = []
        try:
            # The aspect.data is a Struct (protobuf)
            data_dict = dict(schema_aspect.data)
            logger.debug(
                f"Schema aspect data keys for entry {entry_id}: {list(data_dict.keys())}"
            )

            # Common field names in schema aspects: columns, fields, schema
            schema_fields_data = (
                data_dict.get("columns")
                or data_dict.get("fields")
                or data_dict.get("schema")
            )

            if not schema_fields_data:
                logger.debug(
                    f"No column/field data found in schema aspect for entry {entry_id}. "
                    f"Available keys: {list(data_dict.keys())}"
                )
                return None

            # The schema_fields_data can be either:
            # 1. A protobuf Value with list_value attribute (from some aspects)
            # 2. A RepeatedComposite (proto.marshal list-like object) that can be iterated directly
            logger.debug(
                f"Processing schema fields for entry {entry_id}, type: {type(schema_fields_data).__name__}"
            )

            # Try to iterate schema_fields_data - it could be a RepeatedComposite or list_value
            schema_items = None
            if hasattr(schema_fields_data, "list_value"):
                # Protobuf Value with list_value
                schema_items = schema_fields_data.list_value.values
                logger.debug(
                    f"Found {len(schema_items)} fields in list_value for entry {entry_id}"
                )
            elif hasattr(schema_fields_data, "__iter__"):
                # RepeatedComposite or other iterable (can iterate directly)
                schema_items = list(schema_fields_data)
                logger.debug(
                    f"Found {len(schema_items)} fields in iterable for entry {entry_id}"
                )

            if schema_items:
                for field_value in schema_items:
                    field_data = self._process_schema_field_item(field_value, entry_id)
                    if field_data:
                        # Extract field name, type, and description using helper
                        field_name = self._extract_field_value(
                            field_data, "name"
                        ) or self._extract_field_value(field_data, "column")
                        field_type = self._extract_field_value(
                            field_data, "type"
                        ) or self._extract_field_value(field_data, "dataType", "string")
                        field_desc = self._extract_field_value(
                            field_data, "description"
                        )

                        if field_name:
                            # Map the type string to DataHub schema type
                            datahub_type = self._map_aspect_type_to_datahub(
                                str(field_type)
                            )

                            schema_field = SchemaFieldClass(
                                fieldPath=str(field_name),
                                type=datahub_type,
                                nativeDataType=str(field_type),
                                description=field_desc,
                                nullable=True,
                                recursive=False,
                            )
                            fields.append(schema_field)
                            logger.debug(
                                f"Extracted field '{field_name}' ({field_type}) for entry {entry_id}"
                            )

            if not fields:
                logger.debug(
                    f"No schema fields extracted from entry {entry_id} aspects"
                )
                return None

            logger.info(f"Extracted {len(fields)} schema fields for entry {entry_id}")
            return SchemaMetadataClass(
                schemaName=entry_id,
                platform=str(DataPlatformUrn(platform)),
                version=0,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
                fields=fields,
            )

        except Exception as e:
            logger.warning(
                f"Failed to extract schema from entry {entry_id} aspects: {e}",
                exc_info=True,
            )
            return None

    def _map_aspect_type_to_datahub(self, type_str: str) -> SchemaFieldDataTypeClass:
        """Map aspect schema type string to DataHub schema type.

        Args:
            type_str: Type string from aspect data (e.g., "STRING", "INTEGER", "BOOLEAN")

        Returns:
            SchemaFieldDataTypeClass for DataHub
        """
        type_str_upper = type_str.upper()

        # Map common types
        if type_str_upper in ("STRING", "VARCHAR", "CHAR", "TEXT"):
            return SchemaFieldDataTypeClass(type=StringTypeClass())
        elif type_str_upper in (
            "INTEGER",
            "INT",
            "INT64",
            "LONG",
        ) or type_str_upper in ("FLOAT", "DOUBLE", "NUMERIC", "DECIMAL"):
            return SchemaFieldDataTypeClass(type=NumberTypeClass())
        elif type_str_upper in ("BOOLEAN", "BOOL"):
            return SchemaFieldDataTypeClass(type=BooleanTypeClass())
        elif type_str_upper in ("TIMESTAMP", "DATETIME", "DATE", "TIME"):
            return SchemaFieldDataTypeClass(type=TimeTypeClass())
        elif type_str_upper in ("BYTES", "BINARY"):
            return SchemaFieldDataTypeClass(type=BytesTypeClass())
        elif type_str_upper in ("RECORD", "STRUCT"):
            return SchemaFieldDataTypeClass(type=RecordTypeClass())
        elif type_str_upper == "ARRAY":
            return SchemaFieldDataTypeClass(type=ArrayTypeClass(nestedType=["string"]))
        else:
            # Default to string for unknown types
            return SchemaFieldDataTypeClass(type=StringTypeClass())

    def _gen_bigquery_project_container(
        self, project_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate BigQuery project container entity."""
        database_container_key = ProjectIdKey(
            project_id=project_id,
            platform="bigquery",
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

        yield from gen_database_container(
            database=project_id,
            database_container_key=database_container_key,
            sub_types=[DatasetContainerSubTypes.BIGQUERY_PROJECT],
            name=project_id,
            qualified_name=project_id,
        )

    def _gen_bigquery_dataset_container(
        self, project_id: str, dataset_id: str
    ) -> Iterable[MetadataWorkUnit]:
        """Generate BigQuery dataset container entity."""
        # Create keys for project and dataset containers
        database_container_key: ContainerKey = ProjectIdKey(
            project_id=project_id,
            platform="bigquery",
            env=self.config.env,
            backcompat_env_as_instance=True,
        )

        schema_container_key = make_bigquery_dataset_container_key(
            project_id=project_id,
            dataset_id=dataset_id,
            platform="bigquery",
            env=self.config.env,
        )

        yield from gen_schema_container(
            database=project_id,
            schema=dataset_id,
            qualified_name=f"{project_id}.{dataset_id}",
            sub_types=[DatasetContainerSubTypes.BIGQUERY_DATASET],
            schema_container_key=schema_container_key,
            database_container_key=database_container_key,
        )

    def _gen_bigquery_containers(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """
        Generate BigQuery container entities for a project.

        Creates project container and dataset containers for all datasets
        discovered from Dataplex entities.
        """
        datasets = self.bq_containers.get(project_id, set())
        if not datasets:
            return

        logger.info(
            f"Creating BigQuery containers for project {project_id}: {len(datasets)} datasets"
        )

        # Emit project container first
        yield from self._gen_bigquery_project_container(project_id)

        # Emit dataset containers
        for dataset_id in sorted(datasets):  # Sort for deterministic order
            yield from self._gen_bigquery_dataset_container(project_id, dataset_id)

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
