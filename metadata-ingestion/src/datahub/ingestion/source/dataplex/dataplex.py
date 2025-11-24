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
from typing import Iterable, Optional

from google.api_core import exceptions
from google.cloud import dataplex_v1
from google.oauth2 import service_account

try:
    from google.cloud.datacatalog.lineage_v1 import LineageClient

    LINEAGE_AVAILABLE = True
except ImportError:
    LINEAGE_AVAILABLE = False
    LineageClient = None

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import gen_containers
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
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    EntityDataTuple,
    extract_entity_metadata,
    make_asset_container_key,
    make_audit_stamp,
    make_entity_dataset_urn,
    make_lake_container_key,
    make_project_container_key,
    make_source_dataset_urn,
    make_zone_container_key,
    map_dataplex_field_to_datahub,
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
from datahub.metadata.com.linkedin.pegasus2avro.common import Siblings
from datahub.metadata.schema_classes import (
    ContainerClass,
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    OtherSchemaClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformUrn
from datahub.utilities.threaded_iterator_executor import ThreadedIteratorExecutor

logger = logging.getLogger(__name__)


@platform_name("Dataplex", id="dataplex")
@config_class(DataplexConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.CONTAINERS,
    "Maps Projects, Lakes, and Zones to hierarchical Containers",
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

        if self.config.extract_lineage:
            if not LINEAGE_AVAILABLE:
                logger.warning(
                    "Lineage extraction is enabled but google-cloud-datacatalog-lineage is not installed. "
                    "Install it with: pip install 'google-cloud-datacatalog-lineage>=0.3.0'"
                )
                self.lineage_client = None
                self.lineage_extractor = None
            else:
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
        """Process all Dataplex resources for a single project."""
        yield from self._gen_project_workunits(project_id)

        if self.config.extract_lakes:
            yield from auto_workunit(self._get_lakes_mcps(project_id))

        if self.config.extract_zones:
            yield from auto_workunit(self._get_zones_mcps(project_id))

        if self.config.extract_assets:
            yield from auto_workunit(self._get_assets_mcps(project_id))

        if self.config.extract_entities:
            yield from auto_workunit(self._get_entities_mcps(project_id))

        if self.config.extract_entry_groups:
            yield from auto_workunit(self._get_entry_groups_mcps(project_id))

        if self.config.extract_entries:
            yield from auto_workunit(self._get_entries_mcps(project_id))

        # Extract lineage for entities (after entities have been processed)
        if self.config.extract_lineage and self.lineage_extractor:
            yield from self._get_lineage_workunits(project_id)

    def _gen_project_workunits(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for GCP Project as a Container."""
        project_container_key = make_project_container_key(
            project_id=project_id,
            platform=self.platform,
            env=self.config.env,
        )

        yield from gen_containers(
            container_key=project_container_key,
            name=project_id,
            description=f"Google Cloud Project: {project_id}",
            sub_types=["GCP Project"],
            extra_properties={
                "location": self.config.location,
            },
        )

    def _get_lakes_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch lakes from Dataplex and generate corresponding MCPs as Containers."""
        parent = f"projects/{project_id}/locations/{self.config.location}"

        try:
            with self.report.dataplex_api_timer:
                request = dataplex_v1.ListLakesRequest(parent=parent)
                lakes = self.dataplex_client.list_lakes(request=request)

            for lake in lakes:
                lake_id = lake.name.split("/")[-1]

                if not self.config.filter_config.lake_pattern.allowed(lake_id):
                    logger.debug(f"Lake {lake_id} filtered out by pattern")
                    self.report.report_lake_scanned(lake_id, filtered=True)
                    continue

                self.report.report_lake_scanned(lake_id)
                logger.info(f"Processing lake: {lake_id} in project: {project_id}")

                # Create lake container key
                lake_container_key = make_lake_container_key(
                    project_id=project_id,
                    lake_id=lake_id,
                    platform=self.platform,
                    env=self.config.env,
                )

                # Create parent project container key
                project_container_key = make_project_container_key(
                    project_id=project_id,
                    platform=self.platform,
                    env=self.config.env,
                )

                # Convert timestamps to milliseconds
                created_ts = (
                    int(lake.create_time.timestamp() * 1000)
                    if lake.create_time
                    else None
                )
                modified_ts = (
                    int(lake.update_time.timestamp() * 1000)
                    if lake.update_time
                    else None
                )

                yield from gen_containers(
                    container_key=lake_container_key,
                    name=lake.display_name or lake_id,
                    description=lake.description or "",
                    sub_types=["Dataplex Lake"],
                    parent_container_key=project_container_key,
                    created=created_ts,
                    last_modified=modified_ts,
                )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list lakes",
                message=f"Error listing lakes in project {project_id}",
                exc=e,
            )

    def _get_zones_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch zones from Dataplex and generate corresponding MCPs as Sub-containers."""
        parent = f"projects/{project_id}/locations/{self.config.location}"

        try:
            with self.report.dataplex_api_timer:
                request = dataplex_v1.ListLakesRequest(parent=parent)
                lakes = self.dataplex_client.list_lakes(request=request)

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
                            logger.debug(f"Zone {zone_id} filtered out by pattern")
                            self.report.report_zone_scanned(zone_id, filtered=True)
                            continue

                        self.report.report_zone_scanned(zone_id)
                        logger.info(
                            f"Processing zone: {zone_id} in lake: {lake_id}, project: {project_id}"
                        )

                        # Create zone container key
                        zone_container_key = make_zone_container_key(
                            project_id=project_id,
                            lake_id=lake_id,
                            zone_id=zone_id,
                            platform=self.platform,
                            env=self.config.env,
                        )

                        # Create parent lake container key
                        lake_container_key = make_lake_container_key(
                            project_id=project_id,
                            lake_id=lake_id,
                            platform=self.platform,
                            env=self.config.env,
                        )

                        zone_type_tag = (
                            "Raw Data Zone"
                            if zone.type_.name == "RAW"
                            else "Curated Data Zone"
                        )

                        # Convert timestamps to milliseconds
                        created_ts = (
                            int(zone.create_time.timestamp() * 1000)
                            if zone.create_time
                            else None
                        )
                        modified_ts = (
                            int(zone.update_time.timestamp() * 1000)
                            if zone.update_time
                            else None
                        )

                        yield from gen_containers(
                            container_key=zone_container_key,
                            name=zone.display_name or zone_id,
                            description=zone.description or "",
                            sub_types=["Dataplex Zone", zone_type_tag],
                            parent_container_key=lake_container_key,
                            created=created_ts,
                            last_modified=modified_ts,
                        )

                except exceptions.GoogleAPICallError as e:
                    self.report.report_failure(
                        title=f"Failed to list zones in lake {lake_id}",
                        message=f"Error listing zones in project {project_id}",
                        exc=e,
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list lakes for zones",
                message=f"Error listing lakes in project {project_id}",
                exc=e,
            )

    def _get_assets_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch assets from Dataplex and generate corresponding MCPs as Asset Containers."""
        parent = f"projects/{project_id}/locations/{self.config.location}"
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

                        # List assets in this zone
                        assets_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}/zones/{zone_id}"
                        assets_request = dataplex_v1.ListAssetsRequest(
                            parent=assets_parent
                        )

                        try:
                            assets = self.dataplex_client.list_assets(
                                request=assets_request
                            )

                            for asset in assets:
                                logger.info(
                                    f"Processing asset: {asset.display_name} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                                )
                                asset_id = asset.display_name

                                if not self.config.filter_config.asset_pattern.allowed(
                                    asset_id
                                ):
                                    logger.debug(
                                        f"Asset {asset_id} filtered out by pattern"
                                    )
                                    self.report.report_asset_scanned(
                                        asset_id, filtered=True
                                    )
                                    continue

                                self.report.report_asset_scanned(asset_id)
                                logger.info(
                                    f"Processing asset: {asset_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                                )

                                # Create zone container key
                                zone_container_key = make_zone_container_key(
                                    project_id=project_id,
                                    lake_id=lake_id,
                                    zone_id=zone_id,
                                    platform=self.platform,
                                    env=self.config.env,
                                )

                                # Create asset container key (child of zone)
                                asset_container_key = make_asset_container_key(
                                    project_id=project_id,
                                    lake_id=lake_id,
                                    zone_id=zone_id,
                                    asset_id=asset_id,
                                    platform=self.platform,
                                    env=self.config.env,
                                )

                                # Convert timestamps to milliseconds
                                created_ts = (
                                    int(asset.create_time.timestamp() * 1000)
                                    if asset.create_time
                                    else None
                                )
                                modified_ts = (
                                    int(asset.update_time.timestamp() * 1000)
                                    if asset.update_time
                                    else None
                                )

                                yield from gen_containers(
                                    container_key=asset_container_key,
                                    name=asset_id,
                                    description=asset.description or "",
                                    sub_types=["Dataplex Asset"],
                                    parent_container_key=zone_container_key,
                                    created=created_ts,
                                    last_modified=modified_ts,
                                )

                        except exceptions.GoogleAPICallError as e:
                            self.report.report_failure(
                                title=f"Failed to list assets in zone {zone_id}",
                                message=f"Error listing assets in project {project_id}, lake {lake_id}, zone {zone_id}",
                                exc=e,
                            )

                except exceptions.GoogleAPICallError as e:
                    self.report.report_failure(
                        title=f"Failed to list zones in lake {lake_id}",
                        message=f"Error listing zones for asset extraction in project {project_id}",
                        exc=e,
                    )

        except exceptions.GoogleAPICallError as e:
            self.report.report_failure(
                title="Failed to list lakes for asset extraction",
                message=f"Error listing lakes in project {project_id}",
                exc=e,
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
                    self.report.report_entity_scanned(entity_id, filtered=True)
                    continue

                self.report.report_entity_scanned(entity_id)
                logger.info(
                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
                )

                # Determine source platform and dataset id from asset (bigquery, gcs, etc.)
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

                # Track entity ID for lineage extraction
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

                # Generate dataset URN with dataplex platform
                dataset_urn = make_entity_dataset_urn(
                    entity_id,
                    project_id,
                    self.config.env,
                    dataset_id=dataset_id,
                    platform="dataplex",
                )

                # Extract schema metadata
                schema_metadata = self._extract_schema_metadata(
                    entity_full, dataset_urn
                )

                # Build dataset properties
                custom_properties = {
                    "lake": lake_id,
                    "zone": zone_id,
                    "entity_id": entity_id,
                    "source_platform": source_platform,
                }

                if entity_full.data_path:
                    custom_properties["data_path"] = entity_full.data_path

                if entity_full.system:
                    custom_properties["system"] = entity_full.system.name

                if entity_full.format:
                    custom_properties["format"] = entity_full.format.format_.name

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
                        platform=str(DataPlatformUrn(self.platform))
                    ),
                    SubTypesClass(
                        typeNames=[
                            "Dataplex Entity",
                            entity_full.type_.name,
                        ]
                    ),
                ]

                # Add schema metadata if available
                if schema_metadata:
                    aspects.append(schema_metadata)

                asset_container_key = make_asset_container_key(
                    project_id=project_id,
                    lake_id=lake_id,
                    zone_id=zone_id,
                    asset_id=entity.asset,
                    platform=self.platform,
                    env=self.config.env,
                )
                asset_container_urn = asset_container_key.as_urn()
                aspects.append(ContainerClass(container=asset_container_urn))

                yield from MetadataChangeProposalWrapper.construct_many(
                    entityUrn=dataset_urn,
                    aspects=aspects,
                )

                # Create sibling relationship if enabled and source platform is different
                if (
                    self.config.create_sibling_relationships
                    and source_platform != "dataplex"
                ):
                    yield from self._gen_sibling_workunits(
                        dataset_urn,
                        entity_id,
                        project_id,
                        source_platform,
                        dataset_id,
                    )

        except exceptions.GoogleAPICallError as e:
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

    def _get_entry_groups_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entry groups from Universal Catalog."""
        logger.info(
            f"Entry groups extraction not yet implemented for project {project_id} (Phase 2)"
        )
        return
        yield

    def _get_entries_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entries from Universal Catalog."""
        logger.info(
            f"Entries extraction not yet implemented for project {project_id} (Phase 2)"
        )
        return
        yield

    def _extract_schema_metadata(
        self, entity: dataplex_v1.Entity, dataset_urn: str
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
            platform=str(DataPlatformUrn(self.platform)),
            version=0,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
            fields=fields,
        )

    def _gen_sibling_workunits(
        self,
        dataplex_dataset_urn: str,
        entity_id: str,
        project_id: str,
        source_platform: str,
        dataset_id: str,
    ) -> Iterable[MetadataWorkUnit]:
        """Generate sibling workunits linking Dataplex entity to source platform entity.

        This creates bidirectional sibling relationships based on the dataplex_is_primary_sibling config:
        - When dataplex_is_primary_sibling=False (default):
          - Dataplex entity (primary=False) -> Source platform entity
          - Source platform entity (primary=True) -> Dataplex entity
        - When dataplex_is_primary_sibling=True:
          - Dataplex entity (primary=True) -> Source platform entity
          - Source platform entity (primary=False) -> Dataplex entity

        By default, the source platform entity is marked as primary since it's the canonical representation.
        """
        # Create source dataset URN (e.g., bigquery://project.entity)
        source_dataset_urn = make_source_dataset_urn(
            entity_id, project_id, source_platform, self.config.env, dataset_id
        )

        # Dataplex entity sibling aspect
        yield MetadataChangeProposalWrapper(
            entityUrn=dataplex_dataset_urn,
            aspect=Siblings(
                primary=self.config.dataplex_is_primary_sibling,
                siblings=[source_dataset_urn],
            ),
        ).as_workunit()

        # Source entity sibling aspect (inverse of Dataplex primary setting)
        yield MetadataChangeProposalWrapper(
            entityUrn=source_dataset_urn,
            aspect=Siblings(
                primary=not self.config.dataplex_is_primary_sibling,
                siblings=[dataplex_dataset_urn],
            ),
        ).as_workunit(is_primary_source=False)

        # Track that we created a sibling relationship
        self.report.report_sibling_relationship_created()

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
