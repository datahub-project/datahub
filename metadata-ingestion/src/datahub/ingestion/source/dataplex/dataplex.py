"""Google Dataplex source for DataHub ingestion.

This source extracts metadata from Google Dataplex, including:
- Projects as Containers
- Lakes as Domains
- Zones as Sub-domains
- Assets as Data Products
- Entities (discovered tables/filesets) as Datasets

Reference implementation based on VertexAI and BigQuery V2 sources.
"""

import logging
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
from datahub.ingestion.api.source import Source, SourceCapability
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.dataplex.dataplex_config import DataplexConfig
from datahub.ingestion.source.dataplex.dataplex_helpers import (
    determine_entity_platform,
    make_asset_data_product_urn,
    make_audit_stamp,
    make_entity_dataset_urn,
    make_lake_domain_urn,
    make_project_container_urn,
    make_zone_domain_urn,
    map_dataplex_field_to_datahub,
)
from datahub.ingestion.source.dataplex.dataplex_report import DataplexReport
from datahub.metadata.schema_classes import (
    ContainerClass,
    ContainerPropertiesClass,
    DataPlatformInstanceClass,
    DataProductPropertiesClass,
    DatasetPropertiesClass,
    DomainPropertiesClass,
    DomainsClass,
    RecordTypeClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
    SubTypesClass,
)
from datahub.metadata.urns import DataPlatformUrn

logger = logging.getLogger(__name__)


@platform_name("Dataplex", id="dataplex")
@config_class(DataplexConfig)
@support_status(SupportStatus.INCUBATING)
@capability(
    SourceCapability.DOMAINS,
    "Maps Dataplex Lakes to Domains and Zones to Sub-domains",
)
@capability(
    SourceCapability.CONTAINERS,
    "Maps Projects to Containers and Assets to Data Products",
)
@capability(
    SourceCapability.SCHEMA_METADATA,
    "Extract schema information from discovered entities",
)
@capability(
    SourceCapability.LINEAGE_COARSE,
    "Extract lineage from Dataplex Lineage API",
)
class DataplexSource(Source):
    """Source to ingest metadata from Google Dataplex."""

    platform: str = "dataplex"

    def __init__(self, ctx: PipelineContext, config: DataplexConfig):
        super().__init__(ctx)
        self.config = config
        self.report = DataplexReport()

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
        self.catalog_client = dataplex_v1.CatalogServiceClient(credentials=credentials)

        if self.config.extract_lineage:
            self.lineage_client = LineageClient(credentials=credentials)
        else:
            self.lineage_client = None

    def get_report(self) -> DataplexReport:
        """Return the ingestion report."""
        return self.report

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

    def _gen_project_workunits(self, project_id: str) -> Iterable[MetadataWorkUnit]:
        """Generate workunits for GCP Project as a Container."""
        container_urn = make_project_container_urn(project_id)

        yield from MetadataChangeProposalWrapper.construct_many(
            entityUrn=container_urn,
            aspects=[
                ContainerPropertiesClass(
                    name=project_id,
                    description=f"Google Cloud Project: {project_id}",
                    customProperties={
                        "location": self.config.location,
                    },
                ),
                SubTypesClass(typeNames=["GCP Project"]),
                DataPlatformInstanceClass(platform=str(DataPlatformUrn(self.platform))),
            ],
        )

    def _get_lakes_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch lakes from Dataplex and generate corresponding MCPs as Domains."""
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

                # Use project-scoped domain name to avoid conflicts across projects
                domain_urn = make_lake_domain_urn(project_id, lake_id)

                yield from MetadataChangeProposalWrapper.construct_many(
                    entityUrn=domain_urn,
                    aspects=[
                        DomainPropertiesClass(
                            name=lake.display_name or lake_id,
                            description=lake.description or "",
                            created=make_audit_stamp(lake.create_time),
                        ),
                        ContainerClass(
                            container=make_project_container_urn(project_id)
                        ),
                        SubTypesClass(typeNames=["Dataplex Lake"]),
                    ],
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
        """Fetch zones from Dataplex and generate corresponding MCPs as Sub-domains."""
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

                        # Use project-scoped names to avoid conflicts across projects
                        domain_urn = make_zone_domain_urn(project_id, lake_id, zone_id)
                        parent_domain_urn = make_lake_domain_urn(project_id, lake_id)

                        zone_type_tag = (
                            "Raw Data Zone"
                            if zone.type_.name == "RAW"
                            else "Curated Data Zone"
                        )

                        yield from MetadataChangeProposalWrapper.construct_many(
                            entityUrn=domain_urn,
                            aspects=[
                                DomainPropertiesClass(
                                    name=zone.display_name or zone_id,
                                    description=zone.description or "",
                                    parentDomain=parent_domain_urn,
                                    created=make_audit_stamp(zone.create_time),
                                ),
                                ContainerClass(
                                    container=make_project_container_urn(project_id)
                                ),
                                SubTypesClass(
                                    typeNames=["Dataplex Zone", zone_type_tag]
                                ),
                            ],
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
        """Fetch assets from Dataplex and generate corresponding MCPs as Data Products."""
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

                                # Generate data product URN
                                data_product_urn = make_asset_data_product_urn(
                                    project_id, lake_id, zone_id, asset_id
                                )

                                # Link to parent zone domain
                                zone_domain_urn = make_zone_domain_urn(
                                    project_id, lake_id, zone_id
                                )

                                yield from MetadataChangeProposalWrapper.construct_many(
                                    entityUrn=data_product_urn,
                                    aspects=[
                                        DataProductPropertiesClass(
                                            name=asset_id,
                                            description=asset.description or "",
                                        ),
                                        DomainsClass(domains=[zone_domain_urn]),
                                    ],
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

    def _get_entities_mcps(
        self, project_id: str
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """Fetch entities from Dataplex and generate corresponding MCPs as Datasets."""
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

                        # List entities in this zone
                        entities_parent = f"projects/{project_id}/locations/{self.config.location}/lakes/{lake_id}/zones/{zone_id}"
                        entities_request = dataplex_v1.ListEntitiesRequest(
                            parent=entities_parent
                        )

                        try:
                            entities = self.metadata_client.list_entities(
                                request=entities_request
                            )

                            for entity in entities:
                                entity_id = entity.id

                                if not self.config.filter_config.entity_pattern.allowed(
                                    entity_id
                                ):
                                    logger.debug(
                                        f"Entity {entity_id} filtered out by pattern"
                                    )
                                    self.report.report_entity_scanned(
                                        entity_id, filtered=True
                                    )
                                    continue

                                self.report.report_entity_scanned(entity_id)
                                logger.info(
                                    f"Processing entity: {entity_id} in zone: {zone_id}, lake: {lake_id}, project: {project_id}"
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

                                # Determine platform from asset
                                platform = determine_entity_platform(
                                    entity_full,
                                    project_id,
                                    lake_id,
                                    zone_id,
                                    self.config.location,
                                    self.dataplex_client,
                                )

                                # Generate dataset URN
                                dataset_urn = make_entity_dataset_urn(
                                    entity_id, platform, project_id, self.config.env
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
                                    "platform": platform,
                                }

                                if entity_full.data_path:
                                    custom_properties["data_path"] = (
                                        entity_full.data_path
                                    )

                                if entity_full.system:
                                    custom_properties["system"] = entity_full.system

                                if entity_full.format:
                                    custom_properties["format"] = (
                                        entity_full.format.format_.name
                                    )

                                # Build aspects list
                                aspects = [
                                    DatasetPropertiesClass(
                                        name=entity_id,
                                        description=entity_full.description or "",
                                        customProperties=custom_properties,
                                        created=make_audit_stamp(
                                            entity_full.create_time
                                        ),
                                        lastModified=make_audit_stamp(
                                            entity_full.update_time
                                        ),
                                    ),
                                    DataPlatformInstanceClass(
                                        platform=str(DataPlatformUrn(platform))
                                    ),
                                    SubTypesClass(
                                        typeNames=["Dataplex Entity", entity_full.type_]
                                    ),
                                ]

                                # Add schema metadata if available
                                if schema_metadata:
                                    aspects.append(schema_metadata)

                                # Link to parent zone domain
                                domain_urn = make_zone_domain_urn(
                                    project_id, lake_id, zone_id
                                )
                                aspects.append(ContainerClass(container=domain_urn))

                                yield from MetadataChangeProposalWrapper.construct_many(
                                    entityUrn=dataset_urn,
                                    aspects=aspects,
                                )

                        except exceptions.GoogleAPICallError as e:
                            self.report.report_failure(
                                title=f"Failed to list entities in zone {zone_id}",
                                message=f"Error listing entities in project {project_id}, lake {lake_id}, zone {zone_id}",
                                exc=e,
                            )

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
            platformSchema=None,
            fields=fields,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "DataplexSource":
        """Factory method to create DataplexSource instance."""
        config = DataplexConfig.model_validate(config_dict)
        return cls(ctx, config)
