from typing import Iterable, List, Optional

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_container_urn,
    make_data_platform_urn,
    make_dataplatform_instance_urn,
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
    make_tag_urn,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import add_dataset_to_container, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.grafana.field_utils import extract_fields_from_panel
from datahub.ingestion.source.grafana.grafana_api import GrafanaAPIClient
from datahub.ingestion.source.grafana.grafana_config import (
    GrafanaSourceConfig,
)
from datahub.ingestion.source.grafana.lineage import LineageExtractor
from datahub.ingestion.source.grafana.models import (
    Dashboard,
    DashboardContainerKey,
    Folder,
    FolderKey,
    Panel,
)
from datahub.ingestion.source.grafana.report import (
    GrafanaSourceReport,
)
from datahub.ingestion.source.grafana.snapshots import (
    build_chart_mce,
    build_dashboard_mce,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata._schema_classes import GlobalTagsClass, TagAssociationClass
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    DatasetPropertiesClass,
    DatasetSnapshotClass,
    InputFieldClass,
    InputFieldsClass,
    MetadataChangeEventClass,
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
)


@platform_name("Grafana")
@config_class(GrafanaSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
class GrafanaSource(StatefulIngestionSourceBase):
    """
    This plugin extracts metadata from Grafana and ingests it into DataHub. It connects to Grafana's API
    to extract metadata about dashboards, charts, and data sources. The following types of metadata are extracted:

    - Container Entities:
        - Folders: Top-level organizational units in Grafana
        - Dashboards: Collections of panels and charts
        - The full container hierarchy is preserved (Folders -> Dashboards -> Charts/Datasets)

    - Charts and Visualizations:
        - All panel types (graphs, tables, stat panels, etc.)
        - Chart configuration and properties
        - Links to the original Grafana UI
        - Custom properties including panel types and data source information
        - Input fields and schema information when available

    - Data Sources and Datasets:
        - Physical datasets representing Grafana's data sources
        - Dataset schema information extracted from queries and panel configurations
        - Support for various data source types (SQL, Prometheus, etc.)
        - Custom properties including data source type and configuration

    - Lineage Information:
        - Dataset-level lineage showing relationships between:
            - Source data systems and Grafana datasets
            - Grafana datasets and charts
        - Column-level lineage for SQL-based data sources
        - Support for external source systems through configurable platform mappings

    - Tags and Ownership:
        - Dashboard and chart tags
        - Ownership information derived from:
            - Dashboard creators
            - Technical owners based on dashboard UIDs
            - Custom ownership assignments

    The source supports the following capabilities:
    - Platform instance support for multi-Grafana deployments
    - Stateful ingestion with support for soft-deletes
    - Fine-grained lineage at both dataset and column levels
    - Automated tag extraction
    - Support for both HTTP and HTTPS connections with optional SSL verification

    Prerequisites:
    1. A running Grafana instance
    2. A service account token with permissions to:
       - Read dashboards and folders
       - Access data source configurations
       - View user information

    A sample configuration file:
    ```yaml
    source:
        type: grafana
        config:
            # Coordinates
            platform_instance: production # optional
            env: PROD # optional
            url: https://grafana.company.com
            service_account_token: ${GRAFANA_SERVICE_ACCOUNT_TOKEN}

            # SSL verification for HTTPS connections
            verify_ssl: true # optional, default is true

            # Source type mapping for lineage
            connection_to_platform_map:
                postgres:
                    platform: postgres
                    database: grafana  # optional
                    database_schema: grafana  # optional
                    platform_instance: database_2  # optional
                    env: PROD  # optional
                mysql_uid_1:  # Grafana datasource UID
                    platform: mysql
                    platform_instance: database_1  # optional
                    database: my_database  # optional
    ```
    """

    config: GrafanaSourceConfig
    report: GrafanaSourceReport

    def __init__(self, config: GrafanaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.config = config
        self.ctx = ctx
        self.platform = config.platform
        self.platform_instance = self.config.platform_instance
        self.env = self.config.env
        self.report = GrafanaSourceReport()

        self.client = GrafanaAPIClient(
            base_url=self.config.url,
            token=self.config.service_account_token,
            verify_ssl=self.config.verify_ssl,
            report=self.report,
        )

        # Initialize lineage extractor with graph
        self.lineage_extractor = LineageExtractor(
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            connection_to_platform_map=self.config.connection_to_platform_map,
            graph=self.ctx.graph,
            report=self.report,
        )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GrafanaSource":
        config = GrafanaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        processors = super().get_workunit_processors()
        processors.append(
            StaleEntityRemovalHandler.create(
                self, self.config, self.ctx
            ).workunit_processor
        )
        return processors

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        """Generate metadata work units"""
        # Process folders
        for folder in self.client.get_folders():
            self.report.report_folder_scanned()
            yield from self._process_folder(folder)

        # Process dashboards and their panels
        dashboards = self.client.get_dashboards()
        for dashboard in dashboards:
            self.report.report_dashboard_scanned()
            yield from self._process_dashboard(dashboard)

    def _process_folder(self, folder: Folder) -> Iterable[MetadataWorkUnit]:
        """Process Grafana folder metadata"""
        folder_key = FolderKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            folder_id=folder.id,
        )

        yield from gen_containers(
            container_key=folder_key,
            name=folder.title,
            sub_types=["Folder"],
            description=folder.description,
        )

    def _process_dashboard(self, dashboard: Dashboard) -> Iterable[MetadataWorkUnit]:
        """Process dashboard and its panels"""
        chart_urns = []

        # First create the dashboard container
        dashboard_container_key = DashboardContainerKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            dashboard_id=dashboard.uid,
        )

        # Generate dashboard container first
        yield from gen_containers(
            container_key=dashboard_container_key,
            name=dashboard.title,
            sub_types=["Dashboard"],
            description=dashboard.description,
        )

        # If dashboard is in a folder, add it to folder container
        if dashboard.folder_id:
            folder_key = FolderKey(
                platform=self.config.platform,
                instance=self.config.platform_instance,
                folder_id=dashboard.folder_id,
            )

            yield from add_dataset_to_container(
                container_key=folder_key,
                dataset_urn=make_container_urn(dashboard_container_key),
            )

        # Process all panels first
        for panel in dashboard.panels:
            self.report.report_chart_scanned()

            # First emit the dataset for each panel's datasource
            yield from self._process_panel_dataset(
                panel, dashboard.uid, self.config.ingest_tags
            )

            # Process lineage
            lineage = self.lineage_extractor.extract_panel_lineage(panel)
            if lineage:
                yield lineage.as_workunit()

            # Create chart MCE
            dataset_urn, chart_mce = build_chart_mce(
                panel=panel,
                dashboard=dashboard,
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                base_url=self.config.url,
                ingest_tags=self.config.ingest_tags,
            )
            chart_urns.append(chart_mce.urn)

            yield MetadataWorkUnit(
                id=f"grafana-chart-{dashboard.uid}-{panel.id}",
                mce=MetadataChangeEventClass(proposedSnapshot=chart_mce),
            )

            # Add chart to dashboard container
            chart_urn = make_chart_urn(
                self.platform,
                f"{dashboard.uid}.{panel.id}",
                self.platform_instance,
            )
            if dataset_urn:
                input_fields = extract_fields_from_panel(panel)
                if input_fields:
                    yield from self._add_input_fields_to_chart(
                        chart_urn=chart_urn,
                        dataset_urn=dataset_urn,
                        input_fields=input_fields,
                    )

            yield from add_dataset_to_container(
                container_key=dashboard_container_key,
                dataset_urn=chart_urn,
            )

        # Create dashboard MCE
        dashboard_mce = build_dashboard_mce(
            dashboard=dashboard,
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            chart_urns=chart_urns,
            base_url=self.config.url,
            ingest_owners=self.config.ingest_owners,
            ingest_tags=self.config.ingest_tags,
        )

        yield MetadataWorkUnit(
            id=f"grafana-dashboard-{dashboard.uid}",
            mce=MetadataChangeEventClass(proposedSnapshot=dashboard_mce),
        )

        # Add dashboard entity to its container
        yield from add_dataset_to_container(
            container_key=dashboard_container_key,
            dataset_urn=dashboard_mce.urn,
        )

    def _add_dashboard_to_folder(
        self, dashboard: Dashboard
    ) -> Iterable[MetadataWorkUnit]:
        """Add dashboard to folder container"""
        folder_key = FolderKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            folder_id=str(dashboard.folder_id),
        )

        dashboard_key = DashboardContainerKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            dashboard_id=dashboard.uid,
        )

        yield from add_dataset_to_container(
            container_key=folder_key,
            dataset_urn=dashboard_key.as_urn(),
        )

    def _add_input_fields_to_chart(
        self, chart_urn: str, dataset_urn: str, input_fields: List[SchemaFieldClass]
    ) -> Iterable[MetadataWorkUnit]:
        """Add input fields aspect to chart"""
        if not input_fields:
            return

        yield MetadataChangeProposalWrapper(
            entityUrn=chart_urn,
            aspect=InputFieldsClass(
                fields=[
                    InputFieldClass(
                        schemaField=field,
                        schemaFieldUrn=make_schema_field_urn(
                            dataset_urn, field.fieldPath
                        ),
                    )
                    for field in input_fields
                ]
            ),
        ).as_workunit()

    def _process_panel_dataset(
        self, panel: Panel, dashboard_uid: str, ingest_tags: bool
    ) -> Iterable[MetadataWorkUnit]:
        """Process dataset metadata for a panel"""
        if not panel.datasource:
            return

        ds_type = panel.datasource.get("type", "unknown")
        ds_uid = panel.datasource.get("uid", "unknown")

        # Build dataset name
        dataset_name = f"{ds_type}.{ds_uid}.{panel.id}"

        # Create dataset URN
        dataset_urn = make_dataset_urn_with_platform_instance(
            platform=self.platform,
            name=dataset_name,
            platform_instance=self.platform_instance,
            env=self.env,
        )

        # Create dataset snapshot
        dataset_snapshot = DatasetSnapshotClass(
            urn=dataset_urn,
            aspects=[
                DataPlatformInstanceClass(
                    platform=make_data_platform_urn(self.platform),
                    instance=make_dataplatform_instance_urn(
                        platform=self.platform,
                        instance=self.platform_instance,
                    )
                    if self.platform_instance
                    else None,
                ),
                DatasetPropertiesClass(
                    name=f"{ds_uid} ({panel.title or panel.id})",
                    description="",
                    customProperties={
                        "type": ds_type,
                        "uid": ds_uid,
                        "full_path": dataset_name,
                    },
                ),
                StatusClass(removed=False),
            ],
        )

        # Add schema metadata if available
        schema_fields = extract_fields_from_panel(panel)
        if schema_fields:
            schema_metadata = SchemaMetadataClass(
                schemaName=f"{ds_type}.{ds_uid}.{panel.id}",
                platform=make_data_platform_urn(self.platform),
                version=0,
                fields=schema_fields,
                hash="",
                platformSchema=OtherSchemaClass(rawSchema=""),
            )
            dataset_snapshot.aspects.append(schema_metadata)

        if dashboard_uid and self.config.ingest_tags:
            dashboard = self.client.get_dashboard(dashboard_uid)
            if dashboard and dashboard.tags:
                tags = []
                for tag in dashboard.tags:
                    if ":" in tag:
                        key, value = tag.split(":", 1)
                        tag_urn = make_tag_urn(f"{key}.{value}")
                    else:
                        tag_urn = make_tag_urn(tag)
                    tags.append(TagAssociationClass(tag=tag_urn))

                if tags:
                    dataset_snapshot.aspects.append(GlobalTagsClass(tags=tags))

        self.report.report_dataset_scanned()
        yield MetadataWorkUnit(
            id=f"grafana-dataset-{ds_uid}-{panel.id}",
            mce=MetadataChangeEventClass(proposedSnapshot=dataset_snapshot),
        )

        # Add dataset to dashboard container
        if dashboard_uid:
            dashboard_key = DashboardContainerKey(
                platform=self.platform,
                instance=self.platform_instance,
                dashboard_id=dashboard_uid,
            )
            yield from add_dataset_to_container(
                container_key=dashboard_key,
                dataset_urn=dataset_urn,
            )

    def get_report(self) -> GrafanaSourceReport:
        return self.report
