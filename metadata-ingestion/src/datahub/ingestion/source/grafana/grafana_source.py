import logging
from typing import Iterable, List, Optional

import requests

from datahub.emitter.mce_builder import (
    make_chart_urn,
    make_container_urn,
    make_dashboard_urn,
    make_schema_field_urn,
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
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.entity_mcp_builder import (
    build_chart_mcps,
    build_dashboard_mcps,
    build_datasource_mcps,
)
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
)
from datahub.ingestion.source.grafana.report import (
    GrafanaSourceReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import (
    DashboardInfoClass,
    InputFieldClass,
    InputFieldsClass,
    SchemaFieldClass,
    StatusClass,
)

# Grafana-specific ingestion stages
GRAFANA_BASIC_EXTRACTION = "Grafana Basic Dashboard Extraction"
GRAFANA_FOLDER_EXTRACTION = "Grafana Folder Extraction"
GRAFANA_DASHBOARD_EXTRACTION = "Grafana Dashboard Extraction"
GRAFANA_PANEL_EXTRACTION = "Grafana Panel Extraction"

logger = logging.getLogger(__name__)


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
            - Dashboard creators (Technical owner)

    The source supports the following capabilities:
    - Platform instance support for multi-Grafana deployments
    - Stateful ingestion with support for soft-deletes
    - Fine-grained lineage at both dataset and column levels
    - Automated tag extraction
    - Support for both HTTP and HTTPS connections with optional SSL verification
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

        self.api_client = GrafanaAPIClient(
            base_url=self.config.url,
            token=self.config.service_account_token,
            verify_ssl=self.config.verify_ssl,
            page_size=self.config.page_size,
            report=self.report,
            skip_text_panels=self.config.skip_text_panels,
        )

        # Initialize lineage extractor with graph
        self.lineage_extractor = None
        if self.config.include_lineage:
            self.lineage_extractor = LineageExtractor(
                platform=self.config.platform,
                platform_instance=self.config.platform_instance,
                env=self.config.env,
                connection_to_platform_map=self.config.connection_to_platform_map,
                graph=self.ctx.graph,
                report=self.report,
                include_column_lineage=self.config.include_column_lineage,
            )

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "GrafanaSource":
        config = GrafanaSourceConfig.model_validate(config_dict)
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
        """Main extraction logic"""

        # Check if we should use basic mode
        if self.config.basic_mode:
            logger.info("Running in basic mode - extracting dashboard metadata only")
            yield from self._get_workunits_basic_mode()
            return

        # Enhanced mode - extract full hierarchy and details
        yield from self._get_workunits_enhanced_mode()

    def _get_workunits_basic_mode(self) -> Iterable[MetadataWorkUnit]:
        """Basic extraction mode - only dashboard metadata (backwards compatible)"""
        with self.report.new_stage(GRAFANA_BASIC_EXTRACTION):
            headers = {
                "Authorization": f"Bearer {self.config.service_account_token.get_secret_value()}",
                "Content-Type": "application/json",
            }

            try:
                response = requests.get(
                    f"{self.config.url}/api/search",
                    headers=headers,
                    verify=self.config.verify_ssl,
                )
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                self.report.report_failure(
                    title="Dashboard Search Error",
                    message="Failed to fetch dashboards in basic mode",
                    context=str(e),
                    exc=e,
                )
                return

            dashboards = response.json()

            for item in dashboards:
                if not self.config.dashboard_pattern.allowed(item.get("title", "")):
                    continue

                uid = item["uid"]
                title = item["title"]
                url_path = item["url"]
                full_url = f"{self.config.url}{url_path}"

                dashboard_urn = make_dashboard_urn(
                    platform=self.platform,
                    name=uid,
                    platform_instance=self.platform_instance,
                )

                # Create basic dashboard info
                dashboard_info = DashboardInfoClass(
                    description="",
                    title=title,
                    charts=[],
                    lastModified=ChangeAuditStamps(),
                    externalUrl=full_url,
                    customProperties={
                        key: str(value)
                        for key, value in {
                            "displayName": title,
                            "id": item["id"],
                            "uid": uid,
                            "title": title,
                            "uri": item["uri"],
                            "type": item["type"],
                            "folderId": item.get("folderId"),
                            "folderUid": item.get("folderUid"),
                            "folderTitle": item.get("folderTitle"),
                        }.items()
                        if value is not None
                    },
                )

                # Yield dashboard workunit
                yield MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=dashboard_info,
                ).as_workunit()

                yield MetadataChangeProposalWrapper(
                    entityUrn=dashboard_urn,
                    aspect=StatusClass(removed=False),
                ).as_workunit()

                self.report.report_dashboard_scanned()

    def _get_workunits_enhanced_mode(self) -> Iterable[MetadataWorkUnit]:
        """Enhanced extraction mode - full hierarchy, panels, and lineage"""
        # Process folders first
        with self.report.new_stage(GRAFANA_FOLDER_EXTRACTION):
            for folder in self.api_client.get_folders():
                if self.config.folder_pattern.allowed(folder.title):
                    self.report.report_folder_scanned()
                    yield from self._process_folder(folder)

        # Process dashboards
        with self.report.new_stage(GRAFANA_DASHBOARD_EXTRACTION):
            for dashboard in self.api_client.get_dashboards():
                if self.config.dashboard_pattern.allowed(dashboard.title):
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
            sub_types=[BIContainerSubTypes.LOOKER_FOLDER],
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
            folder_id=dashboard.folder_id,
        )

        # Generate dashboard container first
        yield from gen_containers(
            container_key=dashboard_container_key,
            name=dashboard.title,
            sub_types=[BIContainerSubTypes.GRAFANA_DASHBOARD],
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

        # Process panels and create per-panel datasets
        yield from build_datasource_mcps(
            dashboard=dashboard,
            dashboard_container_key=dashboard_container_key,
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            env=self.config.env,
            connection_to_platform_map=self.config.connection_to_platform_map,
            graph=self.ctx.graph,
            report=self.report,
            config=self.config,
            lineage_extractor=self.lineage_extractor,
            logger=logger,
            add_dataset_to_container_fn=add_dataset_to_container,
        )

        # Process panels and create charts
        with self.report.new_stage(GRAFANA_PANEL_EXTRACTION):
            for panel in dashboard.panels:
                self.report.report_chart_scanned()

                # Create chart MCE
                dataset_urn, chart_urn, chart_mcps = build_chart_mcps(
                    panel=panel,
                    dashboard=dashboard,
                    platform=self.config.platform,
                    platform_instance=self.config.platform_instance,
                    env=self.config.env,
                    base_url=self.config.url,
                    ingest_tags=self.config.ingest_tags,
                )
                chart_urns.append(chart_urn)

                for mcp in chart_mcps:
                    yield mcp.as_workunit()

                # Add chart to dashboard container
                chart_urn = make_chart_urn(
                    self.platform,
                    f"{dashboard.uid}.{panel.id}",
                    self.platform_instance,
                )
                if dataset_urn:
                    input_fields = extract_fields_from_panel(
                        panel,
                        self.config.connection_to_platform_map,
                        self.ctx.graph,
                        self.report,
                    )
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

        # Create dashboard MCPs
        dashboard_urn, dashboard_mcps = build_dashboard_mcps(
            dashboard=dashboard,
            platform=self.config.platform,
            platform_instance=self.config.platform_instance,
            chart_urns=chart_urns,
            base_url=self.config.url,
            ingest_owners=self.config.ingest_owners,
            remove_email_suffix=self.config.remove_email_suffix,
            ingest_tags=self.config.ingest_tags,
        )

        # Add each dashboard MCP as a work unit
        for mcp in dashboard_mcps:
            yield mcp.as_workunit()

        # Add dashboard entity to its container
        yield from add_dataset_to_container(
            container_key=dashboard_container_key,
            dataset_urn=dashboard_urn,
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
            folder_id=dashboard.folder_id,
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

    def get_report(self) -> GrafanaSourceReport:
        return self.report
