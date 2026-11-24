import logging
from typing import Dict, Iterable, List, Literal

import requests
from typing_extensions import assert_never

from datahub.emitter.mce_builder import (
    make_chart_urn,
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
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.common.subtypes import BIContainerSubTypes
from datahub.ingestion.source.grafana.constants import (
    DASHBOARD_FOLDER_NOT_INGESTED_MESSAGE,
    DASHBOARD_FOLDER_NOT_INGESTED_TITLE,
    DASHBOARD_SEARCH_ERROR_TITLE,
    DASHBOARD_SEARCH_UNEXPECTED_RESPONSE_MESSAGE,
    DASHBOARD_WITHOUT_FOLDER_ID_MESSAGE,
    DASHBOARD_WITHOUT_FOLDER_ID_TITLE,
    DASHBOARDS_SKIPPED_MESSAGE,
    DASHBOARDS_SKIPPED_TITLE,
    FOLDERS_UNPARSEABLE_MESSAGE,
    FOLDERS_UNPARSEABLE_TITLE,
    GRAFANA_BASIC_EXTRACTION,
    GRAFANA_DASHBOARD_EXTRACTION,
    GRAFANA_FOLDER_EXTRACTION,
    GRAFANA_GENERAL_FOLDER_ID,
    GRAFANA_PANEL_EXTRACTION,
    PANEL_SKIPPED_MESSAGE,
    PANEL_SKIPPED_TITLE,
    FolderPlacement,
)
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

logger = logging.getLogger(__name__)


@platform_name("Grafana")
@config_class(GrafanaSourceConfig)
@support_status(SupportStatus.GA)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DELETION_DETECTION, "Enabled by default")
@capability(SourceCapability.LINEAGE_COARSE, "Enabled by default")
@capability(SourceCapability.LINEAGE_FINE, "Enabled by default")
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
@capability(SourceCapability.TAGS, "Enabled by default")
class GrafanaSource(StatefulIngestionSourceBase):
    """
    Source that extracts dashboards, charts, and datasources from Grafana via REST API.

    Implementation notes:
    - Uses Grafana HTTP API for metadata extraction
    - Parses SQL queries from panel targets for lineage when possible
    - Supports two modes: enhanced (requires admin token) and basic (limited permissions)
    - Uses connection_to_platform_map to map Grafana datasource UIDs to DataHub platforms
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
        # Filled by the folder stage, read by the dashboard stage that follows it.
        self._folder_placements: Dict[
            str, Literal[FolderPlacement.PLACED, FolderPlacement.FILTERED]
        ] = {}

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
                results = response.json()
            except requests.exceptions.RequestException as e:
                self.report.failure(
                    title=DASHBOARD_SEARCH_ERROR_TITLE,
                    message="Failed to fetch dashboards in basic mode",
                    exc=e,
                )
                return
            if not isinstance(results, list):
                self.report.failure(
                    title=DASHBOARD_SEARCH_ERROR_TITLE,
                    message=DASHBOARD_SEARCH_UNEXPECTED_RESPONSE_MESSAGE,
                    context=str(results),
                )
                return

            hits = self.api_client.parse_search_hits(
                results, require_title_and_url=True
            )
            self._fail_run_if_dashboards_skipped(sum(1 for hit in hits if hit is None))
            for hit in hits:
                if hit is None:
                    continue
                assert hit.title is not None and hit.url is not None
                if not self.config.dashboard_pattern.allowed(hit.title):
                    continue

                uid = hit.uid
                title = hit.title
                full_url = f"{self.config.url}{hit.url}"

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
                            "id": hit.id,
                            "uid": uid,
                            "title": title,
                            "uri": hit.uri,
                            "type": hit.type,
                            "folderId": hit.folder_id,
                            "folderUid": hit.folder_uid,
                            "folderTitle": hit.folder_title,
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
            listing = self.api_client.get_folders()
            if listing.unparseable and self._removes_stale_metadata():
                # Stale-entity removal is skipped only on a failure. Without one
                # it would soft-delete the skipped folders' containers while
                # dashboards from earlier runs still point at them.
                self.report.failure(
                    title=FOLDERS_UNPARSEABLE_TITLE,
                    message=FOLDERS_UNPARSEABLE_MESSAGE,
                    context=f"{listing.unparseable} folder(s)",
                )
            for folder in listing.folders:
                if self.config.folder_pattern.allowed(folder.title):
                    self.report.report_folder_scanned()
                    yield from self._process_folder(folder)
                    self._folder_placements[folder.id] = FolderPlacement.PLACED
                else:
                    self._folder_placements[folder.id] = FolderPlacement.FILTERED

        # Process dashboards
        with self.report.new_stage(GRAFANA_DASHBOARD_EXTRACTION):
            dashboard_listing = self.api_client.get_dashboards()
            self._fail_run_if_dashboards_skipped(dashboard_listing.skipped)
            for dashboard in dashboard_listing.dashboards:
                if self.config.dashboard_pattern.allowed(dashboard.title):
                    self.report.report_dashboard_scanned()
                    yield from self._process_dashboard(dashboard)

    def _process_folder(self, folder: Folder) -> Iterable[MetadataWorkUnit]:
        """Process Grafana folder metadata"""
        yield from gen_containers(
            container_key=self._folder_key(folder.id),
            name=folder.title,
            sub_types=[BIContainerSubTypes.LOOKER_FOLDER],
            description=folder.description,
        )

    def _fail_run_if_dashboards_skipped(self, skipped: int) -> None:
        # Stale-entity removal is skipped only on a failure. Without one it
        # would soft-delete dashboards that were skipped only because Grafana
        # returned them in a shape that could not be read.
        if skipped and self._removes_stale_metadata():
            self.report.failure(
                title=DASHBOARDS_SKIPPED_TITLE,
                message=DASHBOARDS_SKIPPED_MESSAGE,
                context=f"{skipped} dashboard(s)",
            )

    def _process_dashboard(self, dashboard: Dashboard) -> Iterable[MetadataWorkUnit]:
        """Process dashboard and its panels"""
        chart_urns = []

        for skipped_panel in dashboard.skipped_panels:
            self.report.report_panel_parsing_warning()
            self.report.warning(
                title=PANEL_SKIPPED_TITLE,
                message=PANEL_SKIPPED_MESSAGE,
                context=f"{dashboard.title} (uid {dashboard.uid}): {skipped_panel}",
            )

        dashboard_container_key = self._dashboard_container_key(dashboard)

        # Generate dashboard container first
        yield from gen_containers(
            container_key=dashboard_container_key,
            name=dashboard.title,
            sub_types=[BIContainerSubTypes.GRAFANA_DASHBOARD],
            description=dashboard.description,
        )

        yield from self._add_dashboard_to_folder(dashboard, dashboard_container_key)

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

    def _folder_key(self, folder_id: str) -> FolderKey:
        return FolderKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            folder_id=folder_id,
        )

    def _dashboard_container_key(self, dashboard: Dashboard) -> DashboardContainerKey:
        return DashboardContainerKey(
            platform=self.config.platform,
            instance=self.config.platform_instance,
            dashboard_id=dashboard.uid,
            folder_id=dashboard.folder_id,
        )

    def _add_dashboard_to_folder(
        self, dashboard: Dashboard, dashboard_container_key: DashboardContainerKey
    ) -> Iterable[MetadataWorkUnit]:
        """Place the dashboard in its folder, but only if that folder's container
        was emitted this run, so the dashboard never points at a missing parent."""
        folder_id = dashboard.folder_id
        if folder_id is None:
            self.report.report_dashboard_without_folder_id()
            self.report.warning(
                title=DASHBOARD_WITHOUT_FOLDER_ID_TITLE,
                message=DASHBOARD_WITHOUT_FOLDER_ID_MESSAGE,
                context=f"{dashboard.title} (uid {dashboard.uid})",
            )
            return

        placement = self._folder_placement(folder_id)
        if placement == FolderPlacement.PLACED:
            yield from add_dataset_to_container(
                container_key=self._folder_key(folder_id),
                dataset_urn=dashboard_container_key.as_urn(),
            )
        elif placement == FolderPlacement.GENERAL:
            pass
        elif placement == FolderPlacement.FILTERED:
            self.report.report_dashboard_in_filtered_folder()
        elif placement == FolderPlacement.NOT_INGESTED:
            self.report.report_dashboard_without_folder_container()
            self.report.warning(
                title=DASHBOARD_FOLDER_NOT_INGESTED_TITLE,
                message=DASHBOARD_FOLDER_NOT_INGESTED_MESSAGE,
                context=f"{dashboard.title} (uid {dashboard.uid}, folder id {folder_id})",
            )
        else:
            assert_never(placement)

    def _folder_placement(self, folder_id: str) -> FolderPlacement:
        if folder_id == GRAFANA_GENERAL_FOLDER_ID:
            return FolderPlacement.GENERAL
        return self._folder_placements.get(folder_id, FolderPlacement.NOT_INGESTED)

    def _removes_stale_metadata(self) -> bool:
        stateful = self.config.stateful_ingestion
        return (
            stateful is not None and stateful.enabled and stateful.remove_stale_metadata
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
