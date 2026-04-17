import json
import logging
import sys
from typing import Any, Dict, List, Literal, Optional, cast

import requests

from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_resolver
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FIELD_TYPE_MAPPING,
    App,
    AppDashboard,
    AppReport,
    Column,
    Dashboard,
    FabricArtifact,
    Measure,
    PowerBIDataset,
    Report,
    ReportType,
    Table,
    Tile,
    User,
    Workspace,
    new_powerbi_dataset,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    RegularAPIResolver,
)

# Logger instance
logger = logging.getLogger(__name__)


def form_full_table_name(
    config: PowerBiDashboardSourceConfig,
    workspace: Workspace,
    dataset_name: str,
    table_name: str,
) -> str:
    full_table_name: str = "{}.{}".format(
        dataset_name.replace(" ", "_"), table_name.replace(" ", "_")
    )

    if config.include_workspace_name_in_dataset_urn:
        workspace_identifier: str = (
            workspace.id
            if config.workspace_id_as_urn_part
            else workspace.name.replace(" ", "_").lower()
        )
        full_table_name = f"{workspace_identifier}.{full_table_name}"

    return full_table_name


class PowerBiAPI:
    def __init__(
        self,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
    ) -> None:
        self.__config: PowerBiDashboardSourceConfig = config
        self.__reporter = reporter

        self.__regular_api_resolver = RegularAPIResolver(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret.get_secret_value(),
            tenant_id=self.__config.tenant_id,
            metadata_api_timeout=self.__config.metadata_api_timeout,
            environment=self.__config.environment,
        )

        self.__admin_api_resolver = AdminAPIResolver(
            client_id=self.__config.client_id,
            client_secret=self.__config.client_secret.get_secret_value(),
            tenant_id=self.__config.tenant_id,
            metadata_api_timeout=self.__config.metadata_api_timeout,
            environment=self.__config.environment,
        )

        self.reporter: PowerBiDashboardSourceReport = reporter

        # A report or tile in one workspace can be built using a dataset from another workspace.
        # We need to store the dataset ID (which is a UUID) mapped to its dataset instance.
        # This mapping will allow us to retrieve the appropriate dataset for
        # reports and tiles across different workspaces.
        self.dataset_registry: Dict[str, PowerBIDataset] = {}

    def log_http_error(self, message: str) -> Any:
        logger.warning(message)
        _, e, _ = sys.exc_info()
        if isinstance(e, requests.exceptions.HTTPError):
            logger.warning(f"HTTP status-code = {e.response.status_code}")

        if isinstance(e, requests.exceptions.Timeout):
            url: str = e.request.url if e.request else "URL not available"
            self.reporter.warning(
                title="Metadata API Timeout",
                message="Metadata endpoints are not reachable. Check network connectivity to PowerBI Service.",
                context=f"url={url}",
            )

        logger.debug(msg=message, exc_info=e)

        return e

    def _get_dashboard_endorsements(
        self, scan_result: Optional[dict]
    ) -> Dict[str, List[str]]:
        """
        Store saved dashboard endorsements into a dict with dashboard id as key and
        endorsements or tags as list of strings
        """
        results: Dict[str, List[str]] = {}
        if scan_result is None:
            return results

        for scanned_dashboard in scan_result.get(Constant.DASHBOARDS) or []:
            # Iterate through response and create a list of PowerBiAPI.Dashboard
            dashboard_id = scanned_dashboard.get("id")
            tags = self._parse_endorsement(
                scanned_dashboard.get(Constant.ENDORSEMENT_DETAIL, None)
            )
            results[dashboard_id] = tags

        return results

    def _get_report_endorsements(
        self, scan_result: Optional[dict]
    ) -> Dict[str, List[str]]:
        results: Dict[str, List[str]] = {}

        if scan_result is None:
            return results

        reports: List[dict] = scan_result.get(Constant.REPORTS) or []

        for report in reports:
            report_id = report.get(Constant.ID)
            if report_id is None:
                logger.warning(
                    f"Report id is none. Skipping endorsement tag for report instance {report}"
                )
                continue
            endorsements = self._parse_endorsement(
                report.get(Constant.ENDORSEMENT_DETAIL)
            )
            results[report_id] = endorsements

        return results

    @staticmethod
    def _parse_users_from_scan_result(raw_users: List[dict]) -> List[User]:
        """Parse User objects from scan result user arrays.

        The scan result (with getArtifactUsers=True) includes the same user
        fields as the /admin/{entity}/{id}/users endpoint, so we can avoid
        making a separate API call per entity.
        """
        return [
            User(
                id=raw_user.get(Constant.IDENTIFIER, ""),
                displayName=raw_user.get(Constant.DISPLAY_NAME, ""),
                emailAddress=raw_user.get(Constant.EMAIL_ADDRESS, ""),
                graphId=raw_user.get(Constant.GRAPH_ID, ""),
                principalType=raw_user.get(Constant.PRINCIPAL_TYPE, ""),
                datasetUserAccessRight=raw_user.get(Constant.DATASET_USER_ACCESS_RIGHT),
                reportUserAccessRight=raw_user.get(Constant.REPORT_USER_ACCESS_RIGHT),
                dashboardUserAccessRight=raw_user.get(
                    Constant.DASHBOARD_USER_ACCESS_RIGHT
                ),
                groupUserAccessRight=raw_user.get(Constant.GROUP_USER_ACCESS_RIGHT),
            )
            for raw_user in raw_users
        ]

    def _get_reports_from_scan_result(self, workspace: Workspace) -> Dict[str, Report]:
        """
        Build Report objects directly from the scan result, avoiding redundant
        GET /admin/groups/{ws}/reports and GET /admin/reports/{id}/users calls
        when admin_apis_only is enabled.

        The scan result contains: id, name, reportType, datasetId, description,
        and users (when getArtifactUsers=True).
        webUrl is not in the scan result but can be constructed from known patterns.
        Pages are not available via Admin API (returns []) so we set them empty.
        """
        reports: Dict[str, Report] = {}
        scan_result = workspace.scan_result
        if not scan_result:
            return reports

        raw_reports: List[dict] = scan_result.get(Constant.REPORTS) or []
        base_url = self.__config.environment.web_app_base_url
        parse_users = self.__config.extract_ownership

        for raw_report in raw_reports:
            report_id = raw_report.get(Constant.ID)
            if report_id is None:
                continue

            # Skip app-duplicate reports — same filter as data_resolver.get_reports()
            if Constant.APP_ID in raw_report:
                continue

            report_type_str: Optional[str] = raw_report.get(Constant.REPORT_TYPE)
            try:
                report_type = (
                    ReportType[report_type_str]
                    if report_type_str
                    else ReportType.PowerBIReport
                )
            except KeyError:
                logger.warning(
                    f"Unknown report type '{report_type_str}' for report {report_id}, "
                    f"defaulting to PowerBIReport"
                )
                report_type = ReportType.PowerBIReport

            # Construct webUrl from workspace and report IDs
            if report_type == ReportType.PaginatedReport:
                web_url = f"{base_url}/groups/{workspace.id}/rdlreports/{report_id}"
            else:
                web_url = f"{base_url}/groups/{workspace.id}/reports/{report_id}"

            # Parse users from scan result instead of calling /admin/reports/{id}/users
            users: List[User] = []
            if parse_users:
                raw_users = raw_report.get(Constant.USERS, [])
                users = self._parse_users_from_scan_result(raw_users)

            report = Report(
                id=report_id,
                name=raw_report.get(Constant.NAME, ""),
                type=report_type,
                webUrl=web_url,
                embedUrl=None,
                description=raw_report.get(Constant.DESCRIPTION, ""),
                pages=[],  # Pages are not available via Admin API
                dataset_id=raw_report.get(Constant.DATASET_ID),
                users=users,
                tags=[],
                dataset=None,
            )
            reports[report_id] = report

        return reports

    def _get_dashboards_from_scan_result(
        self, workspace: Workspace
    ) -> Dict[str, Dashboard]:
        """
        Build Dashboard objects (with tiles and users) directly from the scan
        result, avoiding redundant GET /admin/groups/{ws}/dashboards,
        GET /admin/dashboards/{id}/tiles, and GET /admin/dashboards/{id}/users
        calls when admin_apis_only is enabled.
        """
        dashboards: Dict[str, Dashboard] = {}
        scan_result = workspace.scan_result
        if not scan_result:
            return dashboards

        raw_dashboards: List[dict] = scan_result.get(Constant.DASHBOARDS) or []
        base_url = self.__config.environment.web_app_base_url
        parse_users = self.__config.extract_ownership

        for raw_dashboard in raw_dashboards:
            dashboard_id = raw_dashboard.get(Constant.ID)
            if dashboard_id is None:
                continue

            # Skip app-duplicate dashboards
            if Constant.APP_ID in raw_dashboard:
                continue

            web_url = f"{base_url}/groups/{workspace.id}/dashboards/{dashboard_id}"

            # Parse tiles from scan result
            raw_tiles: List[dict] = raw_dashboard.get(Constant.TILES, [])
            tiles: List[Tile] = [
                Tile(
                    id=raw_tile.get(Constant.ID, ""),
                    title=raw_tile.get(Constant.TITLE, ""),
                    embedUrl=raw_tile.get(Constant.EMBED_URL),
                    dataset_id=raw_tile.get(Constant.DATASET_ID),
                    report_id=raw_tile.get(Constant.REPORT_ID),
                    dataset=None,
                    report=None,
                    # In the past we considered that only one of the two report_id or dataset_id would be present
                    # but we have seen cases where both are present. If both are present, we prioritize the report.
                    createdFrom=(
                        Tile.CreatedFrom.REPORT
                        if raw_tile.get(Constant.REPORT_ID)
                        else Tile.CreatedFrom.DATASET
                        if raw_tile.get(Constant.DATASET_ID)
                        else Tile.CreatedFrom.VISUALIZATION
                    ),
                )
                for raw_tile in raw_tiles
                if raw_tile is not None
            ]

            # Parse users from scan result
            users: List[User] = []
            if parse_users:
                raw_users = raw_dashboard.get(Constant.USERS, [])
                users = self._parse_users_from_scan_result(raw_users)

            dashboard = Dashboard(
                id=dashboard_id,
                displayName=raw_dashboard.get(Constant.DISPLAY_NAME, ""),
                description=raw_dashboard.get(Constant.DESCRIPTION, ""),
                embedUrl=raw_dashboard.get(Constant.EMBED_URL, ""),
                isReadOnly=raw_dashboard.get(Constant.IS_READ_ONLY),
                webUrl=web_url,
                workspace_id=workspace.id,
                workspace_name=workspace.name,
                tiles=tiles,
                users=users,
                tags=[],
            )
            dashboards[dashboard_id] = dashboard

        return dashboards

    def _get_resolver(self):
        if self.__config.admin_apis_only:
            return self.__admin_api_resolver
        return self.__regular_api_resolver

    def _get_entity_users(
        self, workspace_id: str, entity_name: str, entity_id: str
    ) -> List[User]:
        """
        Return list of dashboard users
        """
        users: List[User] = []
        if self.__config.extract_ownership is False:
            logger.info(
                "Extract ownership capabilities is disabled from configuration and hence returning empty users list"
            )
            return users

        try:
            users = self.__admin_api_resolver.get_users(
                workspace_id=workspace_id,
                entity=entity_name,
                entity_id=entity_id,
            )
        except Exception:
            e = self.log_http_error(
                message=f"Unable to fetch users for {entity_name}({entity_id})."
            )
            if data_resolver.is_permission_error(cast(Exception, e)):
                logger.warning(
                    f"{entity_name} users would not get ingested as admin permission is not enabled on "
                    "configured Azure AD Application",
                )

        return users

    def get_dashboard_users(self, dashboard: Dashboard) -> List[User]:
        return self._get_entity_users(
            dashboard.workspace_id, Constant.DASHBOARDS, dashboard.id
        )

    def get_report_users(self, workspace_id: str, report_id: str) -> List[User]:
        return self._get_entity_users(workspace_id, Constant.REPORTS, report_id)

    def get_reports(self, workspace: Workspace) -> Dict[str, Report]:
        """
        Fetch the report from PowerBi for the given Workspace.
        If reports were already populated from the scan result
        (use_scan_result_only mode), reuse them instead of making
        a redundant API call.
        """
        # When use_scan_result_only is enabled, reports and their users are
        # already built from the scan result — skip API calls
        reports_from_scan = self.__config.use_scan_result_only

        if reports_from_scan:
            reports = workspace.reports
            logger.info(
                f"Using {len(reports)} reports from scan result for workspace "
                f"{workspace.name}, skipping redundant API calls"
            )
        else:
            reports = {}
            try:
                reports = {
                    report.id: report
                    for report in self._get_resolver().get_reports(workspace)
                }
            except Exception:
                self.log_http_error(
                    message=f"Unable to fetch reports for workspace {workspace.name}"
                )

        # Resolve dataset references from the global registry
        for report in reports.values():
            if report.dataset_id:
                report.dataset = self.dataset_registry.get(report.dataset_id)
                if report.dataset is None:
                    self.reporter.info(
                        title="Missing Lineage For Report",
                        message="A cross-workspace reference that failed to be resolved. Please ensure that no global workspace is being filtered out due to the workspace_id_pattern.",
                        context=f"report-name: {report.name} and dataset-id: {report.dataset_id}",
                    )

        def fill_ownership() -> None:
            if self.__config.extract_ownership is False:
                logger.info(
                    "Skipping user retrieval for report as extract_ownership is set to false"
                )
                return

            if reports_from_scan:
                # Users already parsed from scan result (getArtifactUsers=True)
                return

            for report in reports.values():
                report.users = self.get_report_users(
                    workspace_id=workspace.id, report_id=report.id
                )

        def fill_tags() -> None:
            if self.__config.extract_endorsements_to_tags is False:
                logger.info(
                    "Skipping endorsements tags retrieval for report as extract_endorsements_to_tags is set to false"
                )
                return

            for report in reports.values():
                report.tags = workspace.report_endorsements.get(report.id, [])

        fill_ownership()
        fill_tags()
        return reports

    def get_workspaces(self) -> List[Workspace]:
        modified_workspace_ids: List[str] = []

        if self.__config.modified_since:
            modified_workspace_ids = self.get_modified_workspaces()

        groups: List[dict] = []
        filter_: Dict[str, str] = {}
        try:
            if modified_workspace_ids:
                id_filter: List[str] = []

                for id_ in modified_workspace_ids:
                    id_filter.append(f"id eq {id_}")

                filter_["$filter"] = " or ".join(id_filter)

            groups = self._get_resolver().get_groups(filter_=filter_)

        except Exception:
            self.log_http_error(message="Unable to fetch list of workspaces")
            # raise  # we want this exception to bubble up

        base_url = self.__config.environment.web_app_base_url
        workspaces = [
            Workspace(
                id=workspace[Constant.ID],
                name=workspace[Constant.NAME],
                type=workspace[Constant.TYPE],
                webUrl=f"{base_url}/groups/{workspace[Constant.ID]}",
                datasets={},
                dashboards={},
                reports={},
                report_endorsements={},
                dashboard_endorsements={},
                scan_result={},
                independent_datasets={},
                app=None,  # It will be populated in _fill_metadata_from_scan_result method
            )
            for workspace in groups
        ]
        return workspaces

    def get_modified_workspaces(self) -> List[str]:
        modified_workspace_ids: List[str] = []

        if self.__config.modified_since is None:
            return modified_workspace_ids

        try:
            modified_workspace_ids = self.__admin_api_resolver.get_modified_workspaces(
                self.__config.modified_since
            )
        except Exception:
            self.log_http_error(message="Unable to fetch list of modified workspaces.")

        return modified_workspace_ids

    def _get_scan_result(self, workspace_ids: List[str]) -> Any:
        scan_id: Optional[str] = None
        try:
            scan_id = self.__admin_api_resolver.create_scan_job(
                workspace_ids=workspace_ids
            )
        except Exception:
            e = self.log_http_error(message="Unable to fetch get scan result.")
            if data_resolver.is_permission_error(cast(Exception, e)):
                logger.warning(
                    "Dataset lineage can not be ingestion because this user does not have access to the PowerBI Admin "
                    "API. "
                )
            return None

        logger.debug("Waiting for scan to complete")
        if (
            self.__admin_api_resolver.wait_for_scan_to_complete(
                scan_id=scan_id, timeout=self.__config.scan_timeout
            )
            is False
        ):
            raise ValueError(
                "Workspace detail is not available. Please increase the scan_timeout configuration value to wait "
                "longer for the scan job to complete."
            )

        # Scan is complete lets take the result
        scan_result = self.__admin_api_resolver.get_scan_result(scan_id=scan_id)

        # Original json.dumps kept intentionally — measuring its memory impact
        pretty_json: str = json.dumps(scan_result, indent=1)
        logger.debug(f"scan result = {pretty_json}")

        # Log scan result stats + json.dumps size for memory debugging
        if scan_result:
            ws_count = len(scan_result.get("workspaces", []))
            ds_count = sum(
                len(w.get("datasets", []))
                for w in scan_result.get("workspaces", [])
            )
            logger.info(
                f"[MEM1] Scan result: {ws_count} workspaces, {ds_count} datasets, "
                f"json.dumps size={len(pretty_json) / (1024 * 1024):.1f}MB"
            )

        return scan_result

    @staticmethod
    def _parse_endorsement(endorsements: Optional[dict]) -> List[str]:
        if not endorsements:
            return []

        endorsement = endorsements.get(Constant.ENDORSEMENT)
        if not endorsement:
            return []

        return [endorsement]

    @staticmethod
    def _parse_fabric_artifacts(
        workspace_metadata: dict,
    ) -> Dict[str, FabricArtifact]:
        """Parse Lakehouse, Warehouse, and SQLAnalyticsEndpoint artifacts from scan result.

        These artifacts are used for DirectLake lineage extraction.
        SQLAnalyticsEndpoint is a TDS layer; we resolve to physical Lakehouse/Warehouse ids
        via relations so lineage URNs match the OneLake connector. We do not model
        SQLAnalyticsEndpoint as an entity for now; if we do later, it should be in sync with
        the OneLake connector.

        Args:
            workspace_metadata: The workspace scan result from PowerBI Admin API

        Returns:
            Dict mapping artifact ID to FabricArtifact
        """
        artifacts: Dict[str, FabricArtifact] = {}
        workspace_id = workspace_metadata.get(Constant.ID, "")

        # Mapping of API response key to artifact type and description
        # Note: API key casing varies ("Lakehouse" vs "warehouses" vs "SQLAnalyticsEndpoint")
        artifact_configs: List[
            tuple[
                str,
                Literal["Lakehouse", "Warehouse", "SQLAnalyticsEndpoint"],
                str,
            ]
        ] = [
            (Constant.PARSING_KEY_LAKEHOUSE, "Lakehouse", "Lakehouse"),
            (Constant.PARSING_KEY_WAREHOUSES, "Warehouse", "Warehouse"),
            (
                Constant.PARSING_KEY_SQL_ANALYTICS_ENDPOINT,
                "SQLAnalyticsEndpoint",
                "SQLAnalyticsEndpoint",
            ),
        ]

        for api_key, artifact_type, log_name in artifact_configs:
            for artifact_data in workspace_metadata.get(api_key, []):
                artifact_id = artifact_data.get(Constant.ID)
                logger.info(f"Processing artifact: {artifact_id}")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Processing artifact: {artifact_data}")
                if artifact_id:
                    relation_dependent_ids: Optional[List[str]] = None
                    for rel in artifact_data.get(Constant.RELATIONS, []):
                        dep_id = rel.get(Constant.DEPENDENT_ON_ARTIFACT_ID)
                        if dep_id:
                            if relation_dependent_ids is None:
                                relation_dependent_ids = []
                            relation_dependent_ids.append(dep_id)
                    artifacts[artifact_id] = FabricArtifact(
                        id=artifact_id,
                        name=artifact_data.get(Constant.NAME, ""),
                        artifact_type=artifact_type,
                        workspace_id=workspace_id,
                        relation_dependent_ids=relation_dependent_ids,
                    )
                    logger.debug(f"Parsed {log_name} artifact: {artifact_id}")

        # Resolve physical_item_ids for SQLAnalyticsEndpoint: use relations.dependentOnArtifactId
        # that point to a Lakehouse/Warehouse in this workspace (so lineage URNs match OneLake).
        for artifact in artifacts.values():
            if (
                artifact.artifact_type == "SQLAnalyticsEndpoint"
                and artifact.relation_dependent_ids
            ):
                artifact.physical_item_ids = [
                    rid
                    for rid in artifact.relation_dependent_ids
                    if rid in artifacts
                    and artifacts[rid].artifact_type in ("Lakehouse", "Warehouse")
                ]

        if artifacts:
            logger.info(
                f"Parsed {len(artifacts)} Fabric artifacts from workspace {workspace_id}"
            )

        return artifacts

    def _get_workspace_datasets(self, workspace: Workspace) -> dict:
        """
        Filter out "dataset" from scan_result and return Dataset instance set
        """
        dataset_map: dict = {}
        scan_result = workspace.scan_result

        if scan_result is None:
            return dataset_map

        datasets: Optional[Any] = scan_result.get(Constant.DATASETS)
        if datasets is None or len(datasets) == 0:
            logger.warning(
                f"Workspace {scan_result[Constant.NAME]}({scan_result[Constant.ID]}) does not have datasets"
            )

            logger.info("Returning empty datasets")
            return dataset_map

        logger.debug("Processing scan result for datasets")

        base_url = self.__config.environment.web_app_base_url

        for dataset_dict in datasets:
            dataset_id = dataset_dict[Constant.ID]
            try:
                if self.__config.use_scan_result_only:
                    # Build dataset directly from scan result — avoids a
                    # GET /admin/groups/{ws}/datasets?$filter=id eq '{id}' call per dataset
                    dataset_instance = new_powerbi_dataset(workspace, dataset_dict)
                    # Construct webUrl only if scan result didn't provide one
                    if dataset_instance.webUrl is None:
                        dataset_instance.webUrl = f"{base_url}/groups/{workspace.id}/datasets/{dataset_id}/details"
                else:
                    dataset_instance = self._get_resolver().get_dataset(
                        workspace=workspace,
                        dataset_id=dataset_id,
                    )
                if dataset_instance is None:
                    continue
            except Exception as e:
                self.reporter.warning(
                    title="Unable to fetch dataset details",
                    message="Skipping this dataset due to the error. Metadata will be incomplete.",
                    context=f"workspace={workspace.name}, dataset-id={dataset_id}",
                    exc=e,
                )
                continue

            # fetch + set dataset parameters
            try:
                dataset_parameters = self._get_resolver().get_dataset_parameters(
                    workspace_id=workspace.id,
                    dataset_id=dataset_id,
                )
                dataset_instance.parameters = dataset_parameters
            except Exception as e:
                logger.info(f"Unable to fetch dataset parameters for {dataset_id}: {e}")

            if self.__config.extract_endorsements_to_tags:
                dataset_instance.tags = self._parse_endorsement(
                    dataset_dict.get(Constant.ENDORSEMENT_DETAIL)
                )

            # Extract dependent artifact ID from scan result relations (for DirectLake lineage)
            # The individual dataset API doesn't return relations, but the scan result does
            relations = dataset_dict.get(Constant.RELATIONS, [])
            for relation in relations:
                if relation.get(Constant.DEPENDENT_ON_ARTIFACT_ID):
                    dataset_instance.dependent_on_artifact_id = relation[
                        Constant.DEPENDENT_ON_ARTIFACT_ID
                    ]
                    logger.debug(
                        f"Dataset {dataset_id} depends on artifact: {dataset_instance.dependent_on_artifact_id}"
                    )
                    break

            dataset_map[dataset_instance.id] = dataset_instance
            # set dataset-name
            dataset_name: str = (
                dataset_instance.name
                if dataset_instance.name is not None
                else dataset_instance.id
            )
            logger.debug(f"dataset_dict = {dataset_dict}")
            for table_dict in dataset_dict.get(Constant.TABLES) or []:
                expression: Optional[str] = (
                    table_dict[Constant.SOURCE][0][Constant.EXPRESSION]
                    if table_dict.get(Constant.SOURCE) is not None
                    and len(table_dict[Constant.SOURCE]) > 0
                    else None
                )

                # Extract DirectLake fields from table source
                source_list = table_dict.get(Constant.SOURCE, [])
                source_schema: Optional[str] = None
                source_expression: Optional[str] = None
                if source_list:
                    first_source = source_list[0]
                    source_schema = first_source.get(
                        Constant.SCHEMA_NAME
                    )  # e.g., "dbo"
                    source_expression = first_source.get(
                        Constant.EXPRESSION
                    )  # upstream table name

                # Get storage mode (e.g., "DirectLake", "Import", "DirectQuery")
                storage_mode: Optional[str] = table_dict.get(Constant.STORAGE_MODE)

                table = Table(
                    name=table_dict[Constant.NAME],
                    full_name=form_full_table_name(
                        config=self.__config,
                        workspace=workspace,
                        dataset_name=dataset_name,
                        table_name=table_dict[Constant.NAME],
                    ),
                    expression=expression,
                    columns=[
                        Column(
                            **column,
                            datahubDataType=FIELD_TYPE_MAPPING.get(
                                column["dataType"], FIELD_TYPE_MAPPING["Null"]
                            ),
                        )
                        for column in table_dict.get("columns") or []
                    ],
                    measures=[
                        Measure(**measure)
                        for measure in table_dict.get("measures") or []
                    ],
                    dataset=dataset_instance,
                    row_count=None,
                    column_count=None,
                    storage_mode=storage_mode,
                    source_schema=source_schema,
                    source_expression=source_expression,
                )
                if self.__config.profiling.enabled:
                    self._get_resolver().profile_dataset(
                        dataset_instance,
                        table,
                        workspace.name,
                        self.__config.profile_pattern,
                    )
                dataset_instance.tables.append(table)
        return dataset_map

    def get_app(
        self,
        app_id: str,
    ) -> Optional[App]:
        return self.__admin_api_resolver.get_app(
            app_id=app_id,
        )

    def _populate_app_details(
        self, workspace: Workspace, workspace_metadata: Dict
    ) -> None:
        # App_id is not present at the root level of workspace_metadata.
        # It can be found in the workspace_metadata.dashboards or workspace_metadata.reports lists.

        # Workspace_metadata contains duplicate entries for all dashboards and reports that we have included
        # in the app.
        # The duplicate entries for a report contain key `originalReportObjectId` referencing to
        # an actual report id of workspace. The duplicate entries for a dashboard contain `displayName` where
        # displayName is generated from displayName of original dashboard with prefix "App"
        app_id: Optional[str] = None
        app_reports: List[AppReport] = []
        # Filter app reports
        for report in workspace_metadata.get(Constant.REPORTS) or []:
            if report.get(Constant.APP_ID):
                app_reports.append(
                    AppReport(
                        id=report[Constant.ID],
                        original_report_id=report[Constant.ORIGINAL_REPORT_OBJECT_ID],
                    )
                )
                if app_id is None:  # In PowerBI one workspace can have one app
                    app_id = report[Constant.APP_ID]

        raw_app_dashboards: List[Dict] = []
        # Filter app dashboards
        for dashboard in workspace_metadata.get(Constant.DASHBOARDS) or []:
            if dashboard.get(Constant.APP_ID):
                raw_app_dashboards.append(dashboard)
                if app_id is None:  # In PowerBI, one workspace contains one app
                    app_id = dashboard[Constant.APP_ID]

        # workspace doesn't have an App. Above two loops can be avoided
        # if app_id is available at root level in workspace_metadata
        if app_id is None:
            logger.debug(f"Workspace {workspace.name} does not contain an app.")
            return

        app: Optional[App] = self.get_app(app_id=app_id)
        if app is None:
            self.__reporter.info(
                title="App Not Found",
                message="The workspace includes an app, but its metadata is missing from the API response.",
                context=f"workspace_name={workspace.name}",
            )
            return

        # Map to find out which dashboards belongs to the App
        workspace_dashboard_map: Dict[str, Dict] = {
            raw_dashboard[Constant.DISPLAY_NAME]: raw_dashboard
            for raw_dashboard in raw_app_dashboards
        }

        app_dashboards: List[AppDashboard] = []
        for dashboard in workspace_metadata.get(Constant.DASHBOARDS) or []:
            app_dashboard_display_name = f"[App] {dashboard[Constant.DISPLAY_NAME]}"  # A Dashboard is considered part of an App if the workspace_metadata contains a Dashboard with a label formatted as "[App] <DashboardName>".
            if (
                app_dashboard_display_name in workspace_dashboard_map
            ):  # This dashboard is part of the App
                app_dashboards.append(
                    AppDashboard(
                        id=workspace_dashboard_map[app_dashboard_display_name][
                            Constant.ID
                        ],
                        original_dashboard_id=dashboard[Constant.ID],
                    )
                )

        app.reports = app_reports
        app.dashboards = app_dashboards
        workspace.app = app

    def fill_metadata_from_scan_result(
        self,
        workspaces: List[Workspace],
    ) -> set:
        """Scan workspaces and populate datasets + dataset_registry.

        Mutates workspaces in-place. Must be called for ALL batches before
        fill_regular_metadata_detail so that cross-workspace dataset references
        resolve correctly.

        Returns the set of workspace IDs that were successfully scanned
        (active and not filtered). The caller should use this to filter out
        inactive workspaces before Phase 2.
        """
        from datahub.ingestion.source.powerbi.config import _get_rss_mb

        workspaces_by_id = {workspace.id: workspace for workspace in workspaces}
        rss_before_scan = _get_rss_mb()
        scan_result = self._get_scan_result(list(workspaces_by_id.keys()))
        rss_after_scan = _get_rss_mb()
        logger.info(
            f"[MEM1] SCAN-API | {len(workspaces_by_id)} ws requested | "
            f"RSS={rss_before_scan:.0f}->{rss_after_scan:.0f}MB(+{rss_after_scan - rss_before_scan:.0f})"
        )
        if not scan_result:
            return set()

        scanned_ids: set = set()
        for workspace_metadata in scan_result["workspaces"]:
            if (
                workspace_metadata.get(Constant.STATE) != Constant.ACTIVE
                or workspace_metadata.get(Constant.TYPE)
                not in self.__config.workspace_type_filter
            ):
                wrk_identifier: str = (
                    workspace_metadata[Constant.NAME]
                    if workspace_metadata.get(Constant.NAME)
                    else workspace_metadata.get(Constant.ID)
                )
                self.__reporter.info(
                    title="Skipped Workspace",
                    message="Workspace was skipped due to the workspace_type_filter",
                    context=f"workspace={wrk_identifier}",
                )
                continue

            ws_id = workspace_metadata[Constant.ID]
            cur_workspace = workspaces_by_id.get(ws_id)
            if not cur_workspace:
                continue

            scanned_ids.add(ws_id)
            cur_workspace.scan_result = workspace_metadata
            cur_workspace.fabric_artifacts = self._parse_fabric_artifacts(
                workspace_metadata
            )
            cur_workspace.datasets = self._get_workspace_datasets(cur_workspace)
            self.dataset_registry.update(cur_workspace.datasets)

            # Fetch endorsement tags if enabled
            if self.__config.extract_endorsements_to_tags:
                cur_workspace.dashboard_endorsements = self._get_dashboard_endorsements(
                    cur_workspace.scan_result
                )
                cur_workspace.report_endorsements = self._get_report_endorsements(
                    cur_workspace.scan_result
                )

            self._populate_app_details(
                workspace=cur_workspace,
                workspace_metadata=workspace_metadata,
            )

            # When use_scan_result_only is enabled, build reports and dashboards
            # from scan result to avoid redundant per-workspace API calls
            if self.__config.use_scan_result_only:
                if self.__config.extract_reports:
                    cur_workspace.reports = self._get_reports_from_scan_result(
                        cur_workspace
                    )
                if self.__config.extract_dashboards:
                    cur_workspace.dashboards = self._get_dashboards_from_scan_result(
                        cur_workspace
                    )

            # Free the raw scan result — all needed data has been extracted
            # into lightweight parsed fields. This will save memory for large workspaces
            cur_workspace.scan_result = {}

            # Log per-workspace parse results
            ds_count = len(cur_workspace.datasets)
            tbl_count = sum(len(d.tables) for d in cur_workspace.datasets.values())
            rpt_count = len(cur_workspace.reports) if cur_workspace.reports else 0
            dash_count = len(cur_workspace.dashboards) if cur_workspace.dashboards else 0
            logger.info(
                f"[MEM1] P1-PARSE | {cur_workspace.name} | "
                f"{ds_count}ds {tbl_count}tbl {rpt_count}rpt {dash_count}dash | "
                f"scan_result={'cleared' if not cur_workspace.scan_result else 'RETAINED'} | "
                f"registry_total={len(self.dataset_registry)}"
            )

        return scanned_ids

    def _fill_independent_datasets(self, workspace: Workspace) -> None:
        reachable_datasets: List[str] = []
        # Find out reachable datasets
        for dashboard in workspace.dashboards.values():
            for tile in dashboard.tiles:
                if tile.dataset is not None:
                    reachable_datasets.append(tile.dataset.id)

        for report in workspace.reports.values():
            if report.dataset is not None:
                reachable_datasets.append(report.dataset.id)

        # Set datasets not present in reachable_datasets
        for dataset in workspace.datasets.values():
            if dataset.id not in reachable_datasets:
                workspace.independent_datasets[dataset.id] = dataset

    def fill_regular_metadata_detail(self, workspace: Workspace) -> None:
        """Fill reports, dashboards, endorsements, and app details for a workspace.

        Must be called after fill_metadata_from_scan_result has run for ALL
        batches, so that the dataset_registry is complete and cross-workspace
        references resolve correctly.
        """

        def fill_dashboards() -> None:
            dashboards_from_scan = self.__config.use_scan_result_only

            if dashboards_from_scan:
                # Dashboards + tiles already built from scan result
                logger.info(
                    f"Using {len(workspace.dashboards)} dashboards from scan result "
                    f"for workspace {workspace.name}, skipping redundant API calls"
                )
            else:
                workspace.dashboards = {
                    dashboard.id: dashboard
                    for dashboard in self._get_resolver().get_dashboards(workspace)
                }
                # Fetch tiles via API
                for dashboard in workspace.dashboards.values():
                    dashboard.tiles = self._get_resolver().get_tiles(
                        workspace, dashboard=dashboard
                    )

            # Resolve tile → report and tile → dataset references
            for dashboard in workspace.dashboards.values():
                for tile in dashboard.tiles:
                    # In Power BI, dashboards, reports, and datasets are tightly scoped to the workspace they belong to.
                    # https://learn.microsoft.com/en-us/power-bi/collaborate-share/service-new-workspaces
                    if tile.report_id:
                        tile.report = workspace.reports.get(tile.report_id)
                        if tile.report is None:
                            self.reporter.info(
                                title="Missing Report Lineage For Tile",
                                message="A Report reference that failed to be resolved. Please ensure that 'extract_reports' is set to True in the configuration.",
                                context=f"workspace-name: {workspace.name}, tile-name: {tile.title}, report-id: {tile.report_id}",
                            )
                    # However, semantic models (aka datasets) can be shared accross workspaces
                    # https://learn.microsoft.com/en-us/fabric/admin/portal-workspace#use-semantic-models-across-workspaces
                    # That's why the global 'dataset_registry' is required
                    if tile.dataset_id:
                        tile.dataset = self.dataset_registry.get(tile.dataset_id)
                        if tile.dataset is None:
                            self.reporter.info(
                                title="Missing Dataset Lineage For Tile",
                                message="A cross-workspace reference that failed to be resolved. Please ensure that no global workspace is being filtered out due to the workspace_id_pattern.",
                                context=f"workspace-name: {workspace.name}, tile-name: {tile.title}, dataset-id: {tile.dataset_id}",
                            )

        def fill_reports() -> None:
            if self.__config.extract_reports is False:
                logger.info(
                    "Skipping report retrieval as extract_reports is set to false"
                )
                return
            workspace.reports = self.get_reports(workspace)

        def fill_dashboard_tags() -> None:
            if self.__config.extract_endorsements_to_tags is False:
                logger.info(
                    "Skipping tag retrieval for dashboard as extract_endorsements_to_tags is set to false"
                )
                return
            for dashboard in workspace.dashboards.values():
                dashboard.tags = workspace.dashboard_endorsements.get(dashboard.id, [])

        # Endorsements and app details are already populated by
        # fill_metadata_from_scan_result (Phase 1).

        # fill reports first since some dashboard may reference a report
        fill_reports()
        if self.__config.extract_dashboards:
            fill_dashboards()
        fill_dashboard_tags()
        self._fill_independent_datasets(workspace=workspace)
