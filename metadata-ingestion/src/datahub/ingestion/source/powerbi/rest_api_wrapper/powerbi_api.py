import json
import logging
import sys
from typing import (
    Any,
    Dict,
    List,
    Literal,
    MutableMapping,
    Optional,
    Set,
    Type,
    TypeVar,
    cast,
)

import requests
from typing_extensions import LiteralString

from datahub.ingestion.source.powerbi.config import (
    Constant,
    PowerBiDashboardSourceConfig,
    PowerBiDashboardSourceReport,
    PowerBiEnvironment,
    PowerBiMetadataApiMode,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper import data_resolver
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_classes import (
    FABRIC_ARTIFACT_LAKEHOUSE,
    FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT,
    FABRIC_ARTIFACT_WAREHOUSE,
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
    User,
    Workspace,
    new_powerbi_dashboards,
    new_powerbi_dataset,
    new_powerbi_reports,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.data_resolver import (
    AdminAPIResolver,
    DataResolverBase,
    RegularAPIResolver,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.fabric_resolver import (
    FABRIC_FORMAT_TMDL,
    FABRIC_ITEM_LAKEHOUSE,
    FABRIC_ITEM_SQL_ENDPOINT,
    FABRIC_ITEM_WAREHOUSE,
    FABRIC_KIND_PAGINATED_REPORTS,
    FABRIC_KIND_REPORTS,
    FABRIC_KIND_SEMANTIC_MODELS,
    FabricDefinitionResolver,
    FabricItem,
    FabricLroTimeoutError,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.report_definition_parser import (
    ParsedPage,
    parse_rdl_dataset_binding,
    parse_report_definition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import (
    PARTITION_KIND_CALCULATED,
    PARTITION_KIND_ENTITY,
    PARTITION_KIND_M,
    TMDL_TABLES_PREFIX,
    ParsedModel,
    ParsedTable,
    parse_semantic_model,
)
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
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


R = TypeVar("R", bound=DataResolverBase)


def make_resolver(config: PowerBiDashboardSourceConfig, resolver_cls: Type[R]) -> R:
    # Shared with the probe so both authenticate and time out identically.
    # The constructor fetches an MSAL token.
    return resolver_cls(
        client_id=config.client_id,
        client_secret=config.client_secret.get_secret_value(),
        tenant_id=config.tenant_id,
        metadata_api_timeout=config.metadata_api_timeout,
        environment=config.environment,
    )


def groups_filter(modified_workspace_ids: List[str]) -> Dict[str, str]:
    # An empty list means no $filter at all, i.e. every workspace is listed.
    if not modified_workspace_ids:
        return {}
    return {"$filter": " or ".join(f"id eq {id_}" for id_ in modified_workspace_ids)}


def workspace_from_group(
    group: Dict[str, Any], environment: PowerBiEnvironment
) -> Workspace:
    return Workspace(
        id=group[Constant.ID],
        name=group[Constant.NAME],
        type=group[Constant.TYPE],
        webUrl=environment.workspace_url(
            workspace_id=group[Constant.ID],
            workspace_type=group[Constant.TYPE],
        ),
        datasets={},
        dashboards={},
        reports={},
        report_endorsements={},
        dashboard_endorsements={},
        scan_result={},
        independent_datasets={},
        app=None,  # populated in fill_metadata_from_scan_result
        # fabric_artifacts requires the scan response (Lakehouse /
        # warehouses / SQLAnalyticsEndpoint keys are absent from the
        # groups payload); set in fill_metadata_from_scan_result.
    )


class PowerBiAPI:
    def __init__(
        self,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
    ) -> None:
        self.__config: PowerBiDashboardSourceConfig = config
        self.__reporter = reporter

        self.__regular_api_resolver = make_resolver(config, RegularAPIResolver)

        self.__admin_api_resolver = make_resolver(config, AdminAPIResolver)

        self.__fabric_resolver: Optional[FabricDefinitionResolver] = None
        if config.metadata_api_mode == PowerBiMetadataApiMode.WORKSPACE_APIS:
            self.__fabric_resolver = FabricDefinitionResolver(
                msal_client=self.__regular_api_resolver.msal_client,
                request_session=self.__regular_api_resolver.request_session,
                lro_timeout_seconds=config.scan_timeout,
            )
            # Fail fast, like the Power BI token in DataResolverBase: without
            # a Fabric token this mode cannot ingest a single semantic model.
            self.__fabric_resolver.get_access_token()

        # Workspace roles stand in for per-item users in workspace_apis mode;
        # fetched once per workspace.
        self.__workspace_users: Dict[str, List[User]] = {}

        self.reporter: PowerBiDashboardSourceReport = reporter

        # A report or tile in one workspace can be built using a dataset from another workspace.
        # We need to store the dataset ID (which is a UUID) mapped to its dataset instance.
        # This mapping will allow us to retrieve the appropriate dataset for
        # reports and tiles across different workspaces.
        #
        # It spans every scanned workspace, so on large tenants it can hold tens
        # of thousands of datasets. It is file-backed so the bulk spills to disk
        # rather than staying resident, which previously caused OOMs.
        #
        # Snapshot semantics: unlike the old plain dict, a lookup that misses the
        # cache returns a freshly-unpickled copy, not the same instance stored in
        # workspace.datasets. Entries are read-only snapshots for cross-workspace
        # lineage; mutating a returned dataset would not write through.
        self._file_backed_conn = ConnectionWrapper()
        self.dataset_registry: MutableMapping[str, PowerBIDataset] = FileBackedDict(
            shared_connection=self._file_backed_conn,
            tablename="dataset_registry",
        )

    def close(self) -> None:
        self._file_backed_conn.close()

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
        # ``not scan_result`` covers both None (scan never ran / failed) and
        # {} (workspace was scan-omitted post two-phase refactor); aligns
        # with the same guard in _get_workspace_datasets.
        if not scan_result:
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

        # See _get_dashboard_endorsements for why this is ``not scan_result``
        # rather than ``is None``.
        if not scan_result:
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

        if self.__config.metadata_api_mode == PowerBiMetadataApiMode.WORKSPACE_APIS:
            return self.get_workspace_users(workspace_id)

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

    def _resolve_paginated_report_lineage(
        self, report: Report, workspace: Workspace
    ) -> None:
        """Resolve lineage for an RDL paginated report.

        Paginated reports don't carry a datasetId in the scan response, so we
        fall back to /reports/{id}/datasources to discover either an embedded
        external datasource or a binding to a shared Power BI dataset.
        """
        context = (
            f"workspace={workspace.name}, "
            f"report_name={report.name}, report_id={report.id}"
        )
        if self.__config.metadata_api_mode == PowerBiMetadataApiMode.WORKSPACE_APIS:
            bound_from_rdl = self._paginated_report_binding_from_rdl(report, workspace)
            if bound_from_rdl is not None:
                report.dataset_id = bound_from_rdl
                report.dataset = self.dataset_registry.get(bound_from_rdl)
                if report.dataset is None:
                    self.reporter.info(
                        title="Missing Lineage For Paginated Report",
                        message=(
                            "Paginated report is bound to a Power BI dataset that "
                            "was not found in any ingested workspace (likely a "
                            "cross-workspace reference filtered by "
                            "workspace_id_pattern); lineage will be missing."
                        ),
                        context=f"{context}, dataset_id={bound_from_rdl}",
                    )
                return
        if self.__config.admin_apis_only:
            # /reports/{id}/datasources has no admin-API variant, so paginated
            # report lineage cannot be fetched in this mode.
            self.reporter.info(
                title="Paginated Report Lineage Unavailable",
                message=(
                    "Paginated report lineage requires the regular "
                    "/reports/{id}/datasources API, which is unavailable in "
                    "admin_apis_only mode. Disable admin_apis_only or grant "
                    "Report.Read.All to the service principal to enable it."
                ),
                context=context,
            )
            return
        try:
            report.datasources = self._get_resolver().get_report_datasources(
                workspace=workspace, report_id=report.id
            )
        except Exception:
            self.log_http_error(
                message=(
                    f"Unable to fetch datasources for paginated report "
                    f"{report.name}({report.id}) in workspace {workspace.name}"
                )
            )
            self.reporter.warning(
                title="Paginated Report Datasources Fetch Failed",
                message=(
                    "Unable to fetch /datasources for a paginated report; "
                    "upstream lineage will be empty. See connector logs for "
                    "the HTTP error."
                ),
                context=context,
            )
            return

        bound_dataset_id = next(
            (
                ds.powerbi_dataset_id
                for ds in report.datasources
                if ds.powerbi_dataset_id is not None
            ),
            None,
        )
        if bound_dataset_id is not None:
            # RDL report bound to a shared semantic model: reuse the same
            # dataset->report lineage as a regular report.
            report.dataset_id = bound_dataset_id
            report.dataset = self.dataset_registry.get(bound_dataset_id)
            if report.dataset is None:
                self.reporter.info(
                    title="Missing Lineage For Paginated Report",
                    message=(
                        "Paginated report is bound to a Power BI dataset that "
                        "was not found in any scanned workspace (likely a "
                        "cross-workspace reference filtered by "
                        "workspace_id_pattern); lineage will be missing."
                    ),
                    context=f"{context}, dataset_id={bound_dataset_id}",
                )
        elif not report.datasources:
            # Succeeded with no rows: the report genuinely has no datasources
            # (admin_apis_only is handled earlier).
            self.reporter.info(
                title="Missing Lineage For Paginated Report",
                message=(
                    "Paginated report has no shared dataset and the "
                    "/datasources endpoint returned no rows; upstream lineage "
                    "will be empty."
                ),
                context=context,
            )

    def _report_scan_fallback(
        self, workspace: Workspace, title: LiteralString, message: LiteralString
    ) -> None:
        # In workspace_apis mode the per-workspace endpoints are the primary
        # path, not a fallback, so the info would only be noise there.
        if self.__config.metadata_api_mode == PowerBiMetadataApiMode.ADMIN_SCAN:
            self.reporter.info(
                title=title, message=message, context=f"workspace={workspace.name}"
            )

    def get_reports(self, workspace: Workspace) -> Dict[str, Report]:
        """
        Fetch the report from PowerBi for the given Workspace
        """
        reports: Dict[str, Report] = {}
        if workspace.scan_result:
            # Pre-filter None and App duplicates so the count-diff below reflects only
            # malformed entries (missing id/name/reportType), not duplicates.
            # new_powerbi_reports applies the same filters internally for safety when called
            # from non-scan paths (data_resolver.get_reports) that don't pre-filter.
            raw_reports = [
                r
                for r in workspace.scan_result.get(Constant.REPORTS, [])
                if r is not None and Constant.APP_ID not in r
            ]
            reports_list = new_powerbi_reports(workspace, raw_reports)
            if len(reports_list) < len(raw_reports):
                self.reporter.warning(
                    title="Skipped Reports",
                    message=f"Skipped {len(raw_reports) - len(reports_list)} report(s) with missing required fields (id, name, or reportType). Check logs for details.",
                    context=f"workspace={workspace.name}",
                )
            reports = {r.id: r for r in reports_list}
            # Warn when ownership is requested but scan result has no user data for any report.
            # Covers both missing key (None) and present-but-empty list ([]).
            if (
                self.__config.extract_ownership
                and reports_list
                and not any(r.users for r in reports_list)
                and not any(raw.get(Constant.USERS) for raw in raw_reports)
            ):
                self.reporter.info(
                    title="Report Users Not in Scan Result",
                    message=(
                        "No user data found in scan result for any report. "
                        "If ownership is expected, ensure the scan job is configured "
                        "with getArtifactUsers=True."
                    ),
                    context=f"workspace={workspace.name}",
                )
        else:
            self._report_scan_fallback(
                workspace,
                title="Report Scan Fallback Active",
                message="Workspace scan returned no data; falling back to per-workspace reports endpoint.",
            )
            try:
                reports = {
                    report.id: report
                    for report in self._get_resolver().get_reports(workspace)
                }
            except Exception:
                self.log_http_error(
                    message=f"Unable to fetch reports for workspace {workspace.name}"
                )
                if (
                    self.__config.metadata_api_mode
                    == PowerBiMetadataApiMode.WORKSPACE_APIS
                ):
                    # The per-workspace endpoint is the only source of reports
                    # here; an empty result would soft-delete them under
                    # stale-entity removal.
                    self.reporter.failure(
                        title="Reports Fetch Failed",
                        message="Unable to fetch reports for workspace; its reports are missing from this run.",
                        context=f"workspace={workspace.name}",
                    )
                else:
                    self.reporter.warning(
                        title="Reports Fetch Failed",
                        message="Unable to fetch reports for workspace; reports will be empty.",
                        context=f"workspace={workspace.name}",
                    )
            for report in reports.values():
                # _get_entity_users (called by get_report_users) already handles and swallows
                # HTTP/network exceptions internally, so no outer try/except is needed here.
                report.users = self.get_report_users(workspace.id, report.id)

        for report in reports.values():
            try:
                report.pages = self._get_resolver().get_pages_by_report(
                    workspace=workspace, report_id=report.id
                )
            except Exception:
                self.log_http_error(
                    message=f"Unable to fetch pages for report {report.name}({report.id}) in workspace {workspace.name}"
                )
                self.reporter.warning(
                    title="Report Pages Not Fetched",
                    message="Report pages could not be fetched; the report will appear in DataHub without chart children.",
                    context=f"workspace={workspace.name}, report_name={report.name}, report_id={report.id}",
                )
                report.pages = []
            if self.__config.extract_report_field_usage:
                self._fill_report_field_usage(workspace, report)
            if report.dataset_id:
                report.dataset = self.dataset_registry.get(report.dataset_id)
                if report.dataset is None:
                    self.reporter.info(
                        title="Missing Lineage For Report",
                        message="A cross-workspace reference that failed to be resolved. Please ensure that no global workspace is being filtered out due to the workspace_id_pattern.",
                        context=f"report-name: {report.name} and dataset-id: {report.dataset_id}",
                    )
            elif report.type is ReportType.PaginatedReport:
                self._resolve_paginated_report_lineage(report, workspace)

        def fill_tags() -> None:
            if self.__config.extract_endorsements_to_tags is False:
                logger.info(
                    "Skipping endorsements tags retrieval for report as extract_endorsements_to_tags is set to false"
                )
                return

            for report in reports.values():
                report.tags = workspace.report_endorsements.get(report.id, [])

        fill_tags()
        return reports

    def get_workspaces(self) -> List[Workspace]:
        modified_workspace_ids: List[str] = []

        if self.__config.modified_since:
            modified_workspace_ids = self.get_modified_workspaces()

        groups: List[dict] = []
        try:
            groups = self._get_resolver().get_groups(
                filter_=groups_filter(modified_workspace_ids)
            )

        except Exception:
            self.log_http_error(message="Unable to fetch list of workspaces")
            # raise  # we want this exception to bubble up

        workspaces = [
            workspace_from_group(group, self.__config.environment) for group in groups
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
        # Guard the dump: json.dumps on a large scan result builds a huge string
        # (hundreds of MB) that is pure waste when debug logging is off.
        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("scan result = %s", json.dumps(scan_result, indent=1))

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

        if not scan_result:
            return dataset_map

        datasets: Optional[Any] = scan_result.get(Constant.DATASETS)
        if datasets is None or len(datasets) == 0:
            logger.warning(
                f"Workspace {scan_result.get(Constant.NAME) or '<unknown>'}({scan_result.get(Constant.ID) or '<unknown>'}) does not have datasets"
            )

            logger.info("Returning empty datasets")
            return dataset_map

        logger.debug("Processing scan result for datasets")

        for dataset_dict in datasets:
            dataset_id = dataset_dict[Constant.ID]
            dataset_instance = new_powerbi_dataset(workspace, dataset_dict)

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

            self._fill_refresh_history(workspace, dataset_instance)

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

    def _require_fabric_resolver(self) -> FabricDefinitionResolver:
        if self.__fabric_resolver is None:
            raise RuntimeError(
                "Fabric resolver is only created for metadata_api_mode: workspace_apis"
            )
        return self.__fabric_resolver

    def get_workspace_users(self, workspace_id: str) -> List[User]:
        if workspace_id in self.__workspace_users:
            return self.__workspace_users[workspace_id]
        users: List[User] = []
        try:
            users = self.__regular_api_resolver.get_workspace_users(workspace_id)
        except Exception as e:
            self.log_http_error(
                message=f"Unable to fetch users for workspace {workspace_id}."
            )
            self.reporter.warning(
                title="Workspace Users Not Fetched",
                message="Workspace role assignments could not be fetched; assets in this workspace will have no owners.",
                context=f"workspace_id={workspace_id}",
                exc=e,
            )
        self.__workspace_users[workspace_id] = users
        return users

    def _fill_refresh_history(
        self, workspace: Workspace, dataset: PowerBIDataset
    ) -> None:
        if not self.__config.extract_refresh_history:
            return
        try:
            dataset.last_refresh = (
                self.__regular_api_resolver.get_latest_dataset_refresh(
                    workspace_id=workspace.id, dataset_id=dataset.id
                )
            )
            self.reporter.refresh_history_fetched += 1
        except Exception as e:
            self.log_http_error(
                message=f"Unable to fetch refresh history for dataset {dataset.id}"
            )
            self.reporter.warning(
                title="Refresh History Not Fetched",
                message="Dataset refresh history could not be fetched; no Operation aspect will be emitted for its tables.",
                context=f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}",
                exc=e,
            )

    def _paginated_report_binding_from_rdl(
        self, report: Report, workspace: Workspace
    ) -> Optional[str]:
        try:
            definition = self._require_fabric_resolver().get_item_definition(
                workspace_id=workspace.id,
                item_id=report.id,
                item_kind=FABRIC_KIND_PAGINATED_REPORTS,
            )
            self.reporter.fabric_definitions_fetched += 1
        except Exception as e:
            self._report_definition_failure(
                e,
                f"workspace={workspace.name}, report={report.name} id={report.id}",
                FABRIC_KIND_PAGINATED_REPORTS,
            )
            return None
        return parse_rdl_dataset_binding(definition.parts)

    def _report_definition_failure(
        self, e: Exception, context: str, item_kind: str
    ) -> None:
        self.reporter.fabric_definitions_failed += 1
        if isinstance(e, FabricLroTimeoutError):
            self.reporter.fabric_lro_timeouts += 1
        self.log_http_error(
            message=f"Unable to fetch Fabric item definition ({context})"
        )
        permission_hint = (
            "Grant the service principal the Contributor role on the workspace, "
            "enable the tenant setting 'Service principals can use Fabric APIs', "
            "and check for an encrypted sensitivity label on the item."
        )
        if item_kind == FABRIC_KIND_SEMANTIC_MODELS:
            # Without its definition a semantic model has no tables this run;
            # with stale-entity removal a green run would soft-delete them, so
            # any cause (refused, failed operation, timeout) is a failure.
            if data_resolver.is_permission_error(e):
                self.reporter.failure(
                    title="Fabric API Permission Denied",
                    message=f"getDefinition was refused for this semantic model. {permission_hint}",
                    context=context,
                    exc=e,
                )
            else:
                self.reporter.failure(
                    title="Semantic Model Definition Not Fetched",
                    message=(
                        "getDefinition failed for this semantic model; its tables, "
                        "schema and lineage are missing from this run. If the "
                        "long-running operation timed out, increase scan_timeout."
                    ),
                    context=context,
                    exc=e,
                )
            return
        # Report definitions only enrich existing entities (visual field usage,
        # paginated-report dataset binding); losing them does not lose entities.
        if data_resolver.is_permission_error(e):
            self.reporter.fabric_report_definitions_denied += 1
            self.reporter.warning(
                title="Report Definition Permission Denied",
                message=f"getDefinition was refused for this report; its visual field usage or dataset binding is skipped. {permission_hint}",
                context=context,
                exc=e,
            )
            return
        self.reporter.warning(
            title="Report Definition Not Fetched",
            message=(
                "getDefinition failed for this report; its visual field usage or "
                "dataset binding is skipped. If the long-running operation timed "
                "out, increase scan_timeout."
            ),
            context=context,
            exc=e,
        )

    def _fill_report_field_usage(self, workspace: Workspace, report: Report) -> None:
        if report.type is ReportType.PaginatedReport or not report.pages:
            return
        context = f"workspace={workspace.name}, report={report.name} id={report.id}"
        try:
            definition = self._require_fabric_resolver().get_item_definition(
                workspace_id=workspace.id,
                item_id=report.id,
                item_kind=FABRIC_KIND_REPORTS,
            )
            self.reporter.fabric_definitions_fetched += 1
        except Exception as e:
            self._report_definition_failure(e, context, FABRIC_KIND_REPORTS)
            return

        parsed = parse_report_definition(definition.parts)
        self.reporter.report_definitions_parsed += 1
        failed_parts = definition.failed_parts + parsed.failed_parts
        if failed_parts:
            self.reporter.report_definition_parts_failed += len(failed_parts)
            self.reporter.warning(
                title="Report Definition Part Not Parsed",
                message="Some parts of the report definition were not valid JSON; the visuals they describe are missing from the page's inputFields.",
                context=f"{context}, parts={failed_parts}",
            )
        for page in report.pages:
            parsed_page: Optional[ParsedPage] = parsed.pages.get(page.name)
            if parsed_page is None and page.displayName:
                parsed_page = parsed.page_by_display_name(page.displayName)
            if parsed_page is None:
                self.reporter.report_pages_unmatched += 1
                logger.debug(
                    "Page %s of report %s not found in its definition",
                    page.name,
                    report.id,
                )
                continue
            page.visual_count = len(parsed_page.visuals)
            page.visual_types = parsed_page.visual_types
            page.field_references = parsed_page.field_refs

    def _fabric_artifacts_from_items(
        self, workspace: Workspace, items: List[FabricItem]
    ) -> Dict[str, FabricArtifact]:
        """Lakehouse / Warehouse items plus their SQL analytics endpoints.

        A DirectLake model's shared expression names the SQL endpoint id, not
        the lakehouse, so each endpoint is registered as a SQLAnalyticsEndpoint
        artifact whose physical_item_ids point at the lakehouse; the existing
        DirectLake lineage path then resolves it exactly as it does for scan
        results. A warehouse's endpoint id is the warehouse id itself.
        """
        artifacts: Dict[str, FabricArtifact] = {}
        for item in items:
            if item.type == FABRIC_ITEM_LAKEHOUSE:
                artifacts[item.id] = FabricArtifact(
                    id=item.id,
                    name=item.display_name,
                    artifact_type=FABRIC_ARTIFACT_LAKEHOUSE,
                    workspace_id=workspace.id,
                )
                endpoint_id = self._lakehouse_sql_endpoint_id(workspace, item)
                if endpoint_id:
                    artifacts[endpoint_id] = FabricArtifact(
                        id=endpoint_id,
                        name=item.display_name,
                        artifact_type=FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT,
                        workspace_id=workspace.id,
                        relation_dependent_ids=[item.id],
                        physical_item_ids=[item.id],
                    )
            elif item.type == FABRIC_ITEM_WAREHOUSE:
                artifacts[item.id] = FabricArtifact(
                    id=item.id,
                    name=item.display_name,
                    artifact_type=FABRIC_ARTIFACT_WAREHOUSE,
                    workspace_id=workspace.id,
                )
        for item in items:
            if item.type == FABRIC_ITEM_SQL_ENDPOINT and item.id not in artifacts:
                # Endpoint whose lakehouse is not in this workspace listing (or
                # whose properties call failed); kept so the skip is explicit.
                artifacts[item.id] = FabricArtifact(
                    id=item.id,
                    name=item.display_name,
                    artifact_type=FABRIC_ARTIFACT_SQL_ANALYTICS_ENDPOINT,
                    workspace_id=workspace.id,
                )
        return artifacts

    def _lakehouse_sql_endpoint_id(
        self, workspace: Workspace, item: FabricItem
    ) -> Optional[str]:
        try:
            return self._require_fabric_resolver().get_lakehouse_sql_endpoint_id(
                workspace_id=workspace.id, lakehouse_id=item.id
            )
        except Exception as e:
            self.log_http_error(
                message=f"Unable to fetch lakehouse {item.id} properties"
            )
            self.reporter.warning(
                title="Lakehouse SQL Endpoint Not Resolved",
                message="Lakehouse properties could not be fetched; DirectLake tables over it will have no lineage.",
                context=f"workspace={workspace.name}, lakehouse={item.display_name} id={item.id}",
                exc=e,
            )
            return None

    def _table_from_parsed(
        self,
        workspace: Workspace,
        dataset: PowerBIDataset,
        dataset_name: str,
        parsed_table: ParsedTable,
    ) -> Table:
        partition = parsed_table.partition
        expression: Optional[str] = None
        calculated_expression: Optional[str] = None
        storage_mode: Optional[str] = None
        source_schema: Optional[str] = None
        source_expression: Optional[str] = None
        if partition is not None:
            if partition.kind == PARTITION_KIND_M:
                expression = partition.source
            elif partition.kind == PARTITION_KIND_CALCULATED:
                calculated_expression = partition.source
            elif partition.kind == PARTITION_KIND_ENTITY:
                storage_mode = Constant.DIRECT_LAKE
                source_schema = partition.schema_name
                source_expression = partition.entity_name
            # `query` partitions hold native SQL, which the M parser cannot
            # consume; the table is emitted without an expression or lineage.

        columns = [
            Column(
                name=column.name,
                dataType=column.data_type or Constant.NULL_DATA_TYPE,
                isHidden=column.is_hidden,
                datahubDataType=FIELD_TYPE_MAPPING.get(
                    column.data_type or Constant.NULL_DATA_TYPE,
                    FIELD_TYPE_MAPPING[Constant.NULL_DATA_TYPE],
                ),
                columnType=Constant.COLUMN_TYPE_CALCULATED
                if column.expression
                else Constant.COLUMN_TYPE_DATA,
                expression=column.expression,
                description=column.description,
            )
            for column in parsed_table.columns
        ]
        measures = [
            Measure(
                name=measure.name,
                expression=measure.expression,
                isHidden=measure.is_hidden,
                description=measure.description,
                references=measure.references,
            )
            for measure in parsed_table.measures
        ]
        return Table(
            name=parsed_table.name,
            full_name=form_full_table_name(
                config=self.__config,
                workspace=workspace,
                dataset_name=dataset_name,
                table_name=parsed_table.name,
            ),
            expression=expression,
            columns=columns,
            measures=measures,
            dataset=dataset,
            row_count=None,
            column_count=None,
            storage_mode=storage_mode,
            source_schema=source_schema,
            source_expression=source_expression,
            calculated_expression=calculated_expression,
            description=parsed_table.description,
        )

    @staticmethod
    def _directlake_sql_endpoint_id(parsed_model: ParsedModel) -> Optional[str]:
        """SQL analytics endpoint referenced by the model's DirectLake
        partitions through their shared `expressionSource`."""
        for table in parsed_model.tables:
            partition = table.partition
            if partition is None or partition.kind != PARTITION_KIND_ENTITY:
                continue
            shared = (
                parsed_model.shared_expressions.get(partition.expression_source)
                if partition.expression_source
                else None
            )
            if shared is not None and shared.sql_endpoint_id:
                return shared.sql_endpoint_id
        return None

    def _fill_dataset_from_definition(
        self, workspace: Workspace, dataset: PowerBIDataset
    ) -> None:
        context = f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}"
        try:
            definition = self._require_fabric_resolver().get_item_definition(
                workspace_id=workspace.id,
                item_id=dataset.id,
                item_kind=FABRIC_KIND_SEMANTIC_MODELS,
                definition_format=FABRIC_FORMAT_TMDL,
            )
            self.reporter.fabric_definitions_fetched += 1
        except Exception as e:
            self._report_definition_failure(e, context, FABRIC_KIND_SEMANTIC_MODELS)
            return

        parsed_model = parse_semantic_model(definition.parts)
        failed_parts = definition.failed_parts + parsed_model.failed_parts
        if failed_parts:
            self.reporter.tmdl_parts_failed += len(failed_parts)
            failed_tables = [
                part for part in failed_parts if part.startswith(TMDL_TABLES_PREFIX)
            ]
            if failed_tables:
                # A table part that did not parse is a table missing from this
                # run; under stale-entity removal a green run would soft-delete
                # it, so this is a failure like a refused definition.
                self.reporter.failure(
                    title="TMDL Table Not Parsed",
                    message="Some table parts of the semantic-model definition could not be decoded or parsed; those tables are missing from this run.",
                    context=f"{context}, parts={failed_tables}",
                )
            else:
                self.reporter.warning(
                    title="TMDL Part Not Parsed",
                    message="Some non-table parts of the semantic-model definition could not be decoded or parsed; relationships or DirectLake lineage from them are missing.",
                    context=f"{context}, parts={failed_parts}",
                )
        dataset.dependent_on_artifact_id = self._directlake_sql_endpoint_id(
            parsed_model
        )
        dataset_name = dataset.name if dataset.name is not None else dataset.id
        for parsed_table in parsed_model.tables:
            table = self._table_from_parsed(
                workspace, dataset, dataset_name, parsed_table
            )
            if self.__config.profiling.enabled:
                self.__regular_api_resolver.profile_dataset(
                    dataset, table, workspace.name, self.__config.profile_pattern
                )
            dataset.tables.append(table)
            self.reporter.tmdl_tables_parsed += 1
        if self.__config.extract_model_relationships:
            dataset.relationships = parsed_model.relationships
            self.reporter.relationships_parsed += len(dataset.relationships)
            if parsed_model.skipped_relationships:
                self.reporter.relationships_skipped += len(
                    parsed_model.skipped_relationships
                )
                self.reporter.warning(
                    title="Relationship Not Parsed",
                    message="Some semantic-model relationships had unparseable fromColumn/toColumn references and are not emitted as foreign keys.",
                    context=f"{context}, relationships={parsed_model.skipped_relationships}",
                )

    def _fill_workspace_datasets_from_definitions(self, workspace: Workspace) -> None:
        try:
            raw_datasets = self.__regular_api_resolver.get_datasets(workspace)
        except Exception as e:
            self.log_http_error(
                message=f"Unable to list datasets for workspace {workspace.name}"
            )
            if data_resolver.is_permission_error(e):
                self.reporter.failure(
                    title="Workspace Datasets Permission Denied",
                    message="GET /groups/{id}/datasets was refused; the workspace is ingested without datasets or lineage. Grant the service principal the Contributor role on the workspace.",
                    context=f"workspace={workspace.name} id={workspace.id}",
                    exc=e,
                )
            else:
                # Same stale-entity reasoning as the permission case: the
                # workspace's tables would be soft-deleted on a green run.
                self.reporter.failure(
                    title="Datasets Not Listed",
                    message="GET /groups/{id}/datasets failed; the workspace is ingested without datasets or lineage.",
                    context=f"workspace={workspace.name} id={workspace.id}",
                    exc=e,
                )
            return

        workspace.datasets = {}
        for raw_dataset in raw_datasets:
            if not raw_dataset.get(Constant.ID):
                continue
            dataset = new_powerbi_dataset(workspace, raw_dataset)
            # One malformed or unreadable model must not take the rest of the
            # workspace (or the batch) down with it.
            try:
                try:
                    dataset.parameters = (
                        self.__regular_api_resolver.get_dataset_parameters(
                            workspace_id=workspace.id, dataset_id=dataset.id
                        )
                    )
                except Exception as e:
                    self.reporter.dataset_parameters_failed += 1
                    self.reporter.warning(
                        title="Dataset Parameters Not Fetched",
                        message="Dataset parameters could not be fetched; M-Query parameters in this model's sources are not resolved, which can drop lineage for parameterized sources.",
                        context=f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}",
                        exc=e,
                    )
                self._fill_dataset_from_definition(workspace, dataset)
                self._fill_refresh_history(workspace, dataset)
            except Exception as e:
                self.reporter.semantic_models_not_processed += 1
                self.reporter.failure(
                    title="Semantic Model Not Processed",
                    message="Unexpected error while building this semantic model's tables; it is skipped.",
                    context=f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}",
                    exc=e,
                )
                continue
            workspace.datasets[dataset.id] = dataset
        self.dataset_registry.update(workspace.datasets)

    def fill_metadata_from_definitions(self, workspaces: List[Workspace]) -> Set[str]:
        """workspace_apis-mode counterpart of fill_metadata_from_scan_result.

        Populates datasets (from GET /groups/{id}/datasets plus each model's
        TMDL definition) and fabric_artifacts (from the Fabric item listing)
        in place, registers the datasets for cross-workspace lookups, and
        returns the ids of workspaces excluded by workspace_type_filter.
        """
        excluded_ids: Set[str] = set()
        for workspace in workspaces:
            if workspace.type not in self.__config.workspace_type_filter:
                self.reporter.info(
                    title="Skipped Workspace",
                    message="Workspace was skipped due to the workspace_type_filter",
                    context=f"workspace={workspace.name}",
                )
                excluded_ids.add(workspace.id)
                continue

            try:
                items = self._require_fabric_resolver().list_items(workspace.id)
                workspace.fabric_artifacts = self._fabric_artifacts_from_items(
                    workspace, items
                )
            except Exception as e:
                self.log_http_error(
                    message=f"Unable to list Fabric items for workspace {workspace.name}"
                )
                self.reporter.warning(
                    title="Fabric Items Not Listed",
                    message="The Fabric item listing failed; DirectLake lineage will be missing for this workspace.",
                    context=f"workspace={workspace.name} id={workspace.id}",
                    exc=e,
                )

            self._fill_workspace_datasets_from_definitions(workspace)

        return excluded_ids

    def report_if_no_definitions_fetched(self) -> None:
        """Call once after every batch has been processed: with every
        getDefinition failed the run would otherwise emit no semantic-model
        metadata at all while looking healthy."""
        if (
            self.reporter.fabric_definitions_failed
            and not self.reporter.fabric_definitions_fetched
        ):
            self.reporter.failure(
                title="No Item Definitions Fetched",
                message="Every Fabric getDefinition call failed; no semantic-model schema or lineage has been ingested. Check the Contributor role, the 'Service principals can use Fabric APIs' tenant setting and scan_timeout.",
                context=f"failed={self.reporter.fabric_definitions_failed}",
            )

    def fill_metadata_from_scan_result(
        self,
        workspaces: List[Workspace],
    ) -> Set[str]:
        """Populate scan-derived metadata (scan_result, fabric_artifacts,
        datasets) on the given workspaces in place.

        Returns the set of workspace IDs that the scan excluded (inactive or
        excluded by workspace_type_filter). Callers should skip these in
        downstream phases to avoid redundant per-workspace API calls.
        """
        workspaces_by_id = {workspace.id: workspace for workspace in workspaces}
        scan_result = self._get_scan_result(list(workspaces_by_id))
        excluded_ids: Set[str] = set()
        if not scan_result:
            # _get_scan_result returns None when create_scan_job raises any
            # exception (HTTP/permission/network/etc., logged via
            # log_http_error) and may also return an empty payload from the
            # admin API. We cannot distinguish these here, so surface a
            # cause-neutral per-workspace warning. Scan timeouts and
            # result-fetch failures propagate as exceptions instead and are
            # handled by the Phase 1 try/except in get_workunits_internal.
            for ws in workspaces:
                self.__reporter.warning(
                    title="Incomplete Scan Metadata",
                    message=(
                        "Scan returned no metadata for this workspace; it "
                        "will be ingested without scan-derived metadata "
                        "(endorsements, app, fabric artifacts, "
                        "cross-workspace dataset lineage). Check logs for "
                        "any underlying HTTP or scan error."
                    ),
                    context=f"workspace={ws.name} id={ws.id}",
                )
            return excluded_ids

        returned_ids: Set[str] = set()
        for workspace_metadata in scan_result.get("workspaces") or []:
            ws_id_in_scan = workspace_metadata.get(Constant.ID)
            if ws_id_in_scan:
                returned_ids.add(ws_id_in_scan)
            if (
                workspace_metadata.get(Constant.STATE) != Constant.ACTIVE
                or workspace_metadata.get(Constant.TYPE)
                not in self.__config.workspace_type_filter
            ):
                # if the state is not "Active" then in some state like Not Found, "name" attribute is not present
                wrk_identifier: str = (
                    workspace_metadata.get(Constant.NAME)
                    or workspace_metadata.get(Constant.ID)
                    or "<unknown>"
                )
                self.__reporter.info(
                    title="Skipped Workspace",
                    message="Workspace was skipped due to the workspace_type_filter",
                    context=f"workspace={wrk_identifier}",
                )
                if ws_id_in_scan and ws_id_in_scan in workspaces_by_id:
                    excluded_ids.add(ws_id_in_scan)
                continue

            if not ws_id_in_scan:
                # Active workspace entry without an id is a malformed scan
                # response; nothing to correlate it to so skip safely.
                logger.debug(
                    "Scan returned an active workspace entry with no id; skipping."
                )
                continue

            cur_workspace = workspaces_by_id.get(ws_id_in_scan)
            if not cur_workspace:
                # Scan can surface workspace IDs that weren't requested in this
                # batch; ignore them here (they get processed in their own batch
                # if allowed) and log for debuggability.
                logger.debug(
                    "Scan returned workspace id %s outside the request batch; skipping.",
                    ws_id_in_scan,
                )
                continue

            cur_workspace.scan_result = workspace_metadata
            cur_workspace.fabric_artifacts = self._parse_fabric_artifacts(
                workspace_metadata
            )
            cur_workspace.datasets = self._get_workspace_datasets(cur_workspace)
            # collect all datasets in the registry
            self.dataset_registry.update(cur_workspace.datasets)

        # Surface workspaces the scan API silently omitted. They still reach
        # Phase 2 with empty scan metadata, but ingesters need visibility into
        # the gap so they can investigate (typically an admin API permissions
        # issue or a transient scan failure).
        missing_ids = set(workspaces_by_id) - returned_ids - excluded_ids
        for missing_id in missing_ids:
            missing_ws = workspaces_by_id[missing_id]
            self.__reporter.warning(
                title="Incomplete Scan Metadata",
                message=(
                    "Workspace was requested but not present in the scan "
                    "response; downstream metadata (endorsements, app, "
                    "dataset lineage) may be incomplete for this workspace."
                ),
                context=f"workspace={missing_ws.name} id={missing_id}",
            )

        return excluded_ids

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
        def fill_dashboards() -> None:
            if workspace.scan_result:
                # Pre-filter None and App duplicates here so the count-diff below
                # reflects only malformed entries (missing id/displayName), not duplicates.
                # new_powerbi_dashboards applies the same filters internally for safety
                # when called from non-scan paths that don't pre-filter.
                raw_dashboards = [
                    r
                    for r in workspace.scan_result.get(Constant.DASHBOARDS, [])
                    if r is not None and Constant.APP_ID not in r
                ]
                dashboards_list = new_powerbi_dashboards(workspace, raw_dashboards)
                if len(dashboards_list) < len(raw_dashboards):
                    self.__reporter.warning(
                        title="Skipped Dashboards",
                        message=f"Skipped {len(raw_dashboards) - len(dashboards_list)} dashboard(s) with missing required fields (id or displayName). Check logs for details.",
                        context=f"workspace={workspace.name}",
                    )
                # Warn when ownership is requested but scan result has no user data for any
                # dashboard. Checks for both missing key (None) and present-but-empty list ([]).
                if (
                    self.__config.extract_ownership
                    and dashboards_list
                    and not any(d.users for d in dashboards_list)
                    and not any(r.get(Constant.USERS) for r in raw_dashboards)
                ):
                    self.__reporter.info(
                        title="Dashboard Users Not in Scan Result",
                        message=(
                            "No user data found in scan result for any dashboard. "
                            "If ownership is expected, ensure the scan job is configured "
                            "with getArtifactUsers=True."
                        ),
                        context=f"workspace={workspace.name}",
                    )
                # Surface tile skips caused by missing id fields in the scan result.
                total_raw_tiles = sum(
                    len([t for t in r.get(Constant.TILES) or [] if t is not None])
                    for r in raw_dashboards
                    if r.get(Constant.ID) in {d.id for d in dashboards_list}
                )
                total_built_tiles = sum(len(d.tiles) for d in dashboards_list)
                if total_built_tiles < total_raw_tiles:
                    self.__reporter.warning(
                        title="Skipped Tiles",
                        message=f"Skipped {total_raw_tiles - total_built_tiles} tile(s) with missing id field in scan result. Check logs for details.",
                        context=f"workspace={workspace.name}",
                    )
                workspace.dashboards = {d.id: d for d in dashboards_list}
            else:
                self._report_scan_fallback(
                    workspace,
                    title="Dashboard Scan Fallback Active",
                    message="Workspace scan returned no data; falling back to per-dashboard get_dashboards/get_tiles/get_dashboard_users calls.",
                )
                workspace.dashboards = {
                    dashboard.id: dashboard
                    for dashboard in self._get_resolver().get_dashboards(workspace)
                }
                for dashboard in workspace.dashboards.values():
                    dashboard.tiles = self._get_resolver().get_tiles(
                        workspace, dashboard=dashboard
                    )
                    dashboard.users = self.get_dashboard_users(dashboard)

            # set tiles of Dashboard
            for dashboard in workspace.dashboards.values():
                # set the dataset and the report for tiles
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

        # Fetch endorsement tag if it is enabled from configuration
        if self.__config.extract_endorsements_to_tags:
            workspace.dashboard_endorsements = self._get_dashboard_endorsements(
                workspace.scan_result
            )
            workspace.report_endorsements = self._get_report_endorsements(
                workspace.scan_result
            )
        else:
            logger.info(
                "Skipping endorsements tag as extract_endorsements_to_tags is not enabled"
            )

        self._populate_app_details(
            workspace=workspace,
            workspace_metadata=workspace.scan_result,
        )

        # fill reports first since some dashboard may reference a report
        fill_reports()
        if self.__config.extract_dashboards:
            fill_dashboards()
        fill_dashboard_tags()
        self._fill_independent_datasets(workspace=workspace)
