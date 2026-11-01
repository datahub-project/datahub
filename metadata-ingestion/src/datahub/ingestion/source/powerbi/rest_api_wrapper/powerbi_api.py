import json
import logging
import sys
from dataclasses import dataclass
from http import HTTPStatus
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
    Union,
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
from datahub.ingestion.source.powerbi.rest_api_wrapper.composite_model import (
    analysis_services_targets,
    remote_model_key,
)
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
    RemoteModelMatch,
    RemoteModelReference,
    Report,
    ReportType,
    RlsTableFilter,
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
    ItemDefinition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.report_definition_parser import (
    ParsedPage,
    filter_targets,
    parse_rdl_dataset_binding,
    parse_report_definition,
)
from datahub.ingestion.source.powerbi.rest_api_wrapper.tmdl_parser import (
    PARTITION_KIND_CALCULATED,
    PARTITION_KIND_ENTITY,
    PARTITION_KIND_M,
    PARTITION_MODE_DIRECT_LAKE,
    PARTITION_MODE_DIRECT_QUERY,
    PARTITION_MODE_DUAL,
    PARTITION_MODE_IMPORT,
    PARTITION_MODE_PUSH,
    TMDL_TABLES_PREFIX,
    ParsedColumn,
    ParsedMeasure,
    ParsedModel,
    ParsedPartition,
    ParsedTable,
    parse_semantic_model,
)
from datahub.utilities.file_backed_collections import (
    ConnectionWrapper,
    FileBackedDict,
)

# Logger instance
logger = logging.getLogger(__name__)


# TMDL partition modes are camelCase; the scan API's `storageMode` uses the
# TOM enum casing, which Table.storage_mode keeps. `directLake` is deliberately
# absent: DirectLake is recognised by the `entity` partition kind alone, so a
# stray `mode: directLake` on an M / calculated / query partition never routes
# the table into the DirectLake lineage branch. Unknown modes pass through.
PARTITION_MODE_TO_STORAGE_MODE: Dict[str, str] = {
    PARTITION_MODE_IMPORT: Constant.STORAGE_MODE_IMPORT,
    PARTITION_MODE_DIRECT_QUERY: Constant.STORAGE_MODE_DIRECT_QUERY,
    PARTITION_MODE_DUAL: Constant.STORAGE_MODE_DUAL,
    PARTITION_MODE_PUSH: Constant.STORAGE_MODE_PUSH,
}


def _field_has_properties(
    field: Union[ParsedColumn, ParsedMeasure],
) -> bool:
    return (
        field.model_properties is not None
        and field.model_properties.has_non_default_properties(field.is_hidden)
    )


def _storage_mode_from_partition(partition: ParsedPartition) -> Optional[str]:
    if partition.kind == PARTITION_KIND_ENTITY:
        return Constant.DIRECT_LAKE
    if partition.mode is not None and partition.mode != PARTITION_MODE_DIRECT_LAKE:
        return PARTITION_MODE_TO_STORAGE_MODE.get(partition.mode, partition.mode)
    return None


@dataclass(frozen=True)
class _PartitionFields:
    """What a table's first TMDL partition contributes to the Table: the M
    expression, a calculated table's DAX, a DirectLake entity binding or a
    composite model's remote table, and the storage mode. `query` partitions
    hold native SQL, which the M parser cannot consume; such a table carries
    no expression or lineage."""

    expression: Optional[str] = None
    calculated_expression: Optional[str] = None
    source_schema: Optional[str] = None
    source_expression: Optional[str] = None
    storage_mode: Optional[str] = None
    remote_model: Optional[RemoteModelReference] = None
    remote_table_name: Optional[str] = None


def _partition_fields(
    partition: Optional[ParsedPartition],
    table_name: str,
    analysis_services: Dict[str, Optional[RemoteModelReference]],
) -> _PartitionFields:
    if partition is None:
        return _PartitionFields()
    if partition.kind == PARTITION_KIND_ENTITY:
        entity = PowerBiAPI._entity_partition_source(
            table_name, partition, analysis_services
        )
        return _PartitionFields(
            source_schema=entity.source_schema,
            source_expression=entity.source_expression,
            storage_mode=entity.storage_mode,
            remote_model=entity.remote_model,
            remote_table_name=entity.remote_table_name,
        )
    storage_mode = _storage_mode_from_partition(partition)
    if partition.kind == PARTITION_KIND_M:
        return _PartitionFields(expression=partition.source, storage_mode=storage_mode)
    if partition.kind == PARTITION_KIND_CALCULATED:
        return _PartitionFields(
            calculated_expression=partition.source, storage_mode=storage_mode
        )
    return _PartitionFields(storage_mode=storage_mode)


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


def registry_key(dataset_id: str) -> str:
    """Key of dataset_registry: dataset ids are GUIDs whose casing differs
    between the REST listing and TMDL / RDL references."""
    return dataset_id.casefold()


@dataclass
class EntityPartitionSource:
    """What a TMDL `entity` partition reads: a Fabric lakehouse / warehouse
    table (DirectLake) or, in a composite model, a table of an Analysis
    Services database over DirectQuery."""

    storage_mode: Optional[str]
    source_schema: Optional[str] = None
    source_expression: Optional[str] = None
    remote_model: Optional[RemoteModelReference] = None
    remote_table_name: Optional[str] = None


def _http_status(e: Exception) -> Optional[int]:
    if isinstance(e, requests.exceptions.HTTPError) and e.response is not None:
        return e.response.status_code
    return None


def _http_error_body(e: Exception) -> Dict[str, Any]:
    """The Power BI error envelope of an HTTPError, or {} when there is none."""
    if not isinstance(e, requests.exceptions.HTTPError) or e.response is None:
        return {}
    try:
        body = e.response.json()
    except ValueError:
        return {}
    if not isinstance(body, dict):
        return {}
    error = body.get(Constant.ERROR)
    return error if isinstance(error, dict) else {}


def _is_refresh_schedule_not_supported(error: Dict[str, Any]) -> bool:
    if error.get(Constant.CODE) not in Constant.REFRESH_SCHEDULE_NOT_SUPPORTED_CODES:
        return False
    message = str(error.get(Constant.MESSAGE) or "").lower()
    return any(
        phrase in message for phrase in Constant.REFRESH_SCHEDULE_NOT_SUPPORTED_PHRASES
    )


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
        if config.item_definitions_available:
            self.__fabric_resolver = self._new_fabric_resolver(config, reporter)

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
        # A composite model names its source model by workspace and model
        # name, not by id. Keyed by remote_model_key; same snapshot semantics
        # as dataset_registry. Built in Phase 1 of workspace_apis mode. A key
        # that two datasets claim (same name twice in a workspace, or two
        # workspaces with one display name) is dropped and remembered as
        # ambiguous so it never resolves to whichever was listed last.
        self.datasets_by_workspace_and_name: MutableMapping[str, PowerBIDataset] = (
            FileBackedDict(
                shared_connection=self._file_backed_conn,
                tablename="datasets_by_workspace_and_name",
            )
        )
        self.ambiguous_remote_model_keys: Set[str] = set()

    def _new_fabric_resolver(
        self,
        config: PowerBiDashboardSourceConfig,
        reporter: PowerBiDashboardSourceReport,
    ) -> Optional[FabricDefinitionResolver]:
        resolver = FabricDefinitionResolver(
            msal_client=self.__regular_api_resolver.msal_client,
            request_session=self.__regular_api_resolver.request_session,
            lro_timeout_seconds=config.scan_timeout,
        )
        if config.metadata_api_mode == PowerBiMetadataApiMode.WORKSPACE_APIS:
            # Fail fast, like the Power BI token in DataResolverBase: without
            # a Fabric token this mode cannot ingest a single semantic model.
            resolver.get_access_token()
            return resolver
        # enrich_from_item_definitions: the scan still provides every entity,
        # so a missing Fabric token only loses the enrichment.
        try:
            resolver.get_access_token()
        except Exception as e:
            reporter.warning(
                title=Constant.FABRIC_TOKEN_NOT_ACQUIRED,
                message=(
                    "No Fabric API token could be acquired; item definitions are "
                    "not read and enrich_from_item_definitions has no effect this "
                    "run. Check the tenant setting 'Service principals can use "
                    "Fabric APIs'."
                ),
                exc=e,
            )
            return None
        return resolver

    def close(self) -> None:
        self._file_backed_conn.close()

    def _index_datasets_by_name(self, workspace: Workspace) -> None:
        for dataset in workspace.datasets.values():
            if not dataset.name:
                continue
            key = remote_model_key(workspace.name, dataset.name)
            if key in self.ambiguous_remote_model_keys:
                continue
            existing = self.datasets_by_workspace_and_name.get(key)
            if existing is not None and registry_key(existing.id) != registry_key(
                dataset.id
            ):
                del self.datasets_by_workspace_and_name[key]
                self.ambiguous_remote_model_keys.add(key)
                continue
            self.datasets_by_workspace_and_name[key] = dataset

    def find_remote_model(self, reference: RemoteModelReference) -> RemoteModelMatch:
        """The ingested dataset a composite model's connection points at. A
        dataset id in the connection is authoritative: when it is not in the
        registry the name is not tried, because a model of the same name
        could be a different (recreated) dataset."""
        if reference.dataset_id:
            return RemoteModelMatch(
                dataset=self.dataset_registry.get(registry_key(reference.dataset_id))
            )
        key = remote_model_key(reference.workspace_name, reference.model_name)
        if key in self.ambiguous_remote_model_keys:
            return RemoteModelMatch(ambiguous=True)
        return RemoteModelMatch(dataset=self.datasets_by_workspace_and_name.get(key))

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
                report.dataset = self.dataset_registry.get(registry_key(bound_from_rdl))
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
            report.dataset = self.dataset_registry.get(registry_key(bound_dataset_id))
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
                report.dataset = self.dataset_registry.get(
                    registry_key(report.dataset_id)
                )
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
                "Fabric resolver is only created for metadata_api_mode: workspace_apis "
                "or enrich_from_item_definitions: true"
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
        context = f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}"
        try:
            refreshes = self.__regular_api_resolver.get_dataset_refreshes(
                workspace_id=workspace.id, dataset_id=dataset.id
            )
        except Exception as e:
            # System datasets (e.g. the workspace usage-metrics model) cannot
            # be refreshed and answer 415 to the refresh-history call.
            if _http_status(e) == HTTPStatus.UNSUPPORTED_MEDIA_TYPE:
                self.reporter.datasets_not_refreshable += 1
                logger.debug(f"Dataset is not refreshable, skipping: {context}")
                return
            self.log_http_error(
                message=f"Unable to fetch refresh history for dataset {dataset.id}"
            )
            self.reporter.warning(
                title="Refresh History Not Fetched",
                message="Dataset refresh history could not be fetched; no Operation aspect will be emitted for its tables. "
                "The refresh schedule is still fetched separately; if that fails too, the tables carry no refreshSchedule properties either.",
                context=context,
                exc=e,
            )
            refreshes = None
        if refreshes is not None:
            self.reporter.refresh_history_fetched += 1
            # Entries are newest first.
            dataset.last_refresh = next(
                (
                    refresh
                    for refresh in refreshes
                    if refresh.status == Constant.REFRESH_STATUS_COMPLETED
                ),
                None,
            )
            dataset.last_failed_refresh = next(
                (
                    refresh
                    for refresh in refreshes
                    if refresh.status == Constant.REFRESH_STATUS_FAILED
                ),
                None,
            )
        self._fill_refresh_schedule(workspace, dataset, context)

    def _fill_refresh_schedule(
        self, workspace: Workspace, dataset: PowerBIDataset, context: str
    ) -> None:
        try:
            dataset.refresh_schedule = (
                self.__regular_api_resolver.get_dataset_refresh_schedule(
                    workspace_id=workspace.id, dataset_id=dataset.id
                )
            )
            self.reporter.refresh_schedule_fetched += 1
        except Exception as e:
            status = _http_status(e)
            # 404 means no schedule is configured: a normal state, not a loss.
            if status == HTTPStatus.NOT_FOUND:
                self.reporter.refresh_schedule_not_configured += 1
                logger.debug(f"No refresh schedule configured: {context}")
                return
            if status == HTTPStatus.BAD_REQUEST:
                error = _http_error_body(e)
                logger.info(f"Refresh schedule returned 400 for {context}: {error}")
                if _is_refresh_schedule_not_supported(error):
                    self.reporter.refresh_schedule_not_supported += 1
                    self.reporter.info(
                        title="Refresh Schedule Not Applicable",
                        message="The dataset kind has no import refresh schedule (DirectQuery, live-connection or push dataset); no refreshSchedule properties are emitted for its tables.",
                        context=context,
                    )
                    return
            self.reporter.refresh_schedule_failed += 1
            self.log_http_error(
                message=f"Unable to fetch refresh schedule for dataset {dataset.id}"
            )
            self.reporter.warning(
                title="Refresh Schedule Not Fetched",
                message="Dataset refresh schedule could not be fetched; its tables will carry no refreshSchedule custom properties.",
                context=context,
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
        if self.__config.enrich_from_item_definitions:
            self._report_enrichment_definition_failure(
                e, context, item_kind, permission_hint
            )
            return
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

    def _report_enrichment_definition_failure(
        self, e: Exception, context: str, item_kind: str, permission_hint: str
    ) -> None:
        """enrich_from_item_definitions: the admin scan already provided the
        entities, so a definition that cannot be read only loses the overlay,
        whatever the cause."""
        if (
            item_kind != FABRIC_KIND_SEMANTIC_MODELS
            and data_resolver.is_permission_error(e)
        ):
            self.reporter.fabric_report_definitions_denied += 1
        self.reporter.warning(
            title=Constant.ITEM_DEFINITION_NOT_FETCHED,
            message=(
                f"getDefinition failed for this {item_kind} item; the scan "
                "metadata is kept but the definition-derived enrichment "
                "(sourceColumn, DAX references, relationships, model properties, "
                f"report field usage) is skipped. {permission_hint} If the "
                "long-running operation timed out, increase scan_timeout."
            ),
            context=context,
            exc=e,
        )

    def _fill_report_field_usage(self, workspace: Workspace, report: Report) -> None:
        if report.type is ReportType.PaginatedReport or not report.pages:
            return
        if self.__fabric_resolver is None:
            # enrich_from_item_definitions without a Fabric token; reported
            # once at startup.
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

        try:
            parsed = parse_report_definition(definition.parts)
        except Exception as e:
            # Enrichment only: a definition the parser cannot handle must not
            # take the report, or the rest of the workspace, with it.
            self.reporter.report_definitions_failed += 1
            self.reporter.warning(
                title="Report Definition Not Parsed",
                message="The report definition could not be parsed; the report keeps its pages but has no visual field usage.",
                context=context,
                exc=e,
            )
            return
        self.reporter.report_definitions_parsed += 1
        failed_parts = definition.failed_parts + parsed.failed_parts
        if failed_parts:
            self.reporter.report_definition_parts_failed += len(failed_parts)
            self.reporter.warning(
                title="Report Definition Part Not Parsed",
                message="Some parts of the report definition were not valid JSON; the visuals they describe are missing from the page's inputFields.",
                context=f"{context}, parts={failed_parts}",
            )
        if parsed.failed_filters:
            self.reporter.report_filters_failed += len(parsed.failed_filters)
            self.reporter.warning(
                title="Report Filter Not Parsed",
                message="Some report or page filters in the report definition could not be read; they are missing from the `filters` properties and the page's inputFields.",
                context=f"{context}, filters={parsed.failed_filters}",
            )
        report.filters = parsed.filters
        self.reporter.report_filters_parsed += len(parsed.filters)
        # A report-level filter applies to every page, so its target field is
        # part of each page's lineage too.
        report_filter_targets = filter_targets(parsed.filters)
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
            page.filters = parsed_page.filters
            self.reporter.report_filters_parsed += len(parsed_page.filters)
            page.field_references = parsed_page.field_refs + [
                ref
                for ref in report_filter_targets
                if ref not in parsed_page.field_refs
            ]

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

    @staticmethod
    def _entity_partition_source(
        table_name: str,
        partition: ParsedPartition,
        analysis_services: Dict[str, Optional[RemoteModelReference]],
    ) -> EntityPartitionSource:
        """An `entity` partition is DirectLake over a Fabric SQL endpoint
        unless the shared expression it names is an AnalysisServices call:
        then it is a DirectQuery table of a Power BI semantic model (linked
        with extract_composite_model_lineage) or of some other Analysis
        Services server (never linked)."""
        if partition.expression_source in analysis_services:
            return EntityPartitionSource(
                storage_mode=Constant.DIRECT_QUERY,
                remote_model=analysis_services[partition.expression_source],
                remote_table_name=partition.entity_name or table_name,
            )
        return EntityPartitionSource(
            storage_mode=Constant.DIRECT_LAKE,
            source_schema=partition.schema_name,
            source_expression=partition.entity_name,
        )

    def _table_from_parsed(
        self,
        workspace: Workspace,
        dataset: PowerBIDataset,
        dataset_name: str,
        parsed_table: ParsedTable,
        analysis_services: Dict[str, Optional[RemoteModelReference]],
    ) -> Table:
        partition = _partition_fields(
            parsed_table.partition, parsed_table.name, analysis_services
        )
        columns = [self._column_from_parsed(column) for column in parsed_table.columns]
        measures = [
            self._measure_from_parsed(measure) for measure in parsed_table.measures
        ]
        return Table(
            name=parsed_table.name,
            full_name=form_full_table_name(
                config=self.__config,
                workspace=workspace,
                dataset_name=dataset_name,
                table_name=parsed_table.name,
            ),
            expression=partition.expression,
            columns=columns,
            measures=measures,
            dataset=dataset,
            row_count=None,
            column_count=None,
            storage_mode=partition.storage_mode,
            source_schema=partition.source_schema,
            source_expression=partition.source_expression,
            calculated_expression=partition.calculated_expression,
            description=parsed_table.description,
            partition_count=parsed_table.partition_count,
            refresh_policy=parsed_table.refresh_policy,
            remote_model=partition.remote_model,
            remote_table_name=partition.remote_table_name,
        )

    @staticmethod
    def _column_from_parsed(column: ParsedColumn) -> Column:
        data_type = column.data_type or Constant.NULL_DATA_TYPE
        return Column(
            name=column.name,
            dataType=data_type,
            isHidden=column.is_hidden,
            datahubDataType=FIELD_TYPE_MAPPING.get(
                data_type, FIELD_TYPE_MAPPING[Constant.NULL_DATA_TYPE]
            ),
            columnType=Constant.COLUMN_TYPE_CALCULATED
            if column.is_calculated
            else Constant.COLUMN_TYPE_DATA,
            expression=column.expression,
            description=column.description,
            source_column=column.source_column,
            references=column.references,
            has_row_context_iterators=column.has_row_context_iterators,
            model_properties=column.model_properties,
        )

    @staticmethod
    def _measure_from_parsed(measure: ParsedMeasure) -> Measure:
        return Measure(
            name=measure.name,
            expression=measure.expression,
            isHidden=measure.is_hidden,
            description=measure.description,
            references=measure.references,
            has_row_context_iterators=measure.has_row_context_iterators,
            model_properties=measure.model_properties,
        )

    def _apply_model_properties(
        self, parsed_model: ParsedModel, dataset: PowerBIDataset, context: str
    ) -> None:
        """extract_model_properties: attach RLS filters to the tables they
        apply to and count what the mapper will emit. Role members are never
        read from the definition."""
        tables_by_name = {table.name: table for table in dataset.tables}
        for role in parsed_model.roles:
            for table_name, dax_filter in role.table_permissions.items():
                table = tables_by_name.get(table_name)
                if table is None:
                    self.reporter.rls_filters_unresolved += 1
                    self.reporter.warning(
                        title=Constant.RLS_FILTER_TABLE_NOT_FOUND,
                        message="A row-level-security role filters a table that is not in the parsed model; that filter is not emitted.",
                        context=f"{context}, role={role.name}, table={table_name}",
                    )
                    continue
                table.rls_filters.append(
                    RlsTableFilter(role=role.name, filter=dax_filter)
                )
        self.reporter.rls_roles_parsed += len(parsed_model.roles)
        if parsed_model.failed_roles:
            self.reporter.tmdl_roles_failed += len(parsed_model.failed_roles)
            self.reporter.warning(
                title=Constant.TMDL_ROLE_NOT_PARSED,
                message="Some role parts of the semantic-model definition could not be parsed; row-level-security roles from them are not emitted.",
                context=f"{context}, parts={parsed_model.failed_roles}",
            )
        self.reporter.dataset_parameters_emitted += len(dataset.parameters)
        for parsed_table in parsed_model.tables:
            if parsed_table.name not in tables_by_name:
                # A definition-only table is not emitted (admin-scan overlay).
                continue
            fields: List[Union[ParsedColumn, ParsedMeasure]] = [
                *parsed_table.columns,
                *parsed_table.measures,
            ]
            self.reporter.model_fields_with_properties += sum(
                1 for field in fields if _field_has_properties(field)
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

    def _fetch_semantic_model_definition(
        self, workspace: Workspace, dataset: PowerBIDataset, context: str
    ) -> Optional[ItemDefinition]:
        try:
            definition = self._require_fabric_resolver().get_item_definition(
                workspace_id=workspace.id,
                item_id=dataset.id,
                item_kind=FABRIC_KIND_SEMANTIC_MODELS,
                definition_format=FABRIC_FORMAT_TMDL,
            )
        except Exception as e:
            self._report_definition_failure(e, context, FABRIC_KIND_SEMANTIC_MODELS)
            return None
        self.reporter.fabric_definitions_fetched += 1
        return definition

    def _parse_semantic_model_definition(
        self, definition: ItemDefinition, context: str
    ) -> ParsedModel:
        parsed_model = parse_semantic_model(definition.parts)
        failed_parts = definition.failed_parts + parsed_model.failed_parts
        if parsed_model.incomplete_parts:
            # The tables themselves are emitted; only some of their columns,
            # measures or partition sources are missing, so this is a warning.
            self.reporter.tmdl_parts_incomplete += len(parsed_model.incomplete_parts)
            self.reporter.warning(
                title="TMDL Object Not Parsed",
                message="Some column, measure or partition lines in table parts of the semantic-model definition could not be parsed; those objects (or the table's lineage) are missing.",
                context=f"{context}, parts={parsed_model.incomplete_parts}",
            )
        if not failed_parts:
            return parsed_model
        self.reporter.tmdl_parts_failed += len(failed_parts)
        failed_tables = [
            part for part in failed_parts if part.startswith(TMDL_TABLES_PREFIX)
        ]
        if not failed_tables:
            self.reporter.warning(
                title=Constant.TMDL_PART_NOT_PARSED,
                message="Some non-table parts of the semantic-model definition could not be decoded or parsed; relationships or DirectLake lineage from them are missing.",
                context=f"{context}, parts={failed_parts}",
            )
        elif self.__config.enrich_from_item_definitions:
            self.reporter.warning(
                title=Constant.TMDL_TABLE_NOT_PARSED,
                message="Some table parts of the semantic-model definition could not be decoded or parsed; those tables keep their scan metadata without enrichment.",
                context=f"{context}, parts={failed_tables}",
            )
        else:
            # A table part that did not parse is a table missing from this
            # run; under stale-entity removal a green run would soft-delete
            # it, so this is a failure like a refused definition.
            self.reporter.failure(
                title=Constant.TMDL_TABLE_NOT_PARSED,
                message="Some table parts of the semantic-model definition could not be decoded or parsed; those tables are missing from this run.",
                context=f"{context}, parts={failed_tables}",
            )
        return parsed_model

    def _apply_relationships(
        self, parsed_model: ParsedModel, dataset: PowerBIDataset, context: str
    ) -> None:
        """extract_model_relationships: keep the model's relationships on the
        dataset; the mapper turns them into foreign keys."""
        if not self.__config.extract_model_relationships:
            return
        dataset.relationships = parsed_model.relationships
        self.reporter.relationships_parsed += len(dataset.relationships)
        if parsed_model.skipped_relationships:
            self.reporter.relationships_skipped += len(
                parsed_model.skipped_relationships
            )
            self.reporter.warning(
                title=Constant.RELATIONSHIP_NOT_PARSED,
                message="Some semantic-model relationships had unparseable fromColumn/toColumn references and are not emitted as foreign keys.",
                context=f"{context}, relationships={parsed_model.skipped_relationships}",
            )

    def _apply_definition_enrichment(
        self, parsed_model: ParsedModel, dataset: PowerBIDataset, context: str
    ) -> None:
        """The definition-derived steps both metadata API modes share once the
        dataset's tables exist: DirectLake endpoint, model properties and
        relationships. The scan's dependent artifact (when it had one) wins."""
        if dataset.dependent_on_artifact_id is None:
            dataset.dependent_on_artifact_id = self._directlake_sql_endpoint_id(
                parsed_model
            )
        if self.__config.extract_model_properties:
            self._apply_model_properties(parsed_model, dataset, context)
        self._apply_relationships(parsed_model, dataset, context)

    def _fill_dataset_from_definition(
        self, workspace: Workspace, dataset: PowerBIDataset
    ) -> None:
        context = f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}"
        definition = self._fetch_semantic_model_definition(workspace, dataset, context)
        if definition is None:
            return
        parsed_model = self._parse_semantic_model_definition(definition, context)
        dataset_name = dataset.name if dataset.name is not None else dataset.id
        analysis_services = analysis_services_targets(
            {
                name: shared.expression
                for name, shared in parsed_model.shared_expressions.items()
            }
        )
        for parsed_table in parsed_model.tables:
            table = self._table_from_parsed(
                workspace, dataset, dataset_name, parsed_table, analysis_services
            )
            if self.__config.profiling.enabled:
                self.__regular_api_resolver.profile_dataset(
                    dataset, table, workspace.name, self.__config.profile_pattern
                )
            dataset.tables.append(table)
            self.reporter.tmdl_tables_parsed += 1
        self._apply_definition_enrichment(parsed_model, dataset, context)

    def _enrich_workspace_from_definitions(self, workspace: Workspace) -> None:
        """enrich_from_item_definitions: overlay each scanned model's TMDL
        definition onto the tables the scan built. Nothing here can lose an
        entity, so every problem is a warning."""
        if self.__fabric_resolver is None:
            # No Fabric token; reported once at startup.
            self.reporter.scan_datasets_not_enriched += len(workspace.datasets)
            return
        for dataset in workspace.datasets.values():
            context = (
                f"workspace={workspace.name}, dataset={dataset.name} id={dataset.id}"
            )
            try:
                self._enrich_dataset_from_definition(workspace, dataset, context)
            except Exception as e:
                self.reporter.scan_datasets_not_enriched += 1
                self.reporter.warning(
                    title=Constant.SEMANTIC_MODEL_NOT_ENRICHED,
                    message="Unexpected error while overlaying this semantic model's definition; its scan metadata is kept without enrichment.",
                    context=context,
                    exc=e,
                )

    def _enrich_dataset_from_definition(
        self, workspace: Workspace, dataset: PowerBIDataset, context: str
    ) -> None:
        definition = self._fetch_semantic_model_definition(workspace, dataset, context)
        if definition is None:
            self.reporter.scan_datasets_not_enriched += 1
            return
        parsed_model = self._parse_semantic_model_definition(definition, context)
        self._overlay_tables(parsed_model, dataset, context)
        self._apply_definition_enrichment(parsed_model, dataset, context)
        self.reporter.scan_datasets_enriched += 1

    def _overlay_tables(
        self, parsed_model: ParsedModel, dataset: PowerBIDataset, context: str
    ) -> None:
        """The scan is the source of truth for the table set: tables are
        matched by name, never added or removed; both differences are
        counted and reported."""
        parsed_by_name = {table.name: table for table in parsed_model.tables}
        scan_by_name = {table.name: table for table in dataset.tables}
        analysis_services = analysis_services_targets(
            {
                name: shared.expression
                for name, shared in parsed_model.shared_expressions.items()
            }
        )
        for name, table in scan_by_name.items():
            parsed_table = parsed_by_name.get(name)
            if parsed_table is not None:
                self._overlay_table(table, parsed_table, analysis_services, context)
        scan_only = sorted(set(scan_by_name) - set(parsed_by_name))
        if scan_only:
            self.reporter.scan_tables_not_in_definition += len(scan_only)
            self.reporter.warning(
                title=Constant.SCAN_TABLE_NOT_IN_DEFINITION,
                message="Some scanned tables are not in the semantic-model definition; they keep their scan metadata without enrichment.",
                context=f"{context}, tables={scan_only}",
            )
        definition_only = sorted(set(parsed_by_name) - set(scan_by_name))
        if definition_only:
            self.reporter.definition_tables_not_in_scan += len(definition_only)
            self.reporter.warning(
                title=Constant.DEFINITION_TABLE_NOT_IN_SCAN,
                message="Some tables of the semantic-model definition are not in the admin scan; the scan is the source of truth, so they are not added.",
                context=f"{context}, tables={definition_only}",
            )

    def _overlay_table(
        self,
        table: Table,
        parsed_table: ParsedTable,
        analysis_services: Dict[str, Optional[RemoteModelReference]],
        context: str,
    ) -> None:
        """TMDL enrichment on a scan-built table. The scan keeps its name,
        field set and every value it carries (M expression, data types,
        descriptions, isHidden); the definition supplies what the scan does
        not model and fills what the scan left empty. Fields are matched by
        name; fields on one side only are counted and reported."""
        if not table.description:
            table.description = parsed_table.description
        table.partition_count = parsed_table.partition_count
        table.refresh_policy = parsed_table.refresh_policy
        partition = _partition_fields(
            parsed_table.partition, parsed_table.name, analysis_services
        )
        table.calculated_expression = partition.calculated_expression
        if table.expression is None:
            table.expression = partition.expression
        if table.source_schema is None:
            table.source_schema = partition.source_schema
        if table.source_expression is None:
            table.source_expression = partition.source_expression
        if table.storage_mode is None:
            table.storage_mode = partition.storage_mode
        # The scan does not model composite-model connections; the definition
        # is the only source for them (extract_composite_model_lineage).
        table.remote_model = partition.remote_model
        table.remote_table_name = partition.remote_table_name
        columns_by_name = {column.name: column for column in table.columns or []}
        measures_by_name = {measure.name: measure for measure in table.measures or []}
        matched: Set[str] = set()
        for parsed_column in parsed_table.columns:
            column = columns_by_name.get(parsed_column.name)
            if column is not None:
                matched.add(parsed_column.name)
                self._overlay_column(column, parsed_column)
        for parsed_measure in parsed_table.measures:
            measure = measures_by_name.get(parsed_measure.name)
            if measure is not None:
                matched.add(parsed_measure.name)
                self._overlay_measure(measure, parsed_measure)
        definition_names = {column.name for column in parsed_table.columns} | {
            measure.name for measure in parsed_table.measures
        }
        self._report_field_mismatches(
            scan_only=sorted((set(columns_by_name) | set(measures_by_name)) - matched),
            definition_only=sorted(definition_names - matched),
            context=f"{context}, table={table.name}",
        )

    def _report_field_mismatches(
        self, scan_only: List[str], definition_only: List[str], context: str
    ) -> None:
        if scan_only:
            self.reporter.scan_fields_not_in_definition += len(scan_only)
            self.reporter.warning(
                title=Constant.SCAN_FIELD_NOT_IN_DEFINITION,
                message="Some scanned columns or measures have no field of that name in the semantic-model definition (renamed, or dropped from the model); they keep their scan metadata without enrichment.",
                context=f"{context}, fields={scan_only}",
            )
        if definition_only:
            self.reporter.definition_fields_not_in_scan += len(definition_only)
            self.reporter.warning(
                title=Constant.DEFINITION_FIELD_NOT_IN_SCAN,
                message="Some columns or measures of the semantic-model definition are not in the admin scan's table; the scan is the source of truth, so they are not added.",
                context=f"{context}, fields={definition_only}",
            )

    @staticmethod
    def _overlay_column(column: Column, parsed: ParsedColumn) -> None:
        column.source_column = parsed.source_column
        column.references = parsed.references
        column.has_row_context_iterators = parsed.has_row_context_iterators
        if parsed.is_calculated:
            column.columnType = Constant.COLUMN_TYPE_CALCULATED
            if column.expression is None:
                column.expression = parsed.expression
        if not column.description:
            column.description = parsed.description
        column.model_properties = parsed.model_properties

    @staticmethod
    def _overlay_measure(measure: Measure, parsed: ParsedMeasure) -> None:
        measure.references = parsed.references
        if not measure.description:
            measure.description = parsed.description
        measure.model_properties = parsed.model_properties

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
        self.dataset_registry.update(
            {
                registry_key(dataset_id): dataset
                for dataset_id, dataset in workspace.datasets.items()
            }
        )
        self._index_datasets_by_name(workspace)

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
            not self.reporter.fabric_definitions_failed
            or self.reporter.fabric_definitions_fetched
        ):
            return
        permission_hint = (
            "Check the Contributor role, the 'Service principals can use Fabric "
            "APIs' tenant setting and scan_timeout."
        )
        context = f"failed={self.reporter.fabric_definitions_failed}"
        if self.__config.enrich_from_item_definitions:
            self.reporter.warning(
                title=Constant.NO_ITEM_DEFINITIONS_FETCHED,
                message=f"Every Fabric getDefinition call failed; the scan metadata is ingested without definition-derived enrichment. {permission_hint}",
                context=context,
            )
            return
        self.reporter.failure(
            title=Constant.NO_ITEM_DEFINITIONS_FETCHED,
            message=f"Every Fabric getDefinition call failed; no semantic-model schema or lineage has been ingested. {permission_hint}",
            context=context,
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
            if self.__config.enrich_from_item_definitions:
                self._enrich_workspace_from_definitions(cur_workspace)
                # Composite models name their source by workspace and model
                # name; the index lets extract_composite_model_lineage resolve
                # them in admin_scan mode too.
                self._index_datasets_by_name(cur_workspace)
            # collect all datasets in the registry
            self.dataset_registry.update(
                {
                    registry_key(dataset_id): dataset
                    for dataset_id, dataset in cur_workspace.datasets.items()
                }
            )

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
                        tile.dataset = self.dataset_registry.get(
                            registry_key(tile.dataset_id)
                        )
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
