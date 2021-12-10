from functools import lru_cache
from typing import Dict, Iterable, Optional, Tuple, Union

import dateutil.parser as dp
import requests
from pydantic import validator
from requests.models import HTTPBasicAuth, HTTPError
from sqllineage.runner import LineageRunner

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.common import (
    AuditStamp,
    ChangeAuditStamps,
)
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import (
    ChartSnapshot,
    DashboardSnapshot,
)
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import (
    BrowsePathsClass,
    ChartInfoClass,
    ChartQueryClass,
    ChartQueryTypeClass,
    ChartTypeClass,
    DashboardInfoClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities import config_clean


class ModeConfig(ConfigModel):
    # See https://mode.com/developer/api-reference/authentication/
    # for authentication
    connect_uri: str = "https://app.mode.com"
    token: Optional[str] = None
    password: Optional[str] = None
    workspace: Optional[str] = None
    default_schema: str = "public"
    owner_username_instead_of_email: Optional[bool] = True
    env: str = builder.DEFAULT_ENV

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


class ModeSource(Source):
    config: ModeConfig
    report: SourceReport
    platform = "mode"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: ModeConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self.session = requests.session()
        self.session.auth = HTTPBasicAuth(self.config.token, self.config.password)
        self.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/hal+json",
            }
        )

        # Test the connection
        try:
            test_response = self.session.get(f"{self.config.connect_uri}/api/account")
            test_response.raise_for_status()
        except HTTPError as http_error:
            self.report.report_failure(
                key="mode-session",
                reason=f"Unable to retrieve user "
                f"{self.config.token} information, "
                f"{str(http_error)}",
            )

        self.workspace_uri = (
            f"{self.config.connect_uri}/api/" f"{self.config.workspace}"
        )
        self.space_tokens = self._get_space_name_and_tokens()

    def construct_dashboard(
        self, space_name: str, report_info: dict
    ) -> DashboardSnapshot:
        report_token = report_info.get("token", "")
        dashboard_urn = builder.make_dashboard_urn(
            self.platform, report_info.get("id", "")
        )
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        modified_actor = builder.make_user_urn(
            self._get_creator(
                report_info.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        modified_ts = int(
            dp.parse(f"{report_info.get('last_saved_at', 'now')}").timestamp() * 1000
        )
        created_ts = int(
            dp.parse(f"{report_info.get('created_at', 'now')}").timestamp() * 1000
        )
        title = report_info.get("name", "") or ""
        description = report_info.get("description", "") or ""
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=created_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        dashboard_info_class = DashboardInfoClass(
            description=description,
            title=title,
            charts=self._get_chart_urns(report_token),
            lastModified=last_modified,
            dashboardUrl=f"{self.config.connect_uri}/"
            f"{self.config.workspace}/"
            f"reports/{report_token}",
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info_class)

        # browse path
        browse_path = BrowsePathsClass(
            paths=[
                f"/mode/{self.config.workspace}/"
                f"{space_name}/"
                f"{report_info.get('name')}"
            ]
        )
        dashboard_snapshot.aspects.append(browse_path)

        # Ownership
        ownership = self._get_ownership(
            self._get_creator(
                report_info.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        if ownership is not None:
            dashboard_snapshot.aspects.append(ownership)

        return dashboard_snapshot

    @lru_cache(maxsize=None)
    def _get_ownership(self, user: str) -> Optional[OwnershipClass]:
        owner_urn = builder.make_user_urn(user)
        if owner_urn is not None:
            ownership: OwnershipClass = OwnershipClass(
                owners=[
                    OwnerClass(
                        owner=owner_urn,
                        type=OwnershipTypeClass.DATAOWNER,
                    )
                ]
            )
            return ownership

        return None

    @lru_cache(maxsize=None)
    def _get_creator(self, href: str) -> str:
        user = self.session.get(f"{self.config.connect_uri}{href}")
        user_json = user.json()
        if self.config.owner_username_instead_of_email:
            return user_json.get("username", "unknown")
        else:
            return user_json.get("email", "unknown")

    def _get_chart_urns(self, report_token: str) -> list:
        chart_urns = []
        queries = self._get_queries(report_token)
        for query in queries:
            charts = self._get_charts(report_token, query.get("token", ""))
            # build chart urns
            for chart in charts:
                chart_urn = builder.make_chart_urn(
                    self.platform, chart.get("token", "")
                )
                chart_urns.append(chart_urn)

        return chart_urns

    def _get_space_name_and_tokens(self) -> dict:
        space_info = {}
        try:
            workspace_response = self.session.get(f"{self.workspace_uri}/spaces")
            workspace_response.raise_for_status()
            payload = workspace_response.json()
            spaces = payload.get("_embedded", {}).get("spaces", {})

            for s in spaces:
                space_info[s.get("token", "")] = s.get("name", "")
        except HTTPError as http_error:
            self.report.report_failure(
                key="mode-spaces",
                reason=f"Unable to retrieve spaces/collections for {self.workspace_uri}, "
                f"Reason: {str(http_error)}",
            )

        return space_info

    def _get_chart_type(self, token: str, display_type: str) -> Optional[str]:
        type_mapping = {
            "table": ChartTypeClass.TABLE,
            "bar": ChartTypeClass.BAR,
            "line": ChartTypeClass.LINE,
            "stackedBar100": ChartTypeClass.BAR,
            "stackedBar": ChartTypeClass.BAR,
            "hStackedBar": ChartTypeClass.BAR,
            "hStackedBar100": ChartTypeClass.BAR,
            "hBar": ChartTypeClass.BAR,
            "area": ChartTypeClass.AREA,
            "totalArea": ChartTypeClass.AREA,
            "pie": ChartTypeClass.PIE,
            "donut": ChartTypeClass.PIE,
            "scatter": ChartTypeClass.SCATTER,
            "bigValue": ChartTypeClass.TEXT,
            "pivotTable": ChartTypeClass.TABLE,
            "linePlusBar": None,
        }
        if not display_type:
            self.report.report_warning(
                key=f"mode-chart-{token}",
                reason=f"Chart type {display_type} is missing. " f"Setting to None",
            )
            return None
        try:
            chart_type = type_mapping[display_type]
        except KeyError:
            self.report.report_warning(
                key=f"mode-chart-{token}",
                reason=f"Chart type {display_type} not supported. " f"Setting to None",
            )
            chart_type = None

        return chart_type

    def construct_chart_custom_properties(
        self, chart_detail: dict, chart_type: str
    ) -> Dict:

        custom_properties = {}
        metadata = chart_detail.get("encoding", {})
        if chart_type == "table":
            columns = list(chart_detail.get("fieldFormats", {}).keys())
            str_columns = ",".join([c[1:-1] for c in columns])
            filters = metadata.get("filter", [])
            filters = filters[0].get("formula", "") if len(filters) else ""

            custom_properties = {
                "Columns": str_columns,
                "Filters": filters[1:-1] if len(filters) else "",
            }

        elif chart_type == "pivotTable":
            pivot_table = chart_detail.get("pivotTable", {})
            columns = pivot_table.get("columns", [])
            rows = pivot_table.get("rows", [])
            values = pivot_table.get("values", [])
            filters = pivot_table.get("filters", [])

            custom_properties = {
                "Columns": ", ".join(columns) if len(columns) else "",
                "Rows": ", ".join(rows) if len(rows) else "",
                "Metrics": ", ".join(values) if len(values) else "",
                "Filters": ", ".join(filters) if len(filters) else "",
            }
            # list filters in their own row
            for filter in filters:
                custom_properties[f"Filter: {filter}"] = ", ".join(
                    pivot_table.get("filterValues", {}).get(filter, "")
                )
        # Chart
        else:
            x = metadata.get("x", [])
            x2 = metadata.get("x2", [])
            y = metadata.get("y", [])
            y2 = metadata.get("y2", [])
            value = metadata.get("value", [])
            filters = metadata.get("filter", [])

            custom_properties = {
                "X": x[0].get("formula", "") if len(x) else "",
                "Y": y[0].get("formula", "") if len(y) else "",
                "X2": x2[0].get("formula", "") if len(x2) else "",
                "Y2": y2[0].get("formula", "") if len(y2) else "",
                "Metrics": value[0].get("formula", "") if len(value) else "",
                "Filters": filters[0].get("formula", "") if len(filters) else "",
            }

        return custom_properties

    def _get_datahub_friendly_platform(self, adapter, platform):
        # Map adaptor names to what datahub expects in
        # https://github.com/linkedin/datahub/blob/master/metadata-service/war/src/main/resources/boot/data_platforms.json

        platform_mapping = {
            "jdbc:athena": "athena",
            "jdbc:bigquery": "bigquery",
            "jdbc:druid": "druid",
            "jdbc:hive": "hive",
            "jdbc:mysql": "mysql",
            "jdbc:oracle": "oracle",
            "jdbc:postgresql": "postgres",
            "jdbc:presto": "presto",
            "jdbc:redshift": "redshift",
            "jdbc:snowflake": "snowflake",
            "jdbc:spark": "spark",
            "jdbc:sqlserver": "mssql",
            "jdbc:teradata": "teradata",
        }
        if adapter in platform_mapping:
            return platform_mapping[adapter]
        else:
            self.report.report_warning(
                key=f"mode-platform-{adapter}",
                reason=f"Platform was not found in DataHub. "
                f"Using {platform} name as is",
            )

        return platform

    @lru_cache(maxsize=None)
    def _get_platform_and_dbname(
        self, data_source_id: int
    ) -> Union[Tuple[str, str], Tuple[None, None]]:
        ds_response = self.session.get(f"{self.workspace_uri}/data_sources")
        ds_json = ds_response.json()
        data_sources = ds_json.get("_embedded", {}).get("data_sources", {})
        if not data_sources:
            self.report.report_failure(
                key=f"mode-datasource-{data_source_id}",
                reason=f"No data sources found for datasource id: " f"{data_source_id}",
            )
            return None, None

        for data_source in data_sources:
            if data_source.get("id", -1) == data_source_id:
                platform = self._get_datahub_friendly_platform(
                    data_source.get("adapter", ""), data_source.get("name", "")
                )
                database = data_source.get("database", "")
                return platform, database
        else:
            self.report.report_failure(
                key=f"mode-datasource-{data_source_id}",
                reason=f"Cannot create datasource urn for datasource id: "
                f"{data_source_id}",
            )
        return None, None

    @lru_cache(maxsize=None)
    def _get_source_from_query(self, raw_query: str) -> set:
        parser = LineageRunner(raw_query)
        source_paths = set()
        for table in parser.source_tables:
            source_schema, source_table = str(table).split(".")
            if source_schema == "<default>":
                source_schema = str(self.config.default_schema)

            source_paths.add(f"{source_schema}.{source_table}")

        return source_paths

    def _get_datasource_urn(self, platform, database, source_tables):
        dataset_urn = None
        if platform or database is not None:
            dataset_urn = [
                builder.make_dataset_urn(
                    platform, f"{database}.{s_table}", self.config.env
                )
                for s_table in source_tables
            ]

        return dataset_urn

    def construct_chart_from_api_data(
        self, chart_data: dict, query: dict, path: str
    ) -> ChartSnapshot:
        chart_urn = builder.make_chart_urn(self.platform, chart_data.get("token", ""))
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = builder.make_user_urn(
            self._get_creator(
                chart_data.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        created_ts = int(
            dp.parse(chart_data.get("created_at", "now")).timestamp() * 1000
        )
        modified_ts = int(
            dp.parse(chart_data.get("updated_at", "now")).timestamp() * 1000
        )
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=created_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_detail = (
            chart_data.get("view", {})
            if len(chart_data.get("view", {})) != 0
            else chart_data.get("view_vegas", {})
        )

        mode_chart_type = chart_detail.get("chartType", "") or chart_detail.get(
            "selectedChart", ""
        )
        chart_type = self._get_chart_type(chart_data.get("token", ""), mode_chart_type)
        description = (
            chart_detail.get("description")
            or chart_detail.get("chartDescription")
            or ""
        )
        title = chart_detail.get("title") or chart_detail.get("chartTitle") or ""

        # create datasource urn
        platform, db_name = self._get_platform_and_dbname(query.get("data_source_id"))
        source_tables = self._get_source_from_query(query.get("raw_query"))
        datasource_urn = self._get_datasource_urn(platform, db_name, source_tables)
        custom_properties = self.construct_chart_custom_properties(
            chart_detail, mode_chart_type
        )

        # Chart Info
        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=f"{self.config.connect_uri}"
            f"{chart_data.get('_links', {}).get('report_viz_web', {}).get('href', '')}",
            inputs=datasource_urn,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)

        # Browse Path
        browse_path = BrowsePathsClass(paths=[path])
        chart_snapshot.aspects.append(browse_path)

        # Query
        chart_query = ChartQueryClass(
            rawQuery=query.get("raw_query", ""),
            type=ChartQueryTypeClass.SQL,
        )
        chart_snapshot.aspects.append(chart_query)

        # Ownership
        ownership = self._get_ownership(
            self._get_creator(
                chart_data.get("_links", {}).get("creator", {}).get("href", "")
            )
        )
        if ownership is not None:
            chart_snapshot.aspects.append(ownership)

        return chart_snapshot

    @lru_cache(maxsize=None)
    def _get_reports(self, space_token: str) -> list:
        reports = []
        try:
            reports_response = self.session.get(
                f"{self.workspace_uri}/spaces/{space_token}/reports"
            )
            reports_response.raise_for_status()
            reports_json = reports_response.json()
            reports = reports_json.get("_embedded", {}).get("reports", {})
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"mode-report-{space_token}",
                reason=f"Unable to retrieve reports for space token: {space_token}, "
                f"Reason: {str(http_error)}",
            )
        return reports

    @lru_cache(maxsize=None)
    def _get_queries(self, report_token: str) -> list:
        queries = []
        try:
            queries_response = self.session.get(
                f"{self.workspace_uri}/reports/{report_token}/queries"
            )
            queries_response.raise_for_status()
            queries_json = queries_response.json()
            queries = queries_json.get("_embedded", {}).get("queries", {})
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"mode-query-{report_token}",
                reason=f"Unable to retrieve queries for report token: {report_token}, "
                f"Reason: {str(http_error)}",
            )
        return queries

    @lru_cache(maxsize=None)
    def _get_charts(self, report_token: str, query_token: str) -> list:
        charts = []
        try:
            chart_response = self.session.get(
                f"{self.workspace_uri}/reports/{report_token}"
                f"/queries/{query_token}/charts"
            )
            chart_response.raise_for_status()
            charts_json = chart_response.json()
            charts = charts_json.get("_embedded", {}).get("charts", {})
        except HTTPError as http_error:
            self.report.report_failure(
                key=f"mode-chart-{report_token}-{query_token}",
                reason=f"Unable to retrieve charts: "
                f"Report token: {report_token} "
                f"Query token: {query_token}, "
                f"Reason: {str(http_error)}",
            )
        return charts

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        for space_token, space_name in self.space_tokens.items():
            reports = self._get_reports(space_token)
            for report in reports:
                dashboard_snapshot_from_report = self.construct_dashboard(
                    space_name, report
                )

                mce = MetadataChangeEvent(
                    proposedSnapshot=dashboard_snapshot_from_report
                )
                wu = MetadataWorkUnit(id=dashboard_snapshot_from_report.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        # Space/collection -> report -> query -> Chart
        for space_token, space_name in self.space_tokens.items():
            reports = self._get_reports(space_token)
            for report in reports:
                report_token = report.get("token", "")
                queries = self._get_queries(report_token)
                for query in queries:
                    charts = self._get_charts(report_token, query.get("token", ""))
                    # build charts
                    for chart in charts:
                        view = chart.get("view") or chart.get("view_vegas")
                        chart_name = view.get("title") or view.get("chartTitle") or ""
                        path = (
                            f"/mode/{self.config.workspace}/{space_name}"
                            f"/{report.get('name')}/{query.get('name')}/"
                            f"{chart_name}"
                        )
                        chart_snapshot = self.construct_chart_from_api_data(
                            chart, query, path
                        )
                        mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                        wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                        self.report.report_workunit(wu)

                        yield wu

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = ModeConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report
