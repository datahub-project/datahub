import json
from functools import lru_cache
from typing import Dict, Iterable, Optional

import dateutil.parser as dp
import requests
from pydantic.class_validators import validator
from pydantic.fields import Field

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.sql import sql_common
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
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
)
from datahub.utilities import config_clean

PAGE_SIZE = 25


chart_type_from_viz_type = {
    "line": ChartTypeClass.LINE,
    "big_number": ChartTypeClass.LINE,
    "table": ChartTypeClass.TABLE,
    "dist_bar": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "bar": ChartTypeClass.BAR,
    "pie": ChartTypeClass.PIE,
    "histogram": ChartTypeClass.HISTOGRAM,
    "big_number_total": ChartTypeClass.LINE,
    "dual_line": ChartTypeClass.LINE,
    "line_multi": ChartTypeClass.LINE,
    "treemap": ChartTypeClass.AREA,
    "box_plot": ChartTypeClass.BAR,
}


class SupersetConfig(ConfigModel):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = Field(default="localhost:8088", description="Superset host URL.")
    username: Optional[str] = Field(default=None, description="Superset username.")
    password: Optional[str] = Field(default=None, description="Superset password.")
    provider: str = Field(default="db", description="Superset provider.")
    options: Dict = Field(default={}, description="")
    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs",
    )
    database_alias: Dict[str, str] = Field(
        default={},
        description="Can be used to change mapping for database names in superset to what you have in datahub",
    )

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


def get_metric_name(metric):
    if not metric:
        return ""
    if isinstance(metric, str):
        return metric
    label = metric.get("label")
    if not label:
        return ""
    return label


def get_filter_name(filter_obj):
    sql_expression = filter_obj.get("sqlExpression")
    if sql_expression:
        return sql_expression

    clause = filter_obj.get("clause")
    column = filter_obj.get("subject")
    operator = filter_obj.get("operator")
    comparator = filter_obj.get("comparator")
    return f"{clause} {column} {operator} {comparator}"


@platform_name("Superset")
@config_class(SupersetConfig)
@support_status(SupportStatus.CERTIFIED)
class SupersetSource(Source):
    """
    This plugin extracts the following:
    - Charts, dashboards, and associated metadata

    See documentation for superset's /security/login at https://superset.apache.org/docs/rest-api for more details on superset's login api.
    """

    config: SupersetConfig
    report: SourceReport
    platform = "superset"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        login_response = requests.post(
            f"{self.config.connect_uri}/api/v1/security/login",
            None,
            {
                "username": self.config.username,
                "password": self.config.password,
                "refresh": True,
                "provider": self.config.provider,
            },
        )

        self.access_token = login_response.json()["access_token"]

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}/api/v1/database")
        if test_response.status_code == 200:
            pass
            # TODO(Gabe): how should we message about this error?

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = SupersetConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @lru_cache(maxsize=None)
    def get_platform_from_database_id(self, database_id):
        database_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/database/{database_id}"
        ).json()
        sqlalchemy_uri = database_response.get("result", {}).get("sqlalchemy_uri")
        return sql_common.get_platform_from_sqlalchemy_uri(sqlalchemy_uri)

    @lru_cache(maxsize=None)
    def get_datasource_urn_from_id(self, datasource_id):
        dataset_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/dataset/{datasource_id}"
        ).json()
        schema_name = dataset_response.get("result", {}).get("schema")
        table_name = dataset_response.get("result", {}).get("table_name")
        database_id = dataset_response.get("result", {}).get("database", {}).get("id")
        database_name = (
            dataset_response.get("result", {}).get("database", {}).get("database_name")
        )
        database_name = self.config.database_alias.get(database_name, database_name)

        if database_id and table_name:
            platform = self.get_platform_from_database_id(database_id)
            platform_urn = f"urn:li:dataPlatform:{platform}"
            dataset_urn = (
                f"urn:li:dataset:("
                f"{platform_urn},{database_name + '.' if database_name else ''}"
                f"{schema_name + '.' if schema_name else ''}"
                f"{table_name},{self.config.env})"
            )
            return dataset_urn
        return None

    def construct_dashboard_from_api_data(self, dashboard_data):
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_data['id']})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{(dashboard_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(dashboard_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        title = dashboard_data.get("dashboard_title", "")
        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        dashboard_url = f"{self.config.connect_uri}{dashboard_data.get('url', '')}"

        chart_urns = []
        raw_position_data = dashboard_data.get("position_json", "{}")
        position_data = (
            json.loads(raw_position_data) if raw_position_data is not None else {}
        )
        for key, value in position_data.items():
            if not key.startswith("CHART-"):
                continue
            chart_urns.append(
                f"urn:li:chart:({self.platform},{value.get('meta', {}).get('chartId', 'unknown')})"
            )

        dashboard_info = DashboardInfoClass(
            description="",
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info)
        return dashboard_snapshot

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        current_dashboard_page = 0
        # we will set total dashboards to the actual number after we get the response
        total_dashboards = PAGE_SIZE

        while current_dashboard_page * PAGE_SIZE <= total_dashboards:
            dashboard_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/dashboard",
                params=f"q=(page:{current_dashboard_page},page_size:{PAGE_SIZE})",
            )
            payload = dashboard_response.json()
            total_dashboards = payload.get("count") or 0

            current_dashboard_page += 1

            payload = dashboard_response.json()
            for dashboard_data in payload["result"]:
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_data
                )
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def construct_chart_from_chart_data(self, chart_data):
        chart_urn = f"urn:li:chart:({self.platform},{chart_data['id']})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{(chart_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(chart_data.get("changed_on_utc", "now")).timestamp() * 1000
        )
        title = chart_data.get("slice_name", "")

        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        chart_type = chart_type_from_viz_type.get(chart_data.get("viz_type", ""))
        chart_url = f"{self.config.connect_uri}{chart_data.get('url', '')}"

        datasource_id = chart_data.get("datasource_id")
        datasource_urn = self.get_datasource_urn_from_id(datasource_id)

        params = json.loads(chart_data.get("params"))
        metrics = [
            get_metric_name(metric)
            for metric in (params.get("metrics", []) or [params.get("metric")])
        ]
        filters = [
            get_filter_name(filter_obj)
            for filter_obj in params.get("adhoc_filters", [])
        ]
        group_bys = params.get("groupby", []) or []
        if isinstance(group_bys, str):
            group_bys = [group_bys]

        custom_properties = {
            "Metrics": ", ".join(metrics),
            "Filters": ", ".join(filters),
            "Dimensions": ", ".join(group_bys),
        }

        chart_info = ChartInfoClass(
            type=chart_type,
            description="",
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=[datasource_urn] if datasource_urn else None,
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)
        return chart_snapshot

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        current_chart_page = 0
        # we will set total charts to the actual number after we get the response
        total_charts = PAGE_SIZE

        while current_chart_page * PAGE_SIZE <= total_charts:
            chart_response = self.session.get(
                f"{self.config.connect_uri}/api/v1/chart",
                params=f"q=(page:{current_chart_page},page_size:{PAGE_SIZE})",
            )
            current_chart_page += 1

            payload = chart_response.json()
            total_charts = payload["count"]
            for chart_data in payload["result"]:
                chart_snapshot = self.construct_chart_from_chart_data(chart_data)

                mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report
