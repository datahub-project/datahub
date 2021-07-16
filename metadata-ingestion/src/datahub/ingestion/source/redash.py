import json
from functools import lru_cache
from typing import Iterable, Optional

import dateutil.parser as dp

import pprint as pp
from redash_toolbelt import Redash

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
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
    ChartInfoClass,
    ChartTypeClass,
    DashboardInfoClass,
)

PAGE_SIZE = 25


def get_platform_from_sqlalchemy_uri(sqlalchemy_uri: str) -> str:
    if sqlalchemy_uri.startswith("bigquery"):
        return "bigquery"
    if sqlalchemy_uri.startswith("druid"):
        return "druid"
    if sqlalchemy_uri.startswith("mssql"):
        return "mssql"
    if (
        sqlalchemy_uri.startswith("jdbc:postgres:")
        and sqlalchemy_uri.index("redshift.amazonaws") > 0
    ):
        return "redshift"
    if sqlalchemy_uri.startswith("snowflake"):
        return "snowflake"
    if sqlalchemy_uri.startswith("presto"):
        return "presto"
    if sqlalchemy_uri.startswith("postgresql"):
        return "postgres"
    if sqlalchemy_uri.startswith("pinot"):
        return "pinot"
    if sqlalchemy_uri.startswith("oracle"):
        return "oracle"
    if sqlalchemy_uri.startswith("mysql"):
        return "mysql"
    if sqlalchemy_uri.startswith("mongodb"):
        return "mongodb"
    if sqlalchemy_uri.startswith("hive"):
        return "hive"
    return "external"


chart_type_from_viz_type = {
    "BOXPLOT": ChartTypeClass.BAR,
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
}


class RedashConfig(ConfigModel):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = "http://localhost:5000"
    api_key: str = "REDASH_API_KEY"
    # options: dict = {}
    env: str = DEFAULT_ENV


class RedashSource(Source):
    config: RedashConfig
    report: SourceReport
    platform = "redash"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: RedashConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

    #     # Test the connection
    #     test_response = self.session.get(f"{self.config.connect_uri}/api/v1/database")
    #     if test_response.status_code == 200:
    #         pass
    #         # TODO(Gabe): how should we message about this error?

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = RedashConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @lru_cache(maxsize=None)
    def get_platform_from_database_id(self, database_id):
        database_response = self.session.get(
            f"{self.config.connect_uri}/api/v1/database/{database_id}"
        ).json()
        sqlalchemy_uri = database_response.get("result", {}).get("sqlalchemy_uri")
        return get_platform_from_sqlalchemy_uri(sqlalchemy_uri)

    @lru_cache(maxsize=None)
    def get_datasource_urn_from_id(self, datasource_id):
        dataset_response = self.client._get(f"/api/data_sources/{datasource_id}").json()
        return None

    def construct_dashboard_from_api_data(self, dashboard_data):
        dashboard_id = dashboard_data['id']
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_id})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        description = ""

        modified_actor = f"urn:li:corpuser:{(dashboard_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(dashboard_data.get("updated_at", "now")).timestamp() * 1000
        )
        title = dashboard_data.get("name", "")
        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        dashboard_url = f"{self.config.connect_uri}/dashboard/{dashboard_data.get('slug', '')}"

        chart_urns = []

        widgets = dashboard_data.get("widgets", [])
        for widget in widgets:

            # In Redash, chart is called visualization
            visualization = widget.get("visualization")
            if not visualization:
                options = widget.get("options")
                text = widget.get("text")
                isHidden = widget.get("isHidden")

                # If top most widget is a Textbox, then we assume it is the Description
                if options and text and not isHidden:
                    position = options.get("position")
                    if position:
                        col = position.get("col")
                        row = position.get("row")
                        if col == 0 and row == 0:
                            description = text
                else:
                    continue
            else:
                visualization_id = visualization.get("id", "unknown")
                chart_urns.append(
                    f"urn:li:chart:({self.platform},{visualization_id})"
                )

        dashboard_info = DashboardInfoClass(
            description=f"URL:\t{dashboard_url}\n{description}",
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={
                "dashboardUrl": dashboard_url,
            },
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        return dashboard_snapshot

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        current_dashboards_page = 1

        # we will set total dashboards to the actual number after we get the response
        total_dashboards = PAGE_SIZE

        while current_dashboards_page * PAGE_SIZE <= total_dashboards:
        # while current_dashboards_page * PAGE_SIZE <= total_dashboards and current_dashboards_page < 2:
            dashboards_response = self.client.dashboards(page=current_dashboards_page, page_size=PAGE_SIZE)

            total_dashboards = dashboards_response.get("count") or 0

            current_dashboards_page += 1

            for dashboard_response in dashboards_response["results"]:
                dashboard_slug = dashboard_response["slug"]
                dashboard_data = self.client.dashboard(dashboard_slug)
                dashboard_snapshot = self.construct_dashboard_from_api_data(
                    dashboard_data
                )
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def construct_chart_from_chart_data(self, query_data, chart_data):
        chart_urn = f"urn:li:chart:({self.platform},{chart_data['id']})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{(chart_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(chart_data.get("updated_at", "now")).timestamp() * 1000
        )
        title = f"{query_data.get('name')} {chart_data.get('name', '')}"

        # note: the API does not currently supply created_by usernames due to a bug, but we are required to
        # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
        # modified actor urn
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )
        chart_type = chart_type_from_viz_type.get(chart_data.get("type", ""))
        print(chart_type)
        chart_url = f"{self.config.connect_uri}/queries/{query_data.get('id')}#{chart_data.get('id', '')}"

        datasource_id = query_data.get("data_source_id")
        # datasource_urn = self.get_datasource_urn_from_id(datasource_id)

        description = chart_data.get("description") if chart_data.get("description") else ""
        description = f"URL:\t[{chart_url}]({chart_url})\n{description}"

        custom_properties = {
            "query": query_data.get("query", "")
        }
        chart_info = ChartInfoClass(
            # type=chart_type,
            type=ChartTypeClass.LINE,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=[builder.make_dataset_urn('mysql', 'dummy_dashboard_dataset')], #TODO use real datasource
            customProperties=custom_properties,
        )
        chart_snapshot.aspects.append(chart_info)

        return chart_snapshot

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        current_queries_page = 1
        # we will set total charts to the actual number after we get the response
        total_queries = PAGE_SIZE

        while current_queries_page * PAGE_SIZE <= total_queries:
        # while current_queries_page * PAGE_SIZE <= total_queries and current_queries_page < 2:
            queries_response = self.client.queries(page=current_queries_page, page_size=PAGE_SIZE)
            current_queries_page += 1

            total_queries = queries_response["count"]
            for query_response in queries_response["results"]:
                query_id = query_response["id"]
                query_data = self.client._get(f"/api/queries/{query_id}").json()

                # In Redash, chart is called vlsualization
                for visualization in query_data.get("visualizations", []):
                    chart_snapshot = self.construct_chart_from_chart_data(query_data, visualization)

                    mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                    wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)

                    yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report
