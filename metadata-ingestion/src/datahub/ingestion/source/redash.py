import logging
import math
import sys
from dataclasses import dataclass, field
from functools import lru_cache
from typing import Dict, Iterable, List

import dateutil.parser as dp
from redash_toolbelt import Redash

from datahub.configuration.common import AllowDenyPattern, ConfigModel
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

logger = logging.getLogger(__name__)

PAGE_SIZE = 25

# TODO: update Datahub registered platform name. Currently we use external for unmapped platform
# Data source list from REDASH_BASE_URL/api/data_sources/types or https://redash.io/integrations/
DATA_SOURCE_TO_PLATFORM_MAP = {
    "athena": {"name": "Amazon Athena", "platform": "athena"},
    "aws_es": {"name": "Amazon Elasticsearch Service", "platform": "external"},
    "drill": {"name": "Apache Drill", "platform": "external"},
    "axibasetsd": {"name": "Axibase Time Series Database", "platform": "external"},
    "azure_kusto": {"name": "Azure Data Explorer (Kusto)", "platform": "kusto"},
    "bigquery": {
        "name": "BigQuery",
        "platform": "bigquery",
        "database_name_key": "projectId",
    },
    "Cassandra": {"name": "Cassandra", "platform": "external"},
    "clickhouse": {"name": "ClickHouse", "platform": "external"},
    "cockroach": {"name": "CockroachDB", "platform": "external"},
    "couchbase": {"name": "Couchbase", "platform": "couchbase"},
    "db2": {"name": "DB2", "platform": "external"},
    "databricks": {"name": "Databricks", "platform": "external"},
    "dgraph": {"name": "Dgraph", "platform": "external"},
    "druid": {"name": "Druid", "platform": "druid"},
    "dynamodb_sql": {"name": "DynamoDB (with DQL)", "platform": "external"},
    "elasticsearch": {"name": "Elasticsearch", "platform": "external"},
    "google_analytics": {"name": "Google Analytics", "platform": "external"},
    "google_spreadsheets": {"name": "Google Sheets", "platform": "external"},
    "graphite": {"name": "Graphite", "platform": "external"},
    "hive": {"name": "Hive", "platform": "hive"},
    "hive_http": {"name": "Hive (HTTP)", "platform": "hive"},
    "impala": {"name": "Impala", "platform": "external"},
    "influxdb": {"name": "InfluxDB", "platform": "external"},
    "jirajql": {"name": "JIRA (JQL)", "platform": "external"},
    "json": {"name": "JSON", "platform": "external"},
    "kibana": {"name": "Kibana", "platform": "external"},
    "kylin": {"name": "Kylin", "platform": "external"},
    "mapd": {"name": "Mapd", "platform": "external"},
    "mssql": {"name": "Microsoft SQL Server", "platform": "mssql"},
    "mongodb": {"name": "MongoDB", "platform": "mongodb"},
    "mysql": {"name": "MySQL", "platform": "mysql"},
    "rds_mysql": {"name": "MySQL (Amazon RDS)", "platform": "mysql"},
    "phoenix": {"name": "Phoenix", "platform": "external"},
    "pg": {"name": "PostgreSQL", "platform": "postgres"},
    "presto": {"name": "Presto", "platform": "presto"},
    "prometheus": {"name": "Prometheus", "platform": "external"},
    "qubole": {"name": "Qubole", "platform": "external"},
    "results": {"name": "Query Results", "platform": "external"},
    "redshift": {"name": "Redshift", "platform": "redshift"},
    "rockset": {"name": "Rockset", "platform": "external"},
    "salesforce": {"name": "Salesforce", "platform": "external"},
    "scylla": {"name": "ScyllaDB", "platform": "external"},
    "snowflake": {"name": "Snowflake", "platform": "snowflake"},
    "sqlite": {"name": "Sqlite", "platform": "sqlite"},
    "treasuredata": {"name": "TreasureData", "platform": "external"},
    "uptycs": {"name": "Uptycs", "platform": "external"},
    "vertica": {"name": "Vertica", "platform": "vertica"},
    "yandex_appmetrika": {"name": "Yandex AppMetrica", "platform": "external"},
    "yandex_metrika": {"name": "Yandex Metrica", "platform": "external"},
    "external": {"name": "External Source", "platform": "external"},
}


DEFAULT_VISUALIZATION_TYPE = None

# https://github.com/getredash/redash/tree/master/viz-lib/src/visualizations
# TODO: add more mapping on ChartTypeClass
PLOTLY_CHART_MAP = {
    # TODO: add more Plotly visualization mapping here
    # TODO: need to add more ChartTypeClass in datahub schema_classes.py
    "line": ChartTypeClass.LINE,
    "bar": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "pie": ChartTypeClass.PIE,
    "scatter": ChartTypeClass.SCATTER,
    "bubble": DEFAULT_VISUALIZATION_TYPE,
    "heatmap": DEFAULT_VISUALIZATION_TYPE,
    "box": ChartTypeClass.BOX_PLOT,
}

VISUALIZATION_TYPE_MAP = {
    # TODO: add more Redash visualization mapping here
    # TODO: need to add more ChartTypeClass in datahub schema_classes.py
    "BOXPLOT": ChartTypeClass.BOX_PLOT,
    "CHOROPLETH": DEFAULT_VISUALIZATION_TYPE,
    "COUNTER": DEFAULT_VISUALIZATION_TYPE,
    "DETAILS": ChartTypeClass.TABLE,
    "FUNNEL": DEFAULT_VISUALIZATION_TYPE,
    "MAP": DEFAULT_VISUALIZATION_TYPE,
    "PIVOT": DEFAULT_VISUALIZATION_TYPE,
    "SANKEY": DEFAULT_VISUALIZATION_TYPE,
    "SUNBURST_SEQUENCE": DEFAULT_VISUALIZATION_TYPE,
    "TABLE": ChartTypeClass.TABLE,
    "WORD_CLOUD": DEFAULT_VISUALIZATION_TYPE,
}


class RedashConfig(ConfigModel):
    # See the Redash API for details
    # https://redash.io/help/user-guide/integrations-and-api/api
    connect_uri: str = "http://localhost:5000"
    api_key: str = "REDASH_API_KEY"
    env: str = DEFAULT_ENV

    # Optionals
    dashboard_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    chart_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    skip_draft: bool = True
    api_page_limit: int = sys.maxsize
    # parse_table_names_from_sql: bool = False  # TODO: _get_upstream_lineage from SQL


@dataclass
class RedashSourceReport(SourceReport):
    items_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_topic_scanned(self, item: str) -> None:
        self.items_scanned += 1

    def report_dropped(self, item: str) -> None:
        self.filtered.append(item)


class RedashSource(Source):
    config: RedashConfig
    report: RedashSourceReport
    platform = "redash"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: RedashConfig):
        super().__init__(ctx)
        self.config = config
        self.report = RedashSourceReport()

        # Handle trailing slash removal
        self.config.connect_uri = self.config.connect_uri.strip("/")

        self.client = Redash(self.config.connect_uri, self.config.api_key)
        self.client.session.headers.update(
            {
                "Content-Type": "application/json",
                "Accept": "application/json",
            }
        )

        self.api_page_limit = self.config.api_page_limit or math.inf

        # Test the connection
        test_response = self.client._get(f"{self.config.connect_uri}/api")
        if test_response.status_code == 200:
            logger.info("Redash API connected succesfully")
            pass

            # # Only for getting data source types available in Redash
            # # We use this as source for `DATA_SOURCE_TO_PLATFORM_MAP` variable
            # data_source_types = self.client._get(f"/api/data_sources/types").json()
            # for dst in data_source_types:
            #     print(f'\'{dst["type"]}\'', ":", {"name": dst["name"], "platform": None, }, ",")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = RedashConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @lru_cache(maxsize=None)
    def _get_chart_data_source(self, data_source_id):
        url = f"/api/data_sources/{data_source_id}"
        resp = self.client._get(url).json()
        return resp

    @lru_cache(maxsize=None)
    def _get_datasource_urn_from_data_source_id(self, data_source_id):
        data_source = self._get_chart_data_source(data_source_id)
        data_source_type = data_source.get("type")

        if data_source_type:
            platform = DATA_SOURCE_TO_PLATFORM_MAP.get(
                data_source_type, {"platform": "external"}
            ).get("platform")
            platform_urn = f"urn:li:dataPlatform:{platform}"

            database_name_key = DATA_SOURCE_TO_PLATFORM_MAP.get(
                data_source_type, {}
            ).get("database_name_key", "db")
            database_name = data_source.get("options", {}).get(database_name_key, "")

            dataset_urn = (
                f"urn:li:dataset:("
                f"{platform_urn},{database_name + '' if database_name else ''}"
                f",{self.config.env})"
            )
            return dataset_urn
        return None

    def _get_dashboard_description_from_widgets(
        self, dashboard_widgets: List[Dict]
    ) -> str:
        description = ""

        for widget in dashboard_widgets:
            visualization = widget.get("visualization")
            if not visualization:
                options = widget.get("options")
                text = widget.get("text")
                isHidden = widget.get("isHidden")

                # TRICKY: If top-left most widget is a Textbox, then we assume it is the Description
                if options and text and not isHidden:
                    position = options.get("position")
                    if position:
                        col = position.get("col")
                        row = position.get("row")
                        if col == 0 and row == 0:
                            description = text
                else:
                    continue

        return description

    def _get_dashboard_chart_urns_from_widgets(
        self, dashboard_widgets: List[Dict]
    ) -> List[str]:
        chart_urns = []
        for widget in dashboard_widgets:
            # In Redash, chart is called visualization
            visualization = widget.get("visualization")
            if visualization:
                visualization_id = visualization.get("id", "unknown")
                chart_urns.append(f"urn:li:chart:({self.platform},{visualization_id})")

        return chart_urns

    def _get_dashboard_snapshot(self, dashboard_data):
        dashboard_id = dashboard_data["id"]
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_id})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{(dashboard_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(dashboard_data.get("updated_at", "now")).timestamp() * 1000
        )
        title = dashboard_data.get("name", "")

        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        dashboard_url = (
            f"{self.config.connect_uri}/dashboard/{dashboard_data.get('slug', '')}"
        )

        widgets = dashboard_data.get("widgets", [])
        description = self._get_dashboard_description_from_widgets(widgets)
        chart_urns = self._get_dashboard_chart_urns_from_widgets(widgets)

        dashboard_info = DashboardInfoClass(
            description=description,
            title=title,
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info)

        return dashboard_snapshot

    def _emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        current_dashboards_page = 0
        skip_draft = self.config.skip_draft

        # we will set total dashboards to the actual number after we get the response
        total_dashboards = PAGE_SIZE

        while (
            current_dashboards_page * PAGE_SIZE <= total_dashboards
            and current_dashboards_page < self.api_page_limit
        ):
            dashboards_response = self.client.dashboards(
                page=current_dashboards_page + 1, page_size=PAGE_SIZE
            )
            total_dashboards = dashboards_response.get("count") or 0
            current_dashboards_page += 1

            for dashboard_response in dashboards_response["results"]:

                dashboard_name = dashboard_response["name"]

                if (not self.config.dashboard_patterns.allowed(dashboard_name)) or (
                    skip_draft and dashboard_response["is_draft"]
                ):
                    self.report.report_dropped(dashboard_name)
                    continue

                # Continue producing MCE
                dashboard_slug = dashboard_response["slug"]
                dashboard_data = self.client.dashboard(dashboard_slug)
                dashboard_snapshot = self._get_dashboard_snapshot(dashboard_data)
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def _get_chart_type_from_viz_data(self, viz_data):
        """
        https://redash.io/help/user-guide/visualizations/visualization-types
        Redash has multiple visualization types. Chart type is actually Plotly.
        So we need to check options returned by API, which series type is being used.
        """
        viz_type = viz_data.get("type", "")
        viz_options = viz_data.get("options", {})
        globalSeriesType = viz_options.get("globalSeriesType", "")
        report_key = f"redash-chart-{viz_data['id']}"

        # handle Plotly chart types
        if viz_type == "CHART":
            chart_type = PLOTLY_CHART_MAP.get(
                globalSeriesType, DEFAULT_VISUALIZATION_TYPE
            )
            if not chart_type:
                self.report.report_warning(
                    key=report_key,
                    reason=f"ChartTypeClass={viz_data['type']} with options.globalSeriesType={globalSeriesType} is missing. Setting to None",
                )

        chart_type = VISUALIZATION_TYPE_MAP.get(viz_type, DEFAULT_VISUALIZATION_TYPE)
        if not chart_type:
            self.report.report_warning(
                key=report_key,
                reason=f"ChartTypeClass={viz_data['type']} is missing. Setting to None",
            )

        return chart_type

    def _get_chart_snapshot(self, query_data, viz_data):
        chart_urn = f"urn:li:chart:({self.platform},{viz_data['id']})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{(viz_data.get('changed_by') or {}).get('username', 'unknown')}"
        modified_ts = int(
            dp.parse(viz_data.get("updated_at", "now")).timestamp() * 1000
        )
        title = f"{query_data.get('name')} {viz_data.get('name', '')}"

        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        # Getting chart type
        chart_type = self._get_chart_type_from_viz_data(viz_data)
        chart_url = f"{self.config.connect_uri}/queries/{query_data.get('id')}#{viz_data.get('id', '')}"
        description = viz_data.get("description") if viz_data.get("description") else ""

        # TODO: Getting table lineage from SQL parsing
        # Currently we only get database level source from `data_source_id` which returns database name or Bigquery's projectId
        # query = query_data.get("query", "")

        data_source_id = query_data.get("data_source_id")
        datasource_urns = [
            self._get_datasource_urn_from_data_source_id(data_source_id),
        ]

        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=datasource_urns if datasource_urns else None,
        )
        chart_snapshot.aspects.append(chart_info)

        return chart_snapshot

    def _emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        current_queries_page = 0
        skip_draft = self.config.skip_draft

        # we will set total charts to the actual number after we get the response
        total_queries = PAGE_SIZE

        while (
            current_queries_page * PAGE_SIZE <= total_queries
            and current_queries_page < self.api_page_limit
        ):
            queries_response = self.client.queries(
                page=current_queries_page + 1, page_size=PAGE_SIZE
            )
            current_queries_page += 1

            total_queries = queries_response["count"]
            for query_response in queries_response["results"]:

                chart_name = query_response["name"]

                if (not self.config.chart_patterns.allowed(chart_name)) or (
                    skip_draft and query_response["is_draft"]
                ):
                    self.report.report_dropped(chart_name)
                    continue

                query_id = query_response["id"]
                query_data = self.client._get(f"/api/queries/{query_id}").json()

                # In Redash, chart is called vlsualization
                for visualization in query_data.get("visualizations", []):
                    chart_snapshot = self._get_chart_snapshot(query_data, visualization)
                    mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                    wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)

                    yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self._emit_dashboard_mces()
        yield from self._emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report
