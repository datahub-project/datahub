import logging
import math
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional

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
logger.setLevel(logging.INFO)

PAGE_SIZE = 25

DEFAULT_DATA_SOURCE_PLATFORM = "external"
DEFAULT_DATA_BASE_NAME = "default"

# TODO: update Datahub registered platform name. Currently we use external for unmapped platform
# Supported data source list from REDASH_BASE_URL/api/data_sources/types or https://redash.io/integrations/
# Not all data source is supported on Datahub
# We also get the database name from connection options https://github.com/getredash/redash/tree/master/redash/query_runner
REDASH_DATA_SOURCE_TO_DATAHUB_MAP = {
    "athena": {"platform": "athena", "db_name_key": "schema"},
    "azure_kusto": {"platform": "kusto", "db_name_key": "database"},
    "bigquery": {"platform": "bigquery", "db_name_key": "projectId"},
    "Cassandra": {"platform": "external", "db_name_key": "keyspace"},
    "clickhouse": {"platform": "external", "db_name_key": "dbname"},
    "cockroach": {"platform": "external", "db_name_key": "db"},
    "couchbase": {"platform": "couchbase"},
    "db2": {"platform": "external", "db_name_key": "dbname"},
    "drill": {"platform": "external", "db_name_key": "dbname"},
    "druid": {"platform": "druid"},
    "hive_http": {"platform": "hive"},
    "hive": {"platform": "hive"},
    "impala": {"platform": "external", "db_name_key": "database"},
    "mapd": {"platform": "external", "db_name_key": "database"},
    "mongodb": {"platform": "mongodb", "db_name_key": "dbName"},
    "mssql": {"platform": "mssql", "db_name_key": "db"},
    "mysql": {"platform": "mysql", "db_name_key": "db"},
    "pg": {"platform": "postgres", "db_name_key": "dbname"},
    "phoenix": {"platform": "external", "db_name_key": "db"},
    "presto": {"platform": "presto", "db_name_key": "schema"},
    "qubole": {"platform": "external", "db_name_key": "cluster"},
    "rds_mysql": {"platform": "mysql", "db_name_key": "db"},
    "redshift": {"platform": "redshift", "db_name_key": "dbname"},
    "scylla": {"platform": "external", "db_name_key": "keyspace"},
    "snowflake": {"platform": "snowflake", "db_name_key": "database"},
    "sqlite": {"platform": "sqlite", "db_name_key": "db"},
    "treasuredata": {"platform": "external", "db_name_key": "db"},
    "vertica": {"platform": "vertica", "db_name_key": "database"},
    "results": {"platform": "external", "db_name_key": "name"},
}


# We assume the default chart type is TABLE
DEFAULT_VISUALIZATION_TYPE = ChartTypeClass.TABLE

# https://github.com/getredash/redash/blob/master/viz-lib/src/visualizations/chart/Editor/ChartTypeSelect.tsx
# TODO: add more mapping on ChartTypeClass
PLOTLY_CHART_MAP = {
    # TODO: add more Plotly visualization mapping here
    # TODO: need to add more ChartTypeClass in datahub schema_classes.py
    "line": ChartTypeClass.LINE,
    "column": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "pie": ChartTypeClass.PIE,
    "scatter": ChartTypeClass.SCATTER,
    "bubble": None,
    "heatmap": None,
    "box": ChartTypeClass.BOX_PLOT,
}

VISUALIZATION_TYPE_MAP = {
    # TODO: add more Redash visualization mapping here
    # https://redash.io/help/user-guide/visualizations/visualization-types
    # https://github.com/getredash/redash/blob/master/viz-lib/src/visualizations/registeredVisualizations.ts
    # TODO: need to add more ChartTypeClass in datahub schema_classes.py
    "BOXPLOT": ChartTypeClass.BOX_PLOT,
    "CHOROPLETH": None,
    "COUNTER": ChartTypeClass.TABLE,
    "DETAILS": ChartTypeClass.TABLE,
    "FUNNEL": None,
    "MAP": None,
    "PIVOT": ChartTypeClass.TABLE,
    "SANKEY": None,
    "SUNBURST_SEQUENCE": None,
    "TABLE": ChartTypeClass.TABLE,
    "WORD_CLOUD": None,
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

    def report_item_scanned(self) -> None:
        self.items_scanned += 1

    def report_dropped(self, item: str) -> None:
        self.filtered.append(item)


class RedashSource(Source):
    config: RedashConfig
    report: RedashSourceReport
    platform = "redash"

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

    def test_connection(self) -> None:
        test_response = self.client._get(f"{self.config.connect_uri}/api")
        if test_response.status_code == 200:
            logger.info("Redash API connected succesfully")
            pass
        else:
            raise ValueError(f"Failed to connect to {self.config.connect_uri}/api")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = RedashConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def _get_chart_data_source(self, data_source_id: int = None) -> Dict:
        url = f"/api/data_sources/{data_source_id}"
        resp = self.client._get(url).json()
        logger.debug(resp)
        return resp

    def _get_datasource_urn_from_data_source(self, data_source: Dict) -> Optional[str]:
        data_source_type = data_source.get("type")
        data_source_name = data_source.get("name")
        data_source_options = data_source.get("options", {})

        if data_source_type:
            map = REDASH_DATA_SOURCE_TO_DATAHUB_MAP.get(
                data_source_type, {"platform": DEFAULT_DATA_SOURCE_PLATFORM}
            )
            platform = map.get("platform")
            platform_urn = f"urn:li:dataPlatform:{platform}"

            db_name_key = map.get("db_name_key", "db")
            db_name = data_source_options.get(db_name_key, DEFAULT_DATA_BASE_NAME)

            # Redash Query Results
            if data_source_type == "results":
                dataset_urn = f"urn:li:dataset:({platform_urn},{data_source_name},{self.config.env})"
                return dataset_urn

            # Other Redash supported data source as in REDASH_DATA_SOURCE_TO_DATAHUB_MAP
            if db_name:
                dataset_urn = (
                    f"urn:li:dataset:({platform_urn},{db_name},{self.config.env})"
                )
                return dataset_urn
        return None

    def _get_dashboard_description_from_widgets(
        self, dashboard_widgets: List[Dict]
    ) -> str:
        description = ""

        for widget in dashboard_widgets:
            visualization = widget.get("visualization")
            if visualization is None:
                options = widget.get("options")
                text = widget.get("text")
                isHidden = widget.get("isHidden")

                # TRICKY: If top-left most widget is a Textbox, then we assume it is the Description
                if options and text and isHidden is None:
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
                visualization_id = visualization.get("id", None)
                if visualization_id is not None:
                    chart_urns.append(
                        f"urn:li:chart:({self.platform},{visualization_id})"
                    )

        return chart_urns

    def _get_dashboard_snapshot(self, dashboard_data):
        dashboard_id = dashboard_data["id"]
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_id})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{dashboard_data.get('changed_by', {}).get('username', 'unknown')}"
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

            logger.info(f"/api/dashboards on page {current_dashboards_page}")

            for dashboard_response in dashboards_response["results"]:

                dashboard_name = dashboard_response["name"]

                self.report.report_item_scanned()

                if (not self.config.dashboard_patterns.allowed(dashboard_name)) or (
                    skip_draft and dashboard_response["is_draft"]
                ):
                    self.report.report_dropped(dashboard_name)
                    continue

                # Continue producing MCE
                dashboard_slug = dashboard_response["slug"]
                dashboard_data = self.client.dashboard(dashboard_slug)
                logger.debug(dashboard_data)
                dashboard_snapshot = self._get_dashboard_snapshot(dashboard_data)
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def _get_chart_type_from_viz_data(self, viz_data: Dict) -> str:
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
            chart_type = PLOTLY_CHART_MAP.get(globalSeriesType)
            if chart_type is None:
                chart_type = DEFAULT_VISUALIZATION_TYPE
                message = f"ChartTypeClass for Redash Visualization Type={viz_type} with options.globalSeriesType={globalSeriesType} is missing. Setting to {DEFAULT_VISUALIZATION_TYPE}"
                self.report.report_warning(key=report_key, reason=message)
                logger.warning(message)
        else:
            chart_type = VISUALIZATION_TYPE_MAP.get(viz_type)
            if chart_type is None:
                chart_type = DEFAULT_VISUALIZATION_TYPE
                message = f"ChartTypeClass for Redash Visualization Type={viz_type} is missing. Setting to {DEFAULT_VISUALIZATION_TYPE}"
                self.report.report_warning(key=report_key, reason=message)
                logger.warning(message)

        return chart_type

    def _get_chart_snapshot(self, query_data: Dict, viz_data: Dict) -> ChartSnapshot:
        viz_id = viz_data["id"]
        chart_urn = f"urn:li:chart:({self.platform},{viz_id})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{viz_data.get('changed_by', {}).get('username', 'unknown')}"
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
        chart_url = f"{self.config.connect_uri}/queries/{query_data.get('id')}#{viz_id}"
        description = (
            viz_data.get("description", "") if viz_data.get("description", "") else ""
        )
        data_source_id = query_data.get("data_source_id")
        data_source = self._get_chart_data_source(data_source_id)
        data_source_type = data_source.get("type")

        # TODO: Getting table lineage from SQL parsing
        # Currently we only get database level source from `data_source_id` which returns database name or Bigquery's projectId
        # query = query_data.get("query", "")
        datasource_urn = self._get_datasource_urn_from_data_source(data_source)

        if not datasource_urn:
            self.report.report_warning(
                key=f"redash-chart-{viz_id}",
                reason=f"data_source_type={data_source_type} not yet implemented. Setting inputs to None",
            )

        chart_info = ChartInfoClass(
            type=chart_type,
            description=description,
            title=title,
            lastModified=last_modified,
            chartUrl=chart_url,
            inputs=[
                datasource_urn,
            ]
            if datasource_urn
            else None,
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
            logger.info(f"/api/queries on page {current_queries_page}")

            total_queries = queries_response["count"]
            for query_response in queries_response["results"]:

                chart_name = query_response["name"]

                self.report.report_item_scanned()

                if (not self.config.chart_patterns.allowed(chart_name)) or (
                    skip_draft and query_response["is_draft"]
                ):
                    self.report.report_dropped(chart_name)
                    continue

                query_id = query_response["id"]
                query_data = self.client._get(f"/api/queries/{query_id}").json()
                logger.debug(query_data)

                # In Redash, chart is called visualization
                for visualization in query_data.get("visualizations", []):
                    chart_snapshot = self._get_chart_snapshot(query_data, visualization)
                    mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                    wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                    self.report.report_workunit(wu)

                    yield wu

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.test_connection()
        yield from self._emit_dashboard_mces()
        yield from self._emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass
