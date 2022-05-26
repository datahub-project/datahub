import importlib
import logging
import math
import sys
from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Optional, Type

import dateutil.parser as dp
from pydantic.fields import Field
from redash_toolbelt import Redash
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (  # SourceCapability,; capability,
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
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
from datahub.utilities.sql_parser import SQLParser

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


@dataclass
class QualifiedNameParser:
    split_char: str
    names: List
    default_schema: Optional[str] = None

    def get_segments(self, table_name: str) -> Dict:
        segments = table_name.split(self.split_char)
        segments.reverse()
        self.segments_dict = dict(zip(list(reversed(self.names)), segments))
        return self.segments_dict

    def get_full_qualified_name(self, database_name: str, table_name: str) -> str:
        self.get_segments(table_name)
        _database_name = self.segments_dict.get("database_name") or database_name
        _schema_name = self.segments_dict.get("schema_name") or self.default_schema
        _table_name = self.segments_dict.get("table_name")

        return f"{_database_name}{self.split_char}{_schema_name}{self.split_char}{_table_name}"


@dataclass
class PostgresQualifiedNameParser(QualifiedNameParser):
    split_char: str = "."
    names: List = field(
        default_factory=lambda: ["database_name", "schema_name", "table_name"]
    )
    default_schema: Optional[str] = "public"


@dataclass
class MssqlQualifiedNameParser(QualifiedNameParser):
    split_char: str = "."
    names: List = field(
        default_factory=lambda: ["database_name", "schema_name", "table_name"]
    )
    default_schema: Optional[str] = "dbo"


@dataclass
class MysqlQualifiedNameParser(QualifiedNameParser):
    split_char: str = "."
    names: List = field(default_factory=lambda: ["database_name", "table_name"])

    def get_full_qualified_name(self, database_name: str, table_name: str) -> str:
        self.get_segments(table_name)
        _database_name = self.segments_dict.get("database_name") or database_name
        _table_name = self.segments_dict.get("table_name")

        return f"{_database_name}{self.split_char}{_table_name}"


@dataclass
class AthenaQualifiedNameParser(QualifiedNameParser):
    split_char: str = "."
    names: List = field(default_factory=lambda: ["database_name", "table_name"])

    def get_full_qualified_name(self, database_name: str, table_name: str) -> str:
        self.get_segments(table_name)
        _database_name = self.segments_dict.get("database_name") or database_name
        _table_name = self.segments_dict.get("table_name")
        return f"{_database_name}{self.split_char}{_table_name}"


@dataclass
class BigqueryQualifiedNameParser(QualifiedNameParser):
    split_char: str = "."
    names: List = field(
        default_factory=lambda: ["database_name", "schema_name", "table_name"]
    )

    def get_full_qualified_name(self, database_name: str, table_name: str) -> str:
        self.get_segments(table_name)
        _database_name = self.segments_dict.get("database_name") or database_name
        _schema_name = self.segments_dict.get("schema_name")
        _table_name = self.segments_dict.get("table_name")

        return f"{_database_name}{self.split_char}{_schema_name}{self.split_char}{_table_name}"


def get_full_qualified_name(platform: str, database_name: str, table_name: str) -> str:
    if platform == "postgres":
        full_qualified_name = PostgresQualifiedNameParser().get_full_qualified_name(
            database_name, table_name
        )
    elif platform == "mysql":
        full_qualified_name = MysqlQualifiedNameParser().get_full_qualified_name(
            database_name, table_name
        )
    elif platform == "mssql":
        full_qualified_name = MssqlQualifiedNameParser().get_full_qualified_name(
            database_name, table_name
        )
    elif platform == "athena":
        full_qualified_name = AthenaQualifiedNameParser().get_full_qualified_name(
            database_name, table_name
        )
    elif platform == "bigquery":
        full_qualified_name = BigqueryQualifiedNameParser().get_full_qualified_name(
            database_name, table_name
        )
    else:
        full_qualified_name = f"{database_name}.{table_name}"
    return full_qualified_name


class RedashConfig(ConfigModel):
    # See the Redash API for details
    # https://redash.io/help/user-guide/integrations-and-api/api
    connect_uri: str = Field(
        default="http://localhost:5000", description="Redash base URL."
    )
    api_key: str = Field(default="REDASH_API_KEY", description="Redash user API key.")

    # Optionals
    dashboard_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for dashboards to filter for ingestion.",
    )
    chart_patterns: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="regex patterns for charts to filter for ingestion.",
    )
    skip_draft: bool = Field(
        default=True, description="Only ingest published dashboards and charts."
    )
    api_page_limit: int = Field(
        default=sys.maxsize,
        description="Limit on ingested dashboards and charts API pagination.",
    )
    parse_table_names_from_sql: bool = Field(
        default=False, description="See note below."
    )
    sql_parser: str = Field(
        default="datahub.utilities.sql_parser.DefaultSQLParser",
        description="custom SQL parser. See note below for details.",
    )

    env: str = Field(
        default=DEFAULT_ENV,
        description="Environment to use in namespace when constructing URNs.",
    )


@dataclass
class RedashSourceReport(SourceReport):
    items_scanned: int = 0
    filtered: List[str] = field(default_factory=list)

    def report_item_scanned(self) -> None:
        self.items_scanned += 1

    def report_dropped(self, item: str) -> None:
        self.filtered.append(item)


@platform_name("Redash")
@config_class(RedashConfig)
@support_status(SupportStatus.CERTIFIED)
class RedashSource(Source):
    """
    This plugin extracts the following:

    - Redash dashboards and queries/visualization
    - Redash chart table lineages (disabled by default)
    """

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

        # Handling retry and backoff
        retries = 3
        backoff_factor = 10
        status_forcelist = (500, 503, 502, 504)
        retry = Retry(
            total=retries,
            read=retries,
            connect=retries,
            backoff_factor=backoff_factor,
            status_forcelist=status_forcelist,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.client.session.mount("http://", adapter)
        self.client.session.mount("https://", adapter)

        self.api_page_limit = self.config.api_page_limit or math.inf

        self.parse_table_names_from_sql = self.config.parse_table_names_from_sql
        self.sql_parser_path = self.config.sql_parser

        logger.info(
            f"Running Redash ingestion with parse_table_names_from_sql={self.parse_table_names_from_sql}"
        )

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

    @classmethod
    def _import_sql_parser_cls(cls, sql_parser_path: str) -> Type[SQLParser]:
        assert "." in sql_parser_path, "sql_parser-path must contain a ."
        module_name, cls_name = sql_parser_path.rsplit(".", 1)
        import sys

        logger.debug(sys.path)
        parser_cls = getattr(importlib.import_module(module_name), cls_name)
        if not issubclass(parser_cls, SQLParser):
            raise ValueError(f"must be derived from {SQLParser}; got {parser_cls}")

        return parser_cls

    @classmethod
    def _get_sql_table_names(cls, sql: str, sql_parser_path: str) -> List[str]:
        parser_cls = cls._import_sql_parser_cls(sql_parser_path)

        sql_table_names: List[str] = parser_cls(sql).get_tables()

        # Remove quotes from table names
        sql_table_names = [t.replace('"', "") for t in sql_table_names]
        sql_table_names = [t.replace("`", "") for t in sql_table_names]

        return sql_table_names

    def _get_chart_data_source(self, data_source_id: int = None) -> Dict:
        url = f"/api/data_sources/{data_source_id}"
        resp = self.client._get(url).json()
        logger.debug(resp)
        return resp

    def _get_platform_based_on_datasource(self, data_source: Dict) -> str:
        data_source_type = data_source.get("type")
        if data_source_type:
            map = REDASH_DATA_SOURCE_TO_DATAHUB_MAP.get(
                data_source_type, {"platform": DEFAULT_DATA_SOURCE_PLATFORM}
            )
            platform = map.get("platform", DEFAULT_DATA_SOURCE_PLATFORM)
            return platform
        return DEFAULT_DATA_SOURCE_PLATFORM

    def _get_database_name_based_on_datasource(
        self, data_source: Dict
    ) -> Optional[str]:
        data_source_type = data_source.get("type", "external")
        data_source_name = data_source.get("name")
        data_source_options = data_source.get("options", {})

        if data_source_type == "results":
            database_name = data_source_name
        else:
            map = REDASH_DATA_SOURCE_TO_DATAHUB_MAP.get(
                data_source_type, {"platform": DEFAULT_DATA_SOURCE_PLATFORM}
            )

            database_name_key = map.get("db_name_key", "db")
            database_name = data_source_options.get(
                database_name_key, DEFAULT_DATA_BASE_NAME
            )

        return database_name

    def _construct_datalineage_urn(
        self, platform: str, database_name: str, sql_table_name: str
    ) -> str:
        full_dataset_name = get_full_qualified_name(
            platform, database_name, sql_table_name
        )
        return builder.make_dataset_urn(platform, full_dataset_name, self.config.env)

    def _get_datasource_urns(
        self, data_source: Dict, sql_query_data: Dict = {}
    ) -> Optional[List[str]]:
        platform = self._get_platform_based_on_datasource(data_source)
        database_name = self._get_database_name_based_on_datasource(data_source)
        data_source_syntax = data_source.get("syntax")

        if database_name:
            query = sql_query_data.get("query", "")

            # Getting table lineage from SQL parsing
            if self.parse_table_names_from_sql and data_source_syntax == "sql":
                try:
                    dataset_urns = list()
                    sql_table_names = self._get_sql_table_names(
                        query, self.sql_parser_path
                    )
                    for sql_table_name in sql_table_names:
                        dataset_urns.append(
                            self._construct_datalineage_urn(
                                platform, database_name, sql_table_name
                            )
                        )
                except Exception as e:
                    logger.error(e)
                    logger.error(query)

                # make sure dataset_urns is not empty list
                return dataset_urns if len(dataset_urns) > 0 else None

            else:
                return [
                    builder.make_dataset_urn(platform, database_name, self.config.env)
                ]

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
        current_dashboards_page = 1
        skip_draft = self.config.skip_draft

        # Get total number of dashboards to calculate maximum page number
        dashboards_response = self.client.dashboards(1, PAGE_SIZE)
        total_dashboards = dashboards_response["count"]
        max_page = total_dashboards // PAGE_SIZE

        while (
            current_dashboards_page <= max_page
            and current_dashboards_page <= self.api_page_limit
        ):
            dashboards_response = self.client.dashboards(
                page=current_dashboards_page, page_size=PAGE_SIZE
            )

            logger.info(f"/api/dashboards on page {current_dashboards_page}")

            current_dashboards_page += 1

            for dashboard_response in dashboards_response["results"]:

                dashboard_name = dashboard_response["name"]

                self.report.report_item_scanned()

                if (not self.config.dashboard_patterns.allowed(dashboard_name)) or (
                    skip_draft and dashboard_response["is_draft"]
                ):
                    self.report.report_dropped(dashboard_name)
                    continue

                # Continue producing MCE
                try:
                    # This is undocumented but checking the Redash source
                    # the API is id based not slug based
                    # Tested the same with a Redash instance
                    dashboard_id = dashboard_response["id"]
                    dashboard_data = self.client._get(
                        "api/dashboards/{}".format(dashboard_id)
                    ).json()
                except Exception:
                    # This does not work in our testing but keeping for now because
                    # people in community are using Redash connector successfully
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

        datasource_urns = self._get_datasource_urns(data_source, query_data)

        if datasource_urns is None:
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
            inputs=datasource_urns or None,
        )
        chart_snapshot.aspects.append(chart_info)

        return chart_snapshot

    def _emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        current_queries_page = 1
        skip_draft = self.config.skip_draft

        # Get total number of queries to calculate maximum page number
        _queries_response = self.client.queries(1, PAGE_SIZE)
        total_queries = _queries_response["count"]
        max_page = total_queries // PAGE_SIZE

        while (
            current_queries_page <= max_page
            and current_queries_page <= self.api_page_limit
        ):
            queries_response = self.client.queries(
                page=current_queries_page, page_size=PAGE_SIZE
            )

            logger.info(f"/api/queries on page {current_queries_page}")

            current_queries_page += 1

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
        yield from self._emit_chart_mces()
        yield from self._emit_dashboard_mces()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass
