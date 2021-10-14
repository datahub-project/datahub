import logging
# import json
from typing import Dict, Iterable, List, Optional
from sql_metadata import Parser

import dateutil.parser as dp
import requests


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

DEFAULT_VISUALIZATION_TYPE = ChartTypeClass.TABLE
chart_type_from_viz_type = {
    "table": ChartTypeClass.TABLE,
    "table-old": ChartTypeClass.TABLE,
    "logs": ChartTypeClass.TABLE,
    "nodeGraph": ChartTypeClass.TABLE,
    "geomap": ChartTypeClass.TABLE,
    "state-timeline": ChartTypeClass.TABLE,
    "status-history": ChartTypeClass.TABLE,
    "bargauge": ChartTypeClass.BAR,
    "barchart": ChartTypeClass.BAR,
    "stat": ChartTypeClass.TABLE,
    "row": ChartTypeClass.TEXT,
    "text": ChartTypeClass.TEXT,
    "alertlist": ChartTypeClass.TEXT,
    "dashlist": ChartTypeClass.TEXT,
    "news": ChartTypeClass.TEXT,
    "annolist": ChartTypeClass.TEXT,
    "grafana-clock-panel": ChartTypeClass.TEXT,
    "graph": ChartTypeClass.LINE,
    "grafana-piechart-panel": ChartTypeClass.PIE,
    "piechart": ChartTypeClass.PIE,
    "gauge": ChartTypeClass.PIE,
    "timeseries": ChartTypeClass.LINE,
    "heatmap": ChartTypeClass.TABLE,
    "histogram": ChartTypeClass.HISTOGRAM,
}

class GrafanaConfig(ConfigModel):
    # See the Grafana API for details
    # https://grafana.com/docs/grafana/latest/http_api/auth/
    connect_uri: str = "http://localhost:3000"
    api_key: str = "GRAFANA_API_KEY"
    env: str = DEFAULT_ENV

    # TODO:
    # # Optionals
    # dashboard_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()
    # chart_patterns: AllowDenyPattern = AllowDenyPattern.allow_all()

class GrafanaSource(Source):
    config: GrafanaConfig
    report: SourceReport
    platform = "grafana"

    def __init__(self, ctx: PipelineContext, config: GrafanaConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        self.connect_uri = self.config.connect_uri.strip("/")

        self.session = requests.Session()
        self.session.headers.update(
            {
                "Authorization": f"Bearer {self.config.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            }   
        )

        response_ds = self.session.get(f"{self.connect_uri}/api/datasources")
        self.response_ds = response_ds.json()

    def test_connection(self) -> None:
        test_response = self.session.get(f"{self.connect_uri}/api/datasources")
        if test_response.status_code == 200:
            logger.info("Grafana API connected succesfully")
            pass
        else:
            raise ValueError(f"Failed to connect to {self.connect_uri}/api/datasources")

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = GrafanaConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_all_dashboard_list(self) -> list:
        response_data = self.session.get(f"{self.connect_uri}/api/search?query=%")
        all_dashboard_list = response_data.json()
        return all_dashboard_list

    # testing ....
    def get_datasource(self, datasource_raw, datasource_name, is_default):

        datasource = {
                "database_type" : None,
                "database_name" : "unknown",
            }
        datasource_datails = {}

        # find datasource type ,database
        if is_default is True:
            for ds in datasource_raw:
                if ds["isDefault"] is True:
                    datasource_datails = ds
                    break

            if datasource_datails.get("type"):
                datasource['database_type'] = datasource_datails.get("type")

            if datasource_datails.get("database"):
                datasource['database_name'] = datasource_datails.get("database")
        else:
            for ds in datasource_raw:
                if ds["name"] == datasource_name:
                    datasource_datails = ds
                    break

            if datasource_datails.get("type"):
                datasource['database_type'] = datasource_datails.get("type")

            if datasource_datails.get("database"):
                datasource['database_name'] = datasource_datails.get("database")

        return datasource

    def construct_chart_input(self,chart_data):

        chart_input = []
        default_chart_datasource_name = chart_data.get("datasource")

        if chart_data.get("targets"): # check none type
            for taget in chart_data.get("targets"):

                is_default = False
                database_type = "unknown"
                database_name = "unknown"

                # get sub datasource name
                if taget.get("datasource"):
                    chart_datasource_name = taget.get("datasource")
                else:
                    chart_datasource_name = default_chart_datasource_name

                if chart_datasource_name is None:
                    is_default = True

                # get database type , database name
                datasource = self.get_datasource(
                        datasource_raw=self.response_ds,
                        datasource_name=chart_datasource_name,
                        is_default=is_default)

                database_type = datasource.get("database_type")
                database_name = datasource.get("database_name")

                if taget.get("rawQuery") is True:
                    sql_parser = Parser(taget.get("rawSql"))
                    sql_parser_tables = sql_parser.tables

                    for table_name in sql_parser_tables:
                        # "urn:li:dataset:(urn:li:dataPlatform:postgres,viu_datalake.viu.campaigns,PROD)"
                        urn = f"urn:li:dataset:(urn:li:dataPlatform:{database_type},{database_name}.{table_name},{self.config.env})"
                        chart_input.append(urn)

        if chart_input:
            return chart_input
        else:
            return None

    def construct_dashboard_from_api_data(self, dashboard_data):
        dashboard_uid = dashboard_data.get("dashboard").get("uid")
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_uid})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        actor = dashboard_data.get('meta').get("updatedBy")
        modified_actor = f"urn:li:corpuser:{actor}"
        
        modified_ts = int(
             dp.parse(dashboard_data.get("meta").get("updated")).timestamp() * 1000
        )
        title = dashboard_data.get("dashboard").get("title")
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        dashboard_suffix_url = dashboard_data.get("meta").get("url")
        dashboard_url = f"{self.connect_uri}{dashboard_suffix_url}"

        # get grafana chart object
        chart_urns = []
        panels = dashboard_data.get("dashboard").get("panels")

        ## TODO include only sqlraw chart
        # for data_panel in panels:

        #     chart_id = data_panel.get("id")
        #     chart_uid = f"{dashboard_uid}_{chart_id}"

        #     chart_urns.append(
        #         f"urn:li:chart:({self.platform},{chart_uid})"
        #     )

        for data_panel in panels:
            chart_input = self.construct_chart_input(data_panel)
            # **NOTE** capture only contain chart input 
            # because if chart input blank or null will error (DataHub 0.8.15)
            if chart_input:
                chart_id = data_panel.get("id")
                chart_uid = f"{dashboard_uid}_{chart_id}"

                chart_urns.append(
                    f"urn:li:chart:({self.platform},{chart_uid})"
                )

        dashboard_info = DashboardInfoClass(
            title=title,
            description="",
            charts=chart_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info)
        return dashboard_snapshot

    def construct_chart_from_chart_data(
        self, dashboard_id, chart_data, modify_actor, 
        updated_at, chart_input, dashboard_url
        ):
        
        chart_id = chart_data.get("id")
        chart_uid = f"{dashboard_id}_{chart_id}"

        chart_description = ""
        if  chart_data.get("description"):
            chart_description = chart_data.get("description")

        chart_type = chart_type_from_viz_type.get(chart_data.get("type"))
        if chart_type is None:
            chart_type = DEFAULT_VISUALIZATION_TYPE

        chart_title = chart_data.get("title")
        custom_properties = {}

        # get raw_sql from chart to customer properties
        targets_list = chart_data.get("targets")
        if targets_list is not None:
            keynum = 1
            for target_data in targets_list:
                key = f"{keynum}_raw_sql"
                custom_properties[key] = target_data.get("rawSql")
                keynum += 1

        chart_urn = f"urn:li:chart:({self.platform},{chart_uid})"
        chart_snapshot = ChartSnapshot(
            urn=chart_urn,
            aspects=[],
        )

        modified_actor = f"urn:li:corpuser:{modify_actor}"
        modified_ts = int(
             dp.parse(updated_at).timestamp() * 1000
        )

        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        chart_info = ChartInfoClass(
            type=DEFAULT_VISUALIZATION_TYPE,
            description=chart_description,
            title=chart_title,
            lastModified=last_modified,
            chartUrl=dashboard_url,
            inputs=chart_input,
            customProperties=custom_properties
        )
        chart_snapshot.aspects.append(chart_info)
        return chart_snapshot

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:

        dashboard_list = self.get_all_dashboard_list()

        for dashboard_data in dashboard_list:
            # get dashboard type : "dash-db" only
            if dashboard_data.get("type") == "dash-db": 
                dashboard_uid = dashboard_data.get("uid")
                response_data = self.session.get(f"{self.connect_uri}/api/dashboards/uid/{dashboard_uid}")
                dashboard_details = response_data.json()

                dashboard_snapshot = self.construct_dashboard_from_api_data(
                        dashboard_data=dashboard_details
                    )
                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def emit_chart_mces(self) -> Iterable[MetadataWorkUnit]:
        
        dashboard_list = self.get_all_dashboard_list()


        for dashboard_data in dashboard_list:
            if dashboard_data.get("type") == "dash-db": 
                dashboard_uid = dashboard_data.get("uid")
                response_data = self.session.get(f"{self.connect_uri}/api/dashboards/uid/{dashboard_uid}")

                dashboard_details = response_data.json()
                charts_list = dashboard_details.get("dashboard").get("panels")

                dashboard_suffix_url = dashboard_details.get("meta").get("url")
                dashboard_url = f"{self.connect_uri}{dashboard_suffix_url}"

                for chart_data in charts_list:
                    chart_input = self.construct_chart_input(chart_data)

                    # **NOTE** capture only contain chart input 
                    # because if chart input blank or null will error (DataHub 0.8.15)
                    if chart_input:
                        chart_snapshot = self.construct_chart_from_chart_data(
                                dashboard_id=dashboard_uid,
                                
                                chart_data=chart_data, 
                                modify_actor=dashboard_details.get('meta').get("updatedBy"), 
                                updated_at=dashboard_details.get("meta").get("updated"),
                                chart_input=chart_input,
                                dashboard_url=dashboard_url
                                )

                        mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                        wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                        self.report.report_workunit(wu)

                        yield wu

    # emit workflow ** final process
    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        self.test_connection()
        yield from self.emit_dashboard_mces()
        yield from self.emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report

    def close(self):
        pass


