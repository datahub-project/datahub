from dataclasses import dataclass, field
from functools import lru_cache
from typing import Iterable, List, Optional

import requests
import json
import dateutil.parser as dp

from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp, ChangeAuditStamps
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DashboardSnapshot, ChartSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DashboardInfoClass, ChartInfoClass, ChartTypeClass

PAGE_SIZE = 25

class SupersetConfig(ConfigModel):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = "localhost:8088"
    username: Optional[str] = None
    password: Optional[str] = None
    provider: str = "db"
    options: dict = {}


def get_platform_from_sqlalchemy_uri(sqlalchemy_uri: str):
    if sqlalchemy_uri.startswith('bigquery'):
        return 'bigquery'
    if sqlalchemy_uri.startswith('druid'):
        return 'druid'
    if sqlalchemy_uri.startswith('mssql'):
        return 'mssql'
    if sqlalchemy_uri.startswith('jdbc:postgres:') and sqlalchemy_uri.index('redshift.amazonaws') > 0:
        return 'redshift'
    if sqlalchemy_uri.startswith('snowflake'):
        return 'snowflake'
    if sqlalchemy_uri.startswith('presto'):
        return 'presto'
    if sqlalchemy_uri.startswith('postgresql'):
        return 'postgres'
    if sqlalchemy_uri.startswith('pinot'):
        return 'pinot'
    if sqlalchemy_uri.startswith('oracle'):
        return 'oracle'
    if sqlalchemy_uri.startswith('mysql'):
        return 'mysql'
    if sqlalchemy_uri.startswith('mongodb'):
        return 'mongo'
    if sqlalchemy_uri.startswith('hive'):
        return 'hive'
    return 'external'


def get_chart_type_from_viz_type(viz_type: str):
    if viz_type == 'line':
        return ChartTypeClass.LINE
    elif viz_type == 'big_number':
        return ChartTypeClass.LINE
    elif viz_type == 'table':
        return ChartTypeClass.TABLE
    elif viz_type == 'filter_box':
        return None
    elif viz_type == 'dist_bar':
        return ChartTypeClass.BAR
    elif viz_type == 'filter_box':
        return None
    elif viz_type == 'area':
        return ChartTypeClass.AREA
    elif viz_type == 'bar':
        return ChartTypeClass.BAR
    elif viz_type == 'pie':
        return ChartTypeClass.PIE
    elif viz_type == 'histogram':
        return ChartTypeClass.HISTOGRAM
    elif viz_type == 'big_number_total':
        return ChartTypeClass.LINE
    elif viz_type == 'dual_line':
        return ChartTypeClass.LINE
    elif viz_type == 'line_multi':
        return ChartTypeClass.LINE
    elif viz_type == 'treemap':
        return ChartTypeClass.AREA
    elif viz_type == 'box_plot':
        return ChartTypeClass.BAR


@dataclass
class SupersetSource(Source):
    config: SupersetConfig
    report: SourceReport

    def __hash__(self):
        return 0

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        options = {}
        if self.config.username is not None:
            options["username"] = self.config.username
        if self.config.password is not None:
            options["password"] = self.config.password
        if self.config.provider is not None:
            options["provider"] = self.config.provider
        if self.config.connect_uri is not None:
            options["connect_uri"] = self.config.connect_uri
        options = {
            **options,
            **self.config.options,
        }

        login_response = requests.post('%s/api/v1/security/login' % options['connect_uri'], None, {
            'username': options['username'],
            'password': options['password'],
            'refresh': True,
            'provider': options['provider']
        })

        self.access_token = login_response.json()['access_token']

        self.headers = {
            'Authorization': 'Bearer %s' % self.access_token,
            'Content-Type': 'application/json',
            'Accept': '*/*',
        }

        # Test the connection
        test_response = requests.get('%s/api/v1/database' % options['connect_uri'], None, headers=self.headers)
        if test_response.status_code == 200:
            pass
            # TODO(Gabe): how should we message about this error?


    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = SupersetConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @lru_cache(maxsize=50000)
    def get_platform_from_database_id(self, database_id, env):
        database_response = requests.get(
            '%s/api/v1/database/%s' % (self.config.connect_uri, database_id),
            headers=self.headers
         ).json()
        sqlalchemy_uri = database_response.get('result', {}).get('sqlalchemy_uri')
        return get_platform_from_sqlalchemy_uri(sqlalchemy_uri)

    @lru_cache(maxsize=50000)
    def get_datasource_urn_from_id(self, datasource_id, env):
        dataset_response = requests.get(
            '%s/api/v1/dataset/%s' % (self.config.connect_uri, datasource_id),
            headers=self.headers,
            ).json()
        schema_name = dataset_response.get('result', {}).get('schema')
        table_name = dataset_response.get('result', {}).get('table_name')
        database_id = dataset_response.get('result', {}).get('database', {}).get('id')
        database_name = dataset_response.get('result', {}).get('database', {}).get('database_name')

        if database_id and table_name:
            platform = self.get_platform_from_database_id(database_id, env)
            platform_urn = f"urn:li:dataPlatform:{platform}"
            dataset_urn = f"urn:li:dataset:(" \
                          f"{platform_urn},{database_name + '.' if database_name else ''}" \
                          f"{schema_name + '.' if schema_name else ''}" \
                          f"{table_name},PROD)"
            return dataset_urn
        return None

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        env = "PROD"
        platform = "superset"

        current_dashboard_page = 0
        # we will set total dashboards to the actual number after we get the response
        total_dashboards = PAGE_SIZE

        while current_dashboard_page * PAGE_SIZE <= total_dashboards:
            dashboard_response = requests.get(
                '%s/api/v1/dashboard' % self.config.connect_uri,
                params={'q': '(page:%s,page_size:%s)' % (current_dashboard_page, PAGE_SIZE)},
                headers=self.headers,
            )
            payload = dashboard_response.json()
            total_dashboards = payload.get('count') or 0

            current_dashboard_page += 1

            payload = dashboard_response.json()
            for dashboard_data in payload['result']:
                dashboard_urn = "urn:li:dashboard:(%s,%s)" % (platform, dashboard_data['id'])
                dashboard_snapshot = DashboardSnapshot(
                    urn=dashboard_urn,
                    aspects=[],
                )

                modified_actor = "urn:li:corpuser:%s" % (dashboard_data.get('changed_by') or {}).get('username', 'unknown')
                modified_ts = int(dp.parse(dashboard_data.get('changed_on_utc', '')).timestamp())
                title = dashboard_data.get('dashboard_title', '')
                # note: the API does not currently supply created_by usernames due to a bug, but we are required to
                # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
                # modified actor urn
                last_modified = ChangeAuditStamps(created=AuditStamp(time=modified_ts, actor=modified_actor),
                                                  lastModified=AuditStamp(time=modified_ts, actor=modified_actor))
                dashboard_url = '%s%s' % (self.config.connect_uri[:-1], dashboard_data.get('url', ''))

                chart_urns = []
                raw_position_data = dashboard_data.get('position_json', '{}')
                position_data = json.loads(raw_position_data)
                for key, value in position_data.items():
                    if not key.startswith('CHART-'):
                        continue
                    chart_urns.append('urn:li:chart:(%s,%s)' % (platform, value.get('meta', {}).get('chartId', 'unknown')))

                dashboard_info = DashboardInfoClass(
                    title=title, charts=chart_urns, lastModified=last_modified, dashboardUrl=dashboard_url
                )
                dashboard_snapshot.aspects.append(dashboard_info)

                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

        current_chart_page = 0
        # we will set total charts to the actual number after we get the response
        total_charts = PAGE_SIZE

        while current_chart_page * PAGE_SIZE <= total_charts:
            chart_response = requests.get(
                '%s/api/v1/chart' % self.config.connect_uri,
                params={'q': '(page:%s,page_size:%s)' % (current_chart_page, PAGE_SIZE)},
                headers=self.headers,
                )
            current_chart_page += 1

            payload = chart_response.json()
            total_charts = payload['count']
            for chart_data in payload['result']:
                chart_urn = "urn:li:chart:(%s,%s)" % (platform, chart_data['id'])
                chart_snapshot = ChartSnapshot(
                    urn=chart_urn,
                    aspects=[],
                )

                modified_actor = "urn:li:corpuser:%s" % (chart_data.get('changed_by') or {}).get('username', 'unknown')
                modified_ts = int(dp.parse(chart_data.get('changed_on_utc', '')).timestamp())
                title = chart_data.get('slice_name', '')

                # note: the API does not currently supply created_by usernames due to a bug, but we are required to
                # provide a created AuditStamp to comply with ChangeAuditStamp model. For now, I sub in the last
                # modified actor urn
                last_modified = ChangeAuditStamps(created=AuditStamp(time=modified_ts, actor=modified_actor),
                                                  lastModified=AuditStamp(time=modified_ts, actor=modified_actor))
                chart_url = '%s%s' % (self.config.connect_uri[:-1], chart_data.get('url', ''))

                datasource_id = chart_data.get('datasource_id')
                datasource_urn = self.get_datasource_urn_from_id(datasource_id, env)
                chart_type = get_chart_type_from_viz_type(
                    chart_data.get('viz_type')
                )

                chart_info = ChartInfoClass(
                    title=title, lastModified=last_modified, chartUrl=chart_url,
                    type=chart_type,
                    inputs=[datasource_urn] if datasource_urn else None
                )
                chart_snapshot.aspects.append(chart_info)

                mce = MetadataChangeEvent(proposedSnapshot=chart_snapshot)
                wu = MetadataWorkUnit(id=chart_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

    def get_report(self) -> SourceReport:
        return self.report