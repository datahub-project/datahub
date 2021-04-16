from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import requests
import json
import dateutil.parser as dp

from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DashboardSnapshot, ChartSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DashboardInfoClass, ChartInfoClass

PAGE_SIZE = 25

class SupersetConfig(ConfigModel):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = "localhost:8088"
    username: Optional[str] = None
    password: Optional[str] = None
    provider: str = "db"
    options: dict = {}


@dataclass
class SupersetSource(Source):
    config: SupersetConfig
    report: SourceReport

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
            self.validated = True


    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext):
        config = SupersetConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_datasource_urn_from_id(self, datasource_id):
        return 'urn:li:datasource:id'

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
            current_dashboard_page += 1

            payload = dashboard_response.json()
            total_dashboards = payload['count']
            for dashboard_data in payload['result']:
                dashboard_urn = "urn:li:dashboard:(%s,%s)" % (platform, dashboard_data['id'])
                dashboard_snapshot = DashboardSnapshot(
                    urn=dashboard_urn,
                    aspects=[],
                )

                modified_actor = "urn:li:corpuser:%s" % (dashboard_data.get('changed_by') or {}).get('username', 'unknown')
                modified_ts = int(dp.parse(dashboard_data.get('changed_on_utc', '')).timestamp())
                title = dashboard_data.get('dashboard_title', '')
                lastModified = AuditStamp(time=modified_ts, actor=modified_actor)
                dashboardUrl = '%s%s' % (self.config.connect_uri, dashboard_data.get('url', ''))

                chart_urns = []
                raw_position_data = dashboard_data.get('position_json', '{}')
                position_data = json.loads(raw_position_data)
                for key, value in position_data.items():
                    if not key.startswith('CHART-'):
                        continue
                    chart_urns.append('urn:li:chart:(%s,%s)' % (platform, value.get('meta', {}).get('chartId', 'unknown')))

                dashboard_info = DashboardInfoClass(
                    title=title, charts=chart_urns, lastModified=lastModified, dashboardUrl=dashboardUrl
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
                lastModified = AuditStamp(time=modified_ts, actor=modified_actor)
                chartUrl = '%s%s' % (self.config.connect_uri, chart_data.get('url', ''))

                chart_urns = []
                raw_param_data = chart_data.get('params', '{}')
                datasource_id = chart_data.get('datasource_id')
                datasource_urn = self.get_datasource_urn_from_id(datasource_id)


                chart_info = ChartInfoClass(
                    title=title, charts=chart_urns, lastModified=lastModified, dashboardUrl=dashboardUrl
                )
                dashboard_snapshot.aspects.append(dashboard_info)

                mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
                wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
                self.report.report_workunit(wu)

                yield wu

        # for database_name in dashboard_names:
        #     if not self.config.database_pattern.allowed(database_name):
        #         self.report.report_dropped(database_name)
        #         continue
        #
        #     database = self.mongo_client[database_name]
        #     collection_names: List[str] = database.list_collection_names()
        #     for collection_name in collection_names:
        #         dataset_name = f"{database_name}.{collection_name}"
        #         if not self.config.collection_pattern.allowed(dataset_name):
        #             self.report.report_dropped(dataset_name)
        #             continue
        #
        #         mce = MetadataChangeEvent()
        #         dataset_snapshot = DatasetSnapshot()
        #         dataset_snapshot.urn = f"urn:li:dataset:(urn:li:dataPlatform:{platform},{dataset_name},{env})"
        #
        #         dataset_properties = DatasetPropertiesClass(
        #             tags=[],
        #             customProperties={},
        #         )
        #         dataset_snapshot.aspects.append(dataset_properties)
        #
        #         # TODO: Guess the schema via sampling
        #         # State of the art seems to be https://github.com/variety/variety.
        #
        #         # TODO: use list_indexes() or index_information() to get index information
        #         # See https://pymongo.readthedocs.io/en/stable/api/pymongo/collection.html#pymongo.collection.Collection.list_indexes.
        #
        #         mce.proposedSnapshot = dataset_snapshot
        #
        #         wu = MetadataWorkUnit(id=dataset_name, mce=mce)
        #         self.report.report_workunit(wu)
        #         yield wu

    def get_report(self) -> SourceReport:
        return self.report