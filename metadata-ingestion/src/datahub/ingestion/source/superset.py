from dataclasses import dataclass, field
from typing import Iterable, List, Optional

import requests
import json

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.source.metadata_common import MetadataWorkUnit
from datahub.metadata.com.linkedin.pegasus2avro.metadata.snapshot import DatasetSnapshot
from datahub.metadata.com.linkedin.pegasus2avro.mxe import MetadataChangeEvent
from datahub.metadata.schema_classes import DatasetPropertiesClass

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
class SupersetSourceReport(SourceReport):
    def __init__(self):
        pass

    filtered: List[str] = field(default_factory=list)

    def report_dropped(self, name: str) -> None:
        self.filtered.append(name)


@dataclass
class SupersetSource(Source):
    config: SupersetConfig
    report: SupersetSourceReport

    def __init__(self, ctx: PipelineContext, config: SupersetConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SupersetSourceReport()

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
        print(self.access_token)

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
            print(payload)
            total_dashboards = payload['count']
            dashboards_in_payload = len(payload['result'])
            for dashboard_data in payload['result']:
                chart_ids = 
                raw_position_data = dashboard_data['position_json']
                position_data = json.loads(raw_position_data)
                for key, value in position_data:
                    if not key.startswith('CHART-'):
                        continue



            print(len(dashboard_response.json()['result']))

            current_dashboard_page += 1

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

    def get_report(self) -> SupersetSourceReport:
        return self.report