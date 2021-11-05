import json
from functools import lru_cache
from typing import Dict, Iterable, Optional

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import DEFAULT_ENV
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit

import requests
from pydantic.class_validators import validator

from datahub.utilities import config_clean


class MetabaseConfig(ConfigModel):
    # See the Superset /security/login endpoint for details
    # https://superset.apache.org/docs/rest-api
    connect_uri: str = "localhost:3000"
    username: Optional[str] = None
    password: Optional[str] = None
    options: Dict = {}
    env: str = DEFAULT_ENV

    @validator("connect_uri")
    def remove_trailing_slash(cls, v):
        return config_clean.remove_trailing_slashes(v)


class MetabaseSource(Source):
    config: MetabaseConfig
    report: SourceReport
    platform = "metabase"

    def __hash__(self):
        return id(self)

    def __init__(self, ctx: PipelineContext, config: MetabaseConfig):
        super().__init__(ctx)
        self.config = config
        self.report = SourceReport()

        login_response = requests.post(
            f"{self.config.connect_uri}/api/session",
            None,
            {
                "username": self.config.username,
                "password": self.config.password,
            },
        )

        self.access_token = login_response.json()["id"]

        self.session = requests.Session()
        self.session.headers.update(
            {
                "X-Metabase-Session": f"{self.access_token}",
                "Content-Type": "application/json",
                "Accept": "*/*",
            }
        )

        # Test the connection
        test_response = self.session.get(f"{self.config.connect_uri}/api/user/current")
        if test_response.status_code == 200:
            pass

        #TODO handle 401 and inform
        #TODO override close() to end user session

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        pass
        # yield from self.emit_dashboard_mces()
        # yield from self.emit_chart_mces()

    def get_report(self) -> SourceReport:
        return self.report