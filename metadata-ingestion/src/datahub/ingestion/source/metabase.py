import json
from functools import lru_cache
from typing import Dict, Iterable, Optional

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

import dateutil.parser as dp
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

    def emit_dashboard_mces(self) -> Iterable[MetadataWorkUnit]:
        dashboard_response = self.session.get(
            f"{self.config.connect_uri}/api/dashboard"
        )
        payload = dashboard_response.json()
        for dashboard_info in payload:
            dashboard_snapshot = self.construct_dashboard_from_api_data(dashboard_info)

            mce = MetadataChangeEvent(proposedSnapshot=dashboard_snapshot)
            wu = MetadataWorkUnit(id=dashboard_snapshot.urn, mce=mce)
            self.report.report_workunit(wu)

            yield wu

    def construct_dashboard_from_api_data(self, dashboard_info) -> DashboardSnapshot:
        dashboard_url = f"{self.config.connect_uri}/api/dashboard/{dashboard_info['id']}"
        dashboard_response = self.session.get(dashboard_url)
        dashboard_details = dashboard_response.json()
        dashboard_urn = f"urn:li:dashboard:({self.platform},{dashboard_details['id']})"
        dashboard_snapshot = DashboardSnapshot(
            urn=dashboard_urn,
            aspects=[],
        )
        last_edit_by = (dashboard_details.get('last-edit-info') or {})
        modified_actor = f"urn:li:corpuser:{last_edit_by.get('email', 'unknown')}"
        modified_ts = int(
            dp.parse(f"{last_edit_by.get('timestamp', 'now')}").timestamp() * 1000
        )
        title = dashboard_details.get("name", "") or ""
        description = dashboard_details.get("description", "") or ""
        last_modified = ChangeAuditStamps(
            created=AuditStamp(time=modified_ts, actor=modified_actor),
            lastModified=AuditStamp(time=modified_ts, actor=modified_actor),
        )

        card_urns = []
        cards_data = dashboard_details.get("ordered_cards", "{}")
        for card_info in cards_data:
            card_urns.append(
                f"urn:li:card:({self.platform},{card_info['id']})"
            )

        dashboard_info = DashboardInfoClass(
            description=description,
            title=title,
            charts=card_urns,
            lastModified=last_modified,
            dashboardUrl=dashboard_url,
            customProperties={},
        )
        dashboard_snapshot.aspects.append(dashboard_info)
        return dashboard_snapshot

    def emit_card_mces(self) -> Iterable[MetadataWorkUnit]:
        pass

    def construct_card_from_api_data(self, dashboard_info) -> ChartSnapshot:
        pass

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> Source:
        config = MetabaseConfig.parse_obj(config_dict)
        return cls(ctx, config)

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        yield from self.emit_dashboard_mces()
        yield from self.emit_card_mces()

    def get_report(self) -> SourceReport:
        return self.report