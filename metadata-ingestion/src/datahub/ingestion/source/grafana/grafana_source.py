from typing import Iterable, List, Optional

import requests
from pydantic import Field, SecretStr

import datahub.emitter.mce_builder as builder
from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.source_helpers import auto_workunit
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulIngestionConfigBase,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import ChangeAuditStamps
from datahub.metadata.schema_classes import DashboardInfoClass, StatusClass


class GrafanaSourceConfig(StatefulIngestionConfigBase, PlatformInstanceConfigMixin):
    url: str = Field(
        default="",
        description="Grafana URL in the format http://your-grafana-instance with no trailing slash",
    )
    service_account_token: SecretStr = Field(
        description="Service account token for Grafana"
    )


class GrafanaReport(StaleEntityRemovalSourceReport):
    pass


@platform_name("Grafana")
@config_class(GrafanaSourceConfig)
@support_status(SupportStatus.TESTING)
class GrafanaSource(StatefulIngestionSourceBase):
    """
    This is an experimental source for Grafana.
    Currently only ingests dashboards (no charts)
    """

    def __init__(self, config: GrafanaSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report = GrafanaReport()
        self.platform = "grafana"

    @classmethod
    def create(cls, config_dict, ctx):
        config = GrafanaSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> StatefulIngestionReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        headers = {
            "Authorization": f"Bearer {self.source_config.service_account_token.get_secret_value()}",
            "Content-Type": "application/json",
        }
        try:
            response = requests.get(
                f"{self.source_config.url}/api/search", headers=headers
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.report.report_failure(f"Failed to fetch dashboards: {str(e)}")
            return
        res_json = response.json()
        for item in res_json:
            uid = item["uid"]
            title = item["title"]
            url_path = item["url"]
            full_url = f"{self.source_config.url}{url_path}"
            dashboard_urn = builder.make_dashboard_urn(
                platform=self.platform,
                name=uid,
                platform_instance=self.source_config.platform_instance,
            )

            yield from auto_workunit(
                MetadataChangeProposalWrapper.construct_many(
                    entityUrn=dashboard_urn,
                    aspects=[
                        DashboardInfoClass(
                            description="",
                            title=title,
                            charts=[],
                            lastModified=ChangeAuditStamps(),
                            externalUrl=full_url,
                            customProperties={
                                key: str(value)
                                for key, value in {
                                    "displayName": title,
                                    "id": item["id"],
                                    "uid": uid,
                                    "title": title,
                                    "uri": item["uri"],
                                    "type": item["type"],
                                    "folderId": item.get("folderId"),
                                    "folderUid": item.get("folderUid"),
                                    "folderTitle": item.get("folderTitle"),
                                }.items()
                                if value is not None
                            },
                        ),
                        StatusClass(removed=False),
                    ],
                )
            )
