from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, SecretStr, root_validator
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.datetimes import parse_user_datetime
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import MetadataWorkUnitProcessor
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.hex.api import HexApi, HexApiReport
from datahub.ingestion.source.hex.constants import (
    DATAHUB_API_PAGE_SIZE_DEFAULT,
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
    HEX_PLATFORM_NAME,
)
from datahub.ingestion.source.hex.mapper import Mapper
from datahub.ingestion.source.hex.model import Component, Project
from datahub.ingestion.source.hex.query_fetcher import (
    HexQueryFetcher,
    HexQueryFetcherReport,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionSourceBase,
)
from datahub.ingestion.source_report.ingestion_stage import IngestionStageReport
from datahub.sdk.main_client import DataHubClient


class HexSourceConfig(
    StatefulIngestionConfigBase, PlatformInstanceConfigMixin, EnvConfigMixin
):
    workspace_name: str = Field(
        description="Hex workspace name. You can find this name in your Hex home page URL: https://app.hex.tech/<workspace_name>",
    )
    token: SecretStr = Field(
        description="Hex API token; either PAT or Workflow token - https://learn.hex.tech/docs/api/api-overview#authentication",
    )
    base_url: str = Field(
        default=HEX_API_BASE_URL_DEFAULT,
        description="Hex API base URL. For most Hex users, this will be https://app.hex.tech/api/v1. "
        "Single-tenant app users should replace this with the URL they use to access Hex.",
    )
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Configuration for stateful ingestion and stale metadata removal.",
    )
    include_components: bool = Field(
        default=True,
        desciption="Include Hex Components in the ingestion",
    )
    page_size: int = Field(
        default=HEX_API_PAGE_SIZE_DEFAULT,
        description="Number of items to fetch per Hex API call.",
    )
    patch_metadata: bool = Field(
        default=False,
        description="Emit metadata as patch events",
    )
    collections_as_tags: bool = Field(
        default=True,
        description="Emit Hex Collections as tags",
    )
    status_as_tag: bool = Field(
        default=True,
        description="Emit Hex Status as tags",
    )
    categories_as_tags: bool = Field(
        default=True,
        description="Emit Hex Category as tags",
    )
    project_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for project titles to filter in ingestion.",
    )
    component_title_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex pattern for component titles to filter in ingestion.",
    )
    set_ownership_from_email: bool = Field(
        default=True,
        description="Set ownership identity from owner/creator email",
    )
    include_lineage: bool = Field(
        default=True,
        description='Include Hex lineage, being fetched from DataHub. See "Limitations" section in the docs for more details about the limitations of this feature.',
    )
    lineage_start_time: Optional[datetime] = Field(
        default=None,
        description="Earliest date of lineage to consider. Default: 1 day before lineage end time. You can specify absolute time like '2023-01-01' or relative time like '-7 days' or '-7d'.",
    )
    lineage_end_time: Optional[datetime] = Field(
        default=None,
        description="Latest date of lineage to consider. Default: Current time in UTC. You can specify absolute time like '2023-01-01' or relative time like '-1 day' or '-1d'.",
    )
    datahub_page_size: int = Field(
        default=DATAHUB_API_PAGE_SIZE_DEFAULT,
        description="Number of items to fetch per DataHub API call.",
    )

    @root_validator(pre=True)
    def validate_lineage_times(cls, data: Dict[str, Any]) -> Dict[str, Any]:
        # lineage_end_time default = now
        if "lineage_end_time" not in data or data["lineage_end_time"] is None:
            data["lineage_end_time"] = datetime.now(tz=timezone.utc)
        # if string is given, parse it
        if isinstance(data["lineage_end_time"], str):
            data["lineage_end_time"] = parse_user_datetime(data["lineage_end_time"])
        # if no timezone is given, assume UTC
        if data["lineage_end_time"].tzinfo is None:
            data["lineage_end_time"] = data["lineage_end_time"].replace(
                tzinfo=timezone.utc
            )
        # at this point, we ensure there is a non null datetime with UTC timezone for lineage_end_time
        assert (
            data["lineage_end_time"]
            and isinstance(data["lineage_end_time"], datetime)
            and data["lineage_end_time"].tzinfo is not None
            and data["lineage_end_time"].tzinfo == timezone.utc
        )

        # lineage_start_time default = lineage_end_time - 1 day
        if "lineage_start_time" not in data or data["lineage_start_time"] is None:
            data["lineage_start_time"] = data["lineage_end_time"] - timedelta(days=1)
        # if string is given, parse it
        if isinstance(data["lineage_start_time"], str):
            data["lineage_start_time"] = parse_user_datetime(data["lineage_start_time"])
        # if no timezone is given, assume UTC
        if data["lineage_start_time"].tzinfo is None:
            data["lineage_start_time"] = data["lineage_start_time"].replace(
                tzinfo=timezone.utc
            )
        # at this point, we ensure there is a non null datetime with UTC timezone for lineage_start_time
        assert (
            data["lineage_start_time"]
            and isinstance(data["lineage_start_time"], datetime)
            and data["lineage_start_time"].tzinfo is not None
            and data["lineage_start_time"].tzinfo == timezone.utc
        )

        return data


@dataclass
class HexReport(
    StaleEntityRemovalSourceReport,
    HexApiReport,
    IngestionStageReport,
    HexQueryFetcherReport,
):
    pass


@platform_name("Hex")
@config_class(HexSourceConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.DESCRIPTIONS, "Supported by default")
@capability(SourceCapability.OWNERSHIP, "Supported by default")
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.CONTAINERS, "Enabled by default")
class HexSource(StatefulIngestionSourceBase):
    def __init__(self, config: HexSourceConfig, ctx: PipelineContext):
        super().__init__(config, ctx)
        self.source_config = config
        self.report: HexReport = HexReport()
        self.platform = HEX_PLATFORM_NAME
        self.hex_api = HexApi(
            report=self.report,
            token=self.source_config.token.get_secret_value(),
            base_url=self.source_config.base_url,
            page_size=self.source_config.page_size,
        )
        self.mapper = Mapper(
            workspace_name=self.source_config.workspace_name,
            platform_instance=self.source_config.platform_instance,
            env=self.source_config.env,
            base_url=self.source_config.base_url,
            patch_metadata=self.source_config.patch_metadata,
            collections_as_tags=self.source_config.collections_as_tags,
            status_as_tag=self.source_config.status_as_tag,
            categories_as_tags=self.source_config.categories_as_tags,
            set_ownership_from_email=self.source_config.set_ownership_from_email,
        )
        self.project_registry: Dict[str, Project] = {}
        self.component_registry: Dict[str, Component] = {}

        self.datahub_client: Optional[DataHubClient] = None
        self.query_fetcher: Optional[HexQueryFetcher] = None
        if self.source_config.include_lineage:
            graph = ctx.require_graph("Lineage")
            assert self.source_config.lineage_start_time and isinstance(
                self.source_config.lineage_start_time, datetime
            )
            assert self.source_config.lineage_end_time and isinstance(
                self.source_config.lineage_end_time, datetime
            )
            self.datahub_client = DataHubClient(graph=graph)
            self.query_fetcher = HexQueryFetcher(
                datahub_client=self.datahub_client,
                workspace_name=self.source_config.workspace_name,
                start_datetime=self.source_config.lineage_start_time,
                end_datetime=self.source_config.lineage_end_time,
                report=self.report,
                page_size=self.source_config.datahub_page_size,
            )

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext) -> "HexSource":
        config = HexSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            *super().get_workunit_processors(),
            StaleEntityRemovalHandler.create(
                self, self.source_config, self.ctx
            ).workunit_processor,
        ]

    def get_report(self) -> HexReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        with self.report.new_stage("Fetch Hex assets from Hex API"):
            for project_or_component in self.hex_api.fetch_projects():
                if isinstance(project_or_component, Project):
                    if self.source_config.project_title_pattern.allowed(
                        project_or_component.title
                    ):
                        self.project_registry[project_or_component.id] = (
                            project_or_component
                        )
                elif isinstance(project_or_component, Component):
                    if (
                        self.source_config.include_components
                        and self.source_config.component_title_pattern.allowed(
                            project_or_component.title
                        )
                    ):
                        self.component_registry[project_or_component.id] = (
                            project_or_component
                        )
                else:
                    assert_never(project_or_component)

        if self.source_config.include_lineage:
            assert self.datahub_client and self.query_fetcher

            with self.report.new_stage(
                "Fetch Hex lineage from existing Queries in DataHub"
            ):
                for query_metadata in self.query_fetcher.fetch():
                    project = self.project_registry.get(query_metadata.hex_project_id)
                    if project:
                        project.upstream_datasets.extend(
                            query_metadata.dataset_subjects
                        )
                        project.upstream_schema_fields.extend(
                            query_metadata.schema_field_subjects
                        )
                    else:
                        self.report.report_warning(
                            title="Missing project for lineage",
                            message="Lineage missed because missed project, likely due to filter patterns or deleted project.",
                            context=str(query_metadata),
                        )

        with self.report.new_stage("Emit"):
            yield from self.mapper.map_workspace()

            for project in self.project_registry.values():
                yield from self.mapper.map_project(project=project)
            for component in self.component_registry.values():
                yield from self.mapper.map_component(component=component)
