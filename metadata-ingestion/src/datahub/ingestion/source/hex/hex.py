from typing import Any, Dict, Iterable, List, Optional

from pydantic import Field, SecretStr
from typing_extensions import assert_never

from datahub.configuration.common import AllowDenyPattern
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
    HEX_API_BASE_URL_DEFAULT,
    HEX_API_PAGE_SIZE_DEFAULT,
    HEX_PLATFORM_NAME,
)
from datahub.ingestion.source.hex.mapper import Mapper
from datahub.ingestion.source.hex.model import Component, Project
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
    StaleEntityRemovalSourceReport,
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
    StatefulIngestionReport,
    StatefulIngestionSourceBase,
)


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


class HexReport(StaleEntityRemovalSourceReport, HexApiReport):
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
        self.report = HexReport()
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

    def get_report(self) -> StatefulIngestionReport:
        return self.report

    def get_workunits_internal(self) -> Iterable[MetadataWorkUnit]:
        yield from self.mapper.map_workspace()

        for project_or_component in self.hex_api.fetch_projects():
            if isinstance(project_or_component, Project):
                if self.source_config.project_title_pattern.allowed(
                    project_or_component.title
                ):
                    yield from self.mapper.map_project(project=project_or_component)
            elif isinstance(project_or_component, Component):
                if (
                    self.source_config.include_components
                    and self.source_config.component_title_pattern.allowed(
                        project_or_component.title
                    )
                ):
                    yield from self.mapper.map_component(component=project_or_component)
            else:
                assert_never(project_or_component)
