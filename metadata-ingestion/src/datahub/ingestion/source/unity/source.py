"""
DataBricks Unity Catalog source plugin
"""
from typing import Iterable

import pydantic

from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.unity import emitter, proxy


class UnityCatalogSourceConfig(EnvBasedSourceConfigBase):
    token: str = pydantic.Field(description="Databricks personal access token")
    workspace_url: str = pydantic.Field(description="Databricks workspace url")
    workspace_name: str = pydantic.Field(
        default=None,
        description="Name of the workspace. Default to deployment name present in workspace_url",
    )


@platform_name("Unity Catalog")
@config_class(UnityCatalogSourceConfig)
@support_status(SupportStatus.INCUBATING)
class UnityCatalogSource(Source):
    """
    This plugin extracts the following metadata from Databricks Unity Catalog:
    - metastores
    - schemas
    - tables and column lineage
    """

    source_config: UnityCatalogSourceConfig
    unity_catalog_api_proxy: proxy.UnityCatalogApiProxy
    emitter: emitter.Emitter
    platform_name: str = "unity-catalog"

    def get_workunits(self) -> Iterable[MetadataWorkUnit]:
        # emit metadata work unit to DataHub GMS
        for mcps in self.emitter.emit():
            for mcp in mcps:
                metadata_work_unit: MetadataWorkUnit = MetadataWorkUnit(
                    id=f"{self.platform_name}-{mcp.entityUrn}-{mcp.aspectName}",
                    mcp=mcp,
                )
                self.emitter.report.report_workunit(metadata_work_unit)
                yield metadata_work_unit

    def get_report(self) -> SourceReport:
        return self.emitter.report

    def __init__(self, config: UnityCatalogSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.unity_catalog_api_proxy = proxy.UnityCatalogApiProxy(
            config.workspace_url, config.token
        )
        # Check if we are able to connect to DataBricks workspace url
        if not self.unity_catalog_api_proxy.check_connectivity():
            raise Exception(
                f"Not able to connect to workspace url {config.workspace_url}"
            )
        # Determine the platform_instance_name
        platform_instance_name: str = (
            config.workspace_name
            if config.workspace_name is not None
            else config.workspace_url.split("//")[1].split(".")[0]
        )
        self.emitter = emitter.Emitter(
            platform_name=self.platform_name,
            platform_instance_name=platform_instance_name,
            unity_catalog_api_proxy=self.unity_catalog_api_proxy,
        )

    @classmethod
    def create(cls, config_dict, ctx):
        config = UnityCatalogSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)
