"""
MetaData Ingestion From the Microsoft SSAS Server.
"""

import logging
from dataclasses import dataclass, field as dataclass_field
from typing import Any, Dict, Iterable, List, Optional

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import PlatformKey, gen_containers
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import ChangeTypeClass

from .config import SsasServerHTTPSourceConfig
from .domains import SSASDataSet

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.DEBUG)


class SSASContainerKey(PlatformKey):
    name: str


class Mapper:
    """ """

    class EquableMetadataWorkUnit(MetadataWorkUnit):
        """ """

        def __eq__(self, instance):
            return self.id == self.id

        def __hash__(self):
            return id(self.id)

    def __init__(self, config: SsasServerHTTPSourceConfig):
        self.__config = config

    @staticmethod
    def new_mcp(
        entity_type,
        entity_urn,
        aspect_name,
        aspect,
        change_type=ChangeTypeClass.UPSERT,
    ):
        """
        Create MCP
        """
        return MetadataChangeProposalWrapper(
            entityType=entity_type,
            changeType=change_type,
            entityUrn=entity_urn,
            aspectName=aspect_name,
            aspect=aspect,
        )

    def __to_work_unit(
        self, mcp: MetadataChangeProposalWrapper
    ) -> EquableMetadataWorkUnit:
        return Mapper.EquableMetadataWorkUnit(
            id="{PLATFORM}-{ENTITY_URN}-{ASPECT_NAME}".format(
                PLATFORM=self.__config.platform_name,
                ENTITY_URN=mcp.entityUrn,
                ASPECT_NAME=mcp.aspectName,
            ),
            mcp=mcp,
        )

    def construct_set_workunits(
        self,
        data_set: SSASDataSet,
    ) -> Iterable[MetadataWorkUnit]:

        mcp = MetadataChangeProposalWrapper(
            entityType=data_set.type,
            entityUrn=data_set.urn,
            changeType=ChangeTypeClass.UPSERT,
            **data_set.as_dataplatform_data,
        )

        wu = MetadataWorkUnit(
            id=f"{data_set.platform}.{data_set.name}.{mcp.aspectName}", mcp=mcp
        )
        LOGGER.debug(f"as_dataplatform_data methadata: {wu.get_metadata()}")
        yield wu

        mcp = MetadataChangeProposalWrapper(
            entityType=data_set.type,
            entityUrn=data_set.urn,
            changeType=ChangeTypeClass.UPSERT,
            **data_set.as_dataset_properties_data,
        )

        wu = MetadataWorkUnit(
            id=f"{data_set.platform}.{data_set.name}.{mcp.aspectName}", mcp=mcp
        )
        LOGGER.debug(f"as_dataset_properties_data methadata: {wu.get_metadata()}")
        yield wu
        mcp = MetadataChangeProposalWrapper(
            entityType=data_set.type,
            entityUrn=data_set.urn,
            changeType=ChangeTypeClass.UPSERT,
            **data_set.as_upstream_lineage_aspect_data,
        )

        wu = MetadataWorkUnit(
            id=f"{data_set.platform}.{data_set.name}.{mcp.aspectName}", mcp=mcp
        )
        LOGGER.debug(f"as_upstream_lineage_aspect_data methadata: {wu.get_metadata()}")
        yield wu


@dataclass
class SsasSourceReport(SourceReport):
    scanned_report: int = 0
    filtered_reports: List[str] = dataclass_field(default_factory=list)

    def report_scanned(self, count: int = 1) -> None:
        self.scanned_report += count

    def report_dropped(self, view: str) -> None:
        self.filtered_reports.append(view)


@platform_name("SSAS")
@config_class(SsasServerHTTPSourceConfig)
@support_status(SupportStatus.UNKNOWN)
@capability(SourceCapability.OWNERSHIP, "Enabled by default")
class SsasSource(Source):
    source_config: SsasServerHTTPSourceConfig

    def __init__(self, config: SsasServerHTTPSourceConfig, ctx: PipelineContext):
        super().__init__(ctx)
        self.source_config = config
        self.report = SsasSourceReport()
        self.mapper = Mapper(config)
        self.processed_containers: List[str] = []

    @classmethod
    def create(cls, config_dict: Dict[str, Any], ctx: PipelineContext):
        config = SsasServerHTTPSourceConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def create_emit_containers(
        self,
        container_key,
        name: str,
        sub_types: List[str],
        parent_container_key: Optional[PlatformKey] = None,
        domain_urn: Optional[str] = None,
    ) -> Iterable[MetadataWorkUnit]:
        if container_key.guid() not in self.processed_containers:
            container_wus = gen_containers(
                container_key=container_key,
                name=name,
                sub_types=sub_types,
                parent_container_key=parent_container_key,
                domain_urn=domain_urn,
            )
            self.processed_containers.append(container_key.guid())
            LOGGER.debug(f"Creating container with key: {container_key}")
            for wu in container_wus:
                self.report.report_workunit(wu)
                yield wu

    def gen_key(self, name):
        return SSASContainerKey(
            platform="ssas",
            name=name,
            instance=self.source_config.ssas_instance,
        )

    def get_report(self) -> SsasSourceReport:
        return self.report
