import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Union

from pydantic import validator
from pydantic.fields import Field

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.configuration.common import (
    ConfigModel,
    ConfigurationError,
    VersionedConfig,
)
from datahub.configuration.config_loader import load_config_file
from datahub.configuration.source_common import EnvBasedSourceConfigBase
from datahub.emitter.mce_builder import (
    get_sys_time,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import Source, SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit, UsageStatsWorkUnit

logger = logging.getLogger(__name__)

auditStamp = models.AuditStampClass(
    time=get_sys_time(), actor="urn:li:corpUser:pythonEmitter"
)


class EntityConfig(EnvBasedSourceConfigBase):
    name: str
    type: str
    platform: str
    platform_instance: Optional[str]

    @validator("type")
    def type_must_be_supported(cls, v: str) -> str:
        allowed_types = ["dataset"]
        if v not in allowed_types:
            raise ConfigurationError(
                f"Type must be one of {allowed_types}, {v} is not yet supported."
            )
        return v


class EntityNodeConfig(ConfigModel):
    entity: EntityConfig
    upstream: Optional[List["EntityNodeConfig"]]


# https://pydantic-docs.helpmanual.io/usage/postponed_annotations/ required for when you reference a model within itself
EntityNodeConfig.update_forward_refs()


class LineageFileSourceConfig(ConfigModel):
    file: str = Field(description="Path to lineage file to ingest.")
    preserve_upstream: bool = Field(
        default=True,
        description="Whether we want to query datahub-gms for upstream data. False means it will hard replace upstream data for a given entity. True means it will query the backend for existing upstreams and include it in the ingestion run",
    )


class LineageConfig(VersionedConfig):
    lineage: List[EntityNodeConfig]

    @validator("version")
    def version_must_be_1(cls, v):
        if v != "1":
            raise ValueError("Only version 1 is supported")


@platform_name("File Based Lineage")
@config_class(LineageFileSourceConfig)
@support_status(SupportStatus.CERTIFIED)
@dataclass
class LineageFileSource(Source):
    """
    This plugin pulls lineage metadata from a yaml-formatted file. An example of one such file is located in the examples directory [here](../../../../metadata-ingestion/examples/bootstrap_data/file_lineage.yml).
    """

    config: LineageFileSourceConfig
    report: SourceReport = field(default_factory=SourceReport)

    @classmethod
    def create(
        cls, config_dict: Dict[str, Any], ctx: PipelineContext
    ) -> "LineageFileSource":
        config = LineageFileSourceConfig.parse_obj(config_dict)
        return cls(ctx, config)

    @staticmethod
    def load_lineage_config(file_name: str) -> LineageConfig:
        config = load_config_file(file_name)
        lineage_config = LineageConfig.parse_obj(config)
        return lineage_config

    @staticmethod
    def get_lineage_metadata_change_event_proposal(
        entities: List[EntityNodeConfig], preserve_upstream: bool
    ) -> Iterable[MetadataChangeProposalWrapper]:
        """
        Builds a list of events to be emitted to datahub by going through each entity and its upstream nodes
        :param preserve_upstream: This field determines if we want to query the datahub backend to extract
        the existing upstream lineages for each entity and preserve it
        :param entities: A list of entities we want to build a proposal on
        :return: Returns a list of metadata change event proposals to be emitted to datahub
        """

        def _get_entity_urn(entity_config: EntityConfig) -> Optional[str]:
            """Helper inner function to extract a given entity_urn
            A return value of None represents an unsupported entity type
            """
            if entity_config.type == "dataset":
                return make_dataset_urn_with_platform_instance(
                    platform=entity_config.platform,
                    name=entity_config.name,
                    env=entity_config.env,
                    platform_instance=entity_config.platform_instance,
                )
            logger.warning(f"Entity type: {entity_config.type} is not supported!")
            return None

        # loop through all the entities
        for entity_node in entities:
            new_upstreams: List[models.UpstreamClass] = []
            # if this entity has upstream nodes defined, we'll want to do some work.
            # if no upstream nodes are present, we don't emit an MCP for it.
            if entity_node.upstream:
                entity = entity_node.entity
                logger.info(f"Upstream detected for {entity}. Extracting urn...")
                entity_urn = _get_entity_urn(entity)
                if entity_urn:
                    # extract the old lineage and save it for the new mcp
                    if preserve_upstream:
                        old_upstream_lineage = get_aspects_for_entity(
                            entity_urn=entity_urn,
                            aspects=["upstreamLineage"],
                            typed=True,
                        ).get("upstreamLineage")
                        if old_upstream_lineage:
                            # Can't seem to get mypy to be happy about
                            # `Argument 1 to "list" has incompatible type "Optional[Any]";
                            # expected "Iterable[UpstreamClass]"`
                            new_upstreams.extend(
                                old_upstream_lineage.get("upstreams")  # type: ignore
                            )
                    for upstream_entity_node in entity_node.upstream:
                        upstream_entity = upstream_entity_node.entity
                        upstream_entity_urn = _get_entity_urn(upstream_entity)
                        if upstream_entity_urn:
                            new_upstream = models.UpstreamClass(
                                dataset=upstream_entity_urn,
                                type=models.DatasetLineageTypeClass.TRANSFORMED,
                                auditStamp=auditStamp,
                            )
                            new_upstreams.append(new_upstream)
                        else:
                            logger.warning(
                                f"Entity type: {upstream_entity.type} is unsupported. Upstream lineage will be skipped "
                                f"for {upstream_entity.name}->{entity.name}"
                            )
                    new_upstream_lineage = models.UpstreamLineageClass(
                        upstreams=new_upstreams
                    )
                    yield MetadataChangeProposalWrapper(
                        entityType=entity.type,
                        changeType=models.ChangeTypeClass.UPSERT,
                        entityUrn=entity_urn,
                        aspectName="upstreamLineage",
                        aspect=new_upstream_lineage,
                    )
                else:
                    logger.warning(
                        f"Entity type: {entity.type} is unsupported. Entity node {entity.name} and its "
                        f"upstream lineages will be skipped"
                    )

    def get_workunits(self) -> Iterable[Union[MetadataWorkUnit, UsageStatsWorkUnit]]:
        lineage_config = self.load_lineage_config(self.config.file)
        lineage = lineage_config.lineage
        preserve_upstream = self.config.preserve_upstream
        logger.debug(lineage_config)
        logger.info(f"preserve_upstream is set to {self.config.preserve_upstream}")
        for (
            metadata_change_event_proposal
        ) in self.get_lineage_metadata_change_event_proposal(
            lineage, preserve_upstream
        ):
            work_unit = MetadataWorkUnit(
                f"lineage-{metadata_change_event_proposal.entityUrn}",
                mcp=metadata_change_event_proposal,
            )
            self.report.report_workunit(work_unit)
            yield work_unit

    def get_report(self):
        return self.report
