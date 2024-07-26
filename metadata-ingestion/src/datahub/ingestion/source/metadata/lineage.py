import logging
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Dict, Iterable, List, Optional

from pydantic import validator
from pydantic.fields import Field

import datahub.metadata.schema_classes as models
from datahub.cli.cli_utils import get_aspects_for_entity
from datahub.configuration.common import ConfigModel, VersionedConfig
from datahub.configuration.config_loader import load_config_file
from datahub.configuration.source_common import EnvConfigMixin
from datahub.emitter.mce_builder import (
    get_sys_time,
    make_dataset_urn_with_platform_instance,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.api.source import (
    MetadataWorkUnitProcessor,
    Source,
    SourceCapability,
    SourceReport,
)
from datahub.ingestion.api.source_helpers import (
    auto_status_aspect,
    auto_workunit_reporter,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.graph.client import get_default_graph
from datahub.metadata.com.linkedin.pegasus2avro.dataset import (
    FineGrainedLineageDownstreamType,
    FineGrainedLineageUpstreamType,
)

logger = logging.getLogger(__name__)


class EntityConfig(EnvConfigMixin):
    name: str
    type: str
    platform: str
    platform_instance: Optional[str]

    @validator("type")
    def type_must_be_supported(cls, v: str) -> str:
        allowed_types = ["dataset"]
        if v not in allowed_types:
            raise ValueError(
                f"Type must be one of {allowed_types}, {v} is not yet supported."
            )
        return v

    @validator("name")
    def validate_name(cls, v: str) -> str:
        if v.startswith("urn:li:"):
            raise ValueError(
                "Name should not start with urn:li: - use a plain name, not an urn"
            )
        return v


class FineGrainedLineageConfig(ConfigModel):
    upstreamType: str = "FIELD_SET"
    upstreams: Optional[List[str]]
    downstreamType: str = "FIELD"
    downstreams: Optional[List[str]]
    transformOperation: Optional[str]
    confidenceScore: Optional[float] = 1.0

    @validator("upstreamType")
    def upstream_type_must_be_supported(cls, v: str) -> str:
        allowed_types = [
            FineGrainedLineageUpstreamType.FIELD_SET,
            FineGrainedLineageUpstreamType.DATASET,
            FineGrainedLineageUpstreamType.NONE,
        ]
        if v not in allowed_types:
            raise ValueError(
                f"Upstream Type must be one of {allowed_types}, {v} is not yet supported."
            )
        return v

    @validator("downstreamType")
    def downstream_type_must_be_supported(cls, v: str) -> str:
        allowed_types = [
            FineGrainedLineageDownstreamType.FIELD_SET,
            FineGrainedLineageDownstreamType.FIELD,
        ]
        if v not in allowed_types:
            raise ValueError(
                f"Downstream Type must be one of {allowed_types}, {v} is not yet supported."
            )
        return v


class EntityNodeConfig(ConfigModel):
    entity: EntityConfig
    upstream: Optional[List["EntityNodeConfig"]]
    fineGrainedLineages: Optional[List[FineGrainedLineageConfig]]


# https://pydantic-docs.helpmanual.io/usage/postponed_annotations/ required for when you reference a model within itself
EntityNodeConfig.update_forward_refs()


class LineageFileSourceConfig(ConfigModel):
    file: str = Field(description="File path or URL to lineage file to ingest.")
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
@capability(SourceCapability.LINEAGE_COARSE, "Specified in the lineage file.")
@capability(SourceCapability.LINEAGE_FINE, "Specified in the lineage file.")
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
        config = load_config_file(file_name, resolve_env_vars=True)
        lineage_config = LineageConfig.parse_obj(config)
        return lineage_config

    def get_workunit_processors(self) -> List[Optional[MetadataWorkUnitProcessor]]:
        return [
            auto_status_aspect,
            partial(auto_workunit_reporter, self.get_report()),
        ]

    def get_workunits_internal(
        self,
    ) -> Iterable[MetadataWorkUnit]:
        config = self.load_lineage_config(self.config.file)
        logger.debug(config)
        for entity_node in config.lineage:
            mcp = _get_lineage_mcp(entity_node, self.config.preserve_upstream)
            if mcp:
                yield mcp.as_workunit()

    def get_report(self):
        return self.report


def _get_entity_urn(entity_config: EntityConfig) -> Optional[str]:
    """A return value of None represents an unsupported entity type."""
    if entity_config.type == "dataset":
        return make_dataset_urn_with_platform_instance(
            platform=entity_config.platform,
            name=entity_config.name,
            env=entity_config.env,
            platform_instance=entity_config.platform_instance,
        )
    return None


def _get_lineage_mcp(
    entity_node: EntityNodeConfig, preserve_upstream: bool
) -> Optional[MetadataChangeProposalWrapper]:
    new_upstreams: List[models.UpstreamClass] = []
    new_fine_grained_lineages: List[models.FineGrainedLineageClass] = []
    # if this entity has upstream nodes defined, we'll want to do some work.
    # if no upstream nodes are present, we don't emit an MCP for it.
    if not entity_node.upstream:
        return None

    entity = entity_node.entity
    logger.info(f"Upstream detected for {entity}. Extracting urn...")
    entity_urn = _get_entity_urn(entity)
    if not entity_urn:
        logger.warning(
            f"Entity type: {entity.type} is unsupported. "
            f"Entity node {entity.name} and its upstream lineages will be skipped"
        )
        return None

    # extract the old lineage and save it for the new mcp
    if preserve_upstream:

        client = get_default_graph()

        old_upstream_lineage = get_aspects_for_entity(
            client._session,
            client.config.server,
            entity_urn=entity_urn,
            aspects=["upstreamLineage"],
            typed=True,
        ).get("upstreamLineage")
        if old_upstream_lineage:
            # Can't seem to get mypy to be happy about
            # `Argument 1 to "list" has incompatible type "Optional[Any]";
            # expected "Iterable[UpstreamClass]"`
            new_upstreams.extend(old_upstream_lineage.get("upstreams"))  # type: ignore

    for upstream_entity_node in entity_node.upstream:
        upstream_entity = upstream_entity_node.entity
        upstream_entity_urn = _get_entity_urn(upstream_entity)
        if upstream_entity_urn:
            new_upstreams.append(
                models.UpstreamClass(
                    dataset=upstream_entity_urn,
                    type=models.DatasetLineageTypeClass.TRANSFORMED,
                    auditStamp=models.AuditStampClass(
                        time=get_sys_time(), actor="urn:li:corpUser:ingestion"
                    ),
                )
            )
        else:
            logger.warning(
                f"Entity type: {upstream_entity.type} is unsupported. "
                f"Upstream lineage will be skipped for {upstream_entity.name}->{entity.name}"
            )
    for fine_grained_lineage in entity_node.fineGrainedLineages or []:
        new_fine_grained_lineages.append(
            models.FineGrainedLineageClass(
                upstreams=fine_grained_lineage.upstreams,
                upstreamType=fine_grained_lineage.upstreamType,
                downstreams=fine_grained_lineage.downstreams,
                downstreamType=fine_grained_lineage.downstreamType,
                confidenceScore=fine_grained_lineage.confidenceScore,
                transformOperation=fine_grained_lineage.transformOperation,
            )
        )

    return MetadataChangeProposalWrapper(
        entityUrn=entity_urn,
        aspect=models.UpstreamLineageClass(
            upstreams=new_upstreams,
            fineGrainedLineages=new_fine_grained_lineages,
        ),
    )
