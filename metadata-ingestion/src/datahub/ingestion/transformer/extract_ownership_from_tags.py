from typing import List, Optional, cast

import pydantic

from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import OwnershipClass


class ExtractOwnersFromTagsConfig(TransformerSemanticsConfigModel):
    tag_prefix: str


class ExtractOwnersFromTagsTransformer(DatasetTagsTransformer):
    """Transformer that can be used to set extract ownership from entity tags (currently does not support column level tags)"""

    ctx: PipelineContext
    config: ExtractOwnersFromTagsConfig

    def __init__(self, config: ExtractOwnersFromTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "ExtractOwnersFromTagsConfig":
        config = ExtractOwnersFromTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:

        owner_aspect = OwnershipClass(owners=[])
        return cast(Aspect, owner_aspect)
