from typing import List, Optional, cast

from datahub.configuration.common import (
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import OwnershipClass, GlobalTagsClass, OwnerClass, OwnershipTypeClass
from datahub.utilities.urns.tag_urn import TagUrn


class ExtractOwnersFromTagsConfig(TransformerSemanticsConfigModel):
    tag_prefix: str
    # TODO Add type as an option here and use that instead of hard-coding type of owner


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

        in_tags_aspect: Optional[GlobalTagsClass] = cast(GlobalTagsClass, aspect)
        if in_tags_aspect is None:
            return None
        tags_str = in_tags_aspect.tags
        owners: List[OwnerClass] = []
        for tag in tags_str:
            tag_urn = TagUrn.create_from_string(tag)
            tag_str = tag_urn.get_entity_id()[0]
            if tag_str.startswith(self.config.tag_prefix):

                owner = OwnerClass(owner=tag_str[len(self.config.tag_prefix) :], type=OwnershipTypeClass.TECHNICAL_OWNER)
                owners.append(owner)

        owner_aspect = OwnershipClass(owners=owners)
        return cast(Aspect, owner_aspect)
