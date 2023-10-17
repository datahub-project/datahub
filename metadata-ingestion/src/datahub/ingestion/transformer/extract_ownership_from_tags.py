import re
from functools import lru_cache
from typing import List, Optional, cast

from datahub.configuration.common import TransformerSemanticsConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities.urns.corp_group_urn import CorpGroupUrn
from datahub.utilities.urns.corpuser_urn import CorpuserUrn
from datahub.utilities.urns.tag_urn import TagUrn


class ExtractOwnersFromTagsConfig(TransformerSemanticsConfigModel):
    tag_prefix: str
    is_user: bool = True
    email_domain: Optional[str] = None
    owner_type: str = "TECHNICAL_OWNER"
    owner_type_urn: Optional[str] = None


@lru_cache(maxsize=10)
def get_owner_type(owner_type_str: str) -> str:
    for item in dir(OwnershipTypeClass):
        if str(item) == owner_type_str:
            return item
    return OwnershipTypeClass.CUSTOM


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
    ) -> "ExtractOwnersFromTagsTransformer":
        config = ExtractOwnersFromTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def get_owner_urn(self, owner_str: str) -> str:
        if self.config.email_domain is not None:
            return owner_str + "@" + self.config.email_domain
        return owner_str

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_tags_aspect: Optional[GlobalTagsClass] = cast(GlobalTagsClass, aspect)
        if in_tags_aspect is None:
            return None
        tags = in_tags_aspect.tags
        owners: List[OwnerClass] = []
        for tag_class in tags:
            tag_urn = TagUrn.create_from_string(tag_class.tag)
            tag_str = tag_urn.get_entity_id()[0]
            re_match = re.search(self.config.tag_prefix, tag_str)
            if re_match:
                owner_str = tag_str[re_match.end() :].strip()
                owner_urn_str = self.get_owner_urn(owner_str)
                if self.config.is_user:
                    owner_urn = str(CorpuserUrn.create_from_id(owner_urn_str))
                else:
                    owner_urn = str(CorpGroupUrn.create_from_id(owner_urn_str))
                owner_type = get_owner_type(self.config.owner_type)
                if owner_type == OwnershipTypeClass.CUSTOM:
                    assert (
                        self.config.owner_type_urn is not None
                    ), "owner_type_urn must be set if owner_type is CUSTOM"
                owner = OwnerClass(
                    owner=owner_urn,
                    type=owner_type,
                    typeUrn=self.config.owner_type_urn,
                )
                owners.append(owner)

        owner_aspect = OwnershipClass(owners=owners)
        return cast(Aspect, owner_aspect)
