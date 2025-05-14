import logging
import re
from functools import lru_cache
from typing import Dict, List, Optional, Sequence, Union, cast

from datahub.configuration.common import ConfigModel
from datahub.configuration.validate_field_rename import pydantic_renamed_field
from datahub.emitter.mce_builder import Aspect, make_ownership_type_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    MetadataChangeProposalClass,
    OwnerClass,
    OwnershipClass,
    OwnershipTypeClass,
)
from datahub.utilities.urns.corp_group_urn import CorpGroupUrn
from datahub.utilities.urns.corpuser_urn import CorpuserUrn
from datahub.utilities.urns.tag_urn import TagUrn

logger = logging.getLogger(__name__)


class ExtractOwnersFromTagsConfig(ConfigModel):
    tag_pattern: str = ""
    is_user: bool = True
    tag_character_mapping: Optional[Dict[str, str]] = None
    email_domain: Optional[str] = None
    extract_owner_type_from_tag_pattern: bool = False
    owner_type: str = "TECHNICAL_OWNER"
    owner_type_urn: Optional[str] = None

    _rename_tag_prefix_to_tag_pattern = pydantic_renamed_field(
        "tag_prefix", "tag_pattern"
    )


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
    owner_mcps: List[MetadataChangeProposalWrapper]

    def __init__(self, config: ExtractOwnersFromTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.owner_mcps = []

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

    def convert_tag_as_per_mapping(self, tag: str) -> str:
        """
        Function to modify tag as per provided tag character mapping. It also handles the overlappings in the mapping.
        Eg: '--':'-' & '-':'@' should not cause incorrect mapping.
        """
        if self.config.tag_character_mapping:
            # indices list to keep track of the indices where replacements have been made
            indices: List[int] = list()
            for old_char in sorted(
                self.config.tag_character_mapping.keys(),
                key=len,
                reverse=True,
            ):
                new_char = self.config.tag_character_mapping[old_char]
                index = tag.find(old_char)
                while index != -1:
                    if index not in indices:
                        tag = tag[:index] + new_char + tag[index + len(old_char) :]
                        # Adjust indices for overlapping replacements
                        indices = [
                            (
                                each + (len(new_char) - len(old_char))
                                if each > index
                                else each
                            )
                            for each in indices
                        ]
                        indices.append(index)
                    # Find the next occurrence of old_char, starting from the next index
                    index = tag.find(old_char, index + len(new_char))
        return tag

    def handle_end_of_stream(
        self,
    ) -> Sequence[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        return self.owner_mcps

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_tags_aspect: Optional[GlobalTagsClass] = cast(GlobalTagsClass, aspect)
        if in_tags_aspect is None:
            return None
        tags = in_tags_aspect.tags
        owners: List[OwnerClass] = []

        for tag_class in tags:
            tag_str = TagUrn.from_string(tag_class.tag).name
            tag_str = self.convert_tag_as_per_mapping(tag_str)
            re_match = re.search(self.config.tag_pattern, tag_str)
            if re_match:
                owner_str = tag_str[re_match.end() :].strip()
                owner_urn_str = self.get_owner_urn(owner_str)
                owner_urn = (
                    str(CorpuserUrn(owner_urn_str))
                    if self.config.is_user
                    else str(CorpGroupUrn(owner_urn_str))
                )

                if self.config.extract_owner_type_from_tag_pattern:
                    if re_match.groups():
                        owners.append(
                            OwnerClass(
                                owner=owner_urn,
                                type=OwnershipTypeClass.CUSTOM,
                                typeUrn=make_ownership_type_urn(re_match.group(1)),
                            )
                        )
                else:
                    owner_type = get_owner_type(self.config.owner_type)
                    if owner_type == OwnershipTypeClass.CUSTOM:
                        assert self.config.owner_type_urn is not None, (
                            "owner_type_urn must be set if owner_type is CUSTOM"
                        )

                    owners.append(
                        OwnerClass(
                            owner=owner_urn,
                            type=owner_type,
                            typeUrn=self.config.owner_type_urn,
                        )
                    )

        self.owner_mcps.append(
            MetadataChangeProposalWrapper(
                entityUrn=entity_urn,
                aspect=OwnershipClass(
                    owners=owners,
                ),
            )
        )
        return aspect
