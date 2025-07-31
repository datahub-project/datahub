import logging
import re
from typing import List, Optional, Sequence, Tuple, cast

from pydantic import validator

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    MetadataChangeProposalClass,
    StructuredPropertyDefinitionClass,
)
from datahub.metadata.urns import TagUrn
from datahub.specific.aspect_helpers.structured_properties import (
    HasStructuredPropertiesPatch,
)

logger = logging.getLogger(__name__)


class TagPattern(ConfigModel):
    tag_pattern: re.Pattern = re.compile("")
    split_pattern: Optional[re.Pattern] = None
    structured_property_urn: str = ""

    @validator("tag_pattern")
    def tag_pattern_must_be_regex_with_one_capture_group(cls, value):
        if value.groups != 1:
            raise Exception(f"must have one and only one capture group: {repr(value)}")
        return value


class ExtractStructuredPropertiesFromTagsConfig(ConfigModel):
    entity_types: List[str] = ["*"]
    rules: List[TagPattern] = []


class ExtractStructuredPropertiesFromTagsTransformer(
    BaseTransformer, SingleAspectTransformer
):
    """Transformer that adds owners to datasets according to a callback function."""

    ctx: PipelineContext
    config: ExtractStructuredPropertiesFromTagsConfig

    def __init__(
        self, config: ExtractStructuredPropertiesFromTagsConfig, ctx: PipelineContext
    ):
        super().__init__()
        self.structured_property_mcps: List[MetadataChangeProposalClass] = []
        self.ctx = ctx
        self.config = config

        if self.ctx.graph:
            for r in self.config.rules:
                structured_property = self.ctx.graph.get_aspect(
                    entity_urn=r.structured_property_urn,
                    aspect_type=StructuredPropertyDefinitionClass,
                )
                if structured_property is None:
                    raise Exception(
                        f"Couldn't find StructuredProperty {repr(r.structured_property_urn)}"
                    )

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "ExtractStructuredPropertiesFromTagsTransformer":
        config = ExtractStructuredPropertiesFromTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def entity_types(self) -> List[str]:
        return self.config.entity_types

    def aspect_name(self) -> str:
        return "globalTags"

    def handle_end_of_stream(self) -> Sequence[MetadataChangeProposalClass]:
        return self.structured_property_mcps

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_global_tags = cast(GlobalTagsClass, aspect)
        if not in_global_tags:
            return aspect

        logger.debug(f"Processing {entity_urn}: {aspect}")

        # Iterate over tags, trying to match against the rules for extracting structured
        # properties.
        properties: List[Tuple[str, List[str | float]]] = []
        non_matching_tags = []
        for tag_association in in_global_tags.tags:
            tag_urn = TagUrn.from_string(tag_association.tag)
            for rule in self.config.rules:
                m = re.search(rule.tag_pattern, tag_urn.name)
                if m:
                    matched_text = m[1].strip()
                    if rule.split_pattern:
                        values = re.split(rule.split_pattern, matched_text)
                    elif matched_text:
                        values = [matched_text]
                    else:
                        values = []

                    logger.info(
                        f"Matched {entity_urn} tag {repr(tag_urn.name)} with {repr(rule.tag_pattern.pattern)}, "
                        + (
                            f"splitting with {repr(rule.split_pattern)}, "
                            if rule.split_pattern
                            else ""
                        )
                        + f"setting structured property {rule.structured_property_urn} to {repr(values)}"
                    )
                    properties.append((rule.structured_property_urn, values))
                    break
            else:
                non_matching_tags.append(tag_association)

        # If we matched any tags, then generate an MCP to set structured properties,
        # as well as a new MCP for tags that removes the matched ones.
        if properties:
            patch_builder = HasStructuredPropertiesPatch(entity_urn)
            for property_urn, values in properties:
                patch_builder.add_structured_property(property_urn, values)
            patch_mcps = patch_builder.build()

            # Emit Dataset Patch
            for patch_mcp in patch_mcps:
                # TODO: add modified date, created_by, etc.
                self.structured_property_mcps.append(patch_mcp)

            return cast(Aspect, GlobalTagsClass(tags=non_matching_tags))

        else:
            return aspect
