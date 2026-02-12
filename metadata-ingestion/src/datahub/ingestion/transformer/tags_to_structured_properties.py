import logging
from typing import Dict, Iterable, List, Optional, Tuple, Union, cast

from datahub.configuration.common import ConfigModel, TransformerSemanticsConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    MultipleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    StructuredPropertiesClass,
    StructuredPropertyValueAssignmentClass,
)
from datahub.metadata.urns import StructuredPropertyUrn, TagUrn

logger = logging.getLogger(__name__)


class StructuredPropertyMappingConfig(ConfigModel):
    """
    Configuration for mapping tags to a single structured property.

    The values list specifies which tag keywords should be mapped to this property.
    Future extensions may include values_pattern for regex-based matching.

    Example:
        values: ["Finance", "Sales", "Marketing"]
    """

    values: List[str]
    # Future extensions: values_pattern, etc.


class TagsToStructuredPropertiesConfig(TransformerSemanticsConfigModel):
    """
    Configuration for tags to structured properties transformer.

    This transformer can handle two types of tag formats:
    1. Key-value pair tags: "department:Finance" where department is the property key
    2. Keyword tags: "Finance" which is matched against configured property mappings
    """

    # Key-value tag processing
    process_key_value_tags: bool = False
    key_value_separator: str = ":"
    key_value_property_prefix: str = ""

    # Keyword tag mapping (inverted index)
    tag_structured_property_map: Dict[str, StructuredPropertyMappingConfig] = {}

    # Behavior flags
    remove_original_tags: bool = False


class TagsToStructuredPropertiesTransformer(BaseTransformer, MultipleAspectTransformer):
    """
    Transforms tags to structured properties.

    This transformer converts DataHub tags into structured properties, supporting:
    - Key-value pair tags (e.g., "dept:Finance") where the key becomes the property
    - Keyword tags mapped via configuration to predefined properties

    The transformer can operate in two modes:
    1. Keep original tags and add structured properties (remove_original_tags: false)
    2. Remove original tags and only keep structured properties (remove_original_tags: true)

    Configuration:
        process_key_value_tags: Enable parsing of key:value formatted tags
        key_value_separator: Separator character (default: ":")
        key_value_property_prefix: Prefix for property IDs from key-value tags
        tag_structured_property_map: Map of property ID to matching tag keywords
        remove_original_tags: Whether to remove original tags after conversion

    Example configuration:
        transformers:
          - type: "tags_to_structured_properties"
            config:
              process_key_value_tags: true
              key_value_separator: ":"
              key_value_property_prefix: "io.company."
              tag_structured_property_map:
                "io.company.department":
                  values: ["Finance", "Sales", "Marketing"]
                "io.company.dataClassification":
                  values: ["PII", "Confidential"]
              remove_original_tags: false
    """

    def __init__(self, config: TagsToStructuredPropertiesConfig, ctx: PipelineContext):
        super().__init__()
        self.config = config
        self.ctx = ctx

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "TagsToStructuredPropertiesTransformer":
        config = TagsToStructuredPropertiesConfig.model_validate(config_dict)
        return cls(config, ctx)

    def aspect_name(self) -> str:
        return "globalTags"

    def entity_types(self) -> List[str]:
        # TODO: Add other entity types that support tags or make this configurable
        return ["dataset", "chart", "dashboard", "dataJob", "dataFlow", "schemaField"]

    def transform_aspects(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Iterable[Tuple[str, Optional[Aspect]]]:
        """
        Transform tags into tags + structured properties.

        Returns tuples of (aspect_name, aspect):
        - First tuple: the original globalTags (possibly removed if config says so)
        - Second tuple: the new structuredProperties aspect (if any properties were mapped)
        """

        if not aspect:
            return []

        assert isinstance(aspect, GlobalTagsClass), (
            f"Expected GlobalTagsClass but got {type(aspect)}"
        )
        tags_aspect = aspect
        if not tags_aspect.tags:
            return []

        # Extract tag names (strip urn:li:tag: prefix)
        tag_names = [self._extract_tag_name(tag.tag) for tag in tags_aspect.tags]

        # Build structured properties mapping
        # Dict[property_id, List[values]] - yes, a property can have multiple values
        properties_dict: Dict[str, List[str]] = {}

        for tag_name in tag_names:
            # Try key-value parsing first
            if self.config.process_key_value_tags:
                kv_result = self._parse_key_value_tag(tag_name)
                if kv_result:
                    key, value = kv_result
                    prop_id = f"{self.config.key_value_property_prefix}{key}"
                    properties_dict.setdefault(prop_id, []).append(value)
                    continue

            # Try keyword mapping
            prop_id_result = self._find_property_for_keyword(tag_name)
            if prop_id_result:
                properties_dict.setdefault(prop_id_result, []).append(tag_name)
            else:
                logger.warning(
                    f"Tag '{tag_name}' on entity {entity_urn} does not match any configured property mapping"
                )

        # Build output aspects
        output_aspects: List[Tuple[str, Optional[Aspect]]] = []

        # First: original tags (or None to remove)
        if self.config.remove_original_tags:
            output_aspects.append(("globalTags", None))
        else:
            output_aspects.append(("globalTags", cast(Optional[Aspect], tags_aspect)))

        # Second: structured properties (if any were mapped)
        if properties_dict:
            structured_props = StructuredPropertiesClass(
                properties=[
                    StructuredPropertyValueAssignmentClass(
                        propertyUrn=StructuredPropertyUrn(prop_id).urn(),
                        values=cast(List[Union[str, float]], values),
                    )
                    for prop_id, values in properties_dict.items()
                ]
            )
            output_aspects.append(
                ("structuredProperties", cast(Optional[Aspect], structured_props))
            )

        logger.debug(
            f"Entity {entity_urn}: Transformed {len(tags_aspect.tags) if tags_aspect else 0} tags "
            f"into {len(properties_dict)} structured properties. "
            f"Output aspects: {[name for name, _ in output_aspects]}"
        )
        return output_aspects

    def _extract_tag_name(self, tag_urn: str) -> str:
        """
        Extract tag name from tag URN.

        Args:
            tag_urn: Tag URN like "urn:li:tag:Finance"

        Returns:
            Tag name like "Finance"
        """
        assert tag_urn.startswith("urn:li:tag:"), f"Expected tag URN but got: {tag_urn}"
        return TagUrn.from_string(tag_urn).name

    def _parse_key_value_tag(self, tag_name: str) -> Optional[Tuple[str, str]]:
        """
        Parse key-value formatted tags.

        Args:
            tag_name: Tag like "dept:Finance"

        Returns:
            Tuple of (key, value) or None if not in key-value format
        """
        if self.config.key_value_separator in tag_name:
            parts = tag_name.split(self.config.key_value_separator, 1)
            if len(parts) == 2:
                return parts[0], parts[1]
        return None

    def _find_property_for_keyword(self, keyword: str) -> Optional[str]:
        """
        Find the first property ID that contains this keyword in its values list.

        Args:
            keyword: Tag keyword to look up

        Returns:
            Property ID if found, None otherwise
        """
        for prop_id, mapping_config in self.config.tag_structured_property_map.items():
            if keyword in mapping_config.values:
                return prop_id
        return None
