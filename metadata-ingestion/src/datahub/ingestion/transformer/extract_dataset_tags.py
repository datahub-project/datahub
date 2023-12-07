import re
from enum import Enum
from typing import List, Optional, cast

from datahub.configuration.common import TransformerSemanticsConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTagsTransformer
from datahub.metadata.schema_classes import GlobalTagsClass, TagAssociationClass
from datahub.utilities.urns.dataset_urn import DatasetUrn


class ExtractTagsOption(Enum):
    URN = "urn"


class ExtractDatasetTagsConfig(TransformerSemanticsConfigModel):
    extract_tags_from: ExtractTagsOption = ExtractTagsOption.URN
    extract_tags_regex: str


class ExtractDatasetTags(DatasetTagsTransformer):
    """Transformer that add tags to datasets according to configuration by extracting from metadata. Currently only extracts from name."""

    def __init__(self, config: ExtractDatasetTagsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx: PipelineContext = ctx
        self.config: ExtractDatasetTagsConfig = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ExtractDatasetTags":
        config = ExtractDatasetTagsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def _get_tags_to_add(self, entity_urn: str) -> List[TagAssociationClass]:
        if self.config.extract_tags_from == ExtractTagsOption.URN:
            urn = DatasetUrn.create_from_string(entity_urn)
            match = re.search(self.config.extract_tags_regex, urn.get_dataset_name())
            if match:
                captured_group = match.group(1)
                tag = f"urn:li:tag:{captured_group}"
                return [TagAssociationClass(tag=tag)]
            return []
        else:
            raise NotImplementedError()

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_global_tags_aspect: GlobalTagsClass = cast(GlobalTagsClass, aspect)
        out_global_tags_aspect: GlobalTagsClass = GlobalTagsClass(tags=[])
        self.update_if_keep_existing(
            self.config, in_global_tags_aspect, out_global_tags_aspect
        )

        tags_to_add = self._get_tags_to_add(entity_urn)
        if tags_to_add is not None:
            out_global_tags_aspect.tags.extend(tags_to_add)

        return self.get_result_semantics(
            self.config, self.ctx.graph, entity_urn, out_global_tags_aspect
        )
