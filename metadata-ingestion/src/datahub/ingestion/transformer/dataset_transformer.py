import logging
from abc import ABCMeta
from typing import List, Optional, cast

from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import GlobalTagsClass

log = logging.getLogger(__name__)


class DatasetTransformer(BaseTransformer, SingleAspectTransformer, metaclass=ABCMeta):
    """Transformer that does transform sequentially on each dataset."""

    def __init__(self):
        super().__init__()

    def entity_types(self) -> List[str]:
        return ["dataset"]


class DatasetOwnershipTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "ownership"


class DatasetDomainTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "domains"


class DatasetStatusTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "status"


class DatasetTagsTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "globalTags"

    @staticmethod
    def merge_with_server_global_tags(
        graph: DataHubGraph, urn: str, global_tags_aspect: Optional[GlobalTagsClass]
    ) -> Optional[GlobalTagsClass]:
        if not global_tags_aspect or not global_tags_aspect.tags:
            # nothing to add, no need to consult server
            return None

        # Merge the transformed tags with existing server tags.
        # The transformed tags takes precedence, which may change the tag context.
        server_global_tags_aspect = graph.get_tags(entity_urn=urn)
        if server_global_tags_aspect:
            global_tags_aspect.tags = list(
                {
                    **{tag.tag: tag for tag in server_global_tags_aspect.tags},
                    **{tag.tag: tag for tag in global_tags_aspect.tags},
                }.values()
            )

        return global_tags_aspect

    @staticmethod
    def update_if_keep_existing(
        config: TransformerSemanticsConfigModel,
        in_global_tags_aspect: GlobalTagsClass,
        out_global_tags_aspect: GlobalTagsClass,
    ) -> None:
        """Check if user want to keep existing tags"""
        if in_global_tags_aspect is not None and config.replace_existing is False:
            tags_seen = set()
            for item in in_global_tags_aspect.tags:
                if item.tag not in tags_seen:
                    out_global_tags_aspect.tags.append(item)
                    tags_seen.add(item.tag)

    @staticmethod
    def get_result_semantics(
        config: TransformerSemanticsConfigModel,
        graph: Optional[DataHubGraph],
        urn: str,
        out_global_tags_aspect: Optional[GlobalTagsClass],
    ) -> Optional[Aspect]:
        if config.semantics == TransformerSemantics.PATCH:
            assert graph
            return cast(
                Optional[Aspect],
                DatasetTagsTransformer.merge_with_server_global_tags(
                    graph, urn, out_global_tags_aspect
                ),
            )

        return cast(Aspect, out_global_tags_aspect)


class DatasetTermsTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "glossaryTerms"


class DatasetPropertiesTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "datasetProperties"


class DatasetBrowsePathsTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "browsePaths"


class DatasetSchemaMetadataTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "schemaMetadata"


class DatasetDataproductTransformer(DatasetTransformer, metaclass=ABCMeta):
    def aspect_name(self) -> str:
        return "dataProductProperties"
