import logging
from abc import ABCMeta
from typing import List

from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)

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
