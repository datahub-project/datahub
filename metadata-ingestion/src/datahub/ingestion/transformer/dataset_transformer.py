import logging
from abc import abstractmethod
from typing import List, Optional

from deprecated import deprecated

from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    LegacyMCETransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
)

log = logging.getLogger(__name__)


@deprecated(
    reason="Legacy transformer that supports transforming MCE-s using transform_one method. Use BaseTransformer directly and implement the transform_aspect method"
)
class DatasetTransformer(BaseTransformer, LegacyMCETransformer):
    """Transformer that does transforms sequentially on each dataset."""

    def __init__(self):
        super().__init__()

    def entity_types(self) -> List[str]:
        return ["dataset"]

    @deprecated(
        reason="preserved for backwards compatibility, subclasses should use transform_aspect directly instead"
    )
    @abstractmethod
    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        pass

    def transform_aspect(  # not marked as @abstractmethod to avoid impacting transformers that extend this class
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        """A default implementation for transform_aspect that calls `transform_one` with a fake MCE to preserve compatibility with previous transformers coded against MCE"""
        fake_mce: MetadataChangeEventClass = MetadataChangeEventClass(
            proposedSnapshot=DatasetSnapshotClass(
                urn=entity_urn,
                aspects=[aspect] if aspect else [],  # type: ignore
            )
        )
        transformed_mce = self.transform_one(fake_mce)
        assert transformed_mce.proposedSnapshot
        assert (
            len(transformed_mce.proposedSnapshot.aspects) <= 1
        ), "This implementation assumes that transformers will return at most 1 aspect value back"
        return (
            transformed_mce.proposedSnapshot.aspects[0]  # type: ignore
            if len(transformed_mce.proposedSnapshot.aspects)
            else None
        )


class DatasetOwnershipTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "ownership"


class DatasetStatusTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "status"


class DatasetTagsTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "globalTags"


class DatasetTermsTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "glossaryTerms"


class DatasetPropertiesTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "datasetProperties"


class DatasetBrowsePathsTransformer(DatasetTransformer, SingleAspectTransformer):
    def aspect_name(self) -> str:
        return "browsePaths"
