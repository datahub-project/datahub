from typing import Optional, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetOwnershipTransformer,
)
from datahub.metadata.schema_classes import OwnershipClass


class ClearDatasetOwnershipConfig(ConfigModel):
    pass


class SimpleRemoveDatasetOwnership(DatasetOwnershipTransformer):
    """Transformer that clears all owners on each dataset."""

    def __init__(self, config: ClearDatasetOwnershipConfig, ctx: PipelineContext):
        super().__init__()

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleRemoveDatasetOwnership":
        config = ClearDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_ownership_aspect = cast(OwnershipClass, aspect)
        if in_ownership_aspect is None:
            return None

        in_ownership_aspect.owners = []
        return cast(Aspect, in_ownership_aspect)
