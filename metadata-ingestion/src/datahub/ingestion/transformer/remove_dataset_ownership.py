import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import (
    DatasetSnapshotClass,
    MetadataChangeEventClass,
    OwnershipClass,
)


class ClearDatasetOwnershipConfig(ConfigModel):
    pass


class SimpleRemoveDatasetOwnership(DatasetTransformer):
    """Transformer that clears all owners on each dataset."""

    def __init__(self, config: ClearDatasetOwnershipConfig, ctx: PipelineContext):
        pass

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleRemoveDatasetOwnership":
        config = ClearDatasetOwnershipConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not isinstance(mce.proposedSnapshot, DatasetSnapshotClass):
            return mce
        ownership = builder.get_or_add_aspect(
            mce,
            OwnershipClass(
                owners=[],
            ),
        )
        ownership.owners = []
        return mce
