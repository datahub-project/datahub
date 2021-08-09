import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import DatasetTransformer
from datahub.metadata.schema_classes import MetadataChangeEventClass, StatusClass


class MarkDatasetStatusConfig(ConfigModel):
    removed: bool


class MarkDatasetStatus(DatasetTransformer):
    """Transformer that marks status of each dataset."""

    ctx: PipelineContext
    config: MarkDatasetStatusConfig

    def __init__(self, config: MarkDatasetStatusConfig, ctx: PipelineContext):
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MarkDatasetStatus":
        config = MarkDatasetStatusConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_one(self, mce: MetadataChangeEventClass) -> MetadataChangeEventClass:
        if not self.is_proposed_dataset_snapshot(mce):
            return mce
        status_aspect = builder.get_or_add_aspect(
            mce,
            StatusClass(
                removed=None,
            ),
        )
        status_aspect.removed = self.config.removed
        return mce
