from typing import List, Optional

import datahub.emitter.mce_builder as builder
from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.base_transformer import (
    BaseTransformer,
    SingleAspectTransformer,
)
from datahub.metadata.schema_classes import StatusClass


class MarkDatasetStatusConfig(ConfigModel):
    removed: bool


class MarkDatasetStatus(BaseTransformer, SingleAspectTransformer):
    """Transformer that marks status of each dataset."""

    ctx: PipelineContext
    config: MarkDatasetStatusConfig

    def __init__(self, config: MarkDatasetStatusConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    def aspect_name(self) -> str:
        return "status"

    def entity_types(self) -> List[str]:
        return ["dataset"]

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "MarkDatasetStatus":
        config = MarkDatasetStatusConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[builder.Aspect]
    ) -> Optional[builder.Aspect]:
        assert aspect is None or isinstance(aspect, StatusClass)
        status_aspect: StatusClass = aspect if aspect else StatusClass(removed=None)
        status_aspect.removed = self.config.removed
        return status_aspect  # type: ignore
