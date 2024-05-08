import copy
import re
from typing import Any, Dict, Optional, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetPropertiesTransformer,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass


class ReplaceExternalUrlConfig(ConfigModel):
    input_pattern: str
    replacement: str


class ReplaceExternalUrl(DatasetPropertiesTransformer):
    """Transformer that clean the ownership URN."""

    ctx: PipelineContext
    config: ReplaceExternalUrlConfig

    def __init__(
        self,
        config: ReplaceExternalUrlConfig,
        ctx: PipelineContext,
        **resolver_args: Dict[str, Any],
    ):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.resolver_args = resolver_args

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "ReplaceExternalUrl":
        config = ReplaceExternalUrlConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        in_dataset_properties_aspect: DatasetPropertiesClass = cast(
            DatasetPropertiesClass, aspect
        )

        if (
            not hasattr(in_dataset_properties_aspect, "externalUrl")
            or not in_dataset_properties_aspect.externalUrl
        ):
            return cast(Aspect, in_dataset_properties_aspect)
        else:
            out_dataset_properties_aspect: DatasetPropertiesClass = copy.deepcopy(
                in_dataset_properties_aspect
            )

            pattern = re.compile(self.config.input_pattern)
            replacement = self.config.replacement

            out_dataset_properties_aspect.externalUrl = re.sub(
                pattern, replacement, in_dataset_properties_aspect.externalUrl
            )

            return cast(Aspect, out_dataset_properties_aspect)
