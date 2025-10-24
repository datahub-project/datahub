import copy
import re
from typing import Any, Dict, Optional, cast

from datahub.configuration.common import ConfigModel
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    ContainerPropertiesTransformer,
    DatasetPropertiesTransformer,
)
from datahub.metadata.schema_classes import (
    ContainerPropertiesClass,
    DatasetPropertiesClass,
)


class ReplaceExternalUrlConfig(ConfigModel):
    input_pattern: str
    replacement: str


class ReplaceUrl:
    def replace_url(self, pattern: str, replacement: str, external_url: str) -> str:
        pattern_obj = re.compile(pattern)
        return re.sub(pattern_obj, replacement, external_url)


class ReplaceExternalUrlDataset(DatasetPropertiesTransformer, ReplaceUrl):
    """Transformer that replace the external URL for dataset properties."""

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
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "ReplaceExternalUrlDataset":
        config = ReplaceExternalUrlConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        input_aspect: DatasetPropertiesClass = cast(DatasetPropertiesClass, aspect)

        if not hasattr(input_aspect, "externalUrl") or not input_aspect.externalUrl:
            return cast(Aspect, input_aspect)
        else:
            output_aspect: DatasetPropertiesClass = copy.deepcopy(input_aspect)

            output_aspect.baseExternalUrl = input_aspect.externalUrl
            output_aspect.externalUrl = self.replace_url(
                self.config.input_pattern,
                self.config.replacement,
                input_aspect.externalUrl,
            )

            return cast(Aspect, output_aspect)


class ReplaceExternalUrlContainer(ContainerPropertiesTransformer, ReplaceUrl):
    """Transformer that replace the external URL for container properties."""

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
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "ReplaceExternalUrlContainer":
        config = ReplaceExternalUrlConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        input_aspect: ContainerPropertiesClass = cast(ContainerPropertiesClass, aspect)
        if not hasattr(input_aspect, "externalUrl") or not input_aspect.externalUrl:
            return cast(Aspect, input_aspect)
        else:
            output_aspect: ContainerPropertiesClass = copy.deepcopy(input_aspect)

            output_aspect.baseExternalUrl = input_aspect.externalUrl
            output_aspect.externalUrl = self.replace_url(
                self.config.input_pattern,
                self.config.replacement,
                input_aspect.externalUrl,
            )

            return cast(Aspect, output_aspect)
