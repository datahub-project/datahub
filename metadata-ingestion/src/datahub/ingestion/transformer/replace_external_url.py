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

            out_dataset_properties_aspect.externalUrl = self.replace_url(
                self.config.input_pattern,
                self.config.replacement,
                in_dataset_properties_aspect.externalUrl,
            )

            return cast(Aspect, out_dataset_properties_aspect)


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
        in_container_properties_aspect: ContainerPropertiesClass = cast(
            ContainerPropertiesClass, aspect
        )
        if (
            not hasattr(in_container_properties_aspect, "externalUrl")
            or not in_container_properties_aspect.externalUrl
        ):
            return cast(Aspect, in_container_properties_aspect)
        else:
            out_container_properties_aspect: ContainerPropertiesClass = copy.deepcopy(
                in_container_properties_aspect
            )

            out_container_properties_aspect.externalUrl = self.replace_url(
                self.config.input_pattern,
                self.config.replacement,
                in_container_properties_aspect.externalUrl,
            )

            return cast(Aspect, out_container_properties_aspect)
