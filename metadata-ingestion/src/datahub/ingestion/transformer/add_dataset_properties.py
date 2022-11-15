import copy
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, Type, cast

from datahub.configuration.common import (
    TransformerSemantics,
    TransformerSemanticsConfigModel,
)
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetPropertiesTransformer,
)
from datahub.metadata.schema_classes import DatasetPropertiesClass


class AddDatasetPropertiesResolverBase(ABC):
    @abstractmethod
    def get_properties_to_add(self, entity_urn: str) -> Dict[str, str]:
        pass


class AddDatasetPropertiesConfig(TransformerSemanticsConfigModel):
    add_properties_resolver_class: Type[AddDatasetPropertiesResolverBase]

    class Config:
        arbitrary_types_allowed = True

    _resolve_properties_class = pydantic_resolve_key("add_properties_resolver_class")


class AddDatasetProperties(DatasetPropertiesTransformer):
    """Transformer that adds properties to datasets according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetPropertiesConfig

    def __init__(
        self,
        config: AddDatasetPropertiesConfig,
        ctx: PipelineContext,
        **resolver_args: Dict[str, Any],
    ):
        super().__init__()
        self.ctx = ctx
        self.config = config
        self.resolver_args = resolver_args

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetProperties":
        config = AddDatasetPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)

    @staticmethod
    def get_patch_dataset_properties_aspect(
        graph: DataHubGraph,
        entity_urn: str,
        dataset_properties_aspect: Optional[DatasetPropertiesClass],
    ) -> Optional[DatasetPropertiesClass]:
        assert dataset_properties_aspect

        server_dataset_properties_aspect: Optional[
            DatasetPropertiesClass
        ] = graph.get_dataset_properties(entity_urn)
        # No need to take any action if server properties is None or there is not customProperties in server properties
        if (
            server_dataset_properties_aspect is None
            or not server_dataset_properties_aspect.customProperties
        ):
            return dataset_properties_aspect

        custom_properties_to_add = server_dataset_properties_aspect.customProperties
        # Give precedence to ingestion custom properties
        # if same property exist on server as well as in input aspect then value of input aspect would get set in output
        custom_properties_to_add.update(dataset_properties_aspect.customProperties)

        patch_dataset_properties: DatasetPropertiesClass = copy.deepcopy(
            dataset_properties_aspect
        )
        patch_dataset_properties.customProperties = custom_properties_to_add

        return patch_dataset_properties

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:

        in_dataset_properties_aspect: DatasetPropertiesClass = cast(
            DatasetPropertiesClass, aspect
        )
        if not in_dataset_properties_aspect:
            in_dataset_properties_aspect = DatasetPropertiesClass()

        out_dataset_properties_aspect: DatasetPropertiesClass = copy.deepcopy(
            in_dataset_properties_aspect
        )
        if self.config.replace_existing is True:
            # clean the existing properties
            out_dataset_properties_aspect.customProperties = {}

        properties_to_add = self.config.add_properties_resolver_class(  # type: ignore
            **self.resolver_args
        ).get_properties_to_add(entity_urn)

        out_dataset_properties_aspect.customProperties.update(properties_to_add)
        if self.config.semantics == TransformerSemantics.PATCH:
            assert self.ctx.graph
            patch_dataset_properties_aspect = (
                AddDatasetProperties.get_patch_dataset_properties_aspect(
                    self.ctx.graph, entity_urn, out_dataset_properties_aspect
                )
            )
            out_dataset_properties_aspect = (
                patch_dataset_properties_aspect
                if patch_dataset_properties_aspect is not None
                else out_dataset_properties_aspect
            )

        return cast(Aspect, out_dataset_properties_aspect)


class SimpleAddDatasetPropertiesConfig(TransformerSemanticsConfigModel):
    properties: Dict[str, str]


class SimpleAddDatasetPropertiesResolverClass(AddDatasetPropertiesResolverBase):
    def __init__(self, properties: Dict[str, str]):
        self.properties = properties

    def get_properties_to_add(self, entity_urn: str) -> Dict[str, str]:
        return self.properties


class SimpleAddDatasetProperties(AddDatasetProperties):
    """Transformer that adds a specified set of properties to each dataset."""

    def __init__(self, config: SimpleAddDatasetPropertiesConfig, ctx: PipelineContext):
        generic_config = AddDatasetPropertiesConfig(
            add_properties_resolver_class=SimpleAddDatasetPropertiesResolverClass,
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        resolver_args = {"properties": config.properties}
        super().__init__(generic_config, ctx, **resolver_args)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetProperties":
        config = SimpleAddDatasetPropertiesConfig.parse_obj(config_dict)
        return cls(config, ctx)
