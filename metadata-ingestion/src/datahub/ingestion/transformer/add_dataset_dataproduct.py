import logging
from typing import Callable, Dict, List, Optional, Union

import pydantic

from datahub.configuration.common import ConfigModel, KeyValuePattern
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetDataproductTransformer,
)
from datahub.metadata.schema_classes import ContainerClass, MetadataChangeProposalClass
from datahub.specific.dataproduct import DataProductPatchBuilder

logger = logging.getLogger(__name__)


class AddDatasetDataProductConfig(ConfigModel):
    # dataset_urn -> data product urn
    get_data_product_to_add: Callable[[str], Optional[str]]

    _resolve_data_product_fn = pydantic_resolve_key("get_data_product_to_add")

    is_container: bool = False


class AddDatasetDataProduct(DatasetDataproductTransformer):
    """Transformer that adds dataproduct entity for provided dataset as its asset according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetDataProductConfig

    def __init__(self, config: AddDatasetDataProductConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(cls, config_dict: dict, ctx: PipelineContext) -> "AddDatasetDataProduct":
        config = AddDatasetDataProductConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        return None

    def handle_end_of_stream(
        self,
    ) -> List[Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]]:
        data_products: Dict[str, DataProductPatchBuilder] = {}
        data_products_container: Dict[str, DataProductPatchBuilder] = {}
        logger.debug("Generating dataproducts")
        for entity_urn in self.entity_map.keys():
            data_product_urn = self.config.get_data_product_to_add(entity_urn)
            is_container = self.config.is_container
            if data_product_urn:
                if data_product_urn not in data_products:
                    data_products[data_product_urn] = DataProductPatchBuilder(
                        data_product_urn
                    ).add_asset(entity_urn)
                else:
                    data_products[data_product_urn] = data_products[
                        data_product_urn
                    ].add_asset(entity_urn)

                if is_container:
                    assert self.ctx.graph
                    container_aspect = self.ctx.graph.get_aspect(
                        entity_urn, aspect_type=ContainerClass
                    )
                    if not container_aspect:
                        continue
                    container_urn = container_aspect.container
                    if data_product_urn not in data_products_container:
                        container_product = DataProductPatchBuilder(
                            data_product_urn
                        ).add_asset(container_urn)
                        data_products_container[data_product_urn] = container_product
                    else:
                        data_products_container[
                            data_product_urn
                        ] = data_products_container[data_product_urn].add_asset(
                            container_urn
                        )

        mcps: List[
            Union[MetadataChangeProposalWrapper, MetadataChangeProposalClass]
        ] = []
        for data_product in data_products.values():
            mcps.extend(list(data_product.build()))
        if is_container:
            for data_product in data_products_container.values():
                mcps.extend(list(data_product.build()))
        return mcps


class SimpleDatasetDataProductConfig(ConfigModel):
    dataset_to_data_product_urns: Dict[str, str]


class SimpleAddDatasetDataProduct(AddDatasetDataProduct):
    """Transformer that adds a specified dataproduct entity for provided dataset as its asset."""

    def __init__(self, config: SimpleDatasetDataProductConfig, ctx: PipelineContext):

        generic_config = AddDatasetDataProductConfig(
            get_data_product_to_add=lambda dataset_urn: config.dataset_to_data_product_urns.get(
                dataset_urn
            ),
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetDataProduct":
        config = SimpleDatasetDataProductConfig.parse_obj(config_dict)
        return cls(config, ctx)


class PatternDatasetDataProductConfig(ConfigModel):
    dataset_to_data_product_urns_pattern: KeyValuePattern = KeyValuePattern.all()
    is_container: bool = False

    @pydantic.root_validator(pre=True)
    def validate_pattern_value(cls, values: Dict) -> Dict:
        rules = values["dataset_to_data_product_urns_pattern"]["rules"]
        for key, value in rules.items():
            if isinstance(value, list) and len(value) > 1:
                raise ValueError(
                    "Same dataset cannot be an asset of two different data product."
                )
            elif isinstance(value, str):
                rules[key] = [rules[key]]
        return values


class PatternAddDatasetDataProduct(AddDatasetDataProduct):
    """Transformer that adds a specified dataproduct entity for provided dataset as its asset."""

    def __init__(self, config: PatternDatasetDataProductConfig, ctx: PipelineContext):
        dataset_to_data_product = config.dataset_to_data_product_urns_pattern
        generic_config = AddDatasetDataProductConfig(
            get_data_product_to_add=lambda dataset_urn: (
                dataset_to_data_product.value(dataset_urn)[0]
                if dataset_to_data_product.value(dataset_urn)
                else None
            ),
            is_container=config.is_container,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "PatternAddDatasetDataProduct":
        config = PatternDatasetDataProductConfig.parse_obj(config_dict)
        return cls(config, ctx)
