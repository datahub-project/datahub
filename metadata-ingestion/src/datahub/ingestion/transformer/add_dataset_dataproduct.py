import logging
from typing import Callable, Dict, List, Optional, cast

from datahub.configuration.common import TransformerSemanticsConfigModel
from datahub.configuration.import_resolver import pydantic_resolve_key
from datahub.emitter.mce_builder import Aspect
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.dataset_transformer import (
    DatasetDataproductsTransformer,
)
from datahub.metadata.schema_classes import (
    DataProductAssociationClass,
    DataProductPropertiesClass,
)

logger = logging.getLogger(__name__)


class AddDatasetDataProductsConfig(TransformerSemanticsConfigModel):
    # dataset_urn -> data product urn
    get_data_products_to_add: Callable[[str], Optional[str]]

    _resolve_data_product_fn = pydantic_resolve_key("get_data_products_to_add")


class AddDatasetDataProducts(DatasetDataproductsTransformer):
    """Transformer that adds dataproduct entity for provided dataset as its asset according to a callback function."""

    ctx: PipelineContext
    config: AddDatasetDataProductsConfig

    def __init__(self, config: AddDatasetDataProductsConfig, ctx: PipelineContext):
        super().__init__()
        self.ctx = ctx
        self.config = config

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "AddDatasetDataProducts":
        config = AddDatasetDataProductsConfig.parse_obj(config_dict)
        return cls(config, ctx)

    def transform_aspect(
        self, entity_urn: str, aspect_name: str, aspect: Optional[Aspect]
    ) -> Optional[Aspect]:
        return None

    def handle_end_of_stream(self) -> List[MetadataChangeProposalWrapper]:
        data_products_mcps: Dict[str, MetadataChangeProposalWrapper] = {}

        logger.debug("Generating dataproducts")
        for entity_urn in self.entity_map.keys():
            data_product_urn = self.config.get_data_products_to_add(entity_urn)
            if data_product_urn:
                if data_product_urn not in data_products_mcps:
                    data_products_mcps[
                        data_product_urn
                    ] = MetadataChangeProposalWrapper(
                        entityUrn=data_product_urn,
                        aspect=DataProductPropertiesClass(
                            assets=[
                                DataProductAssociationClass(destinationUrn=entity_urn)
                            ],
                        ),
                    )
                else:
                    aspect = cast(
                        DataProductPropertiesClass,
                        data_products_mcps[data_product_urn].aspect,
                    )
                    if aspect.assets:
                        aspect.assets.append(
                            DataProductAssociationClass(destinationUrn=entity_urn)
                        )
        return list(data_products_mcps.values())


class SimpleDatasetDataProductConfig(TransformerSemanticsConfigModel):
    dataset_to_data_product_urns: Dict[str, str]


class SimpleAddDatasetDataProducts(AddDatasetDataProducts):
    """Transformer that adds a specified dataproduct entity for provided dataset as its asset."""

    def __init__(self, config: SimpleDatasetDataProductConfig, ctx: PipelineContext):

        generic_config = AddDatasetDataProductsConfig(
            get_data_products_to_add=lambda dataset_urn: config.dataset_to_data_product_urns.get(
                dataset_urn
            ),
            replace_existing=config.replace_existing,
            semantics=config.semantics,
        )
        super().__init__(generic_config, ctx)

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "SimpleAddDatasetDataProducts":
        config = SimpleDatasetDataProductConfig.parse_obj(config_dict)
        return cls(config, ctx)
