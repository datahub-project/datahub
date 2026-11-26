from typing import Dict, Optional

from datahub.configuration.common import ConfigModel
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.transformer.add_dataset_dataproduct import (
    AddDatasetDataProduct,
    AddDatasetDataProductConfig,
)


class DatasetTagDataProductMapperConfig(ConfigModel):
    dataproduct_mapping: Dict[str, str]


class DatasetTagDataProductMapper(AddDatasetDataProduct):
    """Transformer that adds a dataset to a data product based on tags present on the dataset."""

    def __init__(self, config: DatasetTagDataProductMapperConfig, ctx: PipelineContext):
        dataproduct_mapping = config.dataproduct_mapping

        def get_data_product_to_add(dataset_urn: str) -> Optional[str]:
            assert ctx.graph
            global_tags = ctx.graph.get_tags(dataset_urn)
            if not global_tags:
                return None
            for tag_item in global_tags.tags:
                tag = tag_item.tag.split("urn:li:tag:")[-1]
                if tag in dataproduct_mapping:
                    return dataproduct_mapping[tag]
            return None

        super().__init__(
            AddDatasetDataProductConfig(
                get_data_product_to_add=get_data_product_to_add
            ),
            ctx,
        )

    @classmethod
    def create(
        cls, config_dict: dict, ctx: PipelineContext
    ) -> "DatasetTagDataProductMapper":
        config = DatasetTagDataProductMapperConfig.model_validate(config_dict)
        return cls(config, ctx)
