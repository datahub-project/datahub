from typing import Dict, Optional

from datahub.configuration.common import ConfigModel, ConfigurationError
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
        graph = ctx.graph
        # Tags are only read at the end of the stream, so a missing graph would
        # otherwise surface after the whole source has run.
        if graph is None:
            raise ConfigurationError(
                "dataproduct_mapping_based_on_tags reads tags from DataHub, so it "
                "requires a datahub_api to connect to. Consider using the "
                "datahub-rest sink or provide a datahub_api: configuration on your "
                "ingestion recipe"
            )
        dataproduct_mapping = config.dataproduct_mapping

        def get_data_product_to_add(dataset_urn: str) -> Optional[str]:
            global_tags = graph.get_tags(dataset_urn)
            if not global_tags:
                return None
            tags = {
                tag_item.tag.split("urn:li:tag:")[-1] for tag_item in global_tags.tags
            }
            # A dataset is added to one data product, as with the other data
            # product transformers. The first mapped tag in the config wins, so
            # the choice doesn't depend on the order of the dataset's tags.
            for tag, data_product_urn in dataproduct_mapping.items():
                if tag in tags:
                    return data_product_urn
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
