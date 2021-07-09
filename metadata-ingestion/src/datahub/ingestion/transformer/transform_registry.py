from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.transformer.add_dataset_ownership import (
    AddDatasetOwnership,
    SimpleAddDatasetOwnership,
)
from datahub.ingestion.transformer.add_dataset_tags import (
    AddDatasetTags,
    SimpleAddDatasetTags,
)
from datahub.ingestion.transformer.modify_browsepath import(
    BrowsePathTransform
)

transform_registry = Registry[Transformer]()

transform_registry.register("add_dataset_ownership", AddDatasetOwnership)
transform_registry.register("simple_add_dataset_ownership", SimpleAddDatasetOwnership)

transform_registry.register("add_dataset_tags", AddDatasetTags)
transform_registry.register("simple_add_dataset_tags", SimpleAddDatasetTags)

transform_registry.register("remove_prod_prefix", BrowsePathTransform)
