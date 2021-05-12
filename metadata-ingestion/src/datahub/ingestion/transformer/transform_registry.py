from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.transform import Transformer

from .add_dataset_ownership import AddDatasetOwnership, SimpleAddDatasetOwnership

transform_registry = Registry[Transformer]()

transform_registry.register("add_dataset_ownership", AddDatasetOwnership)
transform_registry.register("simple_add_dataset_ownership", SimpleAddDatasetOwnership)
