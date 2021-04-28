from datahub.ingestion.api.registry import Registry
from datahub.ingestion.api.transform import Transformer

transform_registry = Registry[Transformer]()
