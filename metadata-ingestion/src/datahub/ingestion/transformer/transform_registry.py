from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.transform import Transformer
from datahub.ingestion.transformer.add_dataset_browse_path import (
    AddDatasetBrowsePathTransformer,
)
from datahub.ingestion.transformer.add_dataset_ownership import (
    AddDatasetOwnership,
    PatternAddDatasetOwnership,
    SimpleAddDatasetOwnership,
)
from datahub.ingestion.transformer.add_dataset_properties import (
    AddDatasetProperties,
    SimpleAddDatasetProperties,
)
from datahub.ingestion.transformer.add_dataset_schema_tags import (
    PatternAddDatasetSchemaTags,
)
from datahub.ingestion.transformer.add_dataset_schema_terms import (
    PatternAddDatasetSchemaTerms,
)
from datahub.ingestion.transformer.add_dataset_tags import (
    AddDatasetTags,
    PatternAddDatasetTags,
    SimpleAddDatasetTags,
)
from datahub.ingestion.transformer.add_dataset_terms import (
    AddDatasetTerms,
    PatternAddDatasetTerms,
    SimpleAddDatasetTerms,
)
from datahub.ingestion.transformer.mark_dataset_status import MarkDatasetStatus
from datahub.ingestion.transformer.remove_dataset_ownership import (
    SimpleRemoveDatasetOwnership,
)

transform_registry = PluginRegistry[Transformer]()

transform_registry.register(
    "simple_remove_dataset_ownership", SimpleRemoveDatasetOwnership
)
transform_registry.register("mark_dataset_status", MarkDatasetStatus)
transform_registry.register("set_dataset_browse_path", AddDatasetBrowsePathTransformer)

transform_registry.register("add_dataset_ownership", AddDatasetOwnership)
transform_registry.register("simple_add_dataset_ownership", SimpleAddDatasetOwnership)
transform_registry.register("pattern_add_dataset_ownership", PatternAddDatasetOwnership)

transform_registry.register("add_dataset_tags", AddDatasetTags)
transform_registry.register("simple_add_dataset_tags", SimpleAddDatasetTags)
transform_registry.register("pattern_add_dataset_tags", PatternAddDatasetTags)

transform_registry.register("add_dataset_terms", AddDatasetTerms)
transform_registry.register("simple_add_dataset_terms", SimpleAddDatasetTerms)
transform_registry.register("pattern_add_dataset_terms", PatternAddDatasetTerms)

transform_registry.register("add_dataset_properties", AddDatasetProperties)
transform_registry.register("simple_add_dataset_properties", SimpleAddDatasetProperties)

transform_registry.register(
    "pattern_add_dataset_schema_terms", PatternAddDatasetSchemaTerms
)
transform_registry.register(
    "pattern_add_dataset_schema_tags", PatternAddDatasetSchemaTags
)
