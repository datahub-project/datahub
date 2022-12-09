from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.transform import Transformer

transform_registry = PluginRegistry[Transformer]()
transform_registry.register_from_entrypoint("datahub.ingestion.transformer.plugins")

# These transformers are always enabled
assert transform_registry.get("simple_remove_dataset_ownership")
assert transform_registry.get("mark_dataset_status")
assert transform_registry.get("set_dataset_browse_path")
assert transform_registry.get("add_dataset_ownership")
assert transform_registry.get("simple_add_dataset_ownership")
assert transform_registry.get("pattern_add_dataset_ownership")
assert transform_registry.get("add_dataset_domain")
assert transform_registry.get("simple_add_dataset_domain")
assert transform_registry.get("pattern_add_dataset_domain")
assert transform_registry.get("add_dataset_tags")
assert transform_registry.get("simple_add_dataset_tags")
assert transform_registry.get("pattern_add_dataset_tags")
assert transform_registry.get("add_dataset_terms")
assert transform_registry.get("simple_add_dataset_terms")
assert transform_registry.get("pattern_add_dataset_terms")
assert transform_registry.get("add_dataset_properties")
assert transform_registry.get("simple_add_dataset_properties")
assert transform_registry.get("pattern_add_dataset_schema_terms")
assert transform_registry.get("pattern_add_dataset_schema_tags")
