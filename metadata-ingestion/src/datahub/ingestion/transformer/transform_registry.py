from datahub.ingestion.api.registry import PluginRegistry
from datahub.ingestion.api.transform import Transformer
from datahub.plugin.plugin_config import PluginCapabilityType
from datahub.plugin.plugin_loader import get_plugin_loader

transform_registry = PluginRegistry[Transformer](
    plugin_loader=get_plugin_loader(),
    registry_type=PluginCapabilityType.TRANSFORMER,
)
transform_registry.register_from_entrypoint("datahub.ingestion.transformer.plugins")
