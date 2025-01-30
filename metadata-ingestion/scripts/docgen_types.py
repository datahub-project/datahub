from dataclasses import dataclass, field
from typing import Dict, List, Optional

from datahub.ingestion.api.decorators import CapabilitySetting, SupportStatus


@dataclass
class Plugin:
    # Required fields
    name: str
    platform_id: str
    platform_name: str
    classname: str

    # Optional documentation fields
    source_docstring: Optional[str] = None
    config_json_schema: Optional[str] = None
    config_md: Optional[str] = None
    custom_docs_pre: Optional[str] = None
    custom_docs_post: Optional[str] = None
    starter_recipe: Optional[str] = None

    # Optional metadata fields
    support_status: SupportStatus = SupportStatus.UNKNOWN
    filename: Optional[str] = None
    doc_order: Optional[int] = None

    # Lists with empty defaults
    capabilities: List[CapabilitySetting] = field(default_factory=list)
    extra_deps: List[str] = field(default_factory=list)


@dataclass
class Platform:
    # Required fields
    id: str
    name: str

    # Optional fields
    custom_docs_pre: Optional[str] = None
    plugins: Dict[str, Plugin] = field(default_factory=dict)

    def add_plugin(self, plugin_name: str, plugin: Plugin) -> None:
        """Helper method to add a plugin to the platform"""
        self.plugins[plugin_name] = plugin
