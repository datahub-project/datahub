import dataclasses
import json
import logging
import pathlib
from typing import Dict, List, Optional

import click
from docgen_types import Plugin

from datahub.ingestion.api.decorators import SourceCapability, SupportStatus
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)


def load_plugin_capabilities(plugin_name: str) -> Optional[Plugin]:
    """Load plugin capabilities without generating full documentation."""
    logger.debug(f"Loading capabilities for {plugin_name}")

    try:
        class_or_exception = source_registry._ensure_not_lazy(plugin_name)
        if isinstance(class_or_exception, Exception):
            raise class_or_exception
        source_type = source_registry.get(plugin_name)
        logger.debug(f"Source class is {source_type}")

        if hasattr(source_type, "get_platform_name"):
            platform_name = source_type.get_platform_name()
        else:
            platform_name = plugin_name.title()

        platform_id = None
        if hasattr(source_type, "get_platform_id"):
            platform_id = source_type.get_platform_id()
        if platform_id is None:
            raise ValueError(f"Platform ID not found for {plugin_name}")

        plugin = Plugin(
            name=plugin_name,
            platform_id=platform_id,
            platform_name=platform_name,
            classname=".".join([source_type.__module__, source_type.__name__]),
        )

        if hasattr(source_type, "get_support_status"):
            plugin.support_status = source_type.get_support_status()

        if hasattr(source_type, "get_capabilities"):
            capabilities = list(source_type.get_capabilities())
            if capabilities:
                capabilities.sort(key=lambda x: x.capability.value)
                plugin.capabilities = capabilities
                return plugin
            else:
                logger.debug(f"Skipping {plugin_name} - no capabilities defined")
                return None
        else:
            logger.debug(f"Skipping {plugin_name} - no get_capabilities method")
            return None

    except Exception as e:
        logger.error(f"Failed to load capabilities for {plugin_name}: {e}")
        return None


@dataclasses.dataclass
class CapabilitySummary:
    """Summary of capabilities across all plugins."""

    total_plugins: int
    plugins_with_capabilities: int
    plugins_skipped: int
    skipped_plugin_names: List[str]  # List of plugin names that were skipped
    total_capabilities: int
    capability_breakdown: Dict[
        str, Dict[str, int]
    ]  # capability -> {supported: count, unsupported: count}
    capabilities_dict: Dict[str, str]  # capability -> description
    platform_summary: Dict[
        str, Dict[str, List[str]]
    ]  # platform_id -> {capability -> [plugin_names]}
    plugin_details: Dict[str, Dict]  # plugin_name -> detailed info


def generate_capability_summary() -> CapabilitySummary:
    """Generate a comprehensive summary of capabilities across all plugins."""

    capability_breakdown: Dict[str, Dict[str, int]] = {}
    capabilities_dict: Dict[str, str] = {}
    platform_summary: Dict[str, Dict[str, List[str]]] = {}
    plugin_details: Dict[str, Dict] = {}

    total_plugins = 0
    plugins_with_capabilities = 0
    plugins_skipped = 0
    skipped_plugin_names: List[str] = []
    total_capabilities = 0

    for capability in SourceCapability:
        capability_breakdown[capability.value] = {"supported": 0, "unsupported": 0}

    for plugin_name in sorted(source_registry.mapping.keys()):
        if plugin_name in {
            "snowflake-summary",
            "snowflake-queries",
            "bigquery-queries",
        }:
            logger.info(f"Skipping {plugin_name} as it is on the deny list")
            continue

        total_plugins += 1
        plugin = load_plugin_capabilities(plugin_name)

        if plugin is None:
            plugins_skipped += 1
            skipped_plugin_names.append(plugin_name)
            continue

        plugin_details[plugin_name] = {
            "platform_id": plugin.platform_id,
            "platform_name": plugin.platform_name,
            "classname": plugin.classname,
            "support_status": plugin.support_status.name
            if plugin.support_status != SupportStatus.UNKNOWN
            else None,
            "capabilities": [],
        }

        if plugin.platform_id not in platform_summary:
            platform_summary[plugin.platform_id] = {}
            for capability in SourceCapability:
                platform_summary[plugin.platform_id][capability.value] = []

        if plugin.capabilities:
            plugins_with_capabilities += 1
            for cap_setting in plugin.capabilities:
                total_capabilities += 1
                capability_name = cap_setting.capability.value

                if capability_name not in capabilities_dict:
                    capabilities_dict[capability_name] = cap_setting.description

                if cap_setting.supported:
                    capability_breakdown[capability_name]["supported"] += 1
                    platform_summary[plugin.platform_id][capability_name].append(
                        plugin_name
                    )
                else:
                    capability_breakdown[capability_name]["unsupported"] += 1

                plugin_details[plugin_name]["capabilities"].append(
                    {
                        "capability": capability_name,
                        "supported": cap_setting.supported,
                        "description": cap_setting.description,
                    }
                )

    return CapabilitySummary(
        total_plugins=total_plugins,
        plugins_with_capabilities=plugins_with_capabilities,
        plugins_skipped=plugins_skipped,
        skipped_plugin_names=skipped_plugin_names,
        total_capabilities=total_capabilities,
        capability_breakdown=capability_breakdown,
        capabilities_dict=capabilities_dict,
        platform_summary=platform_summary,
        plugin_details=plugin_details,
    )


def save_capability_report(summary: CapabilitySummary, output_dir: str) -> None:
    """Save the capability summary as JSON files."""

    output_path = pathlib.Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary_dict = dataclasses.asdict(summary)

    fields_to_exclude = [
        "capability_breakdown",
        "platform_summary",
        "capabilities_dict",
    ]
    for field in fields_to_exclude:
        if field in summary_dict:
            del summary_dict[field]

    summary_file = output_path / "capability_summary.json"
    with open(summary_file, "w") as f:
        json.dump(summary_dict, f, indent=2)

    logger.info(f"Capability summary saved to {summary_file}")


@click.command()
@click.option(
    "--output-dir",
    type=str,
    default="./autogenerated",
    help="Output directory for capability reports",
)
@click.option(
    "--source",
    type=str,
    required=False,
    help="Generate report for specific source only",
)
def generate_capability_report(output_dir: str, source: Optional[str] = None) -> None:
    """Generate a comprehensive capability report for all ingestion sources."""

    logger.info("Starting capability report generation...")

    if source:
        if source not in source_registry.mapping:
            logger.error(f"Source '{source}' not found in registry")
            return
        original_mapping = source_registry.mapping.copy()
        source_registry.mapping = {source: original_mapping[source]}

    try:
        summary = generate_capability_summary()
        save_capability_report(summary, output_dir)

        print("Capability Report Generation Complete")
        print("=====================================")
        print(f"Total plugins processed: {summary.total_plugins}")
        print(f"Plugins with capabilities: {summary.plugins_with_capabilities}")
        if summary.skipped_plugin_names:
            print(f"Plugins skipped (no capabilities): {summary.plugins_skipped}")
            print("Skipped plugin names:")
            for plugin_name in sorted(summary.skipped_plugin_names):
                print(f"  - {plugin_name}")
        else:
            print(f"Plugins skipped (no capabilities): {summary.plugins_skipped}")
        print(f"Output directory: {output_dir}")

    finally:
        if source:
            source_registry.mapping = original_mapping


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    generate_capability_report()
