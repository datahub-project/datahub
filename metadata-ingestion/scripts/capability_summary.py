import dataclasses
import json
import logging
import pathlib
from datetime import datetime, timezone
from typing import Dict, List, Optional

import click
from docgen_types import Plugin

from datahub.ingestion.api.decorators import SupportStatus
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
            else:
                logger.debug(f"No capabilities defined for {plugin_name}")
                plugin.capabilities = []
        else:
            logger.debug(f"No get_capabilities method for {plugin_name}")
            plugin.capabilities = []

        return plugin

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
    plugin_details: Dict[str, Dict]  # plugin_name -> detailed info


def generate_capability_summary() -> CapabilitySummary:
    """Generate a comprehensive summary of capabilities across all plugins."""

    plugin_details: Dict[str, Dict] = {}

    total_plugins = 0
    plugins_with_capabilities = 0
    plugins_skipped = 0
    skipped_plugin_names: List[str] = []
    total_capabilities = 0

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

        if plugin.capabilities:
            plugins_with_capabilities += 1
            for cap_setting in plugin.capabilities:
                total_capabilities += 1
                capability_name = cap_setting.capability.name

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
        plugin_details=plugin_details,
    )


def save_capability_report(summary: CapabilitySummary, output_dir: str) -> None:
    """Save the capability summary as JSON files, but only write if contents have changed."""

    output_path = pathlib.Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    summary_dict = dataclasses.asdict(summary)
    summary_dict["generated_by"] = "metadata-ingestion/scripts/capability_summary.py"
    summary_dict["generated_at"] = datetime.now(timezone.utc).isoformat()
    summary_json = json.dumps(summary_dict, indent=2, sort_keys=True)

    summary_file = output_path / "capability_summary.json"
    write_file = True
    if summary_file.exists():
        try:
            with open(summary_file, "r") as f:
                existing_data = json.load(f)

            # Create copies without generated_at for comparison
            existing_for_comparison = existing_data.copy()
            new_for_comparison = summary_dict.copy()
            existing_for_comparison.pop("generated_at", None)
            new_for_comparison.pop("generated_at", None)

            if json.dumps(
                existing_for_comparison, indent=2, sort_keys=True
            ) == json.dumps(new_for_comparison, indent=2, sort_keys=True):
                logger.info(f"No changes detected in {summary_file}, skipping write.")
                write_file = False
        except Exception as e:
            logger.warning(f"Could not read existing summary file: {e}")
    if write_file:
        with open(summary_file, "w") as f:
            f.write(summary_json)
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
