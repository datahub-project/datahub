import dataclasses
import json
import logging
import pathlib
from typing import Dict, Optional

import click
from docgen_types import Plugin
from utils import should_write_json_file

from datahub.ingestion.api.decorators import SupportStatus
from datahub.ingestion.source.source_registry import source_registry

logger = logging.getLogger(__name__)


def get_package_name(classname: str) -> str:
    """Extract top-level package from classname."""
    if "." not in classname:
        return "unknown"
    return classname.split(".")[0]


DENY_LIST = {
    "snowflake-summary",
    "snowflake-queries",
    "bigquery-queries",
    "datahub-mock-data",
}


def load_plugin_capabilities(plugin_name: str) -> Optional[Plugin]:
    """Load plugin capabilities without generating full documentation."""
    logger.debug(f"Loading capabilities for {plugin_name}")

    try:
        class_or_exception = source_registry._ensure_not_lazy(plugin_name)
        if isinstance(class_or_exception, Exception):
            # Log the specific error but don't re-raise it
            logger.warning(f"Plugin {plugin_name} failed to load: {class_or_exception}")
            return None
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
            logger.warning(f"Platform ID not found for {plugin_name}")
            return None

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
        logger.warning(f"Failed to load capabilities for {plugin_name}: {e}")
        return None


@dataclasses.dataclass
class ConnectorRegistry:
    """Registry of connector metadata across all plugins."""

    plugin_details: Dict[str, Dict]  # plugin_name -> detailed info

    def group_by_package(self) -> Dict[str, "ConnectorRegistry"]:
        """Group plugins by top-level package name."""
        packages: Dict[str, Dict[str, Dict]] = {}

        for plugin_name, plugin_data in self.plugin_details.items():
            classname = plugin_data.get("classname", "")
            package = get_package_name(classname)

            if package not in packages:
                packages[package] = {}
            packages[package][plugin_name] = plugin_data

        return {
            pkg: ConnectorRegistry(plugin_details=plugins)
            for pkg, plugins in packages.items()
        }


def generate_connector_registry() -> ConnectorRegistry:
    """Generate a comprehensive registry of connector metadata across all plugins."""

    plugin_details: Dict[str, Dict] = {}

    for plugin_name in sorted(source_registry.mapping.keys()):
        if plugin_name in DENY_LIST:
            logger.info(f"Skipping {plugin_name} as it is on the deny list")
            continue

        plugin = load_plugin_capabilities(plugin_name)

        if plugin is None:
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
            for cap_setting in plugin.capabilities:
                capability_name = cap_setting.capability.name

                plugin_details[plugin_name]["capabilities"].append(
                    {
                        "capability": capability_name,
                        "supported": cap_setting.supported,
                        "description": cap_setting.description,
                        "subtype_modifier": [m for m in cap_setting.subtype_modifier]
                        if cap_setting.subtype_modifier
                        else None,
                    }
                )

    return ConnectorRegistry(
        plugin_details=plugin_details,
    )


def save_connector_registry(registry: ConnectorRegistry, output_dir: str) -> None:
    """Save the connector registry as package-based JSON files in connector_registry/ directory."""

    output_path = pathlib.Path(output_dir) / "connector_registry"
    output_path.mkdir(parents=True, exist_ok=True)

    # Load existing package files for fallback
    existing_package_data: Dict[str, Dict[str, Dict]] = {}
    if output_path.exists():
        for existing_file in output_path.glob("*.json"):
            if existing_file.stem == "manifest":
                continue
            try:
                with open(existing_file, "r") as f:
                    package_data = json.load(f)
                    package_name = existing_file.stem
                    existing_package_data[package_name] = package_data.get(
                        "plugin_details", {}
                    )
                    logger.info(
                        f"Loaded existing connector data for package '{package_name}': {len(existing_package_data[package_name])} plugins"
                    )
            except Exception as e:
                logger.warning(f"Failed to load existing file {existing_file}: {e}")

    # Group plugins by package
    package_registries = registry.group_by_package()

    # Check for missing plugins and use fallback data
    for package_name, package_registry in package_registries.items():
        if package_name in existing_package_data:
            existing_plugins = existing_package_data[package_name]
            current_plugins = package_registry.plugin_details

            missing_plugins = set(existing_plugins.keys()) - set(current_plugins.keys())
            for plugin_name in missing_plugins:
                logger.warning(
                    f"Plugin {plugin_name} (package: {package_name}) failed to load, using existing connector data as fallback. "
                    f"Manually remove from connector_registry/{package_name}.json if you want to remove it from the registry."
                )
                package_registry.plugin_details[plugin_name] = existing_plugins[
                    plugin_name
                ]

    # Save each package file
    package_names = []
    for package_name, package_registry in sorted(package_registries.items()):
        package_dict = dataclasses.asdict(package_registry)
        package_dict["generated_by"] = (
            "metadata-ingestion/scripts/connector_registry.py"
        )
        package_dict["package"] = package_name

        package_file = output_path / f"{package_name}.json"
        write_file = should_write_json_file(
            package_file,
            package_dict,
            f"connector registry for package '{package_name}'",
        )

        if write_file:
            package_json = json.dumps(package_dict, indent=2, sort_keys=True)
            with open(package_file, "w") as f:
                f.write(package_json)
            logger.info(
                f"Saved {len(package_registry.plugin_details)} connectors for package '{package_name}' to {package_file}"
            )

        package_names.append(package_name)

    # Generate manifest file
    manifest_dict = {"packages": sorted(package_names)}
    manifest_file = output_path / "manifest.json"
    write_manifest = should_write_json_file(
        manifest_file, manifest_dict, "connector registry manifest"
    )

    if write_manifest:
        manifest_json = json.dumps(manifest_dict, indent=2, sort_keys=True)
        with open(manifest_file, "w") as f:
            f.write(manifest_json)
        logger.info(
            f"Saved manifest with {len(package_names)} packages to {manifest_file}"
        )


@click.command()
@click.option(
    "--output-dir",
    type=str,
    default="./autogenerated",
    help="Output directory for connector registry",
)
@click.option(
    "--source",
    type=str,
    required=False,
    help="Generate registry for specific source only",
)
def generate_connector_registry_cli(
    output_dir: str, source: Optional[str] = None
) -> None:
    """Generate a comprehensive connector registry for all ingestion sources."""

    logger.info("Starting connector registry generation...")

    if source:
        if source not in source_registry.mapping:
            logger.error(f"Source '{source}' not found in registry")
            return
        original_mapping = source_registry.mapping.copy()
        source_registry.mapping = {source: original_mapping[source]}

    try:
        registry = generate_connector_registry()
        save_connector_registry(registry, output_dir)

        # Show package breakdown
        package_registries = registry.group_by_package()

        print("Connector Registry Generation Complete")
        print("======================================")
        print(f"Total connectors processed: {len(registry.plugin_details)}")
        print(f"Packages found: {len(package_registries)}")
        for package_name in sorted(package_registries.keys()):
            connector_count = len(package_registries[package_name].plugin_details)
            print(f"  - {package_name}: {connector_count} connectors")
        print(f"Output directory: {output_dir}/connector_registry")

    finally:
        if source:
            source_registry.mapping = original_mapping


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s %(levelname)-8s {%(name)s:%(lineno)d}] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S %Z",
    )
    generate_connector_registry_cli()
