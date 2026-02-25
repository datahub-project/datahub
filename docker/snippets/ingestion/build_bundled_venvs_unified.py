#!/usr/bin/env python3
"""
Self-contained script to create bundled venvs for DataHub ingestion sources.
This script creates virtual environments with predictable names following the pattern:
<plugin-name>-bundled that are leveraged within acryl-executor to run ingestion jobs.
"""

import os
import subprocess
import sys
from typing import List, Tuple

# Adding a new plug-in with a -slim variant has to be defined here
PLUGINS_WITH_SLIM_VARIANT = ['s3']

def generate_venv_mappings(plugins: List[str]) -> List[Tuple[str, str]]:
    """Generate simple venv name mappings using <plugin-name>-bundled pattern."""
    venv_mappings = []

    for plugin in plugins:
        # Simple, predictable naming: <plugin-name>-bundled
        venv_name = f"{plugin}-bundled"
        venv_mappings.append((plugin, venv_name))

    return venv_mappings


def create_venv(plugin: str, venv_name: str, bundled_cli_version: str, venv_base_path: str, slim_mode: bool = False) -> bool:
    """Create a single bundled venv for a plugin.

    Args:
        plugin: Plugin name (e.g., "s3", "demo-data")
        venv_name: Name of the venv directory (e.g., "s3-bundled")
        bundled_cli_version: DataHub CLI version to install
        venv_base_path: Base directory for venvs
        slim_mode: If True, use -slim variants for data lake sources (s3-slim, gcs-slim, abs-slim)
    """
    venv_path = os.path.join(venv_base_path, venv_name)

    print(f"Creating bundled venv for {plugin}: {venv_name}")
    print(f"  Venv Path: {venv_path}")
    if slim_mode:
        print(f"  Slim Mode: Will use -slim variants for data lake sources")

    try:
        # Create the venv
        print(f"  → Creating venv...")
        subprocess.run(['uv', 'venv', venv_path], check=True, capture_output=True)

        # Install packages in the venv
        print(f"  → Installing base packages...")
        base_cmd = f'source {venv_path}/bin/activate && uv pip install --upgrade pip wheel setuptools'
        subprocess.run(['bash', '-c', base_cmd], check=True, capture_output=True)

        # Determine which plugin extra to use
        # In slim mode, use -slim suffix for data lake sources to avoid PySpark
        plugin_extra = plugin
        if slim_mode and plugin in PLUGINS_WITH_SLIM_VARIANT:
            plugin_extra = f"{plugin}-slim"
            print(f"  → Using {plugin_extra} extra (slim mode, no PySpark)")

        # Install DataHub with the specific plugin
        print(f"  → Installing datahub with {plugin_extra} plugin...")
        # Use local metadata-ingestion if available (for development), otherwise use PyPI
        if os.path.exists('/metadata-ingestion/setup.py'):
            print(f"  → Using local /metadata-ingestion source")
            datahub_package = f'-e /metadata-ingestion[datahub-rest,datahub-kafka,file,{plugin_extra}]'
            constraints_path = os.path.join(venv_base_path, "constraints.txt")
            install_cmd = f'source {venv_path}/bin/activate && uv pip install {datahub_package} --constraints {constraints_path}'
        else:
            datahub_package = f'acryl-datahub[datahub-rest,datahub-kafka,file,{plugin_extra}]=={bundled_cli_version}'
            constraints_path = os.path.join(venv_base_path, "constraints.txt")
            install_cmd = f'source {venv_path}/bin/activate && uv pip install "{datahub_package}" --constraints {constraints_path}'
        subprocess.run(['bash', '-c', install_cmd], check=True, capture_output=True)

        print(f"  ✅ Successfully created {venv_name}")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Failed to create {venv_name}: {e}")
        # Print stderr if available for debugging
        if e.stderr:
            print(f"     Error output: {e.stderr.decode()}")
        return False


def main():
    """Main function to generate and create all bundled venvs."""
    # Get configuration from environment
    plugins_str = os.environ.get('BUNDLED_VENV_PLUGINS', 's3,demo-data')
    bundled_cli_version = os.environ.get('BUNDLED_CLI_VERSION')
    venv_base_path = os.environ.get('DATAHUB_BUNDLED_VENV_PATH', '/opt/datahub/venvs')
    slim_mode_str = os.environ.get('BUNDLED_VENV_SLIM_MODE', 'false').lower()
    slim_mode = slim_mode_str in ['true', '1', 'yes']

    if not bundled_cli_version:
        print("ERROR: BUNDLED_CLI_VERSION environment variable must be set")
        sys.exit(1)

    # Strip 'v' prefix if present (e.g., v0.12.1 -> 0.12.1)
    if bundled_cli_version.startswith('v'):
        bundled_cli_version = bundled_cli_version[1:]

    # Parse plugins list
    plugins = [p.strip() for p in plugins_str.split(',') if p.strip()]

    print("=" * 60)
    print("DataHub Bundled Venv Builder (Self-Contained)")
    print("=" * 60)
    print(f"DataHub CLI Version: {bundled_cli_version}")
    print(f"Plugins: {', '.join(plugins)}")
    print(f"Venv Base Path: {venv_base_path}")
    print(f"Slim Mode: {slim_mode}")
    print(f"Total Plugins: {len(plugins)}")
    print()

    # Generate venv name mappings using simple pattern
    print("Generating venv name mappings...")
    venv_mappings = generate_venv_mappings(plugins)

    print("Generated venv mappings:")
    for plugin, venv_name in venv_mappings:
        extra_info = ""
        if slim_mode and plugin in PLUGINS_WITH_SLIM_VARIANT:
            extra_info = " (will use -slim extra)"
        print(f"  {plugin} → {venv_name}{extra_info}")
    print()

    # Ensure the venv base directory exists
    os.makedirs(venv_base_path, exist_ok=True)

    # Create each venv
    print("Creating bundled venvs...")
    print("-" * 40)
    success_count = 0
    failed_plugins = []

    for plugin, venv_name in venv_mappings:
        try:
            if create_venv(plugin, venv_name, bundled_cli_version, venv_base_path, slim_mode):
                success_count += 1
            else:
                failed_plugins.append(plugin)
        except Exception as e:
            print(f"Failed to create venv for {plugin}: {e}")
            failed_plugins.append(plugin)
        print()  # Add spacing between venvs

    # Summary
    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total plugins: {len(venv_mappings)}")
    print(f"Successfully created: {success_count}")
    print(f"Failed: {len(failed_plugins)}")

    if failed_plugins:
        print(f"Failed plugins: {', '.join(failed_plugins)}")

    print()
    if success_count == len(venv_mappings):
        print("🎉 All bundled venvs created successfully!")
        return 0
    else:
        print("⚠️  Some bundled venvs failed to create")
        return 1


if __name__ == "__main__":
    sys.exit(main())