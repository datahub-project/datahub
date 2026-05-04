#!/usr/bin/env python3
"""
Self-contained script to create bundled venvs for DataHub ingestion sources.

Groups come from env vars ``BUNDLED_VENV_PLUGINS_<group_id>`` (suffix lowercased for the
group label, e.g. ``BUNDLED_VENV_PLUGINS_COMMON`` -> ``common-venv``). One
``BUNDLED_CLI_VERSION`` installs ``acryl-datahub[...]==`` that version for each group.

When ``BUNDLED_VENV_SLIM_MODE`` is true, eligible extras use ``-slim`` variants and each
slim venv is verified to not import PySpark.
"""

import os
import subprocess
import sys
from pathlib import Path

from bundled_venv_config import (
    BundledVenvGroupPlan,
    build_group_plans,
    extras_to_install_string,
    groups_config_from_plugin_group_env,
)


def create_bundled_venv(
    group_plan: BundledVenvGroupPlan,
    bundled_cli_version: str,
    venv_base_path: str,
    slim_mode: bool,
) -> bool:
    """Create one physical venv at canonical_dir_name with the given extras."""
    venv_name = group_plan.canonical_dir_name
    venv_path = os.path.join(venv_base_path, venv_name)
    extras_str = extras_to_install_string(group_plan.extras)

    print(f"Creating bundled venv for group '{group_plan.label}': {venv_name}")
    print(f"  Members: {', '.join(group_plan.members)}")
    print(f"  Venv Path: {venv_path}")
    print(f"  Extras: [{extras_str}]")
    if slim_mode:
        print("  Slim Mode: using -slim variants for eligible data lake extras")

    constraints_path = os.path.join(venv_base_path, "constraints.txt")
    try:
        print("  → Creating venv...")
        subprocess.run(
            ["uv", "venv", "--clear", venv_path], check=True, capture_output=True
        )

        print("  → Installing base packages...")
        base_cmd = f"source {venv_path}/bin/activate && uv pip install --upgrade pip wheel setuptools --constraint {constraints_path}"
        subprocess.run(["bash", "-c", base_cmd], check=True, capture_output=True)

        print(f"  → Installing datahub with [{extras_str}]...")
        if os.path.exists("/metadata-ingestion/setup.py"):
            print("  → Using local /metadata-ingestion source")
            datahub_package = f"-e /metadata-ingestion[{extras_str}]"
            install_cmd = f"source {venv_path}/bin/activate && uv pip install {datahub_package} --constraint {constraints_path}"
        else:
            datahub_package = f"acryl-datahub[{extras_str}]=={bundled_cli_version}"
            install_cmd = (
                f'source {venv_path}/bin/activate && uv pip install "{datahub_package}" '
                f"--constraint {constraints_path}"
            )
        subprocess.run(["bash", "-c", install_cmd], check=True, capture_output=True)

        if slim_mode:
            python_exe = os.path.join(venv_path, "bin", "python")
            result = subprocess.run(
                [python_exe, "-c", "import pyspark"],
                capture_output=True,
            )
            if result.returncode == 0:
                print(
                    f"  ❌ FAIL: PySpark found in {venv_name} (slim mode requires no PySpark)"
                )
                return False
            print(f"  → Verified: No PySpark in {venv_name}")

        print(f"  ✅ Successfully created {venv_name}")
        return True

    except subprocess.CalledProcessError as e:
        print(f"  ❌ Failed to create {venv_name}: {e}")
        if e.stderr:
            print(f"     Error output: {e.stderr.decode()}")
        return False


def _remove_if_exists(path: str) -> None:
    p = Path(path)
    if p.is_symlink() or p.is_file():
        p.unlink()
    elif p.is_dir():
        raise RuntimeError(f"Refusing to remove unexpected directory: {path}")


def ensure_plugin_symlinks(
    group_plan: BundledVenvGroupPlan,
    venv_base_path: str,
) -> None:
    """
    For groups with multiple members, or a single member whose canonical dir name
    differs from `{plugin}-bundled`, create relative symlinks so each plugin path exists.
    """
    canonical = group_plan.canonical_dir_name
    canonical_path = os.path.join(venv_base_path, canonical)

    for plugin in group_plan.members:
        alias_name = f"{plugin}-bundled"
        if alias_name == canonical:
            continue

        link_path = os.path.join(venv_base_path, alias_name)
        _remove_if_exists(link_path)

        target_rel = os.path.relpath(canonical_path, start=os.path.dirname(link_path))
        os.symlink(target_rel, link_path, target_is_directory=True)
        print(f"  → Symlink {alias_name} -> {target_rel}")


def main() -> int:
    plugins_str = os.environ.get("BUNDLED_VENV_PLUGINS", "s3,demo-data,file")
    bundled_cli_version = os.environ.get("BUNDLED_CLI_VERSION")
    venv_base_path = os.environ.get("DATAHUB_BUNDLED_VENV_PATH", "/opt/datahub/venvs")
    slim_mode_str = os.environ.get("BUNDLED_VENV_SLIM_MODE", "false").lower()
    slim_mode = slim_mode_str in ["true", "1", "yes"]

    if not bundled_cli_version:
        print("ERROR: BUNDLED_CLI_VERSION environment variable must be set")
        return 1

    if bundled_cli_version.startswith("v"):
        bundled_cli_version = bundled_cli_version[1:]

    plugins = [p.strip() for p in plugins_str.split(",") if p.strip()]

    print("=" * 60)
    print("DataHub Bundled Venv Builder (named groups)")
    print("=" * 60)
    print(f"DataHub CLI Version: {bundled_cli_version}")
    print(f"Plugins: {', '.join(plugins)}")
    print(f"Venv Base Path: {venv_base_path}")
    print(f"Slim Mode: {slim_mode}")
    print(f"Total Plugins: {len(plugins)}")
    print()

    try:
        raw_config = groups_config_from_plugin_group_env(os.environ)
        plans = build_group_plans(plugins, raw_config, slim_mode)
    except (ValueError, OSError) as e:
        print(f"ERROR: Invalid bundled venv groups configuration: {e}")
        return 1

    print("Resolved install plan:")
    for plan in plans:
        print(
            f"  group={plan.label!r} -> {plan.canonical_dir_name} "
            f"members={list(plan.members)} extras=[{extras_to_install_string(plan.extras)}]"
        )
    print()

    os.makedirs(venv_base_path, exist_ok=True)

    print("Creating bundled venvs...")
    print("-" * 40)
    success_count = 0
    failed_labels: list[str] = []

    for plan in plans:
        try:
            if create_bundled_venv(
                plan, bundled_cli_version, venv_base_path, slim_mode
            ):
                ensure_plugin_symlinks(plan, venv_base_path)
                success_count += 1
            else:
                failed_labels.append(plan.label)
        except Exception as e:
            print(f"Failed to create venv for group {plan.label!r}: {e}")
            failed_labels.append(plan.label)
        print()

    print("=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total install groups: {len(plans)}")
    print(f"Successfully created: {success_count}")
    print(f"Failed: {len(failed_labels)}")

    if failed_labels:
        print(f"Failed groups: {', '.join(failed_labels)}")

    print()
    if success_count == len(plans):
        print("🎉 All bundled venvs created successfully!")
        return 0
    print("⚠️  Some bundled venvs failed to create")
    return 1


if __name__ == "__main__":
    sys.exit(main())
