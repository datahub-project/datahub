"""CLI commands for the DataHub plugin system.

Registered as ``datahub plugin ...`` in entrypoints.py.
"""

import logging
from typing import Optional

import click

from datahub.plugin.plugin_manager import PluginManager

logger = logging.getLogger(__name__)


@click.group()
def plugin() -> None:
    """Manage external DataHub plugins."""
    pass


# ------------------------------------------------------------------
# Phase 1 commands
# ------------------------------------------------------------------


@plugin.command()
@click.argument("spec")
@click.option(
    "--version",
    "-v",
    type=str,
    default=None,
    help="Specific version to install (overrides version in spec).",
)
def install(spec: str, version: Optional[str]) -> None:
    """Install a DataHub plugin.

    \b
    Supported spec formats:
      salesforce-source           Install by id from a configured registry
      github:owner/repo            Install latest release from GitHub
      github:owner/repo@v1.2.0     Install specific release tag
      /path/to/plugin.whl          Install from a local wheel file
      my-plugin-package==1.0       Install from pip (PyPI or index)
    """
    manager = PluginManager()

    click.echo(f"Installing plugin from {spec}...")
    try:
        # A bare id is resolved via the marketplace index (repo + checksum);
        # every other spec form passes through unchanged.
        target = manager.resolve_install_target(spec, version)
        if target.entry is not None:
            e = target.entry
            verified = " (checksum verified)" if target.expected_sha256 else ""
            click.echo(
                f"Resolved '{spec}' from registry '{e.registry_name}' "
                f"-> github:{e.repo}@{target.version}{verified}"
            )
        plugin_info = manager.install(
            target.spec,
            version=target.version,
            expected_sha256=target.expected_sha256,
        )
    except (RuntimeError, ValueError, OSError) as e:
        logger.debug("Plugin install failed", exc_info=True)
        raise click.ClickException(str(e)) from e

    m = plugin_info.manifest
    click.secho(
        f"Installed {m.id}@{plugin_info.version} ({m.type.value})",
        fg="green",
    )
    click.echo(f"  Entry point: {m.entry_point}")
    click.echo(f"  Package: {plugin_info.package_name}")
    if m.url:
        click.echo(f"  Docs: {m.url}")


@plugin.command()
@click.argument("plugin_id")
def uninstall(plugin_id: str) -> None:
    """Uninstall a DataHub plugin by its ID."""
    manager = PluginManager()

    try:
        manager.uninstall(plugin_id)
    except KeyError as e:
        raise click.ClickException(f"Plugin '{plugin_id}' is not installed.") from e
    except RuntimeError as e:
        logger.debug("Plugin uninstall failed", exc_info=True)
        raise click.ClickException(str(e)) from e

    click.secho(f"Uninstalled {plugin_id}", fg="green")


@plugin.command(name="list")
def list_plugins() -> None:
    """List installed DataHub plugins."""
    manager = PluginManager()
    plugins = manager.list_installed()

    if not plugins:
        click.echo("No plugins installed.")
        click.echo("Install one with: datahub plugin install github:owner/repo")
        return

    click.secho(
        f"{'ID':<30} {'Version':<12} {'Type':<14} {'Package':<25} {'URL'}",
        bold=True,
    )
    click.echo("-" * 100)
    for plugin_info in plugins.values():
        m = plugin_info.manifest
        click.echo(
            f"{m.id:<30} "
            f"{plugin_info.version:<12} "
            f"{m.type.value:<14} "
            f"{plugin_info.package_name:<25} "
            f"{m.url or ''}"
        )


@plugin.command()
@click.argument("plugin_id")
def info(plugin_id: str) -> None:
    """Show detailed information about an installed plugin."""
    manager = PluginManager()
    plugin_info = manager.get_installed(plugin_id)

    if plugin_info is None:
        raise click.ClickException(f"Plugin '{plugin_id}' is not installed.")

    m = plugin_info.manifest
    click.secho(f"Plugin: {m.name}", bold=True)
    click.echo(f"  ID:           {m.id}")
    click.echo(f"  Version:      {plugin_info.version}")
    click.echo(f"  Type:         {m.type.value}")
    click.echo(f"  Package:      {plugin_info.package_name}")
    click.echo(f"  Entry point:  {m.entry_point}")
    if m.description:
        click.echo(f"  Description:  {m.description}")
    if m.author:
        click.echo(f"  Author:       {m.author}")
    if m.support_status:
        click.echo(f"  Support:      {m.support_status.value}")
    if m.capabilities:
        click.echo(f"  Capabilities: {len(m.capabilities)}")
        for cap in m.capabilities:
            marker = "+" if cap.supported else "-"
            click.echo(f"    {marker} {cap.capability}: {cap.description}")


# ------------------------------------------------------------------
# Phase 2 commands
# ------------------------------------------------------------------


@plugin.command()
@click.argument("query", required=False, default="")
@click.option(
    "--type",
    "type_filter",
    type=click.Choice(["source", "sink", "transformer"]),
    default=None,
    help="Filter results by capability type.",
)
def search(query: str, type_filter: Optional[str]) -> None:
    """Search for available community plugins.

    \b
    Examples:
      datahub plugin search salesforce
      datahub plugin search --type source
    """
    from datahub.plugin.plugin_config import PluginCapabilityType
    from datahub.plugin.registry_client import RegistryClient

    client = RegistryClient()
    cap_filter = PluginCapabilityType(type_filter) if type_filter else None
    results = client.search(query, type_filter=cap_filter)

    for warning in client.fetch_warnings:
        click.secho(f"Warning: {warning}", fg="yellow", err=True)

    if not results:
        click.echo("No plugins found.")
        return

    from datahub.plugin.plugin_config import TrustTier

    _TIER_BADGE = {
        TrustTier.OFFICIAL: "[official]",
        TrustTier.VERIFIED: "[verified]",
        TrustTier.COMMUNITY: "",
    }

    click.secho(
        f"{'ID':<30} {'Version':<10} {'Type':<12} {'Author':<20} {'Trust'}",
        bold=True,
    )
    click.echo("-" * 90)
    for entry in results:
        badge = _TIER_BADGE.get(entry.trust_tier, "")
        click.echo(
            f"{entry.id:<30} "
            f"{entry.version:<10} "
            f"{entry.type:<12} "
            f"{entry.author:<20} "
            f"{badge}"
        )
        if entry.description:
            click.echo(f"  {entry.description}")

    click.echo()
    click.echo("Install with: datahub plugin install <id>")


# ------------------------------------------------------------------
# Registry management sub-group
# ------------------------------------------------------------------


@plugin.group(name="registry")
def registry_group() -> None:
    """Manage plugin registries."""
    pass


@registry_group.command(name="list")
def registry_list() -> None:
    """Show configured plugin registries."""
    from datahub.plugin.plugin_config import PluginSystemConfig

    config = PluginSystemConfig.load()
    if not config.registries:
        click.echo("No custom registries configured.")
        click.echo("Using default community registry.")
        return

    click.secho(f"{'Name':<20} {'Enabled':<10} {'URL'}", bold=True)
    click.echo("-" * 80)
    for reg in config.registries:
        status = "yes" if reg.enabled else "no"
        click.echo(f"{reg.name:<20} {status:<10} {reg.url}")


@registry_group.command(name="add")
@click.argument("name")
@click.argument("url")
@click.option("--auth-type", type=click.Choice(["bearer"]), default=None)
@click.option("--token-env", type=str, default=None, help="Env var holding auth token.")
def registry_add(
    name: str,
    url: str,
    auth_type: Optional[str],
    token_env: Optional[str],
) -> None:
    """Add a plugin registry."""
    from datahub.plugin.plugin_config import PluginSystemConfig, RegistryConfig

    config = PluginSystemConfig.load()

    # Check for duplicates
    for reg in config.registries:
        if reg.name == name:
            raise click.ClickException(f"Registry '{name}' already exists.")

    try:
        new_registry = RegistryConfig(
            name=name,
            url=url,
            enabled=True,
            auth_type=auth_type,
            token_env=token_env,
        )
    except ValueError as e:
        raise click.ClickException(str(e)) from e

    config.registries.append(new_registry)
    config.save()
    click.secho(f"Added registry '{name}'", fg="green")


@registry_group.command(name="remove")
@click.argument("name")
def registry_remove(name: str) -> None:
    """Remove a plugin registry."""
    from datahub.plugin.plugin_config import PluginSystemConfig

    config = PluginSystemConfig.load()
    original_len = len(config.registries)
    config.registries = [r for r in config.registries if r.name != name]

    if len(config.registries) == original_len:
        raise click.ClickException(f"Registry '{name}' not found.")

    config.save()
    click.secho(f"Removed registry '{name}'", fg="green")


@registry_group.command(name="refresh")
def registry_refresh() -> None:
    """Refresh (clear) all registry caches so the next search re-fetches."""
    from datahub.plugin.registry_client import RegistryClient

    client = RegistryClient()
    client.refresh()
    click.secho(
        "Registry caches cleared. Next search will fetch fresh data.", fg="green"
    )


# ------------------------------------------------------------------
# Phase 3 commands — Developer experience
# ------------------------------------------------------------------


@plugin.command(name="init")
@click.argument("name")
@click.option(
    "--type",
    "plugin_type",
    type=click.Choice(["source", "sink", "transformer"]),
    default="source",
    help="Type of plugin to scaffold.",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False),
    default=".",
    help="Parent directory for the generated project.",
)
@click.option(
    "--description",
    type=str,
    default="",
    help="Short description of the plugin.",
)
def init_plugin(
    name: str,
    plugin_type: str,
    output_dir: str,
    description: str,
) -> None:
    """Scaffold a new DataHub plugin project.

    \b
    Example:
      datahub plugin init my-salesforce-source --type=source
    """
    from datahub.plugin.scaffold import scaffold_plugin

    try:
        project_dir = scaffold_plugin(
            name=name,
            plugin_type=plugin_type,
            output_dir=output_dir,
            description=description,
        )
    except (ValueError, OSError) as e:
        raise click.ClickException(str(e)) from e

    click.secho(f"Created plugin project at {project_dir}", fg="green")
    click.echo()
    click.echo("Next steps:")
    click.echo(f"  cd {project_dir}")
    click.echo("  pip install -e .")
    click.echo("  pytest tests/")
    click.echo()
    click.echo("To publish:")
    click.echo("  git init && git add -A && git commit -m 'Initial commit'")
    click.echo("  git tag v0.1.0 && git push --tags")


@plugin.command(name="sync")
@click.argument(
    "path",
    required=False,
    default=".",
    type=click.Path(exists=True, file_okay=False),
)
def sync_manifest(path: str) -> None:
    """Sync datahub-plugin.yaml with decorator metadata from the source class.

    Reads @support_status, @capability, and @platform_name decorators from
    the entry point class and updates the manifest YAML to match. Manual
    fields (description, author, icon_url, etc.) are preserved.

    \b
    The package must be installed first (pip install -e .).
    """
    import yaml

    from datahub.plugin.plugin_config import MANIFEST_FILENAME, PluginManifest

    manifest_path = _find_manifest(path)
    if manifest_path is None:
        raise click.ClickException(
            f"No {MANIFEST_FILENAME} found at {path}. Run 'datahub plugin init' first."
        )

    with open(manifest_path) as f:
        raw_data = yaml.safe_load(f)

    try:
        manifest = PluginManifest.model_validate(raw_data)
    except Exception as e:
        raise click.ClickException(f"Invalid {MANIFEST_FILENAME}: {e}") from e

    source_cls = _try_import_entry_point(manifest.entry_point)
    if source_cls is None:
        raise click.ClickException(
            f"Could not import '{manifest.entry_point}'. "
            "Install the package first with: pip install -e ."
        )

    updates = _extract_manifest_fields_from_class(source_cls)
    if not updates:
        click.echo("No decorator metadata found on the source class. Nothing to sync.")
        return

    # Merge: update raw YAML dict with extracted fields, preserving manual fields
    raw_data.update(updates)

    # Re-validate the merged result
    try:
        PluginManifest.model_validate(raw_data)
    except Exception as e:
        raise click.ClickException(f"Merged manifest is invalid: {e}") from e

    with open(manifest_path, "w") as f:
        yaml.dump(raw_data, f, default_flow_style=False, sort_keys=False)

    click.secho(f"Synced {manifest_path}", fg="green")
    for key, value in updates.items():
        if key == "capabilities":
            click.echo(f"  {key}: {len(value)} capabilities")
        else:
            click.echo(f"  {key}: {value}")


def _extract_manifest_fields_from_class(cls: type) -> dict:
    """Extract support_status and capabilities from a decorated source class."""
    fields: dict = {}
    if hasattr(cls, "get_support_status"):
        fields["support_status"] = cls.get_support_status().name
    if hasattr(cls, "get_capabilities"):
        fields["capabilities"] = [
            {
                "capability": cap.capability.name,
                "description": cap.description,
                "supported": cap.supported,
            }
            for cap in cls.get_capabilities()
        ]
    if hasattr(cls, "get_platform_name"):
        fields["name"] = cls.get_platform_name()
    return fields


@plugin.command(name="index-build")
@click.option(
    "--sources",
    "-s",
    "sources_path",
    required=True,
    type=click.Path(exists=True, dir_okay=False),
    help="Path to the sources file (YAML list of plugins to index).",
)
@click.option(
    "--out",
    "-o",
    "out_path",
    required=True,
    type=click.Path(dir_okay=False),
    help="Path to write the generated index.json.",
)
def index_build(sources_path: str, out_path: str) -> None:
    """Build a registry index.json from a curated sources list.

    Each source (``repo`` + ``version``) is resolved to its release wheel; the
    wheel's checksum and its bundled ``datahub-plugin.yaml`` populate the entry,
    so capabilities and support status come straight from the plugin's decorators.
    Only the curated list (which plugins, which versions, what trust tier) is
    maintained by hand.
    """
    import json

    from datahub.plugin.index_builder import build_index, load_sources

    src = load_sources(sources_path)
    if not src.plugins:
        raise click.ClickException(f"No plugins listed in {sources_path}")

    click.echo(f"Building index from {len(src.plugins)} source(s)...")
    result = build_index(src)

    payload = {
        "plugins": [
            entry.model_dump(mode="json", exclude_none=True, exclude={"registry_name"})
            for entry in result.entries
        ]
    }
    with open(out_path, "w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    click.secho(f"Wrote {len(result.entries)} entries to {out_path}", fg="green")
    for err in result.errors:
        click.secho(f"  skipped {err}", fg="yellow", err=True)
    if result.errors and not result.entries:
        raise click.ClickException("No index entries could be built.")


@plugin.command()
@click.argument(
    "path",
    required=False,
    default=".",
    type=click.Path(exists=True, file_okay=False),
)
@click.option(
    "--import-check/--no-import-check",
    default=True,
    help="Try to import the entry point class (requires package to be installed).",
)
def validate(path: str, import_check: bool) -> None:
    """Validate a DataHub plugin project.

    Checks that the project has a valid manifest, correct entry points,
    and that declared classes can be imported.
    """
    import os

    import yaml

    from datahub.plugin.plugin_config import MANIFEST_FILENAME, PluginManifest

    errors: list[str] = []
    warnings: list[str] = []

    # 1. Check manifest exists
    manifest_path = _find_manifest(path)
    if manifest_path is None:
        errors.append(f"Missing {MANIFEST_FILENAME}")
        _print_validation_results(errors, warnings)
        return

    # 2. Parse manifest
    try:
        with open(manifest_path) as f:
            data = yaml.safe_load(f)
        manifest = PluginManifest.model_validate(data)
    except Exception as e:
        errors.append(f"Invalid {MANIFEST_FILENAME}: {e}")
        _print_validation_results(errors, warnings)
        return

    click.echo(f"Manifest: {manifest.id} ({manifest.type.value})")

    # 3. Check pyproject.toml
    pyproject_path = os.path.join(path, "pyproject.toml")
    if not os.path.isfile(pyproject_path):
        warnings.append("No pyproject.toml found")
    else:
        click.echo("pyproject.toml: found")

    # 4. Check entry point is importable format
    ep = manifest.entry_point
    if ":" not in ep and "." not in ep:
        errors.append(
            f"Entry point '{ep}' is not in importable format "
            "(expected 'module.path:ClassName')"
        )

    # 5. Check source files exist
    src_dir = os.path.join(path, "src")
    if not os.path.isdir(src_dir):
        warnings.append("No src/ directory found")

    # 6. Try to import the entry point class
    if import_check:
        source_cls = _try_import_entry_point(manifest.entry_point)
        if source_cls is None:
            warnings.append(
                f"Could not import entry point '{manifest.entry_point}'. "
                "Install the package first with: pip install -e ."
            )
        else:
            click.echo(f"Entry point import: OK ({source_cls.__name__})")

            # 7. Check for @support_status decorator and cross-check manifest
            if not hasattr(source_cls, "get_support_status"):
                warnings.append(
                    f"Class {source_cls.__name__} is missing @support_status decorator. "
                    "Add it to declare the connector's maturity tier."
                )
            else:
                status = source_cls.get_support_status()
                click.echo(f"Support status: {status.name}")
                if (
                    manifest.support_status
                    and manifest.support_status.value != status.name
                ):
                    warnings.append(
                        f"Manifest support_status ({manifest.support_status.value}) "
                        f"differs from decorator ({status.name}). "
                        "Run 'datahub plugin sync' to update the manifest."
                    )

            # 8. Check for @capability decorator and cross-check manifest
            if not hasattr(source_cls, "get_capabilities"):
                warnings.append(
                    f"Class {source_cls.__name__} has no @capability decorators. "
                    "Add capabilities to help users understand what this connector can do."
                )
            else:
                caps = list(source_cls.get_capabilities())
                click.echo(f"Capabilities declared: {len(caps)}")
                decorator_cap_names = {cap.capability.name for cap in caps}
                manifest_cap_names = {c.capability for c in manifest.capabilities}
                if manifest.capabilities and decorator_cap_names != manifest_cap_names:
                    warnings.append(
                        "Manifest capabilities differ from decorator capabilities. "
                        "Run 'datahub plugin sync' to update the manifest."
                    )

            # 9. Check for @platform_name decorator
            if not hasattr(source_cls, "get_platform_name"):
                warnings.append(
                    f"Class {source_cls.__name__} is missing @platform_name decorator."
                )

    # 10. Check for tests directory
    tests_dir = os.path.join(path, "tests")
    if not os.path.isdir(tests_dir):
        warnings.append("No tests/ directory found")
    else:
        test_files = [
            f
            for f in os.listdir(tests_dir)
            if f.startswith("test_") and f.endswith(".py")
        ]
        if not test_files:
            warnings.append("tests/ directory exists but contains no test_*.py files")
        else:
            click.echo(f"Test files: {len(test_files)}")

    # 11. Check for README
    readme_found = any(
        os.path.isfile(os.path.join(path, name))
        for name in ("README.md", "README.rst", "README.txt", "README")
    )
    if not readme_found:
        warnings.append("No README file found")

    _print_validation_results(errors, warnings)


def _try_import_entry_point(entry_point: str) -> Optional[type]:
    """Attempt to import the class specified by an entry point string.

    Supports both 'module.path:ClassName' and 'module.path.ClassName' formats.
    Returns the class on success, None on failure.
    """
    # Reuse the framework's canonical importer — it handles both formats, nested
    # attributes, and adds cwd to sys.path (needed for `pip install -e .` dev).
    from datahub.ingestion.api.registry import import_path

    try:
        return import_path(entry_point)
    except Exception as e:
        logger.debug("Failed to import entry point %s: %s", entry_point, e)
        return None


def _find_manifest(path: str) -> Optional[str]:
    """Locate the plugin manifest file at *path* or under its src/ tree."""
    import os

    from datahub.plugin.plugin_config import MANIFEST_FILENAME

    candidate = os.path.join(path, MANIFEST_FILENAME)
    if os.path.isfile(candidate):
        return candidate

    src_dir = os.path.join(path, "src")
    for root, _dirs, files in os.walk(src_dir):
        if MANIFEST_FILENAME in files:
            return os.path.join(root, MANIFEST_FILENAME)

    return None


def _print_validation_results(errors: list[str], warnings: list[str]) -> None:
    if errors:
        click.secho("Errors:", fg="red", bold=True)
        for err in errors:
            click.echo(f"  x {err}")

    if warnings:
        click.secho("Warnings:", fg="yellow", bold=True)
        for warn in warnings:
            click.echo(f"  ! {warn}")

    if not errors:
        click.secho("Validation passed!", fg="green")
    else:
        raise SystemExit(1)
