"""CLI commands for managing DataHub data packs."""

import hashlib
import importlib.resources
import json
import logging
import pathlib
import sys
import time
from datetime import datetime, timezone
from typing import List, Optional

import click

from datahub.cli.datapack.models import DataPackInfo, TrustTier
from datahub.telemetry import telemetry

logger = logging.getLogger(__name__)

_EXPERIMENTAL_NOTICE = (
    "Note: 'datahub datapack' is experimental. "
    "Command surface and behavior may change in future releases."
)


class _DatapackGroup(click.Group):
    """Click group that shows experimental notice and agent context."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)
        formatter.write(f"\n{_EXPERIMENTAL_NOTICE}\n")
        if not sys.stdout.isatty():
            agent_text = (
                importlib.resources.files("datahub.cli.datapack.resources")
                .joinpath("DATAPACK_AGENT_CONTEXT.md")
                .read_text(encoding="utf-8")
            )
            formatter.write("\n")
            formatter.write(agent_text)

    def invoke(self, ctx: click.Context) -> object:
        if ctx.invoked_subcommand:
            click.echo(_EXPERIMENTAL_NOTICE, err=True)
        return super().invoke(ctx)


@click.group(cls=_DatapackGroup)
def datapack() -> None:
    """Manage data packs for loading curated metadata into DataHub.

    Data packs are curated collections of metadata (MCPs) that can be loaded
    into DataHub for demos, testing, or bootstrapping. The registry includes
    verified packs maintained by the DataHub project.
    """
    pass


@datapack.command(name="list")
@click.option("--tag", default=None, help="Filter packs by tag.")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format.",
)
@telemetry.with_telemetry()
def list_cmd(tag: Optional[str], output_format: str) -> None:
    """List all available data packs."""
    from datahub.cli.datapack.registry import list_packs

    packs = list_packs()

    if tag:
        packs = {k: v for k, v in packs.items() if tag in v.tags}

    if not packs:
        click.echo("No data packs found.")
        return

    if output_format == "json":
        click.echo(json.dumps({k: v.model_dump() for k, v in packs.items()}, indent=2))
        return

    # Table output
    try:
        from tabulate import tabulate
    except ImportError:
        # Fallback if tabulate not available
        for name, pack in sorted(packs.items()):
            click.echo(f"  {name}: {pack.description} [{pack.trust.value}]")
        return

    rows = []
    for name, pack in sorted(packs.items()):
        rows.append(
            [
                name,
                pack.description[:60] + ("..." if len(pack.description) > 60 else ""),
                pack.size_hint or "?",
                pack.trust.value,
                ", ".join(pack.tags),
            ]
        )

    click.echo(tabulate(rows, headers=["Name", "Description", "Size", "Trust", "Tags"]))


@datapack.command()
@click.argument("name")
@telemetry.with_telemetry()
def info(name: str) -> None:
    """Show detailed information about a data pack."""
    from datahub.cli.datapack.loader import get_load_record, is_cached
    from datahub.cli.datapack.registry import get_pack

    pack = get_pack(name)

    click.echo(f"Name:            {pack.name}")
    click.echo(f"Description:     {pack.description}")
    click.echo(f"URL:             {pack.url}")
    click.echo(f"Size:            {pack.size_hint or 'unknown'}")
    click.echo(f"Trust:           {pack.trust.value}")
    click.echo(f"Tags:            {', '.join(pack.tags)}")
    click.echo(f"SHA256:          {pack.sha256 or 'none'}")
    click.echo(f"Format version:  {pack.pack_format_version}")

    if pack.min_server_version:
        click.echo(f"Min server:      {pack.min_server_version}")
    if pack.min_cloud_version:
        click.echo(f"Min cloud:       {pack.min_cloud_version}")
    if pack.reference_timestamp:
        ref_dt = datetime.fromtimestamp(
            pack.reference_timestamp / 1000, tz=timezone.utc
        )
        click.echo(f"Reference time:  {ref_dt.isoformat()}")

    click.echo(f"Cached:          {'yes' if is_cached(pack) else 'no'}")

    record = get_load_record(name)
    if record:
        click.echo(
            f"Loaded:          yes (run_id={record.run_id}, at {record.loaded_at})"
        )
    else:
        click.echo("Loaded:          no")


@datapack.command()
@click.argument("name")
@click.option(
    "--url", default=None, help="Load from an arbitrary URL instead of the registry."
)
@click.option("--dry-run", is_flag=True, help="Preview without ingesting.")
@click.option("--no-cache", is_flag=True, help="Force re-download.")
@click.option("--force", is_flag=True, help="Override version compatibility checks.")
@click.option(
    "--as-of",
    default=None,
    type=click.DateTime(),
    help="Target datetime for time-shifting (default: now).",
)
@click.option("--no-time-shift", is_flag=True, help="Load with original timestamps.")
@click.option("--trust-community", is_flag=True, help="Allow loading community packs.")
@click.option(
    "--trust-custom", is_flag=True, help="Allow loading from unverified URLs."
)
@telemetry.with_telemetry(capture_kwargs=["dry_run", "no_time_shift", "no_cache"])
def load(
    name: str,
    url: Optional[str],
    dry_run: bool,
    no_cache: bool,
    force: bool,
    as_of: Optional[datetime],
    no_time_shift: bool,
    trust_community: bool,
    trust_custom: bool,
) -> None:
    """Load a data pack into DataHub."""
    from datahub.cli.datapack.loader import (
        check_trust,
        check_version_compatibility,
        download_pack,
        load_pack_into_datahub,
    )

    if url:
        # Custom URL -- create an ad-hoc pack info
        pack = DataPackInfo(
            name=name,
            description=f"Custom pack from {url}",
            url=url,
            trust=TrustTier.CUSTOM,
        )
    else:
        from datahub.cli.datapack.registry import get_pack

        pack = get_pack(name)

    # Trust check
    check_trust(pack, trust_community=trust_community, trust_custom=trust_custom)

    # Version compatibility check
    if not dry_run:
        check_version_compatibility(pack, force=force)

    # Download
    file_entries = download_pack(pack, no_cache=no_cache)

    # Load
    load_pack_into_datahub(
        pack=pack,
        file_entries=file_entries,
        dry_run=dry_run,
        no_time_shift=no_time_shift,
        as_of=as_of,
    )


@datapack.command()
@click.argument("name")
@click.option("--hard", is_flag=True, help="Hard-delete entities (irreversible).")
@click.option("--dry-run", is_flag=True, help="Show what would be deleted.")
@telemetry.with_telemetry(capture_kwargs=["hard", "dry_run"])
def unload(name: str, hard: bool, dry_run: bool) -> None:
    """Remove all entities loaded by a data pack."""
    from datahub.cli.datapack.loader import unload_pack

    unload_pack(pack_name=name, hard=hard, dry_run=dry_run)


# TODO: Add `publish` (or `export`/`create`) command in a future release.
# This would connect to a running DataHub, query entities by filter,
# and package them as a data pack (data.json + registry-entry.json).


_publish_placeholder = None  # Removed for v1 to keep surface area small


@datapack.command(hidden=True)
@click.argument("name")
@click.option(
    "--output-dir",
    type=click.Path(),
    default=None,
    help="Output directory (default: ./<name>/).",
)
@click.option("--platform", default=None, help="Filter by data platform.")
@click.option(
    "--entity-type",
    multiple=True,
    help="Filter by entity type (can be specified multiple times).",
)
@click.option("--env", default=None, help="Filter by environment (e.g. PROD).")
@click.option("--query", default=None, help="Elasticsearch query string for filtering.")
@click.option("--description", default=None, help="Pack description.")
@click.option("--tags", default=None, help="Comma-separated tags.")
@click.option(
    "--min-server-version",
    default=None,
    help="Minimum DataHub server version for this pack.",
)
@telemetry.with_telemetry()
def publish(
    name: str,
    output_dir: Optional[str],
    platform: Optional[str],
    entity_type: tuple,
    env: Optional[str],
    query: Optional[str],
    description: Optional[str],
    tags: Optional[str],
    min_server_version: Optional[str],
) -> None:
    """Capture metadata from DataHub and package as a data pack."""
    from datahub.ingestion.graph.client import get_default_graph
    from datahub.ingestion.sink.file import write_metadata_file

    out_path = pathlib.Path(output_dir) if output_dir else pathlib.Path(f"./{name}")
    out_path.mkdir(parents=True, exist_ok=True)
    data_file = out_path / "data.json"

    # Connect to DataHub
    click.echo("Connecting to DataHub...")
    graph = get_default_graph()

    # Get server version for min_server_version default
    rest_config = graph.server_config
    detected_version = rest_config.service_version
    if not min_server_version and detected_version:
        min_server_version = detected_version

    # Enumerate entities
    entity_types: Optional[List[str]] = list(entity_type) if entity_type else None

    if not platform and not entity_types and not env and not query:
        if not click.confirm(
            "No filters specified. Export ALL entities?", default=False
        ):
            click.echo("Aborted.")
            return

    click.echo("Enumerating entities...")
    urns = list(
        graph.get_urns_by_filter(
            entity_types=entity_types,
            platform=platform,
            env=env,
            query=query,
        )
    )

    if not urns:
        click.echo("No entities found matching the filters.")
        return

    click.echo(f"Found {len(urns)} entities. Exporting...")

    # Fetch all aspects for each entity
    all_mcps = []
    with click.progressbar(urns, label="Exporting entities") as bar:
        for urn in bar:
            try:
                mcps = graph.get_entity_as_mcps(urn)
                all_mcps.extend(mcps)
            except Exception:
                logger.warning("Failed to export %s", urn, exc_info=True)

    # Write to file
    write_metadata_file(data_file, all_mcps)
    click.echo(f"Wrote {len(all_mcps)} MCPs to {data_file}")

    # Compute SHA256
    sha256 = hashlib.sha256()
    with open(data_file, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    sha256_hex = sha256.hexdigest()

    # Generate registry entry
    reference_timestamp = int(time.time() * 1000)
    tag_list = [t.strip() for t in tags.split(",")] if tags else []

    registry_entry = {
        "name": name,
        "description": description or f"Data pack '{name}' exported from DataHub",
        "url": "<REPLACE_WITH_UPLOAD_URL>",
        "sha256": sha256_hex,
        "size_hint": f"~{data_file.stat().st_size / (1024 * 1024):.1f} MB",
        "tags": tag_list,
        "trust": "community",
        "reference_timestamp": reference_timestamp,
        "min_server_version": min_server_version,
        "pack_format_version": "1",
    }

    registry_file = out_path / "registry-entry.json"
    with open(registry_file, "w") as f:
        json.dump(registry_entry, f, indent=2)

    click.echo(f"Registry entry written to {registry_file}")
    click.echo(
        f"\nPack published to {out_path}/\n"
        f"  data.json:           {len(all_mcps)} MCPs, {sha256_hex[:12]}...\n"
        f"  registry-entry.json: Ready to merge into datapack-registry.json\n"
        f"\nNext steps:\n"
        f"  1. Upload data.json to a public URL (S3, HuggingFace, etc.)\n"
        f"  2. Update the 'url' field in registry-entry.json\n"
        f"  3. Submit a PR adding the entry to datapack-registry.json"
    )
