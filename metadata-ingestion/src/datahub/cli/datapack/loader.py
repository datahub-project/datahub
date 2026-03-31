"""Download, cache, verify, and load data packs into DataHub."""

import hashlib
import json
import logging
import os
import pathlib
import re
import shutil
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Optional
from urllib.parse import urlparse

import click
import requests

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER, load_client_config
from datahub.cli.datapack.models import DataPackInfo, LoadRecord, TrustTier
from datahub.cli.datapack.time_shift import time_shift_file
from datahub.ingestion.graph.config import DatahubClientConfig

logger = logging.getLogger(__name__)

CACHE_DIR = os.path.join(DATAHUB_ROOT_FOLDER, "datapack-cache")
LOADS_DIR = os.path.join(DATAHUB_ROOT_FOLDER, "datapack-loads")


def _cache_key(url: str) -> str:
    """Deterministic cache filename from a URL."""
    return hashlib.sha256(url.encode()).hexdigest()


def _sha256_file(path: pathlib.Path) -> str:
    """Compute SHA256 hex digest of a file."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _cached_path(pack: DataPackInfo) -> pathlib.Path:
    return pathlib.Path(CACHE_DIR) / f"{_cache_key(pack.url)}.json"


def is_cached(pack: DataPackInfo) -> bool:
    """Check if a pack is already downloaded and cached."""
    return _cached_path(pack).exists()


@dataclass
class IndexFileEntry:
    """An entry in an index file, representing a data file to load."""

    path: pathlib.Path
    wait_for_completion: bool = False


def download_pack(pack: DataPackInfo, no_cache: bool = False) -> List[IndexFileEntry]:
    """Download a data pack file, using local cache when available.

    Args:
        pack: The data pack to download.
        no_cache: If True, force re-download even if cached.

    Returns:
        List of IndexFileEntry objects for the pack's data files.

    Raises:
        click.ClickException: On download failure or SHA256 mismatch.
    """
    cache_path = _cached_path(pack)

    if not no_cache and cache_path.exists():
        # Verify cached file integrity if we have a checksum
        if pack.sha256:
            actual = _sha256_file(cache_path)
            if actual == pack.sha256:
                click.echo(f"Using cached pack: {cache_path}")
                return _resolve_index_file(cache_path, pack, no_cache)
            else:
                click.echo("Cached file checksum mismatch, re-downloading...")

        else:
            click.echo(f"Using cached pack: {cache_path}")
            return _resolve_index_file(cache_path, pack, no_cache)

    # Download (or copy for file:// URLs)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    parsed = urlparse(pack.url)

    if parsed.scheme == "file":
        local_path = pathlib.Path(parsed.path)
        if not local_path.exists():
            raise click.ClickException(f"Local file not found: {local_path}")
        click.echo(f"Copying local file: {local_path}")
        shutil.copy2(local_path, cache_path)
    else:
        click.echo(f"Downloading data pack '{pack.name}' from {pack.url}")
        try:
            response = requests.get(pack.url, stream=True, timeout=120)
            response.raise_for_status()
        except requests.RequestException as e:
            raise click.ClickException(f"Failed to download data pack: {e}") from e

        # Stream to cache file with progress
        content_length = response.headers.get("content-length")
        total = int(content_length) if content_length else None

        with open(cache_path, "wb") as f:
            if total:
                with click.progressbar(length=total, label="Downloading") as bar:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                        bar.update(len(chunk))
            else:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)

    # Verify SHA256
    if pack.sha256:
        actual = _sha256_file(cache_path)
        if actual != pack.sha256:
            cache_path.unlink(missing_ok=True)
            raise click.ClickException(
                f"SHA256 mismatch for '{pack.name}'!\n"
                f"  Expected: {pack.sha256}\n"
                f"  Actual:   {actual}\n"
                "The downloaded file may be corrupted or tampered with."
            )
        click.echo("SHA256 verified.")

    # Check if this is an index file (object with "files" key) or data file (array)
    entries = _resolve_index_file(cache_path, pack, no_cache)

    return entries


def _resolve_index_file(
    cache_path: pathlib.Path,
    pack: DataPackInfo,
    no_cache: bool,
) -> List[IndexFileEntry]:
    """If the downloaded file is an index, fetch listed files and return entries.

    Index files are JSON objects with a "files" key. Each entry can be a
    string (filename) or an object with "path" and optional "wait_for_completion".
    Data files (JSON arrays) return a single entry.

    Returns a list of IndexFileEntry objects, one per file to load.
    """
    try:
        with open(cache_path) as f:
            content = json.load(f)
    except (json.JSONDecodeError, OSError):
        return [IndexFileEntry(path=cache_path)]

    if isinstance(content, list):
        return [IndexFileEntry(path=cache_path)]

    if not isinstance(content, dict) or "files" not in content:
        return [IndexFileEntry(path=cache_path)]

    file_list = content["files"]
    if not file_list:
        return [IndexFileEntry(path=cache_path)]

    base_url = pack.url.rsplit("/", 1)[0]
    click.echo(f"Index file detected with {len(file_list)} data files.")

    entries: List[IndexFileEntry] = []
    for item in file_list:
        # Support both string and object entries
        if isinstance(item, str):
            filename = item
            wait = False
        elif isinstance(item, dict):
            filename = item["path"]
            wait = item.get("wait_for_completion", False)
        else:
            continue

        file_url = f"{base_url}/{filename}"
        click.echo(
            f"  Fetching {filename}{'  [wait_for_completion]' if wait else ''}..."
        )

        parsed = urlparse(file_url)
        if parsed.scheme == "file":
            file_path = pathlib.Path(parsed.path)
            if not file_path.exists():
                logger.warning("Index references missing file: %s", file_path)
                continue
            # Copy to cache dir so path is stable
            cached = pathlib.Path(CACHE_DIR) / f"{_cache_key(file_url)}.json"
            cached.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(file_path, cached)
            entries.append(IndexFileEntry(path=cached, wait_for_completion=wait))
        else:
            try:
                response = requests.get(file_url, timeout=120)
                response.raise_for_status()
                cached = pathlib.Path(CACHE_DIR) / f"{_cache_key(file_url)}.json"
                cached.parent.mkdir(parents=True, exist_ok=True)
                with open(cached, "wb") as f:
                    f.write(response.content)
                entries.append(IndexFileEntry(path=cached, wait_for_completion=wait))
            except Exception:
                logger.warning("Failed to fetch %s, skipping", file_url, exc_info=True)

    click.echo(f"Resolved {len(entries)} data files from index.")
    return entries


def _wait_for_entities(
    urns: set[str],
    client_config: DatahubClientConfig,
    timeout_seconds: int = 60,
    poll_interval: float = 0.5,
) -> None:
    """Wait until all given URNs exist on the server."""
    if not urns:
        return

    from datahub.ingestion.graph.client import DataHubGraph

    graph = DataHubGraph(client_config)
    pending = set(urns)
    deadline = time.time() + timeout_seconds

    click.echo(f"Waiting for {len(pending)} entities to be processed...")
    while pending and time.time() < deadline:
        still_pending = set()
        for urn in pending:
            try:
                if not graph.exists(urn):
                    still_pending.add(urn)
            except Exception:
                still_pending.add(urn)
        pending = still_pending
        if pending:
            time.sleep(poll_interval)

    if pending:
        click.echo(
            f"Warning: {len(pending)} entities not yet available after "
            f"{timeout_seconds}s (proceeding anyway)"
        )
    else:
        click.echo("All entities available.")


def check_trust(
    pack: DataPackInfo,
    trust_community: bool = False,
    trust_custom: bool = False,
) -> None:
    """Check trust tier and raise if not authorized.

    Args:
        pack: The data pack to check.
        trust_community: If True, allow community packs without prompting.
        trust_custom: If True, allow custom/unverified packs without prompting.

    Raises:
        click.ClickException: If the trust tier is not authorized.
    """
    if pack.trust == TrustTier.VERIFIED:
        return

    if pack.trust == TrustTier.COMMUNITY and not trust_community:
        raise click.ClickException(
            f"'{pack.name}' is a community-contributed data pack.\n"
            "Community packs are not verified by the DataHub project.\n"
            "Use --trust-community to proceed."
        )

    if pack.trust == TrustTier.CUSTOM and not trust_custom:
        raise click.ClickException(
            "Loading from an unverified URL.\n"
            "Custom data packs are not in the registry and have no checksum.\n"
            "Use --trust-custom to proceed."
        )


def check_version_compatibility(
    pack: DataPackInfo,
    force: bool = False,
) -> None:
    """Check if the data pack is compatible with the running server.

    Raises:
        click.ClickException: If the server version is below the minimum required.
    """
    if not pack.min_server_version and not pack.min_cloud_version:
        return

    try:
        client_config = load_client_config()
        from datahub.ingestion.graph.client import DataHubGraph

        graph = DataHubGraph(client_config)
        server_config = graph.server_config
    except Exception as e:
        if force:
            click.echo(
                "Warning: Could not connect to server to check version compatibility."
            )
            return
        raise click.ClickException(
            "Could not connect to DataHub server to check version compatibility.\n"
            "Use --force to skip version checking."
        ) from e

    rest_config = server_config
    is_cloud = rest_config.is_datahub_cloud

    min_version = pack.min_cloud_version if is_cloud else pack.min_server_version
    if not min_version:
        return

    # Parse the minimum version
    parts = re.match(r"(\d+)\.(\d+)\.(\d+)", min_version)
    if not parts:
        logger.warning("Could not parse min version '%s', skipping check", min_version)
        return

    major, minor, patch = int(parts.group(1)), int(parts.group(2)), int(parts.group(3))

    if not rest_config.is_version_at_least(major, minor, patch):
        server_ver = rest_config.service_version or "unknown"
        platform = "Acryl Cloud" if is_cloud else "DataHub OSS"
        if force:
            click.echo(
                f"Warning: {platform} server {server_ver} is below "
                f"minimum required {min_version} for pack '{pack.name}'."
            )
        else:
            raise click.ClickException(
                f"Pack '{pack.name}' requires {platform} >= {min_version}, "
                f"but server is {server_ver}.\n"
                "Use --force to override this check."
            )


def _generate_run_id(pack_name: str) -> str:
    """Generate a deterministic run ID for a data pack load."""
    epoch_ms = int(time.time() * 1000)
    return f"datapack-{pack_name}-{epoch_ms}"


def _load_record_path(pack_name: str) -> pathlib.Path:
    return pathlib.Path(LOADS_DIR) / f"{pack_name}.json"


def save_load_record(pack: DataPackInfo, run_id: str) -> None:
    """Save a record of the load for unload support."""
    record = LoadRecord(
        pack_name=pack.name,
        run_id=run_id,
        loaded_at=datetime.now(timezone.utc).isoformat(),
        pack_url=pack.url,
        pack_sha256=pack.sha256,
    )
    path = _load_record_path(pack.name)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(record.model_dump(), f, indent=2)
    logger.debug("Load record saved to %s", path)


def get_load_record(pack_name: str) -> Optional[LoadRecord]:
    """Get the load record for a previously loaded pack."""
    path = _load_record_path(pack_name)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            data = json.load(f)
        return LoadRecord.model_validate(data)
    except Exception:
        logger.debug("Failed to read load record for %s", pack_name, exc_info=True)
        return None


def remove_load_record(pack_name: str) -> None:
    """Remove the load record after a successful unload."""
    path = _load_record_path(pack_name)
    path.unlink(missing_ok=True)


def _apply_schema_filter(
    pack_path: pathlib.Path,
    client_config: DatahubClientConfig,
) -> pathlib.Path:
    """Filter out MCPs with aspects unsupported by the target server.

    Returns the original path if no filtering is needed, or a new
    temporary file with incompatible MCPs removed.
    """
    from datahub.cli.datapack.schema_compat import (
        fetch_server_schema,
        is_mcp_compatible,
    )

    token = client_config.token
    server_schema = fetch_server_schema(str(client_config.server), token=token)
    if not server_schema:
        click.echo("Could not fetch server schema -- skipping compatibility filter.")
        return pack_path

    with open(pack_path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        return pack_path

    filtered = []
    skipped_counts: dict[str, int] = {}
    for mcp in data:
        entity_type = mcp.get("entityType", "")
        aspect_name = mcp.get("aspectName", "")

        if is_mcp_compatible(entity_type, aspect_name, server_schema):
            filtered.append(mcp)
        else:
            key = f"{entity_type}/{aspect_name}"
            skipped_counts[key] = skipped_counts.get(key, 0) + 1

    if not skipped_counts:
        click.echo("All MCPs compatible with server schema.")
        return pack_path

    total_skipped = sum(skipped_counts.values())
    click.echo(
        f"Filtered {total_skipped} incompatible MCPs "
        f"({len(filtered)}/{len(data)} remaining):"
    )
    for key, count in sorted(skipped_counts.items(), key=lambda x: -x[1]):
        click.echo(f"  {key}: {count} skipped")

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", delete=False, prefix="datapack-filtered-"
    ) as tmp:
        json.dump(filtered, tmp)
        return pathlib.Path(tmp.name)


def _collect_urn_refs(obj: object) -> list[str]:
    """Collect string values that are standalone URN references."""
    refs: list[str] = []
    if isinstance(obj, str) and obj.startswith("urn:li:"):
        refs.append(obj)
    elif isinstance(obj, dict):
        for v in obj.values():
            refs.extend(_collect_urn_refs(v))
    elif isinstance(obj, list):
        for item in obj:
            refs.extend(_collect_urn_refs(item))
    return refs


def _find_locally_missing_refs(
    data: list[dict],
) -> dict[str, set[str]]:
    """Find URN references in aspect payloads not defined in the pack."""
    defined_urns: set[str] = set()
    for mcp in data:
        urn = mcp.get("entityUrn")
        if urn:
            defined_urns.add(urn)

    locally_missing: dict[str, set[str]] = {}
    for mcp in data:
        entity_urn = mcp.get("entityUrn", "")
        aspect = mcp.get("aspect", {}).get("json", {})
        for ref in _collect_urn_refs(aspect):
            if ref != entity_urn and ref not in defined_urns:
                locally_missing.setdefault(ref, set()).add(entity_urn)

    return locally_missing


# Entity types to skip for server-side existence checks (too many, low value)
_SKIP_SERVER_CHECK_TYPES = frozenset({"schemaField"})


def _resolve_refs_on_server(
    locally_missing: dict[str, set[str]],
    client_config: DatahubClientConfig,
) -> tuple[dict[str, set[str]], int]:
    """Check locally-missing URNs against the server, return truly dangling ones."""
    from datahub.ingestion.graph.client import DataHubGraph

    graph = DataHubGraph(client_config)
    urns_to_check = [
        urn
        for urn in locally_missing
        if urn.split(":")[2] not in _SKIP_SERVER_CHECK_TYPES
    ]
    skipped_count = len(locally_missing) - len(urns_to_check)
    click.echo(
        f"Checking {len(urns_to_check)} references against server"
        + (
            f" (skipping {skipped_count} schemaField refs)..."
            if skipped_count
            else "..."
        )
    )

    truly_dangling: dict[str, set[str]] = {}
    server_resolved = 0
    for urn in urns_to_check:
        try:
            if graph.exists(urn):
                server_resolved += 1
            else:
                truly_dangling[urn] = locally_missing[urn]
        except Exception:
            truly_dangling[urn] = locally_missing[urn]

    return truly_dangling, server_resolved


def _check_referential_integrity(
    pack_path: pathlib.Path,
    client_config: Optional[DatahubClientConfig] = None,
) -> None:
    """Warn about dangling URN references in a data pack.

    Scans all MCPs for URN references not defined in the pack, then
    checks the remote server for their existence.
    """
    with open(pack_path) as f:
        data = json.load(f)

    if not isinstance(data, list):
        return

    locally_missing = _find_locally_missing_refs(data)
    if not locally_missing:
        return

    # Check against remote server
    truly_dangling: dict[str, set[str]]
    server_resolved = 0

    if client_config:
        try:
            truly_dangling, server_resolved = _resolve_refs_on_server(
                locally_missing, client_config
            )
        except Exception:
            logger.debug("Could not connect to server for ref check", exc_info=True)
            truly_dangling = locally_missing
    else:
        truly_dangling = locally_missing

    if server_resolved:
        click.echo(
            f"Resolved {server_resolved}/{len(locally_missing)} references "
            "on the server."
        )

    if not truly_dangling:
        click.echo("All references resolved (pack + server).")
        return

    click.echo(
        f"Warning: {len(truly_dangling)} dangling URN references "
        "(not in pack or on server):"
    )
    for ref_urn, referrers in sorted(truly_dangling.items(), key=lambda x: -len(x[1]))[
        :10
    ]:
        click.echo(f"  {ref_urn} (referenced by {len(referrers)} entities)")


def _extract_urns_from_file(file_path: pathlib.Path) -> set[str]:
    """Extract unique entity URNs from an MCP JSON file."""
    try:
        with open(file_path) as f:
            data = json.load(f)
        if isinstance(data, list):
            return {m.get("entityUrn", "") for m in data if m.get("entityUrn")}
    except Exception:
        pass
    return set()


def _run_pipeline_for_file(
    file_path: pathlib.Path,
    run_id: str,
    sink_config: dict[str, str],
) -> None:
    """Run an ingestion pipeline for a single data file."""
    from datahub.ingestion.run.pipeline import Pipeline

    pipeline_config = {
        "run_id": run_id,
        "source": {
            "type": "file",
            "config": {"path": str(file_path)},
        },
        "sink": {
            "type": "datahub-rest",
            "config": sink_config,
        },
    }
    pipeline = Pipeline.create(pipeline_config)
    pipeline.run()
    pipeline.pretty_print_summary()


def load_pack_into_datahub(
    pack: DataPackInfo,
    file_entries: List[IndexFileEntry],
    dry_run: bool = False,
    no_time_shift: bool = False,
    as_of: Optional[datetime] = None,
) -> str:
    """Build and run ingestion pipelines to load the data pack.

    For index files with multiple entries, runs a separate pipeline per file.
    Files marked with wait_for_completion=True will block until all emitted
    entities exist on the server before proceeding to the next file.

    Args:
        pack: The data pack metadata.
        file_entries: List of IndexFileEntry objects to load.
        dry_run: If True, preview without ingesting.
        no_time_shift: If True, skip time-shifting.
        as_of: Custom target time for time-shifting.

    Returns:
        The run_id used for this load.
    """
    client_config = load_client_config()
    run_id = _generate_run_id(pack.name)

    sink_config: dict[str, str] = {"server": str(client_config.server)}
    if client_config.token:
        sink_config["token"] = client_config.token

    # Process each file entry
    last_pipeline = None
    for i, entry in enumerate(file_entries):
        file_label = entry.path.name
        click.echo(f"\n--- File {i + 1}/{len(file_entries)}: {file_label} ---")

        effective_path = entry.path

        # Apply schema compatibility filter (downshift)
        effective_path = _apply_schema_filter(
            effective_path, client_config=client_config
        )

        # Check referential integrity (only on last file to avoid noise)
        if i == len(file_entries) - 1:
            _check_referential_integrity(effective_path, client_config=client_config)

        # Apply time-shifting if applicable
        if not no_time_shift and pack.reference_timestamp:
            target_ts = int(as_of.timestamp() * 1000) if as_of else None
            effective_path = time_shift_file(
                input_path=effective_path,
                reference_timestamp=pack.reference_timestamp,
                target_timestamp=target_ts,
            )

        if dry_run:
            click.echo(f"Dry run - would load {effective_path}")
            continue

        click.echo(f"Loading {file_label} into DataHub (run_id={run_id})...")

        # Collect URNs before loading if we need to wait
        urns_to_wait: set[str] = set()
        if entry.wait_for_completion:
            urns_to_wait = _extract_urns_from_file(effective_path)

        _run_pipeline_for_file(effective_path, run_id, sink_config)

        # Wait for entities to be processed if flagged
        if entry.wait_for_completion and urns_to_wait:
            _wait_for_entities(urns_to_wait, client_config)

    if dry_run:
        click.echo(f"\nDry run complete for {len(file_entries)} files.")
        return run_id

    # Save load record before raising so unload works even on partial failures
    save_load_record(pack, run_id)

    if last_pipeline:
        last_pipeline.raise_from_status()

    click.echo(f"Data pack '{pack.name}' loaded successfully.")
    return run_id


def unload_pack(
    pack_name: str,
    hard: bool = False,
    dry_run: bool = False,
) -> None:
    """Remove all entities loaded by a data pack.

    Uses the existing rollback infrastructure via run_id.

    Args:
        pack_name: Name of the pack to unload.
        hard: If True, hard-delete entities (irreversible).
        dry_run: If True, show what would be deleted.

    Raises:
        click.ClickException: If the pack has no load record.
    """
    record = get_load_record(pack_name)
    if record is None:
        raise click.ClickException(
            f"No load record found for pack '{pack_name}'. Nothing to unload.\n"
            "Only packs loaded via 'datahub datapack load' can be unloaded."
        )

    click.echo(
        f"Unloading data pack '{pack_name}' (run_id={record.run_id}, "
        f"loaded at {record.loaded_at})..."
    )

    if dry_run:
        click.echo(f"Dry run - would rollback run_id={record.run_id}")
        return

    from datahub.cli import cli_utils
    from datahub.ingestion.graph.client import get_default_graph
    from datahub.ingestion.graph.config import ClientMode

    graph = get_default_graph(ClientMode.CLI)
    payload = {"runId": record.run_id, "dryRun": False, "safe": not hard}

    (
        structured_rows,
        entities_affected,
        aspects_reverted,
        aspects_affected,
        unsafe_entity_count,
        unsafe_entities,
    ) = cli_utils.post_rollback_endpoint(
        graph._session, graph.config.server, payload, "/runs?action=rollback"
    )

    click.echo(
        f"Unload complete: {entities_affected} entities affected, "
        f"{aspects_reverted} aspects reverted."
    )

    if unsafe_entity_count > 0:
        click.echo(
            f"Warning: {unsafe_entity_count} entities had external modifications "
            "and were soft-deleted."
        )

    remove_load_record(pack_name)
    click.echo(f"Data pack '{pack_name}' unloaded successfully.")
