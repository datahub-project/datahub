"""Data pack registry with remote discovery and bundled fallback."""

import importlib.resources
import json
import logging
import os
import pathlib
import time
from typing import Dict, Optional

import requests

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER
from datahub.cli.datapack.models import DataPackInfo, RegistryManifest

logger = logging.getLogger(__name__)

REGISTRY_URL_ENV = "DATAHUB_DATAPACK_REGISTRY_URL"
DEFAULT_REGISTRY_URL = "https://raw.githubusercontent.com/datahub-project/datahub/master/metadata-ingestion/src/datahub/cli/datapack/resources/registry.json"

REGISTRY_CACHE_PATH = os.path.join(DATAHUB_ROOT_FOLDER, "datapack-registry-cache.json")
REGISTRY_CACHE_TTL_SECONDS = 3600  # 1 hour


def _load_bundled_registry() -> RegistryManifest:
    """Load the registry JSON bundled with the Python package."""
    ref = importlib.resources.files("datahub.cli.datapack.resources").joinpath(
        "registry.json"
    )
    data = json.loads(ref.read_text(encoding="utf-8"))
    return RegistryManifest.model_validate(data)


def _get_registry_url() -> str:
    return os.environ.get(REGISTRY_URL_ENV, DEFAULT_REGISTRY_URL)


def _read_cache() -> Optional[RegistryManifest]:
    """Read cached registry if it exists and is not expired."""
    cache_path = pathlib.Path(REGISTRY_CACHE_PATH)
    if not cache_path.exists():
        return None

    try:
        stat = cache_path.stat()
        age_seconds = time.time() - stat.st_mtime
        if age_seconds > REGISTRY_CACHE_TTL_SECONDS:
            logger.debug("Registry cache expired (age=%.0fs)", age_seconds)
            return None

        with open(cache_path) as f:
            data = json.load(f)
        return RegistryManifest.model_validate(data)
    except Exception:
        logger.debug("Failed to read registry cache", exc_info=True)
        return None


def _write_cache(manifest: RegistryManifest) -> None:
    """Write registry manifest to local cache."""
    cache_path = pathlib.Path(REGISTRY_CACHE_PATH)
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(manifest.model_dump(), f)
    except Exception:
        logger.debug("Failed to write registry cache", exc_info=True)


def _fetch_remote_registry() -> Optional[RegistryManifest]:
    """Fetch the registry JSON from the remote URL."""
    url = _get_registry_url()
    try:
        logger.debug("Fetching data pack registry from %s", url)
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        manifest = RegistryManifest.model_validate(data)
        _write_cache(manifest)
        return manifest
    except Exception:
        logger.debug("Failed to fetch remote registry from %s", url, exc_info=True)
        return None


def fetch_registry(no_cache: bool = False) -> RegistryManifest:
    """Fetch the data pack registry, falling back through layers.

    Resolution order:
    1. Local cache (if fresh and no_cache=False)
    2. Remote registry URL
    3. Bundled registry.json (shipped with the package)

    Args:
        no_cache: If True, skip the local cache and always fetch remote.

    Returns:
        The registry manifest.
    """
    # Try cache first
    if not no_cache:
        cached = _read_cache()
        if cached is not None:
            logger.debug("Using cached registry (%d packs)", len(cached.packs))
            return cached

    # Try remote
    remote = _fetch_remote_registry()
    if remote is not None:
        logger.debug("Using remote registry (%d packs)", len(remote.packs))
        return remote

    # Fall back to bundled
    bundled = _load_bundled_registry()
    logger.debug("Using bundled fallback registry (%d packs)", len(bundled.packs))
    return bundled


def list_packs(no_cache: bool = False) -> Dict[str, DataPackInfo]:
    """List all known data packs."""
    registry = fetch_registry(no_cache=no_cache)
    return dict(registry.packs)


def get_pack(name: str, no_cache: bool = False) -> DataPackInfo:
    """Look up a data pack by name.

    Raises:
        click.UsageError: If the pack is not found.
    """
    import click

    packs = list_packs(no_cache=no_cache)
    if name not in packs:
        available = ", ".join(sorted(packs.keys()))
        raise click.UsageError(
            f"Unknown data pack '{name}'. Available packs: {available}"
        )
    return packs[name]
