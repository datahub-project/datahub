"""Schema compatibility filter for data pack loading.

Discovers the running server's supported (entityType, aspectName) pairs
by querying the entity registry API, then filters MCPs to prevent loading
aspects the server doesn't understand (which would poison entire batches).

Uses file-based caching keyed by server URL + commit hash (same pattern
as the GraphQL schema cache in graphql_query_adapter.py).
"""

import hashlib
import json
import logging
import os
import tempfile
from pathlib import Path
from typing import Dict, List, Optional, Set

import requests

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER

logger = logging.getLogger(__name__)

SCHEMA_CACHE_DIR = os.path.join(DATAHUB_ROOT_FOLDER, "openapi_schema_cache")


def _cache_path(server_url: str, commit_hash: str) -> Path:
    """Disk cache file path for a given server + commit hash."""
    key = hashlib.sha256(f"{server_url}:{commit_hash}".encode()).hexdigest()[:32]
    return Path(SCHEMA_CACHE_DIR) / f"{key}.json"


def _load_from_cache(
    server_url: str, commit_hash: str
) -> Optional[Dict[str, List[str]]]:
    """Load cached entity schema from disk. Returns None on miss."""
    try:
        path = _cache_path(server_url, commit_hash)
        if path.is_symlink():
            logger.warning("Ignoring symlinked cache file: %s", path)
            return None
        data = json.loads(path.read_text(encoding="utf-8"))
        logger.debug("Loaded entity schema from disk cache: %s", path)
        return data
    except FileNotFoundError:
        return None
    except Exception:
        logger.debug("Entity schema disk cache miss", exc_info=True)
        return None


def _save_to_cache(
    server_url: str, commit_hash: str, schema: Dict[str, List[str]]
) -> None:
    """Save entity schema to disk cache. Best-effort, never raises."""
    try:
        path = _cache_path(server_url, commit_hash)
        if path.is_symlink():
            logger.warning("Refusing to write to symlinked cache path: %s", path)
            return
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=".entity_schema_"
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(schema, f)
            os.replace(tmp_path, str(path))
        except Exception:
            os.unlink(tmp_path)
            raise
    except Exception:
        logger.debug("Failed to save entity schema to disk cache", exc_info=True)


def _get_commit_hash(gms_url: str, headers: Dict[str, str]) -> Optional[str]:
    """Get the server commit hash from /config."""
    try:
        r = requests.get(f"{gms_url.rstrip('/')}/config", headers=headers, timeout=10)
        r.raise_for_status()
        config = r.json()
        versions = config.get("versions") or {}
        datahub_info = versions.get("acryldata/datahub") or {}
        return datahub_info.get("commit")
    except Exception:
        logger.debug("Could not get server commit hash", exc_info=True)
        return None


def _fetch_entity_registry(
    gms_url: str, headers: Dict[str, str]
) -> Dict[str, Set[str]]:
    """Fetch the entity registry from the v1 registry API.

    Uses /openapi/v1/registry/models/entity/specifications which returns
    all entities and their aspect specs from the running server.
    """
    base = gms_url.rstrip("/")
    url = f"{base}/openapi/v1/registry/models/entity/specifications"

    entity_aspects: Dict[str, Set[str]] = {}
    start = 0

    while True:
        response = requests.get(
            url, headers=headers, params={"start": start, "count": 100}, timeout=30
        )
        response.raise_for_status()
        data = response.json()

        for entity in data.get("elements", []):
            name = entity["name"]
            aspects: Set[str] = set()
            # Add the key aspect
            if "keyAspectName" in entity:
                aspects.add(entity["keyAspectName"])
            # Add all other aspects
            for aspect_spec in entity.get("aspectSpecs", []):
                aspect_name = aspect_spec.get("aspectAnnotation", {}).get("name")
                if aspect_name:
                    aspects.add(aspect_name)
            entity_aspects[name] = aspects

        start += data.get("count", 0)
        if start >= data.get("total", 0):
            break

    return entity_aspects


def fetch_server_schema(
    gms_url: str, token: Optional[str] = None
) -> Dict[str, Set[str]]:
    """Fetch the server's supported entity-aspect pairs.

    Uses the v1 entity registry API for complete coverage of all entity types,
    with file-based caching keyed by server URL + commit hash.

    Args:
        gms_url: The GMS server URL (e.g. http://localhost:8080).
        token: Optional auth token.

    Returns:
        Dict mapping entityType to set of aspectNames.
        Empty dict if the registry cannot be fetched.
    """
    headers: Dict[str, str] = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    # Try disk cache first
    commit_hash = _get_commit_hash(gms_url, headers)
    if commit_hash:
        cached = _load_from_cache(gms_url, commit_hash)
        if cached is not None:
            result = {k: set(v) for k, v in cached.items()}
            logger.info("Using cached entity schema: %d entity types", len(result))
            return result

    # Fetch from entity registry API
    try:
        entity_aspects = _fetch_entity_registry(gms_url, headers)
    except Exception:
        logger.warning(
            "Could not fetch entity registry for schema discovery. "
            "Skipping compatibility filter.",
            exc_info=True,
        )
        return {}

    logger.info(
        "Discovered server schema: %d entity types, %d total aspects",
        len(entity_aspects),
        sum(len(v) for v in entity_aspects.values()),
    )

    # Save to disk cache
    if commit_hash:
        serializable = {k: sorted(v) for k, v in entity_aspects.items()}
        _save_to_cache(gms_url, commit_hash, serializable)

    return entity_aspects


def is_mcp_compatible(
    entity_type: str,
    aspect_name: str,
    server_schema: Dict[str, Set[str]],
) -> bool:
    """Check if an MCP's entityType+aspectName is supported by the server.

    Returns True if the aspect is supported, or if the schema is empty
    or the entity type is unknown (graceful degradation).
    """
    if not server_schema:
        return True

    supported_aspects = server_schema.get(entity_type)
    if supported_aspects is None:
        # Unknown entity type -- allow it, let the server decide
        return True

    return aspect_name in supported_aspects
