"""Fetch and search plugin indexes from community or enterprise registries.

Indexes are JSON files hosted at configurable URLs (typically a raw file in
a GitHub repo). The client caches them locally with a configurable TTL.
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import requests
from pydantic import (
    BaseModel,
    ConfigDict,
    ValidationError,
    ValidationInfo,
    field_validator,
)

from datahub.plugin.plugin_config import (
    PLUGINS_DIR,
    PluginCapabilityType,
    PluginSystemConfig,
    RegistryConfig,
    TrustTier,
)

logger = logging.getLogger(__name__)

DEFAULT_COMMUNITY_REGISTRY = RegistryConfig(
    name="community",
    url="https://raw.githubusercontent.com/datahub-project/datahub-plugins/main/index.json",
    enabled=True,
)

CACHE_DIR = os.path.join(PLUGINS_DIR, ".index_cache")
CACHE_TTL_SECONDS = 3600  # 1 hour


class PluginIndexEntry(BaseModel):
    """A single plugin listed in a registry index.

    Parsed with pydantic so enum fields (``type``, ``trust_tier``) are actually
    validated and coerced at construction, not merely annotated. Unknown keys in
    the index JSON are ignored so the schema can evolve without breaking reads.
    """

    model_config = ConfigDict(extra="ignore", frozen=True)

    id: str
    repo: str
    version: str
    type: PluginCapabilityType = PluginCapabilityType.SOURCE
    description: str = ""
    author: str = ""
    sha256: Optional[str] = None
    trust_tier: TrustTier = TrustTier.COMMUNITY
    registry_name: str = ""
    display_name: str = ""
    icon_url: Optional[str] = None
    recipe_template: Optional[str] = None

    @field_validator("id", "repo", "version")
    @classmethod
    def _not_empty(cls, v: str, info: ValidationInfo) -> str:
        if not v:
            raise ValueError(f"PluginIndexEntry.{info.field_name} must not be empty")
        return v


@dataclass
class RegistryClient:
    """Client for fetching and searching plugin indexes."""

    registries: List[RegistryConfig] = field(default_factory=list)
    fetch_warnings: List[str] = field(default_factory=list, repr=False)

    def __post_init__(self) -> None:
        if not self.registries:
            config = PluginSystemConfig.load()
            self.registries = config.registries or [DEFAULT_COMMUNITY_REGISTRY]

    def _add_warning(self, msg: str) -> None:
        """Record a fetch warning, de-duplicated (fetches repeat per search)."""
        if msg not in self.fetch_warnings:
            self.fetch_warnings.append(msg)

    def _get_enabled_entries(
        self,
    ) -> List[Tuple[RegistryConfig, PluginIndexEntry]]:
        """Return ``(registry, entry)`` pairs from all enabled registries."""
        results: List[Tuple[RegistryConfig, PluginIndexEntry]] = []
        for registry in self.registries:
            if not registry.enabled:
                continue
            for entry in self._fetch_index(registry):
                results.append((registry, entry))
        return results

    def search(
        self, query: str, type_filter: Optional[PluginCapabilityType] = None
    ) -> List[PluginIndexEntry]:
        """Search across all enabled registries for plugins matching *query*."""
        results: List[PluginIndexEntry] = []
        query_lower = query.lower()

        for registry, entry in self._get_enabled_entries():
            if type_filter and entry.type != type_filter:
                continue
            if (
                query_lower in entry.id.lower()
                or query_lower in entry.description.lower()
                or query_lower in entry.author.lower()
            ):
                results.append(
                    entry.model_copy(update={"registry_name": registry.name})
                )

        return results

    def resolve(self, plugin_id: str) -> Optional[PluginIndexEntry]:
        """Find a plugin by exact ID across all registries."""
        for registry, entry in self._get_enabled_entries():
            if entry.id == plugin_id:
                return entry.model_copy(update={"registry_name": registry.name})
        return None

    def list_all(
        self, type_filter: Optional[PluginCapabilityType] = None
    ) -> List[PluginIndexEntry]:
        """List all plugins from all enabled registries."""
        return self.search("", type_filter=type_filter)

    def refresh(self) -> None:
        """Force refresh all registry caches."""
        for registry in self.registries:
            cache_path = self._cache_path(registry)
            if os.path.isfile(cache_path):
                os.remove(cache_path)
        logger.info("Cleared all registry caches")

    def _fetch_index(self, registry: RegistryConfig) -> List[PluginIndexEntry]:
        """Fetch the index from cache or network."""
        cache_path = self._cache_path(registry)

        # Check cache
        if os.path.isfile(cache_path):
            mtime = os.path.getmtime(cache_path)
            if time.time() - mtime < CACHE_TTL_SECONDS:
                return self._load_cache(cache_path)

        # Fetch from network
        headers: Dict[str, str] = {"Accept": "application/json"}
        if registry.auth_type == "bearer" and registry.token_env:
            token = os.environ.get(registry.token_env)
            if token:
                headers["Authorization"] = f"Bearer {token}"
            else:
                msg = (
                    f"Registry '{registry.name}' requires auth token in env var "
                    f"{registry.token_env}, but it is not set. "
                    "Skipping this registry. Set the env var and try again."
                )
                logger.warning(msg)
                self._add_warning(msg)
                return []

        try:
            resp = requests.get(registry.url, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except (requests.RequestException, json.JSONDecodeError):
            logger.warning(
                "Failed to fetch registry index from %s",
                registry.url,
                exc_info=True,
            )
            # Fall back to stale cache if available
            if os.path.isfile(cache_path):
                logger.info(
                    "Using stale cache for registry '%s' due to network failure",
                    registry.name,
                )
                return self._load_cache(cache_path)
            msg = (
                f"Could not reach registry '{registry.name}' at {registry.url} "
                "and no cached data is available."
            )
            logger.warning(msg)
            self._add_warning(msg)
            return []

        # Write cache
        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(data, f)

        return self._parse_index(data)

    def _cache_path(self, registry: RegistryConfig) -> str:
        safe_name = registry.name.replace("/", "_").replace(":", "_")
        return os.path.join(CACHE_DIR, f"{safe_name}.json")

    def _load_cache(self, path: str) -> List[PluginIndexEntry]:
        try:
            with open(path) as f:
                data = json.load(f)
            return self._parse_index(data)
        except (json.JSONDecodeError, OSError):
            logger.warning(
                "Failed to load cache from %s; removing corrupted cache",
                path,
                exc_info=True,
            )
            try:
                os.remove(path)
            except OSError:
                logger.warning(
                    "Failed to remove corrupted cache file %s; "
                    "registry searches may return empty results. "
                    "Manually delete the file to resolve.",
                    path,
                    exc_info=True,
                )
            return []

    def _parse_index(self, data: object) -> List[PluginIndexEntry]:
        """Parse raw JSON into PluginIndexEntry list.

        Supports both a flat list and a ``{"plugins": [...]}`` wrapper.
        """
        if isinstance(data, dict):
            entries_raw = data.get("plugins", [])
        elif isinstance(data, list):
            entries_raw = data
        else:
            logger.warning(
                "Unexpected registry index format: expected dict or list, got %s",
                type(data).__name__,
            )
            return []

        entries: List[PluginIndexEntry] = []
        for item in entries_raw:
            if not isinstance(item, dict):
                logger.debug("Skipping non-dict index entry: %s", type(item).__name__)
                continue
            try:
                entries.append(PluginIndexEntry.model_validate(item))
            except ValidationError:
                logger.warning("Skipping invalid index entry: %s", item, exc_info=True)

        return entries
