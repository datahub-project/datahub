"""Pydantic models for the DataHub plugin system.

Defines the manifest schema (what a plugin declares about itself),
the discovered-plugin record (from scanning importlib.metadata), and
registry configuration.

Plugin discovery uses ``importlib.metadata`` to scan the current Python
environment for packages containing ``datahub-plugin.yaml`` manifests.
Global config (registry URLs, index cache) lives under
``~/.datahub/plugins/``.
"""

import logging
import os
import re
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from datahub.cli.config_utils import DATAHUB_ROOT_FOLDER
from datahub.utilities.str_enum import StrEnum

logger = logging.getLogger(__name__)

# Global: registry configuration and index cache are user-level, not per-venv.
PLUGINS_DIR = os.path.join(DATAHUB_ROOT_FOLDER, "plugins")
PLUGIN_CONFIG_PATH = os.path.join(PLUGINS_DIR, "config.yaml")

MANIFEST_FILENAME = "datahub-plugin.yaml"


class PluginCapabilityType(StrEnum):
    SOURCE = "source"
    SINK = "sink"
    TRANSFORMER = "transformer"


class TrustTier(StrEnum):
    COMMUNITY = "community"
    VERIFIED = "verified"
    OFFICIAL = "official"


class SupportStatusType(StrEnum):
    CERTIFIED = "CERTIFIED"
    INCUBATING = "INCUBATING"
    TESTING = "TESTING"
    COMMUNITY = "COMMUNITY"
    UNKNOWN = "UNKNOWN"


class PluginCapabilityEntry(BaseModel):
    """A single capability declared by a plugin in its manifest."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    capability: str = Field(
        description="Capability name, e.g. 'SCHEMA_METADATA', 'LINEAGE_COARSE'."
    )
    description: str = Field(default="", description="Human-readable description.")
    supported: bool = Field(
        default=True, description="Whether this capability is supported."
    )


class PluginCompatibility(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    datahub_min: Optional[str] = Field(
        default=None, description="Minimum DataHub version required (e.g. '0.14.0')."
    )
    python_min: Optional[str] = Field(
        default=None, description="Minimum Python version required (e.g. '3.9')."
    )

    @field_validator("datahub_min", "python_min")
    @classmethod
    def _validate_version_string(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            from packaging.version import InvalidVersion, Version

            try:
                Version(v)
            except InvalidVersion as e:
                raise ValueError(
                    f"Invalid version string: {v!r}. "
                    "Must be a valid PEP 440 version (e.g. '0.14.0')."
                ) from e
        return v


_PLUGIN_ID_RE = re.compile(r"^[a-z][a-z0-9_-]*$")


class PluginManifest(BaseModel):
    """Parsed from datahub-plugin.yaml bundled with the plugin wheel."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    api_version: Literal["datahub/v1"] = Field(
        default="datahub/v1", description="Manifest schema version."
    )
    id: str = Field(description="Unique plugin identifier (e.g. 'salesforce-source').")
    name: str = Field(description="Human-readable display name.")
    type: PluginCapabilityType = Field(description="Plugin capability type.")
    entry_point: str = Field(
        description="Dotted import path to the plugin class (e.g. 'my_plugin.source:MySource')."
    )
    config_class: Optional[str] = Field(
        default=None,
        description="Dotted import path to the Pydantic config class, if any.",
    )
    description: str = Field(default="", description="Short description of the plugin.")
    author: str = Field(default="", description="Plugin author or organization.")
    url: Optional[str] = Field(
        default=None, description="Documentation or repository URL."
    )
    icon_url: Optional[str] = Field(
        default=None, description="URL to a logo/icon image for the plugin."
    )
    compatibility: PluginCompatibility = Field(
        default_factory=PluginCompatibility,
        description="Version compatibility constraints.",
    )
    support_status: Optional[SupportStatusType] = Field(
        default=None,
        description="Connector maturity tier (e.g. CERTIFIED, INCUBATING, COMMUNITY).",
    )
    capabilities: List[PluginCapabilityEntry] = Field(
        default_factory=list,
        description="Capabilities this plugin supports.",
    )

    @field_validator("capabilities")
    @classmethod
    def _no_duplicate_capabilities(
        cls,
        v: List[PluginCapabilityEntry],
    ) -> List[PluginCapabilityEntry]:
        seen: set[str] = set()
        for entry in v:
            if entry.capability in seen:
                raise ValueError(f"Duplicate capability name: {entry.capability!r}")
            seen.add(entry.capability)
        return v

    @field_validator("id")
    @classmethod
    def _id_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Plugin id must not be empty or whitespace-only")
        if not _PLUGIN_ID_RE.match(v):
            raise ValueError(
                f"Plugin id must start with a lowercase letter and contain only "
                f"lowercase letters, digits, hyphens, and underscores; got {v!r}"
            )
        return v

    @field_validator("name")
    @classmethod
    def _name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Plugin name must not be empty or whitespace-only")
        return v

    @field_validator("entry_point", "config_class")
    @classmethod
    def _import_path_format(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        if ":" in v:
            module, _, attr = v.partition(":")
            if not module or not attr:
                raise ValueError(
                    f"Invalid colon-syntax import path: {v!r}. "
                    "Expected 'module:ClassName'."
                )
        elif "." in v:
            module, _, attr = v.rpartition(".")
            if not module or not attr:
                raise ValueError(
                    f"Invalid dotted import path: {v!r}. "
                    "Expected 'package.module.ClassName'."
                )
        else:
            raise ValueError(
                f"Must be a dotted import path or module:Class, got: {v!r}"
            )
        return v


class PluginManifestFile(BaseModel):
    """A parsed ``datahub-plugin.yaml``, which may declare one or many plugins.

    Two on-disk shapes are accepted:

    * **Flat** (single plugin) — the manifest fields sit at the top level. This
      is the original format and is treated as a one-element list.
    * **Multi** — a top-level ``plugins:`` list, one entry per connector shipped
      by the same wheel. Any *other* top-level keys (``author``, ``url``,
      ``compatibility``, ``icon_url``, …) are shared defaults merged into every
      entry, so package-wide metadata is declared once; a per-entry value wins
      over the shared default.

    Either shape normalizes to ``plugins: List[PluginManifest]`` so downstream
    consumers keep working with a single-plugin object. The ``plugins`` key is
    the sole discriminator between the two shapes — it is not a valid
    ``PluginManifest`` field, so a flat manifest can never contain it.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    plugins: List[PluginManifest] = Field(
        description="One or more plugins declared by this manifest file."
    )

    @model_validator(mode="before")
    @classmethod
    def _normalize_shape(cls, data: object) -> object:
        if not isinstance(data, dict):
            return data
        if "plugins" not in data:
            # Flat (legacy) single-plugin form: wrap it as a one-element list.
            return {"plugins": [data]}
        raw_plugins = data["plugins"]
        if not isinstance(raw_plugins, list):
            raise ValueError("'plugins' must be a list of plugin entries")
        shared = {k: v for k, v in data.items() if k != "plugins"}
        merged: List[object] = []
        for entry in raw_plugins:
            if isinstance(entry, dict):
                # Per-entry values override shared package-level defaults.
                merged.append({**shared, **entry})
            else:
                # Already a PluginManifest (direct construction) — pass through
                # untouched; pydantic validates it as-is.
                merged.append(entry)
        return {"plugins": merged}

    @field_validator("plugins")
    @classmethod
    def _non_empty_and_unique_ids(cls, v: List[PluginManifest]) -> List[PluginManifest]:
        if not v:
            raise ValueError("Manifest must declare at least one plugin")
        seen: set[str] = set()
        for p in v:
            if p.id in seen:
                raise ValueError(f"Duplicate plugin id in manifest: {p.id!r}")
            seen.add(p.id)
        return v


class DiscoveredPlugin(BaseModel):
    """A plugin found by scanning importlib.metadata in the current environment."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    manifest: PluginManifest = Field(description="Parsed plugin manifest.")
    package_name: str = Field(description="Python package name (pip name).")
    version: str = Field(description="Installed package version.")
    location: Optional[str] = Field(
        default=None, description="Filesystem path to the manifest."
    )

    @field_validator("package_name")
    @classmethod
    def _package_name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("package_name must not be empty")
        return v


class RegistryConfig(BaseModel):
    """Configuration for a single plugin registry (community or enterprise)."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(description="Registry identifier used for caching and display.")
    url: str = Field(description="URL to the registry index JSON file.")
    enabled: bool = Field(default=True, description="Whether this registry is active.")
    auth_type: Optional[Literal["bearer"]] = Field(
        default=None, description="Authentication type for the registry."
    )
    token_env: Optional[str] = Field(
        default=None,
        description="Name of the environment variable holding the auth token.",
    )

    @field_validator("name")
    @classmethod
    def _name_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Registry name must not be empty")
        return v

    @field_validator("url")
    @classmethod
    def _url_not_empty_and_valid_scheme(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Registry url must not be empty")
        if not v.startswith(("https://", "http://")):
            raise ValueError(
                f"Registry url must start with https:// or http://, got: {v!r}"
            )
        return v

    @model_validator(mode="after")
    def _auth_requires_token(self) -> "RegistryConfig":
        if self.auth_type is not None and not self.token_env:
            raise ValueError(
                f"auth_type={self.auth_type!r} requires token_env to be set"
            )
        return self


class PluginSystemConfig(BaseModel):
    """Top-level config stored at ~/.datahub/plugins/config.yaml."""

    model_config = ConfigDict(extra="forbid")

    registries: List[RegistryConfig] = Field(
        default_factory=list,
        description="List of plugin registry configurations.",
    )

    @staticmethod
    def load() -> "PluginSystemConfig":
        if not os.path.isfile(PLUGIN_CONFIG_PATH):
            return PluginSystemConfig()
        try:
            with open(PLUGIN_CONFIG_PATH) as f:
                data = yaml.safe_load(f) or {}
            return PluginSystemConfig.model_validate(data)
        except (yaml.YAMLError, ValueError, OSError):
            logger.warning(
                "Failed to parse plugin config at %s; using defaults",
                PLUGIN_CONFIG_PATH,
                exc_info=True,
            )
            return PluginSystemConfig()

    def save(self) -> None:
        os.makedirs(PLUGINS_DIR, exist_ok=True)
        tmp_path = PLUGIN_CONFIG_PATH + ".tmp"
        with open(tmp_path, "w") as f:
            yaml.dump(
                self.model_dump(),
                f,
                default_flow_style=False,
                sort_keys=False,
            )
        os.replace(tmp_path, PLUGIN_CONFIG_PATH)
