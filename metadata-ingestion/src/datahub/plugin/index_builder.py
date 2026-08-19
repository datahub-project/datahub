"""Build a registry ``index.json`` from a curated sources list.

This is the registry-maintainer side of the marketplace. Authors declare their
plugin's capabilities and support status via ``@capability`` / ``@support_status``
decorators, which ``datahub plugin sync`` writes into ``datahub-plugin.yaml`` and
bundles into the wheel. A maintainer curates a small sources list (which plugins,
which versions, what trust tier); for each one this module downloads the release
wheel to obtain both the ``sha256`` and the bundled manifest, then emits a unified
``PluginIndexEntry``. So capabilities are generated from code, and only curation
(the plugin list + trust tier) is maintained by hand.
"""

import hashlib
import logging
import zipfile
from dataclasses import dataclass, field
from typing import List, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field

from datahub.plugin.github_resolver import (
    ResolvedWheel,
    download_wheel,
    resolve_github_spec,
)
from datahub.plugin.plugin_config import (
    MANIFEST_FILENAME,
    PluginManifest,
    PluginManifestFile,
    TrustTier,
)
from datahub.plugin.registry_client import PluginIndexEntry

logger = logging.getLogger(__name__)


class IndexSource(BaseModel):
    """One curated plugin in a sources file (the hand-maintained input)."""

    model_config = ConfigDict(extra="forbid")

    repo: str = Field(description="GitHub 'owner/repo' hosting the plugin's releases.")
    version: str = Field(description="Release version to index.")
    trust_tier: TrustTier = Field(
        default=TrustTier.COMMUNITY,
        description="Governance tier assigned by the maintainer, not the author.",
    )
    package_name: Optional[str] = Field(
        default=None,
        description="Optional PyPI package name (installs via pypi: when set).",
    )


class IndexSources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    plugins: List[IndexSource] = Field(default_factory=list)


@dataclass
class IndexBuildResult:
    entries: List[PluginIndexEntry] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


def load_sources(path: str) -> IndexSources:
    with open(path) as f:
        data = yaml.safe_load(f) or {}
    return IndexSources.model_validate(data)


def read_manifests_from_wheel(wheel_path: str) -> List[PluginManifest]:
    """Extract and parse ``datahub-plugin.yaml`` from a wheel (a zip archive).

    The manifest is bundled as package data at ``<pkg>/datahub-plugin.yaml``; the
    shortest matching path (the package-root manifest) is used if several match.
    A single manifest may declare multiple plugins, so this returns a list.
    """
    with zipfile.ZipFile(wheel_path) as zf:
        candidates = [n for n in zf.namelist() if n.endswith(MANIFEST_FILENAME)]
        if not candidates:
            raise ValueError(f"No {MANIFEST_FILENAME} found in wheel {wheel_path}")
        manifest_name = min(candidates, key=len)
        data = yaml.safe_load(zf.read(manifest_name))
    return PluginManifestFile.model_validate(data).plugins


def _sha256_of_file(path: str) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def build_entries(source: IndexSource) -> List[PluginIndexEntry]:
    """Resolve one curated source into its index entries.

    Downloads the release wheel once and uses it for both the checksum and the
    bundled manifest. A wheel that ships several connectors yields one entry per
    plugin — all sharing the same repo, version, and checksum, but with distinct
    ids, types, and capabilities. Raises ``ValueError`` if the release has no
    wheel asset.
    """
    resolved = resolve_github_spec(f"github:{source.repo}@{source.version}")
    if not isinstance(resolved, ResolvedWheel):
        raise ValueError(
            f"{source.repo}@{source.version} has no release wheel; a wheel asset "
            "is required to compute a checksum and read the manifest."
        )
    wheel_path = download_wheel(resolved)
    sha256 = _sha256_of_file(wheel_path)
    manifests = read_manifests_from_wheel(wheel_path)

    return [
        PluginIndexEntry(
            id=manifest.id,
            repo=source.repo,
            version=source.version,
            type=manifest.type,
            description=manifest.description,
            author=manifest.author,
            display_name=manifest.name,
            icon_url=manifest.icon_url,
            sha256=sha256,
            trust_tier=source.trust_tier,
            support_status=manifest.support_status,
            capabilities=list(manifest.capabilities),
            source_url=manifest.url,
            package_name=source.package_name,
        )
        for manifest in manifests
    ]


def build_index(sources: IndexSources) -> IndexBuildResult:
    """Build entries for all curated sources, skipping and recording failures."""
    result = IndexBuildResult()
    for source in sources.plugins:
        try:
            entries = build_entries(source)
            result.entries.extend(entries)
            logger.info(
                "Indexed %s@%s (%d plugin(s))",
                source.repo,
                source.version,
                len(entries),
            )
        except Exception as e:
            msg = f"{source.repo}@{source.version}: {e}"
            logger.warning("Failed to index %s", msg, exc_info=True)
            result.errors.append(msg)
    return result
