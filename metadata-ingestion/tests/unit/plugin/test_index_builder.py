"""Tests for the registry index builder."""

import hashlib
import zipfile
from pathlib import Path
from typing import Union
from unittest.mock import MagicMock, patch

import pytest

from datahub.plugin.github_resolver import ResolvedGitSource, ResolvedWheel
from datahub.plugin.index_builder import (
    IndexSource,
    IndexSources,
    build_entries,
    build_index,
    load_sources,
    read_manifests_from_wheel,
)
from datahub.plugin.plugin_config import (
    PluginCapabilityType,
    SupportStatusType,
    TrustTier,
)

_MANIFEST_YAML = """\
api_version: datahub/v1
id: my-source
name: My Source
type: source
entry_point: my_pkg.source:MySource
description: A test source
author: tester
url: https://example.com/docs
icon_url: https://example.com/icon.png
support_status: COMMUNITY
capabilities:
  - capability: SCHEMA_METADATA
    description: Extract schema
    supported: true
"""


# A multi-plugin manifest: shared package metadata (author/url) declared once,
# two connectors under a `plugins:` list.
_MULTI_MANIFEST_YAML = """\
api_version: datahub/v1
author: tester
url: https://example.com/docs
plugins:
  - id: source-a
    name: Source A
    type: source
    entry_point: my_pkg.a:SourceA
    support_status: COMMUNITY
  - id: source-b
    name: Source B
    type: source
    entry_point: my_pkg.b:SourceB
    icon_url: https://example.com/b.png
"""


def _make_wheel(tmp_path: Path, manifest: str = _MANIFEST_YAML) -> Path:
    wheel = tmp_path / "my_pkg-0.1.0-py3-none-any.whl"
    with zipfile.ZipFile(wheel, "w") as zf:
        zf.writestr("my_pkg/__init__.py", "")
        zf.writestr("my_pkg/datahub-plugin.yaml", manifest)
    return wheel


class TestReadManifestsFromWheel:
    def test_reads_manifest(self, tmp_path: Path) -> None:
        manifests = read_manifests_from_wheel(str(_make_wheel(tmp_path)))
        assert len(manifests) == 1
        manifest = manifests[0]
        assert manifest.id == "my-source"
        assert manifest.type == PluginCapabilityType.SOURCE
        assert len(manifest.capabilities) == 1
        assert manifest.icon_url == "https://example.com/icon.png"

    def test_reads_multiple_plugins(self, tmp_path: Path) -> None:
        wheel = _make_wheel(tmp_path, manifest=_MULTI_MANIFEST_YAML)
        manifests = read_manifests_from_wheel(str(wheel))
        assert [m.id for m in manifests] == ["source-a", "source-b"]
        # Shared package-level fields propagate to every plugin...
        assert all(m.author == "tester" for m in manifests)
        assert all(m.url == "https://example.com/docs" for m in manifests)
        # ...while a per-entry value stays local to its plugin.
        assert manifests[1].icon_url == "https://example.com/b.png"
        assert manifests[0].icon_url is None

    def test_missing_manifest_raises(self, tmp_path: Path) -> None:
        wheel = tmp_path / "empty-0.1.0-py3-none-any.whl"
        with zipfile.ZipFile(wheel, "w") as zf:
            zf.writestr("empty/__init__.py", "")
        with pytest.raises(ValueError, match="No datahub-plugin.yaml"):
            read_manifests_from_wheel(str(wheel))


class TestBuildEntries:
    @patch("datahub.plugin.index_builder.download_wheel")
    @patch("datahub.plugin.index_builder.resolve_github_spec")
    def test_builds_unified_entry(
        self, mock_resolve: MagicMock, mock_download: MagicMock, tmp_path: Path
    ) -> None:
        wheel = _make_wheel(tmp_path)
        mock_resolve.return_value = ResolvedWheel(
            download_url="https://example.com/w.whl", version="0.1.0"
        )
        mock_download.return_value = str(wheel)

        entries = build_entries(
            IndexSource(
                repo="acme/my-source", version="0.1.0", trust_tier=TrustTier.VERIFIED
            )
        )

        assert len(entries) == 1
        entry = entries[0]
        # Curation fields come from the source; the rest from the manifest.
        assert entry.repo == "acme/my-source"
        assert entry.trust_tier == TrustTier.VERIFIED
        assert entry.id == "my-source"
        assert entry.type == PluginCapabilityType.SOURCE
        assert entry.support_status == SupportStatusType.COMMUNITY
        assert len(entry.capabilities) == 1
        assert entry.source_url == "https://example.com/docs"
        assert entry.icon_url == "https://example.com/icon.png"
        # Checksum is computed from the actual wheel bytes.
        assert entry.sha256 == hashlib.sha256(wheel.read_bytes()).hexdigest()
        mock_resolve.assert_called_once_with("github:acme/my-source@0.1.0")

    @patch("datahub.plugin.index_builder.download_wheel")
    @patch("datahub.plugin.index_builder.resolve_github_spec")
    def test_multi_plugin_wheel_yields_entry_per_plugin(
        self, mock_resolve: MagicMock, mock_download: MagicMock, tmp_path: Path
    ) -> None:
        wheel = _make_wheel(tmp_path, manifest=_MULTI_MANIFEST_YAML)
        mock_resolve.return_value = ResolvedWheel(
            download_url="https://example.com/w.whl", version="0.1.0"
        )
        mock_download.return_value = str(wheel)

        entries = build_entries(IndexSource(repo="acme/bundle", version="0.1.0"))

        assert [e.id for e in entries] == ["source-a", "source-b"]
        # Every entry shares the repo, version, and checksum of the one wheel...
        expected_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
        assert {e.repo for e in entries} == {"acme/bundle"}
        assert {e.sha256 for e in entries} == {expected_sha}
        # ...but keeps its own manifest-derived identity.
        assert entries[1].icon_url == "https://example.com/b.png"

    @patch("datahub.plugin.index_builder.resolve_github_spec")
    def test_no_wheel_release_raises(self, mock_resolve: MagicMock) -> None:
        mock_resolve.return_value = ResolvedGitSource(
            download_url="git+https://github.com/acme/x.git@v1", version="1.0"
        )
        with pytest.raises(ValueError, match="no release wheel"):
            build_entries(IndexSource(repo="acme/x", version="1.0"))


class TestBuildIndex:
    @patch("datahub.plugin.index_builder.download_wheel")
    @patch("datahub.plugin.index_builder.resolve_github_spec")
    def test_skips_and_records_failures(
        self, mock_resolve: MagicMock, mock_download: MagicMock, tmp_path: Path
    ) -> None:
        wheel = _make_wheel(tmp_path)

        def resolve_side_effect(spec: str) -> Union[ResolvedWheel, ResolvedGitSource]:
            if "good" in spec:
                return ResolvedWheel(download_url="https://x/w.whl", version="0.1.0")
            return ResolvedGitSource(download_url="git+https://x.git@v1", version="1.0")

        mock_resolve.side_effect = resolve_side_effect
        mock_download.return_value = str(wheel)

        result = build_index(
            IndexSources(
                plugins=[
                    IndexSource(repo="acme/good", version="0.1.0"),
                    IndexSource(repo="acme/bad", version="1.0"),  # no wheel
                ]
            )
        )

        assert len(result.entries) == 1
        assert result.entries[0].repo == "acme/good"
        assert len(result.errors) == 1
        assert "acme/bad" in result.errors[0]


def test_load_sources(tmp_path: Path) -> None:
    sources_file = tmp_path / "sources.yaml"
    sources_file.write_text(
        "plugins:\n"
        "  - repo: acme/a\n"
        "    version: '1.0'\n"
        "    trust_tier: verified\n"
        "  - repo: acme/b\n"
        "    version: '2.0'\n"
    )
    sources = load_sources(str(sources_file))
    assert len(sources.plugins) == 2
    assert sources.plugins[0].trust_tier == TrustTier.VERIFIED
    assert sources.plugins[1].trust_tier == TrustTier.COMMUNITY  # default
