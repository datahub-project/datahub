"""Tests for the data pack loader."""

import hashlib
import json
import pathlib
from unittest.mock import MagicMock, patch

import click
import pytest

from datahub.cli.datapack.loader import (
    _apply_schema_filter,
    _cache_key,
    _sha256_file,
    check_trust,
    download_pack,
    get_load_record,
    remove_load_record,
    save_load_record,
)
from datahub.cli.datapack.models import DataPackInfo, TrustTier


class TestCacheKey:
    def test_deterministic(self) -> None:
        url = "https://example.com/data.json"
        assert _cache_key(url) == _cache_key(url)

    def test_different_urls_different_keys(self) -> None:
        assert _cache_key("https://a.com/1") != _cache_key("https://a.com/2")

    def test_is_sha256_hex(self) -> None:
        key = _cache_key("https://example.com")
        assert len(key) == 64
        int(key, 16)  # Should not raise


class TestSha256File:
    def test_sha256_file(self, tmp_path: pathlib.Path) -> None:
        test_file = tmp_path / "test.json"
        test_file.write_text('{"hello": "world"}')
        expected = hashlib.sha256(b'{"hello": "world"}').hexdigest()
        assert _sha256_file(test_file) == expected


class TestCheckTrust:
    def test_verified_always_passes(self) -> None:
        pack = DataPackInfo(
            name="test", description="d", url="https://x.com", trust=TrustTier.VERIFIED
        )
        check_trust(pack)  # Should not raise

    def test_community_blocked_without_flag(self) -> None:
        pack = DataPackInfo(
            name="test", description="d", url="https://x.com", trust=TrustTier.COMMUNITY
        )
        with pytest.raises(click.ClickException, match="community"):
            check_trust(pack)

    def test_community_allowed_with_flag(self) -> None:
        pack = DataPackInfo(
            name="test", description="d", url="https://x.com", trust=TrustTier.COMMUNITY
        )
        check_trust(pack, trust_community=True)  # Should not raise

    def test_custom_blocked_without_flag(self) -> None:
        pack = DataPackInfo(
            name="test", description="d", url="https://x.com", trust=TrustTier.CUSTOM
        )
        with pytest.raises(click.ClickException, match="unverified"):
            check_trust(pack)

    def test_custom_allowed_with_flag(self) -> None:
        pack = DataPackInfo(
            name="test", description="d", url="https://x.com", trust=TrustTier.CUSTOM
        )
        check_trust(pack, trust_custom=True)  # Should not raise


class TestLoadRecord:
    def test_save_and_get_record(self, tmp_path: pathlib.Path) -> None:
        pack = DataPackInfo(
            name="test-pack",
            description="Test",
            url="https://example.com/data.json",
            sha256="abc123",
        )
        with patch("datahub.cli.datapack.loader.LOADS_DIR", str(tmp_path)):
            save_load_record(pack, run_id="datapack-test-123")
            record = get_load_record("test-pack")

        assert record is not None
        assert record.run_id == "datapack-test-123"
        assert record.pack_name == "test-pack"
        assert record.pack_sha256 == "abc123"

    def test_get_nonexistent_record(self, tmp_path: pathlib.Path) -> None:
        with patch("datahub.cli.datapack.loader.LOADS_DIR", str(tmp_path)):
            record = get_load_record("does-not-exist")
        assert record is None

    def test_remove_record(self, tmp_path: pathlib.Path) -> None:
        pack = DataPackInfo(
            name="test-pack",
            description="Test",
            url="https://example.com/data.json",
        )
        with patch("datahub.cli.datapack.loader.LOADS_DIR", str(tmp_path)):
            save_load_record(pack, run_id="datapack-test-123")
            remove_load_record("test-pack")
            record = get_load_record("test-pack")
        assert record is None

    def test_save_overwrites_previous(self, tmp_path: pathlib.Path) -> None:
        pack = DataPackInfo(
            name="test-pack",
            description="Test",
            url="https://example.com/data.json",
        )
        with patch("datahub.cli.datapack.loader.LOADS_DIR", str(tmp_path)):
            save_load_record(pack, run_id="run-1")
            save_load_record(pack, run_id="run-2")
            record = get_load_record("test-pack")
        assert record is not None
        assert record.run_id == "run-2"


class TestDownloadPack:
    def test_file_url_copies_local_file(self, tmp_path: pathlib.Path) -> None:
        # Create a local file
        source = tmp_path / "source.json"
        source.write_text('[{"test": true}]')
        sha = _sha256_file(source)

        pack = DataPackInfo(
            name="local-test",
            description="Test",
            url=f"file://{source}",
            sha256=sha,
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            entries = download_pack(pack)

        assert len(entries) == 1
        assert entries[0].path.exists()
        assert json.loads(entries[0].path.read_text()) == [{"test": True}]

    def test_file_url_missing_file_raises(self, tmp_path: pathlib.Path) -> None:
        pack = DataPackInfo(
            name="missing",
            description="Test",
            url="file:///nonexistent/path.json",
        )
        cache_dir = tmp_path / "cache"
        with (
            patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)),
            pytest.raises(click.ClickException, match="Local file not found"),
        ):
            download_pack(pack)

    def test_sha256_mismatch_raises(self, tmp_path: pathlib.Path) -> None:
        source = tmp_path / "source.json"
        source.write_text("[]")

        pack = DataPackInfo(
            name="bad-hash",
            description="Test",
            url=f"file://{source}",
            sha256="0000000000000000000000000000000000000000000000000000000000000000",
        )
        cache_dir = tmp_path / "cache"
        with (
            patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)),
            pytest.raises(click.ClickException, match="SHA256 mismatch"),
        ):
            download_pack(pack)

    def test_cached_file_reused(self, tmp_path: pathlib.Path) -> None:
        source = tmp_path / "source.json"
        source.write_text("[]")
        sha = _sha256_file(source)

        pack = DataPackInfo(
            name="cached",
            description="Test",
            url=f"file://{source}",
            sha256=sha,
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            entries1 = download_pack(pack)
            # Delete the source -- should still work from cache
            source.unlink()
            entries2 = download_pack(pack)

        assert entries1[0].path == entries2[0].path

    def test_no_cache_forces_redownload(self, tmp_path: pathlib.Path) -> None:
        source = tmp_path / "source.json"
        source.write_text("[]")
        sha = _sha256_file(source)

        pack = DataPackInfo(
            name="nocache",
            description="Test",
            url=f"file://{source}",
            sha256=sha,
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            download_pack(pack)
            # Corrupt the cache
            cached = list(cache_dir.glob("*.json"))[0]
            cached.write_text("corrupted")
            # no_cache=True should re-copy from source
            download_pack(pack, no_cache=True)
            assert json.loads(cached.read_text()) == []


class TestApplySchemaFilter:
    def test_filters_incompatible_mcps(self, tmp_path: pathlib.Path) -> None:
        mcps = [
            {"entityType": "dataset", "aspectName": "ownership"},
            {"entityType": "dataset", "aspectName": "cloudOnlyAspect"},
            {"entityType": "dataset", "aspectName": "status"},
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        schema = {"dataset": {"ownership", "status"}}
        mock_config = MagicMock()
        mock_config.server = "http://localhost:8080"
        mock_config.token = "test"

        with patch(
            "datahub.cli.datapack.schema_compat.fetch_server_schema",
            return_value=schema,
        ):
            result = _apply_schema_filter(pack_path, client_config=mock_config)

        filtered = json.loads(result.read_text())
        assert len(filtered) == 2
        assert all(m["aspectName"] in ("ownership", "status") for m in filtered)

    def test_returns_original_when_all_compatible(self, tmp_path: pathlib.Path) -> None:
        mcps = [
            {"entityType": "dataset", "aspectName": "ownership"},
            {"entityType": "dataset", "aspectName": "status"},
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        schema = {"dataset": {"ownership", "status"}}
        mock_config = MagicMock()
        mock_config.server = "http://localhost:8080"
        mock_config.token = "test"

        with patch(
            "datahub.cli.datapack.schema_compat.fetch_server_schema",
            return_value=schema,
        ):
            result = _apply_schema_filter(pack_path, client_config=mock_config)

        assert result == pack_path  # No new file created

    def test_returns_original_when_schema_empty(self, tmp_path: pathlib.Path) -> None:
        pack_path = tmp_path / "data.json"
        pack_path.write_text("[]")

        mock_config = MagicMock()
        mock_config.server = "http://localhost:8080"
        mock_config.token = None

        with patch(
            "datahub.cli.datapack.schema_compat.fetch_server_schema",
            return_value={},
        ):
            result = _apply_schema_filter(pack_path, client_config=mock_config)

        assert result == pack_path

    def test_unknown_entity_passes_through(self, tmp_path: pathlib.Path) -> None:
        mcps = [
            {"entityType": "newEntity", "aspectName": "someAspect"},
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        schema = {"dataset": {"ownership"}}
        mock_config = MagicMock()
        mock_config.server = "http://localhost:8080"
        mock_config.token = "test"

        with patch(
            "datahub.cli.datapack.schema_compat.fetch_server_schema",
            return_value=schema,
        ):
            result = _apply_schema_filter(pack_path, client_config=mock_config)

        assert result == pack_path  # Unknown entity, no filtering


class TestIndexFileResolution:
    def test_data_file_passes_through(self, tmp_path: pathlib.Path) -> None:
        from datahub.cli.datapack.loader import _resolve_index_file

        data = [{"entityType": "dataset", "aspectName": "status"}]
        data_file = tmp_path / "data.json"
        data_file.write_text(json.dumps(data))

        pack = DataPackInfo(name="test", description="t", url=f"file://{data_file}")
        entries = _resolve_index_file(data_file, pack, no_cache=False)
        assert len(entries) == 1
        assert entries[0].path == data_file
        assert not entries[0].wait_for_completion

    def test_index_file_returns_multiple_entries(self, tmp_path: pathlib.Path) -> None:
        from datahub.cli.datapack.loader import _resolve_index_file

        (tmp_path / "01-users.json").write_text(
            json.dumps([{"entityType": "corpuser", "aspectName": "corpUserKey"}])
        )
        (tmp_path / "02-datasets.json").write_text(
            json.dumps([{"entityType": "dataset", "aspectName": "datasetKey"}])
        )

        index = {"files": ["01-users.json", "02-datasets.json"]}
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(name="test", description="t", url=f"file://{index_file}")
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(tmp_path / "cache")):
            entries = _resolve_index_file(index_file, pack, no_cache=False)

        assert len(entries) == 2
        assert all(e.path.exists() for e in entries)

    def test_index_with_wait_for_completion(self, tmp_path: pathlib.Path) -> None:
        from datahub.cli.datapack.loader import _resolve_index_file

        (tmp_path / "defs.json").write_text(
            json.dumps([{"entityType": "structuredProperty"}])
        )
        (tmp_path / "data.json").write_text(json.dumps([{"entityType": "dataset"}]))

        index = {
            "files": [
                {"path": "defs.json", "wait_for_completion": True},
                {"path": "data.json"},
            ]
        }
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(name="test", description="t", url=f"file://{index_file}")
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(tmp_path / "cache")):
            entries = _resolve_index_file(index_file, pack, no_cache=False)

        assert len(entries) == 2
        assert entries[0].wait_for_completion is True
        assert entries[1].wait_for_completion is False

    def test_index_skips_missing_files(self, tmp_path: pathlib.Path) -> None:
        from datahub.cli.datapack.loader import _resolve_index_file

        (tmp_path / "exists.json").write_text(
            json.dumps([{"entityType": "dataset", "aspectName": "status"}])
        )

        index = {"files": ["exists.json", "missing.json"]}
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(name="test", description="t", url=f"file://{index_file}")
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(tmp_path / "cache")):
            entries = _resolve_index_file(index_file, pack, no_cache=False)

        assert len(entries) == 1

    def test_unknown_object_passes_through(self, tmp_path: pathlib.Path) -> None:
        from datahub.cli.datapack.loader import _resolve_index_file

        obj = {"some_key": "some_value"}
        obj_file = tmp_path / "weird.json"
        obj_file.write_text(json.dumps(obj))

        pack = DataPackInfo(name="test", description="t", url=f"file://{obj_file}")
        entries = _resolve_index_file(obj_file, pack, no_cache=False)
        assert len(entries) == 1
        assert entries[0].path == obj_file

    def test_cache_hit_returns_same_entries(self, tmp_path: pathlib.Path) -> None:
        """Cache hit returns the same entries on second call."""
        source = tmp_path / "source.json"
        source.write_text("[]")
        sha = _sha256_file(source)

        pack = DataPackInfo(
            name="simple",
            description="Test",
            url=f"file://{source}",
            sha256=sha,
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            entries1 = download_pack(pack)
            entries2 = download_pack(pack)
            assert len(entries1) == 1
            assert len(entries2) == 1
            assert entries1[0].path == entries2[0].path


class TestCheckVersionCompatibility:
    def test_no_version_required_passes(self) -> None:
        from datahub.cli.datapack.loader import check_version_compatibility

        pack = DataPackInfo(name="t", description="t", url="https://x.com")
        check_version_compatibility(pack)  # Should not raise

    def test_connection_failure_without_force_raises(self) -> None:
        from datahub.cli.datapack.loader import check_version_compatibility

        pack = DataPackInfo(
            name="t", description="t", url="https://x.com", min_server_version="0.14.0"
        )
        with (
            patch(
                "datahub.cli.datapack.loader.load_client_config",
                side_effect=Exception("no config"),
            ),
            pytest.raises(click.ClickException, match="Could not connect"),
        ):
            check_version_compatibility(pack)

    def test_connection_failure_with_force_passes(self) -> None:
        from datahub.cli.datapack.loader import check_version_compatibility

        pack = DataPackInfo(
            name="t", description="t", url="https://x.com", min_server_version="0.14.0"
        )
        with patch(
            "datahub.cli.datapack.loader.load_client_config",
            side_effect=Exception("no config"),
        ):
            check_version_compatibility(pack, force=True)  # Should not raise


class TestReferentialIntegrity:
    def test_detects_dangling_references(
        self, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from datahub.cli.datapack.loader import _check_referential_integrity

        mcps = [
            {
                "entityType": "domain",
                "entityUrn": "urn:li:domain:child",
                "aspectName": "domainProperties",
                "aspect": {
                    "json": {
                        "name": "Child",
                        "parentDomain": "urn:li:domain:missing-parent",
                    }
                },
            },
            {
                "entityType": "domain",
                "entityUrn": "urn:li:domain:child",
                "aspectName": "domainKey",
                "aspect": {"json": {"id": "child"}},
            },
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        _check_referential_integrity(pack_path)
        captured = capsys.readouterr()
        assert "dangling URN references" in captured.out
        assert "urn:li:domain:missing-parent" in captured.out

    def test_no_warning_when_all_refs_present(
        self, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from datahub.cli.datapack.loader import _check_referential_integrity

        mcps = [
            {
                "entityType": "domain",
                "entityUrn": "urn:li:domain:parent",
                "aspectName": "domainKey",
                "aspect": {"json": {"id": "parent"}},
            },
            {
                "entityType": "domain",
                "entityUrn": "urn:li:domain:child",
                "aspectName": "domainProperties",
                "aspect": {
                    "json": {
                        "name": "Child",
                        "parentDomain": "urn:li:domain:parent",
                    }
                },
            },
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        _check_referential_integrity(pack_path)
        captured = capsys.readouterr()
        assert "dangling" not in captured.out

    def test_self_reference_not_flagged(
        self, tmp_path: pathlib.Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        from datahub.cli.datapack.loader import _check_referential_integrity

        mcps = [
            {
                "entityType": "dataset",
                "entityUrn": "urn:li:dataset:self",
                "aspectName": "upstreamLineage",
                "aspect": {"json": {"upstreams": [{"dataset": "urn:li:dataset:self"}]}},
            },
        ]
        pack_path = tmp_path / "data.json"
        pack_path.write_text(json.dumps(mcps))

        _check_referential_integrity(pack_path)
        captured = capsys.readouterr()
        assert "dangling" not in captured.out
