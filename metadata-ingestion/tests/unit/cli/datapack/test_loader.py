"""Tests for the data pack loader."""

import hashlib
import json
import os
import pathlib
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import click
import pytest

from datahub.cli.datapack.loader import (
    EMIT_MODE_ENV,
    IndexFileEntry,
    _apply_schema_filter,
    _build_datapack_sink_config,
    _cache_key,
    _cached_path,
    _datapack_emit_mode,
    _run_pipeline_for_file,
    _sha256_file,
    check_trust,
    download_pack,
    get_load_record,
    ingest_datapack_file_entries,
    is_cached,
    load_pack_into_datahub,
    remove_load_record,
    save_load_record,
)
from datahub.cli.datapack.models import DataPackInfo, TrustTier
from datahub.ingestion.graph.config import DatahubClientConfig


class TestDatapackSinkConfig:
    def test_build_datapack_sink_config_openapi_async_batch(self) -> None:
        config = DatahubClientConfig(server="http://localhost:8080", token="tok")
        sink = _build_datapack_sink_config(config)
        assert sink == {
            "server": "http://localhost:8080",
            "token": "tok",
            "endpoint": "openapi",
            "mode": "async_batch",
        }

    def test_build_datapack_sink_config_no_token(self) -> None:
        config = DatahubClientConfig(server="http://localhost:8080")
        sink = _build_datapack_sink_config(config)
        assert "token" not in sink
        assert sink["endpoint"] == "openapi"
        assert sink["mode"] == "async_batch"

    def test_datapack_emit_mode_async_wait(self) -> None:
        prior = os.environ.get(EMIT_MODE_ENV)
        try:
            with _datapack_emit_mode(True) as mode:
                assert mode == "async_wait"
                assert os.environ[EMIT_MODE_ENV] == "async_wait"
            assert os.environ.get(EMIT_MODE_ENV) == prior
        finally:
            if prior is None:
                os.environ.pop(EMIT_MODE_ENV, None)
            else:
                os.environ[EMIT_MODE_ENV] = prior

    def test_datapack_emit_mode_async(self) -> None:
        with _datapack_emit_mode(False) as mode:
            assert mode == "async"
            assert os.environ[EMIT_MODE_ENV] == "async"

    def test_datapack_emit_mode_restores_prior_value(self) -> None:
        os.environ[EMIT_MODE_ENV] = "sync"
        try:
            with _datapack_emit_mode(True) as mode:
                assert mode == "async_wait"
                assert os.environ[EMIT_MODE_ENV] == "async_wait"
            assert os.environ[EMIT_MODE_ENV] == "sync"
        finally:
            os.environ.pop(EMIT_MODE_ENV, None)

    @patch("datahub.ingestion.run.pipeline.Pipeline")
    def test_run_pipeline_for_file_passes_wait_flag(
        self, mock_pipeline_cls: MagicMock, tmp_path: pathlib.Path
    ) -> None:
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        mock_pipeline = MagicMock()
        mock_pipeline_cls.create.return_value = mock_pipeline

        sink_config = {
            "server": "http://localhost:8080",
            "endpoint": "openapi",
            "mode": "async_batch",
        }

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop(EMIT_MODE_ENV, None)
            _run_pipeline_for_file(
                data_file, "run-1", sink_config, wait_for_completion=True
            )
            assert os.environ.get(EMIT_MODE_ENV) is None

        mock_pipeline_cls.create.assert_called_once()
        mock_pipeline.run.assert_called_once()
        mock_pipeline.raise_from_status.assert_called_once()

    @patch("datahub.ingestion.run.pipeline.Pipeline")
    def test_run_pipeline_for_file_logs_emit_mode_when_waiting(
        self,
        mock_pipeline_cls: MagicMock,
        tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        mock_pipeline_cls.create.return_value = MagicMock()

        sink_config = {
            "server": "http://localhost:8080",
            "endpoint": "openapi",
            "mode": "async_batch",
        }
        _run_pipeline_for_file(
            data_file, "run-1", sink_config, wait_for_completion=True
        )
        assert "async_wait" in capsys.readouterr().out


class TestIngestDatapackFileEntries:
    @patch("datahub.cli.datapack.loader._run_pipeline_for_file")
    @patch("datahub.cli.datapack.loader._check_referential_integrity")
    @patch("datahub.cli.datapack.loader._apply_schema_filter")
    def test_ingest_runs_pipeline_per_entry(
        self,
        mock_filter: MagicMock,
        mock_ref: MagicMock,
        mock_run: MagicMock,
        tmp_path: pathlib.Path,
    ) -> None:
        mock_filter.side_effect = lambda path, **_: path
        first = tmp_path / "defs.json"
        second = tmp_path / "data.json"
        first.write_text("[]")
        second.write_text("[]")
        pack = DataPackInfo(
            name="test-pack",
            description="test",
            url="https://example.com/index.json",
            trust=TrustTier.VERIFIED,
        )
        entries = [
            IndexFileEntry(first, wait_for_completion=True),
            IndexFileEntry(second),
        ]
        client_config = DatahubClientConfig(server="http://localhost:8080", token="tok")

        ingest_datapack_file_entries(
            pack,
            entries,
            "run-abc",
            client_config=client_config,
            log_progress=False,
        )

        assert mock_run.call_count == 2
        mock_run.assert_any_call(
            first,
            "run-abc",
            {
                "server": "http://localhost:8080",
                "token": "tok",
                "endpoint": "openapi",
                "mode": "async_batch",
            },
            wait_for_completion=True,
        )
        mock_ref.assert_called_once_with(second, client_config=client_config)

    @patch("datahub.cli.datapack.loader._run_pipeline_for_file")
    @patch("datahub.cli.datapack.loader._check_referential_integrity")
    @patch("datahub.cli.datapack.loader._apply_schema_filter")
    @patch("datahub.cli.datapack.loader.time_shift_file")
    def test_ingest_time_shifts_with_as_of(
        self,
        mock_shift: MagicMock,
        mock_filter: MagicMock,
        mock_ref: MagicMock,
        mock_run: MagicMock,
        tmp_path: pathlib.Path,
    ) -> None:
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        shifted = tmp_path / "shifted.json"
        shifted.write_text("[]")
        mock_filter.side_effect = lambda path, **_: path
        mock_shift.return_value = shifted

        pack = DataPackInfo(
            name="test-pack",
            description="test",
            url="https://example.com/data.json",
            trust=TrustTier.VERIFIED,
            reference_timestamp=1700000000000,
        )
        as_of = datetime(2024, 6, 1, 12, 0, tzinfo=timezone.utc)
        client_config = DatahubClientConfig(server="http://localhost:8080")

        ingest_datapack_file_entries(
            pack,
            [IndexFileEntry(data_file)],
            "run-abc",
            client_config=client_config,
            no_time_shift=False,
            as_of=as_of,
            log_progress=False,
        )

        mock_shift.assert_called_once_with(
            input_path=data_file,
            reference_timestamp=1700000000000,
            target_timestamp=int(as_of.timestamp() * 1000),
        )
        mock_run.assert_called_once_with(
            shifted,
            "run-abc",
            {
                "server": "http://localhost:8080",
                "endpoint": "openapi",
                "mode": "async_batch",
            },
            wait_for_completion=False,
        )

    @patch("datahub.cli.datapack.loader._run_pipeline_for_file")
    @patch("datahub.cli.datapack.loader._check_referential_integrity")
    @patch("datahub.cli.datapack.loader._apply_schema_filter")
    def test_ingest_logs_progress_when_enabled(
        self,
        mock_filter: MagicMock,
        mock_ref: MagicMock,
        mock_run: MagicMock,
        tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        mock_filter.side_effect = lambda path, **_: path
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        client_config = DatahubClientConfig(server="http://localhost:8080")

        ingest_datapack_file_entries(
            DataPackInfo(
                name="bootstrap",
                description="test",
                url="https://example.com/bootstrap.json",
                trust=TrustTier.VERIFIED,
            ),
            [IndexFileEntry(data_file)],
            "run-abc",
            client_config=client_config,
            log_progress=True,
        )

        output = capsys.readouterr().out
        assert "File 1/1" in output
        assert "data.json" in output

    @patch("datahub.cli.datapack.loader.load_client_config")
    @patch("datahub.cli.datapack.loader._run_pipeline_for_file")
    @patch("datahub.cli.datapack.loader._check_referential_integrity")
    @patch("datahub.cli.datapack.loader._apply_schema_filter")
    def test_ingest_loads_client_config_when_omitted(
        self,
        mock_filter: MagicMock,
        mock_ref: MagicMock,
        mock_run: MagicMock,
        mock_load_config: MagicMock,
        tmp_path: pathlib.Path,
    ) -> None:
        mock_filter.side_effect = lambda path, **_: path
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        mock_load_config.return_value = DatahubClientConfig(
            server="http://gms:8080", token="secret"
        )

        ingest_datapack_file_entries(
            DataPackInfo(
                name="bootstrap",
                description="test",
                url="https://example.com/bootstrap.json",
                trust=TrustTier.VERIFIED,
            ),
            [IndexFileEntry(data_file)],
            "run-xyz",
            log_progress=False,
        )

        mock_load_config.assert_called_once()
        mock_run.assert_called_once()
        assert mock_run.call_args.args[2]["server"] == "http://gms:8080"


class TestLoadPackIntoDatahub:
    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.loader._generate_run_id", return_value="generated-run")
    @patch("datahub.cli.datapack.loader.load_client_config")
    def test_dry_run_skips_ingest(
        self,
        mock_load_config: MagicMock,
        mock_run_id: MagicMock,
        mock_ingest: MagicMock,
        tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        pack = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )
        mock_load_config.return_value = DatahubClientConfig(
            server="http://localhost:8080"
        )

        run_id = load_pack_into_datahub(pack, [IndexFileEntry(data_file)], dry_run=True)

        assert run_id == "generated-run"
        mock_ingest.assert_not_called()
        output = capsys.readouterr().out
        assert "Dry run" in output
        assert "data.json" in output

    @patch("datahub.cli.datapack.loader.save_load_record")
    @patch("datahub.cli.datapack.loader.ingest_datapack_file_entries")
    @patch("datahub.cli.datapack.loader._generate_run_id", return_value="generated-run")
    @patch("datahub.cli.datapack.loader.load_client_config")
    def test_load_pack_ingests_and_records(
        self,
        mock_load_config: MagicMock,
        mock_run_id: MagicMock,
        mock_ingest: MagicMock,
        mock_save: MagicMock,
        tmp_path: pathlib.Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        data_file = tmp_path / "data.json"
        data_file.write_text("[]")
        pack = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )
        client_config = DatahubClientConfig(server="http://localhost:8080")
        mock_load_config.return_value = client_config
        entries = [IndexFileEntry(data_file)]

        run_id = load_pack_into_datahub(pack, entries)

        assert run_id == "generated-run"
        mock_ingest.assert_called_once_with(
            pack,
            entries,
            "generated-run",
            client_config=client_config,
            no_time_shift=False,
            as_of=None,
        )
        mock_save.assert_called_once_with(pack, "generated-run")
        assert "loaded successfully" in capsys.readouterr().out


class TestIsCached:
    def test_is_cached_when_pack_file_exists(self, tmp_path: pathlib.Path) -> None:
        pack = DataPackInfo(
            name="bootstrap",
            description="test",
            url="https://example.com/bootstrap.json",
            trust=TrustTier.VERIFIED,
        )
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir()
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            _cached_path(pack).write_text("[]")
            assert is_cached(pack) is True
            assert (
                is_cached(
                    DataPackInfo(
                        name="other",
                        description="test",
                        url="https://example.com/other.json",
                        trust=TrustTier.VERIFIED,
                    )
                )
                is False
            )


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


class TestIndexVersionCache:
    def test_version_match_uses_cache(self, tmp_path: pathlib.Path) -> None:
        """When remote version matches cached version, data files are not re-fetched."""
        from datahub.cli.datapack.loader import (
            _save_index_version,
        )

        # Create data files
        (tmp_path / "data.json").write_text(json.dumps([{"entityType": "dataset"}]))

        # Create index with version
        index = {"version": "1", "files": ["data.json"]}
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(
            name="versioned", description="t", url=f"file://{index_file}"
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            # First load — downloads everything
            entries1 = download_pack(pack)
            assert len(entries1) == 1

            # Save version to simulate prior load
            _save_index_version(pack, "1")

            # Second load — should use cache (version matches)
            entries2 = download_pack(pack)
            assert len(entries2) == 1
            assert entries2[0].path == entries1[0].path

    def test_version_mismatch_redownloads(self, tmp_path: pathlib.Path) -> None:
        """When remote version differs from cached, data files are re-downloaded."""
        from datahub.cli.datapack.loader import (
            _save_index_version,
        )

        (tmp_path / "data.json").write_text(json.dumps([{"entityType": "dataset"}]))

        index = {"version": "2", "files": ["data.json"]}
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(
            name="versioned", description="t", url=f"file://{index_file}"
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            # First load
            entries1 = download_pack(pack)
            assert len(entries1) == 1

            # Save OLD version
            _save_index_version(pack, "1")

            # Second load — version mismatch, should re-download
            entries2 = download_pack(pack)
            assert len(entries2) == 1

    def test_no_version_field_redownloads(self, tmp_path: pathlib.Path) -> None:
        """Index without version field always re-downloads data files."""
        (tmp_path / "data.json").write_text(json.dumps([{"entityType": "dataset"}]))

        index = {"files": ["data.json"]}  # No version field
        index_file = tmp_path / "index.json"
        index_file.write_text(json.dumps(index))

        pack = DataPackInfo(
            name="unversioned", description="t", url=f"file://{index_file}"
        )
        cache_dir = tmp_path / "cache"
        with patch("datahub.cli.datapack.loader.CACHE_DIR", str(cache_dir)):
            entries = download_pack(pack)
            assert len(entries) == 1

    def test_non_index_pack_unchanged(self, tmp_path: pathlib.Path) -> None:
        """Non-index packs (direct data files) use existing cache behavior."""
        source = tmp_path / "data.json"
        source.write_text(json.dumps([{"entityType": "dataset"}]))
        sha = _sha256_file(source)

        pack = DataPackInfo(
            name="direct", description="t", url=f"file://{source}", sha256=sha
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
