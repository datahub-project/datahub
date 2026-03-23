"""Tests for the schema compatibility filter."""

import pathlib
from unittest.mock import MagicMock, patch

from datahub.cli.datapack.schema_compat import (
    _cache_path,
    _load_from_cache,
    _save_to_cache,
    fetch_server_schema,
    is_mcp_compatible,
)


class TestIsMcpCompatible:
    def test_compatible_aspect(self) -> None:
        schema = {"dataset": {"schemaMetadata", "ownership", "status"}}
        assert is_mcp_compatible("dataset", "schemaMetadata", schema)

    def test_incompatible_aspect(self) -> None:
        schema = {"dataset": {"schemaMetadata", "ownership", "status"}}
        assert not is_mcp_compatible("dataset", "lineageFeatures", schema)

    def test_unknown_entity_allowed(self) -> None:
        schema = {"dataset": {"schemaMetadata"}}
        assert is_mcp_compatible("unknownEntity", "someAspect", schema)

    def test_empty_schema_allows_everything(self) -> None:
        assert is_mcp_compatible("dataset", "anything", {})

    def test_entity_with_no_aspects_rejects(self) -> None:
        schema = {"dataset": set()}
        assert not is_mcp_compatible("dataset", "schemaMetadata", schema)


class TestCachePath:
    def test_deterministic(self) -> None:
        p1 = _cache_path("http://localhost:8080", "abc123")
        p2 = _cache_path("http://localhost:8080", "abc123")
        assert p1 == p2

    def test_different_inputs_different_paths(self) -> None:
        p1 = _cache_path("http://localhost:8080", "abc")
        p2 = _cache_path("http://localhost:8080", "def")
        assert p1 != p2


class TestDiskCache:
    def test_save_and_load(self, tmp_path: pathlib.Path) -> None:
        schema = {"dataset": ["ownership", "status"], "chart": ["chartInfo"]}
        with patch(
            "datahub.cli.datapack.schema_compat.SCHEMA_CACHE_DIR", str(tmp_path)
        ):
            _save_to_cache("http://localhost:8080", "commit1", schema)
            loaded = _load_from_cache("http://localhost:8080", "commit1")

        assert loaded is not None
        assert sorted(loaded["dataset"]) == ["ownership", "status"]
        assert loaded["chart"] == ["chartInfo"]

    def test_load_missing_returns_none(self, tmp_path: pathlib.Path) -> None:
        with patch(
            "datahub.cli.datapack.schema_compat.SCHEMA_CACHE_DIR", str(tmp_path)
        ):
            result = _load_from_cache("http://localhost:8080", "nonexistent")
        assert result is None

    def test_different_commit_hash_misses(self, tmp_path: pathlib.Path) -> None:
        schema = {"dataset": ["ownership"]}
        with patch(
            "datahub.cli.datapack.schema_compat.SCHEMA_CACHE_DIR", str(tmp_path)
        ):
            _save_to_cache("http://localhost:8080", "commit1", schema)
            result = _load_from_cache("http://localhost:8080", "commit2")
        assert result is None

    def test_symlink_rejected_on_load(self, tmp_path: pathlib.Path) -> None:
        target = tmp_path / "target.json"
        target.write_text("{}")
        with patch(
            "datahub.cli.datapack.schema_compat.SCHEMA_CACHE_DIR", str(tmp_path)
        ):
            cache_file = _cache_path("http://localhost:8080", "commit1")
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_file.symlink_to(target)
            result = _load_from_cache("http://localhost:8080", "commit1")
        assert result is None


class TestFetchServerSchema:
    @patch("datahub.cli.datapack.schema_compat._get_commit_hash")
    @patch("datahub.cli.datapack.schema_compat._fetch_entity_registry")
    def test_fetches_and_returns_schema(
        self, mock_fetch: MagicMock, mock_hash: MagicMock
    ) -> None:
        mock_hash.return_value = None  # No caching
        mock_fetch.return_value = {
            "dataset": {"schemaMetadata", "ownership"},
            "chart": {"chartInfo"},
        }
        result = fetch_server_schema("http://localhost:8080", token="test")
        assert "dataset" in result
        assert "schemaMetadata" in result["dataset"]
        mock_fetch.assert_called_once()

    @patch("datahub.cli.datapack.schema_compat._get_commit_hash")
    @patch("datahub.cli.datapack.schema_compat._fetch_entity_registry")
    def test_uses_cache_on_hit(
        self,
        mock_fetch: MagicMock,
        mock_hash: MagicMock,
        tmp_path: pathlib.Path,
    ) -> None:
        mock_hash.return_value = "cached_commit"
        cached_data = {"dataset": ["ownership", "status"]}
        with patch(
            "datahub.cli.datapack.schema_compat.SCHEMA_CACHE_DIR", str(tmp_path)
        ):
            _save_to_cache("http://localhost:8080", "cached_commit", cached_data)
            result = fetch_server_schema("http://localhost:8080", token="test")
        # Should NOT have called the registry API
        mock_fetch.assert_not_called()
        assert "dataset" in result
        assert "ownership" in result["dataset"]

    @patch("datahub.cli.datapack.schema_compat._get_commit_hash")
    @patch("datahub.cli.datapack.schema_compat._fetch_entity_registry")
    def test_returns_empty_on_failure(
        self, mock_fetch: MagicMock, mock_hash: MagicMock
    ) -> None:
        mock_hash.return_value = None
        mock_fetch.side_effect = Exception("connection refused")
        result = fetch_server_schema("http://localhost:8080")
        assert result == {}
