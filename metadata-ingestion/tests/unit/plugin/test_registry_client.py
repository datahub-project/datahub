"""Tests for the registry client."""

import json
import os
import time
from unittest.mock import MagicMock, patch

import pytest
import requests
from pydantic import ValidationError

from datahub.plugin.plugin_config import PluginCapabilityType, RegistryConfig
from datahub.plugin.registry_client import CACHE_DIR, CACHE_TTL_SECONDS, RegistryClient


class TestRegistryClient:
    def test_parse_index_list_format(self) -> None:
        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        data = [
            {
                "id": "salesforce-source",
                "repo": "acme/salesforce",
                "version": "1.0.0",
                "type": "source",
                "description": "Salesforce connector",
                "author": "Acme",
            }
        ]
        entries = client._parse_index(data)
        assert len(entries) == 1
        assert entries[0].id == "salesforce-source"
        assert entries[0].type == "source"

    def test_parse_index_dict_format(self) -> None:
        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        data = {
            "plugins": [
                {
                    "id": "my-sink",
                    "repo": "org/my-sink",
                    "version": "2.0.0",
                    "type": "sink",
                }
            ]
        }
        entries = client._parse_index(data)
        assert len(entries) == 1
        assert entries[0].id == "my-sink"

    def test_parse_index_skips_invalid(self) -> None:
        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        data = [
            {"id": "valid", "repo": "org/valid", "version": "1.0"},
            {"not_id": "invalid"},  # Missing required 'id' field
            "not-a-dict",
        ]
        entries = client._parse_index(data)
        assert len(entries) == 1

    @patch("datahub.plugin.registry_client.requests.get")
    def test_search(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "salesforce-source",
                "repo": "acme/sf",
                "version": "1.0",
                "type": "source",
                "description": "Salesforce metadata",
                "author": "Acme",
            },
            {
                "id": "mysql-source",
                "repo": "org/mysql",
                "version": "2.0",
                "type": "source",
                "description": "MySQL metadata",
                "author": "Org",
            },
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = RegistryClient(
            registries=[
                RegistryConfig(name="test", url="https://example.com/index.json")
            ]
        )
        # Ensure no cache interferes
        client.refresh()

        results = client.search("salesforce")
        assert len(results) == 1
        assert results[0].id == "salesforce-source"

    @patch("datahub.plugin.registry_client.requests.get")
    def test_search_with_type_filter(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "src", "repo": "a/b", "version": "1.0", "type": "source"},
            {"id": "snk", "repo": "a/c", "version": "1.0", "type": "sink"},
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        client.refresh()

        results = client.search("", type_filter=PluginCapabilityType.SINK)
        assert len(results) == 1
        assert results[0].type == "sink"

    @patch("datahub.plugin.registry_client.requests.get")
    def test_resolve_exact_id(self, mock_get: MagicMock) -> None:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "exact-match",
                "repo": "org/exact",
                "version": "1.0",
                "type": "source",
            },
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        client.refresh()

        result = client.resolve("exact-match")
        assert result is not None
        assert result.id == "exact-match"

        assert client.resolve("no-match") is None

    def test_cache_path(self) -> None:
        client = RegistryClient(
            registries=[RegistryConfig(name="my-registry", url="https://example.com")]
        )
        path = client._cache_path(client.registries[0])
        assert "my-registry" in path
        assert path.endswith(".json")

    @patch("datahub.plugin.registry_client.requests.get")
    def test_search_returns_frozen_copies(self, mock_get: MagicMock) -> None:
        """search() returns copies, not cached originals — no aliasing bug."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {
                "id": "my-plugin",
                "repo": "acme/my-plugin",
                "version": "1.0",
                "type": "source",
                "description": "test",
            },
        ]
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        client = RegistryClient(
            registries=[
                RegistryConfig(name="reg-a", url="https://a.example.com"),
                RegistryConfig(name="reg-b", url="https://b.example.com"),
            ]
        )
        client.refresh()

        results_a = client.search("")
        results_b = client.search("")
        # Both return the same plugin but with the registry_name set
        assert results_a[0].registry_name == "reg-a"
        assert results_b[0].registry_name == "reg-a"
        # Frozen: mutation is not possible
        with pytest.raises(ValidationError):
            results_a[0].registry_name = "mutated"  # type: ignore

    def test_bearer_auth_missing_token_returns_empty(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Registry with bearer auth returns [] when the token env var is unset."""
        monkeypatch.delenv("MY_SECRET_TOKEN", raising=False)
        registry = RegistryConfig(
            name="authed",
            url="https://private.example.com/index.json",
            auth_type="bearer",
            token_env="MY_SECRET_TOKEN",
        )
        client = RegistryClient(registries=[registry])
        client.refresh()

        # _fetch_index should warn and return [] — no network call made
        with patch("datahub.plugin.registry_client.requests.get") as mock_get:
            entries = client._fetch_index(registry)
            mock_get.assert_not_called()
        assert entries == []

    def test_parse_index_skips_entries_with_missing_version(self) -> None:
        """Entries without a version field are skipped (not defaulted to '')."""
        client = RegistryClient(
            registries=[RegistryConfig(name="test", url="https://example.com")]
        )
        data = [
            {"id": "good", "repo": "org/good", "version": "1.0", "type": "source"},
            {
                "id": "no-version",
                "repo": "org/bad",
                "type": "source",
            },  # missing version
        ]
        entries = client._parse_index(data)
        assert len(entries) == 1
        assert entries[0].id == "good"

    @patch("datahub.plugin.registry_client.requests.get")
    def test_stale_cache_fallback_on_network_failure(self, mock_get: MagicMock) -> None:
        """Falls back to stale cache when network request fails."""
        registry = RegistryConfig(
            name="stale-test", url="https://stale.example.com/index.json"
        )
        client = RegistryClient(registries=[registry])
        cache_path = client._cache_path(registry)

        # Write a stale cache file (mtime older than TTL)
        os.makedirs(CACHE_DIR, exist_ok=True)
        stale_data = [
            {
                "id": "cached-plugin",
                "repo": "acme/cached",
                "version": "0.9",
                "type": "source",
            }
        ]
        with open(cache_path, "w") as f:
            json.dump(stale_data, f)
        # Set mtime to be older than TTL
        old_time = time.time() - CACHE_TTL_SECONDS - 100
        os.utime(cache_path, (old_time, old_time))

        # Network request fails
        mock_get.side_effect = requests.ConnectionError("network down")

        try:
            results = client.search("")
            assert len(results) == 1
            assert results[0].id == "cached-plugin"
        finally:
            if os.path.isfile(cache_path):
                os.remove(cache_path)

    def test_corrupted_cache_is_removed_and_returns_empty(self) -> None:
        """A cache file with invalid JSON is warned about, deleted, and yields []."""
        registry = RegistryConfig(
            name="corrupt-test", url="https://corrupt.example.com/index.json"
        )
        client = RegistryClient(registries=[registry])
        cache_path = client._cache_path(registry)

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_path, "w") as f:
            f.write("{ this is not valid json")
        try:
            entries = client._load_cache(cache_path)
            assert entries == []
            assert not os.path.isfile(cache_path)  # corrupt cache removed
        finally:
            if os.path.isfile(cache_path):
                os.remove(cache_path)

    @patch("datahub.plugin.registry_client.requests.get")
    def test_network_failure_no_cache_records_warning(
        self, mock_get: MagicMock
    ) -> None:
        """When fetch fails and there is no cache, a fetch_warning is recorded."""
        registry = RegistryConfig(
            name="unreachable", url="https://unreachable.example.com/index.json"
        )
        client = RegistryClient(registries=[registry])
        cache_path = client._cache_path(registry)
        if os.path.isfile(cache_path):
            os.remove(cache_path)

        mock_get.side_effect = requests.ConnectionError("network down")

        entries = client._fetch_index(registry)
        assert entries == []
        assert any("unreachable" in w for w in client.fetch_warnings)

    @patch("datahub.plugin.registry_client.requests.get")
    def test_fresh_cache_hit_skips_network(self, mock_get: MagicMock) -> None:
        """A cache newer than the TTL is used without any network request."""
        registry = RegistryConfig(
            name="fresh-test", url="https://fresh.example.com/index.json"
        )
        client = RegistryClient(registries=[registry])
        cache_path = client._cache_path(registry)

        os.makedirs(CACHE_DIR, exist_ok=True)
        with open(cache_path, "w") as f:
            json.dump(
                [{"id": "c", "repo": "o/c", "version": "1.0", "type": "source"}], f
            )
        # Fresh mtime (now) => within TTL.
        try:
            entries = client._fetch_index(registry)
            assert len(entries) == 1
            mock_get.assert_not_called()
        finally:
            if os.path.isfile(cache_path):
                os.remove(cache_path)

    @patch("datahub.plugin.registry_client.requests.get")
    def test_fetch_warnings_deduplicated(self, mock_get: MagicMock) -> None:
        """Re-fetching a failing registry does not pile up duplicate warnings."""
        registry = RegistryConfig(
            name="dup-test", url="https://dup.example.com/index.json"
        )
        client = RegistryClient(registries=[registry])
        cache_path = client._cache_path(registry)
        if os.path.isfile(cache_path):
            os.remove(cache_path)

        mock_get.side_effect = requests.ConnectionError("down")
        client._fetch_index(registry)
        client._fetch_index(registry)
        assert len(client.fetch_warnings) == 1
