"""Tests for archive handling functionality."""

import tempfile
from pathlib import Path

from datahub.ingestion.recording.archive import (
    REPLAY_DUMMY_MARKER,
    ArchiveManifest,
    compute_checksum,
    prepare_recipe_for_replay,
    redact_secrets,
)
from datahub.ingestion.recording.config import REPLAY_DUMMY_VALUE


class TestRedactSecrets:
    """Tests for secret redaction in recipes."""

    def test_redact_password(self) -> None:
        """Test that password fields are redacted."""
        config = {
            "source": {
                "type": "snowflake",
                "config": {
                    "password": "super_secret",
                    "username": "admin",
                },
            }
        }
        redacted = redact_secrets(config)
        assert redacted["source"]["config"]["password"] == REPLAY_DUMMY_MARKER
        assert redacted["source"]["config"]["username"] == "admin"

    def test_redact_token(self) -> None:
        """Test that token fields are redacted."""
        config = {
            "source": {
                "config": {
                    "access_token": "secret_token",
                    "api_token": "another_token",
                }
            }
        }
        redacted = redact_secrets(config)
        assert redacted["source"]["config"]["access_token"] == REPLAY_DUMMY_MARKER
        assert redacted["source"]["config"]["api_token"] == REPLAY_DUMMY_MARKER

    def test_redact_api_key(self) -> None:
        """Test that api_key fields are redacted."""
        config = {"sink": {"config": {"api_key": "my_key", "endpoint": "http://gms"}}}
        redacted = redact_secrets(config)
        assert redacted["sink"]["config"]["api_key"] == REPLAY_DUMMY_MARKER
        assert redacted["sink"]["config"]["endpoint"] == "http://gms"

    def test_redact_nested(self) -> None:
        """Test redaction in deeply nested structures."""
        config = {
            "source": {
                "config": {
                    "connection": {
                        "credentials": {
                            "password": "nested_secret",
                            "username": "user",
                        }
                    }
                }
            }
        }
        redacted = redact_secrets(config)
        assert (
            redacted["source"]["config"]["connection"]["credentials"]["password"]
            == REPLAY_DUMMY_MARKER
        )
        assert (
            redacted["source"]["config"]["connection"]["credentials"]["username"]
            == "user"
        )

    def test_redact_in_list(self) -> None:
        """Test redaction in list elements."""
        config = {
            "sources": [
                {"password": "secret1", "name": "source1"},
                {"password": "secret2", "name": "source2"},
            ]
        }
        redacted = redact_secrets(config)
        assert redacted["sources"][0]["password"] == REPLAY_DUMMY_MARKER
        assert redacted["sources"][1]["password"] == REPLAY_DUMMY_MARKER
        assert redacted["sources"][0]["name"] == "source1"

    def test_non_secret_fields_preserved(self) -> None:
        """Test that non-secret fields are preserved."""
        config = {
            "source": {
                "type": "mysql",
                "config": {
                    "host": "localhost",
                    "port": 3306,
                    "database": "mydb",
                    "include_tables": True,
                },
            }
        }
        redacted = redact_secrets(config)
        assert redacted == config


class TestPrepareRecipeForReplay:
    """Tests for preparing recipes for replay."""

    def test_replace_markers_simple(self) -> None:
        """Test replacing markers in simple structure."""
        recipe = {
            "source": {
                "config": {
                    "password": REPLAY_DUMMY_MARKER,
                    "username": "admin",
                }
            }
        }
        prepared = prepare_recipe_for_replay(recipe)
        assert prepared["source"]["config"]["password"] == REPLAY_DUMMY_VALUE
        assert prepared["source"]["config"]["username"] == "admin"

    def test_replace_markers_nested(self) -> None:
        """Test replacing markers in nested structures."""
        recipe = {
            "source": {
                "config": {
                    "auth": {
                        "token": REPLAY_DUMMY_MARKER,
                    }
                }
            }
        }
        prepared = prepare_recipe_for_replay(recipe)
        assert prepared["source"]["config"]["auth"]["token"] == REPLAY_DUMMY_VALUE

    def test_replace_markers_in_list(self) -> None:
        """Test replacing markers in lists."""
        recipe = {"secrets": [REPLAY_DUMMY_MARKER, "not_a_marker", REPLAY_DUMMY_MARKER]}
        prepared = prepare_recipe_for_replay(recipe)
        assert prepared["secrets"][0] == REPLAY_DUMMY_VALUE
        assert prepared["secrets"][1] == "not_a_marker"
        assert prepared["secrets"][2] == REPLAY_DUMMY_VALUE

    def test_roundtrip(self) -> None:
        """Test that redact -> prepare produces valid config for replay."""
        original = {
            "source": {
                "type": "snowflake",
                "config": {
                    "username": "admin",
                    "password": "super_secret",
                    "host": "account.snowflakecomputing.com",
                },
            }
        }
        redacted = redact_secrets(original)
        prepared = prepare_recipe_for_replay(redacted)

        # Password should be a valid string (not the marker)
        assert prepared["source"]["config"]["password"] == REPLAY_DUMMY_VALUE
        assert prepared["source"]["config"]["password"] != REPLAY_DUMMY_MARKER
        # Non-secret fields should be preserved
        assert prepared["source"]["config"]["username"] == "admin"
        assert prepared["source"]["config"]["host"] == "account.snowflakecomputing.com"


class TestArchiveManifest:
    """Tests for ArchiveManifest class."""

    def test_create_manifest(self) -> None:
        """Test creating a manifest."""
        manifest = ArchiveManifest(
            run_id="test-run-123",
            source_type="snowflake",
            sink_type="datahub-rest",
            datahub_version="0.13.0",
        )
        assert manifest.run_id == "test-run-123"
        assert manifest.source_type == "snowflake"
        assert manifest.created_at is not None

    def test_manifest_to_dict(self) -> None:
        """Test serializing manifest to dict."""
        manifest = ArchiveManifest(
            run_id="test-run",
            source_type="mysql",
        )
        data = manifest.to_dict()
        assert data["run_id"] == "test-run"
        assert data["source_type"] == "mysql"
        assert "format_version" in data
        assert "created_at" in data

    def test_manifest_from_dict(self) -> None:
        """Test deserializing manifest from dict."""
        data = {
            "run_id": "test-123",
            "source_type": "postgres",
            "sink_type": "datahub-rest",
            "datahub_version": "0.13.0",
            "created_at": "2024-01-01T00:00:00Z",
            "checksums": {"file1.json": "abc123"},
            "format_version": "1.0.0",
        }
        manifest = ArchiveManifest.from_dict(data)
        assert manifest.run_id == "test-123"
        assert manifest.source_type == "postgres"
        assert manifest.checksums == {"file1.json": "abc123"}


class TestComputeChecksum:
    """Tests for checksum computation."""

    def test_compute_checksum(self) -> None:
        """Test computing SHA-256 checksum."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f:
            f.write("test content")
            f.flush()
            checksum = compute_checksum(Path(f.name))

        # SHA-256 of "test content"
        assert len(checksum) == 64  # SHA-256 produces 64 hex chars
        assert checksum.isalnum()

    def test_checksum_different_content(self) -> None:
        """Test that different content produces different checksums."""
        with (
            tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f1,
            tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".txt") as f2,
        ):
            f1.write("content 1")
            f2.write("content 2")
            f1.flush()
            f2.flush()

            checksum1 = compute_checksum(Path(f1.name))
            checksum2 = compute_checksum(Path(f2.name))

        assert checksum1 != checksum2
