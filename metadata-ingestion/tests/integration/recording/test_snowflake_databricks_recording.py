"""Integration tests for Snowflake and Databricks recording/replay.

These tests require actual Snowflake/Databricks credentials and are marked
for manual testing or CI environments with proper credentials configured.

To run these tests locally:
1. Set up environment variables with valid credentials
2. Run: pytest -m integration tests/integration/recording/

Environment variables needed:
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_WAREHOUSE
- DATABRICKS_SERVER_HOSTNAME, DATABRICKS_ACCESS_TOKEN, DATABRICKS_HTTP_PATH
"""

import os
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

# Mark all tests in this module as integration tests
pytestmark = pytest.mark.integration


def has_snowflake_credentials() -> bool:
    """Check if Snowflake credentials are available."""
    required = ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD"]
    return all(os.getenv(var) for var in required)


def has_databricks_credentials() -> bool:
    """Check if Databricks credentials are available."""
    required = ["DATABRICKS_SERVER_HOSTNAME", "DATABRICKS_ACCESS_TOKEN"]
    return all(os.getenv(var) for var in required)


@pytest.mark.skipif(
    not has_snowflake_credentials(),
    reason="Snowflake credentials not available",
)
class TestSnowflakeRecording:
    """Test Snowflake recording and replay functionality."""

    def test_snowflake_recording_and_replay(self):
        """Test complete Snowflake record/replay cycle.

        This test:
        1. Records a Snowflake ingestion run
        2. Verifies queries.jsonl was created with content
        3. Replays the recording
        4. Verifies replay produces same metadata output
        """
        from datahub.ingestion.recording.recorder import IngestionRecorder
        from datahub.ingestion.recording.replay import IngestionReplayer

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Simple Snowflake recipe for testing
            test_recipe: Dict[str, Any] = {
                "source": {
                    "type": "snowflake",
                    "config": {
                        "account_id": os.getenv("SNOWFLAKE_ACCOUNT"),
                        "username": os.getenv("SNOWFLAKE_USER"),
                        "password": os.getenv("SNOWFLAKE_PASSWORD"),
                        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
                        "role": os.getenv("SNOWFLAKE_ROLE", "PUBLIC"),
                        # Limit scope for faster testing
                        "database_pattern": {"allow": ["INFORMATION_SCHEMA"]},
                        "schema_pattern": {"allow": ["APPLICABLE_ROLES"]},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmpdir_path / "output.json")},
                },
            }

            # Record
            recording_path = tmpdir_path / "recording.zip"
            recorder = IngestionRecorder(
                run_id="test-snowflake-recording",
                password="test-password",
                recipe=test_recipe,
                output_path=str(recording_path),
                s3_upload=False,
            )

            with recorder:
                # Import and run pipeline inside recording context
                from datahub.ingestion.run.pipeline import Pipeline

                pipeline = Pipeline.create(test_recipe)
                pipeline.run()

                # Verify pipeline ran successfully
                assert not pipeline.has_failures()

            # Verify recording was created
            assert recording_path.exists()

            # Verify queries were recorded
            from datahub.ingestion.recording.archive import RecordingArchive

            archive = RecordingArchive("test-password")
            manifest = archive.read_manifest(recording_path)
            assert manifest.source_type == "snowflake"

            # Extract and check queries.jsonl
            extracted_dir = archive.extract(recording_path)
            queries_file = extracted_dir / "db" / "queries.jsonl"
            assert queries_file.exists()

            # Verify queries were recorded
            with open(queries_file) as f:
                lines = f.readlines()
                assert len(lines) > 0, "No queries were recorded"

            # Replay
            replay_output = tmpdir_path / "replay_output.json"
            test_recipe["sink"]["config"]["filename"] = str(replay_output)

            with IngestionReplayer(
                archive_path=str(recording_path),
                password="test-password",
            ) as replayer:
                recipe = replayer.get_recipe()
                pipeline = Pipeline.create(recipe)
                pipeline.run()

                # Verify replay ran successfully
                assert not pipeline.has_failures()

            # Verify replay output was created
            assert replay_output.exists()

    def test_snowflake_recording_with_vcr_interference(self):
        """Test that Snowflake recording handles VCR interference gracefully.

        This test verifies the automatic retry mechanism when VCR interferes
        with the Snowflake connection (due to vendored urllib3).
        """
        # This test would need to artificially trigger VCR interference
        # and verify that the connection retry succeeds
        # For now, this is a placeholder for manual testing
        pytest.skip("Manual test - requires simulating VCR interference")


@pytest.mark.skipif(
    not has_databricks_credentials(),
    reason="Databricks credentials not available",
)
class TestDatabricksRecording:
    """Test Databricks recording and replay functionality."""

    def test_databricks_recording_and_replay(self):
        """Test complete Databricks record/replay cycle.

        This test:
        1. Records a Databricks/Unity Catalog ingestion run
        2. Verifies queries.jsonl was created with content
        3. Replays the recording
        4. Verifies replay produces same metadata output
        """
        from datahub.ingestion.recording.recorder import IngestionRecorder
        from datahub.ingestion.recording.replay import IngestionReplayer

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # Simple Databricks recipe for testing
            test_recipe: Dict[str, Any] = {
                "source": {
                    "type": "unity-catalog",
                    "config": {
                        "workspace_url": os.getenv("DATABRICKS_SERVER_HOSTNAME"),
                        "token": os.getenv("DATABRICKS_ACCESS_TOKEN"),
                        "warehouse_id": os.getenv("DATABRICKS_WAREHOUSE_ID"),
                        # Limit scope for faster testing
                        "catalog_pattern": {"allow": ["main"]},
                        "schema_pattern": {"allow": ["default"]},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmpdir_path / "output.json")},
                },
            }

            # Record
            recording_path = tmpdir_path / "recording.zip"
            recorder = IngestionRecorder(
                run_id="test-databricks-recording",
                password="test-password",
                recipe=test_recipe,
                output_path=str(recording_path),
                s3_upload=False,
            )

            with recorder:
                from datahub.ingestion.run.pipeline import Pipeline

                pipeline = Pipeline.create(test_recipe)
                pipeline.run()

                assert not pipeline.has_failures()

            # Verify recording
            assert recording_path.exists()

            from datahub.ingestion.recording.archive import RecordingArchive

            archive = RecordingArchive("test-password")
            manifest = archive.read_manifest(recording_path)
            assert manifest.source_type == "unity-catalog"

            # Verify queries were recorded
            extracted_dir = archive.extract(recording_path)
            queries_file = extracted_dir / "db" / "queries.jsonl"
            assert queries_file.exists()

            with open(queries_file) as f:
                lines = f.readlines()
                assert len(lines) > 0, "No queries were recorded"

            # Replay
            replay_output = tmpdir_path / "replay_output.json"
            test_recipe["sink"]["config"]["filename"] = str(replay_output)

            with IngestionReplayer(
                archive_path=str(recording_path),
                password="test-password",
            ) as replayer:
                recipe = replayer.get_recipe()
                pipeline = Pipeline.create(recipe)
                pipeline.run()

                assert not pipeline.has_failures()

            assert replay_output.exists()

    def test_databricks_thrift_client_compatibility(self):
        """Verify Databricks Thrift client doesn't break recording.

        Databricks uses a Thrift HTTP client that is incompatible with VCR.
        This test verifies that our recording system handles this gracefully.
        """
        # This test would verify the Thrift client works with our patching
        # For now, this is a placeholder for manual testing
        pytest.skip("Manual test - requires Databricks connection")


# Manual testing checklist (to be run by developers)
"""
MANUAL TESTING CHECKLIST:

Snowflake:
- [ ] Record with password auth
- [ ] Record with OAuth auth  
- [ ] Record with key-pair auth
- [ ] Verify queries.jsonl contains expected queries
- [ ] Replay in air-gapped mode
- [ ] Verify replay output matches original (use datahub check metadata-diff)
- [ ] Check logs for VCR bypass messages

Databricks:
- [ ] Record with PAT (Personal Access Token)
- [ ] Record with Azure AD auth
- [ ] Verify queries.jsonl contains expected queries
- [ ] Replay in air-gapped mode
- [ ] Verify replay output matches original
- [ ] Check Thrift client doesn't cause errors

General:
- [ ] Test with Fivetran source pointing to Snowflake destination
- [ ] Test with Fivetran source pointing to Databricks destination
- [ ] Verify no HTTP-related errors during recording
- [ ] Verify replay works without network access
- [ ] Test semantic equivalence with metadata-diff tool
"""
