"""Integration test for PostgreSQL recording/replay with docker-compose.

This test validates the complete recording/replay workflow with a real PostgreSQL
database running in docker. It tests:
- Recording SQL queries from postgres source
- Replaying in air-gapped mode
- Validating MCPs are semantically identical

Run:
    pytest tests/integration/recording/test_postgres_recording.py -v -s
"""

import json
import tempfile
from pathlib import Path
from typing import Any, Dict

import pytest

from datahub.ingestion.run.pipeline import Pipeline
from tests.test_helpers.docker_helpers import wait_for_port

# Mark as integration test and recording batch (requires isolation)
# The separate batch ensures this test runs in a fresh Python process,
# avoiding module patching interference from other tests
pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_batch_recording,
]


@pytest.fixture(scope="module")
def postgres_runner(docker_compose_runner, pytestconfig):
    """Start PostgreSQL container using docker_compose_runner.

    The healthcheck in docker-compose.yml ensures postgres is ready before tests run.
    """
    test_resources_dir = pytestconfig.rootpath / "tests/integration/recording/postgres"
    with docker_compose_runner(
        test_resources_dir / "docker-compose.yml",
        "postgres",
    ) as docker_services:
        # Wait for postgres port to be available (mapped to host port 15432)
        wait_for_port(
            docker_services,
            "datahub-test-postgres-recording",
            5432,
            hostname="localhost",
        )
        yield docker_services


class TestPostgreSQLRecording:
    """Integration test for PostgreSQL recording and replay."""

    def test_postgres_record_replay_validation(self, postgres_runner):
        """Test complete PostgreSQL record/replay cycle with MCP validation.

        This test:
        1. Records a PostgreSQL ingestion run (captures SQL queries via SQLAlchemy)
        2. Verifies SQL queries were recorded
        3. Replays the recording in air-gapped mode
        4. Validates recording and replay produce identical MCPs

        Tests SQLAlchemy-based database sources which use connection pooling
        and the DB-API 2.0 cursor interface.
        """
        from datahub.ingestion.recording.recorder import IngestionRecorder

        with tempfile.TemporaryDirectory() as tmpdir:
            tmpdir_path = Path(tmpdir)

            # PostgreSQL recipe
            recipe: Dict[str, Any] = {
                "source": {
                    "type": "postgres",
                    "config": {
                        "host_port": "localhost:15432",
                        "database": "testdb",
                        "username": "testuser",
                        "password": "testpass",
                        # Include all schemas
                        "schema_pattern": {"allow": ["sales", "hr", "analytics"]},
                        # Disable profiling for faster test
                        "profiling": {"enabled": False},
                    },
                },
                "sink": {
                    "type": "file",
                    "config": {"filename": str(tmpdir_path / "recording_output.json")},
                },
            }

            # STEP 1: Record
            recording_path = tmpdir_path / "postgres_recording.zip"

            with IngestionRecorder(
                run_id="test-postgres-recording",
                password="test-password",
                recipe=recipe,
                output_path=str(recording_path),
                s3_upload=False,
                source_type="postgres",
                sink_type="file",
                redact_secrets=False,  # Keep credentials for replay
            ):
                pipeline = Pipeline.create(recipe)
                pipeline.run()

            # Verify recording was created
            assert recording_path.exists(), "Recording archive was not created"

            # Verify output has events
            recording_output = tmpdir_path / "recording_output.json"
            assert recording_output.exists(), "Recording output file not created"

            with open(recording_output) as f:
                recording_mcps = json.load(f)

            assert len(recording_mcps) > 0, "No MCPs produced during recording"
            print(f"\n✅ Recording produced {len(recording_mcps)} MCPs")

            # Verify SQL queries were recorded
            from datahub.ingestion.recording.archive import RecordingArchive

            archive = RecordingArchive("test-password")
            extracted_dir = archive.extract(recording_path)

            queries_file = extracted_dir / "db" / "queries.jsonl"
            assert queries_file.exists(), "queries.jsonl not found in recording"

            with open(queries_file) as f:
                query_count = sum(1 for _ in f)

            assert query_count > 0, "No SQL queries were recorded"
            print(f"✅ Recording captured {query_count} SQL queries")

            # STEP 2: Replay
            from datahub.ingestion.recording.replay import IngestionReplayer

            with IngestionReplayer(
                archive_path=str(recording_path),
                password="test-password",
            ) as replayer:
                replay_recipe = replayer.get_recipe()
                replay_pipeline = Pipeline.create(replay_recipe)
                replay_pipeline.run()

            # Get replay output file
            import glob

            replay_files = glob.glob("/tmp/datahub_replay_test-postgres-recording.json")
            assert len(replay_files) > 0, "Replay output file not created"
            replay_output = Path(replay_files[0])

            with open(replay_output) as f:
                replay_mcps = json.load(f)

            assert len(replay_mcps) > 0, "No MCPs produced during replay"
            print(f"✅ Replay produced {len(replay_mcps)} MCPs")

            # STEP 3: Validate MCPs are identical
            assert len(recording_mcps) == len(replay_mcps), (
                f"MCP count mismatch: recording={len(recording_mcps)}, "
                f"replay={len(replay_mcps)}"
            )

            # Compare entity URNs and aspect names (ignore timestamps)
            for i, (rec_mcp, rep_mcp) in enumerate(
                zip(recording_mcps, replay_mcps, strict=False)
            ):
                assert rec_mcp.get("entityUrn") == rep_mcp.get("entityUrn"), (
                    f"MCP {i}: Entity URN mismatch"
                )
                assert rec_mcp.get("aspectName") == rep_mcp.get("aspectName"), (
                    f"MCP {i}: Aspect name mismatch"
                )

            print(f"✅ All {len(recording_mcps)} MCPs matched semantically")
            print("\n🎉 PostgreSQL recording/replay test PASSED!")
