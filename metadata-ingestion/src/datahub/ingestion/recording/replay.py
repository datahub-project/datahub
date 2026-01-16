"""Replay module for running ingestion from recorded data.

This module provides the IngestionReplayer class which replays recorded
ingestion runs without making real network or database connections.

Replay modes:
- Air-gapped (default): All HTTP and DB calls served from recordings
- Live sink: Source data from recordings, but emit to real DataHub instance

Note on timestamps:
- systemMetadata.lastObserved and auditStamp.time will differ between
  recording and replay (they reflect when MCPs are emitted, not source data)
- Use `datahub check metadata-diff` to compare MCPs semantically

Usage:
    replayer = IngestionReplayer(
        archive_path="recording.zip",
        password="secret",
    )
    with replayer:
        pipeline.run()  # All data comes from recordings
"""

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlparse

from datahub.ingestion.recording.archive import (
    DB_DIR,
    HTTP_DIR,
    ArchiveManifest,
    RecordingArchive,
    prepare_recipe_for_replay,
)
from datahub.ingestion.recording.config import (
    ReplayConfig,
    check_recording_dependencies,
)
from datahub.ingestion.recording.db_proxy import QueryRecorder
from datahub.ingestion.recording.http_recorder import (
    HTTPRecorder,
    HTTPReplayerForLiveSink,
)
from datahub.ingestion.recording.patcher import ModulePatcher

logger = logging.getLogger(__name__)


class IngestionReplayer:
    """Context manager for replaying recorded ingestion runs.

    Serves all HTTP and database requests from recorded data, enabling
    fully offline debugging and testing.

    Usage:
        with IngestionReplayer(archive_path, password) as replayer:
            # Get the replay-ready recipe
            recipe = replayer.get_recipe()
            # Run pipeline - all calls served from recordings
            pipeline = Pipeline.create(recipe)
            pipeline.run()
    """

    def __init__(
        self,
        archive_path: str,
        password: str,
        live_sink: bool = False,
        gms_server: Optional[str] = None,
        use_responses_library: bool = False,
    ) -> None:
        """Initialize ingestion replayer.

        Args:
            archive_path: Path to recording archive (local file or s3:// URL).
            password: Password for decrypting the archive.
            live_sink: If True, allow real HTTP calls to GMS server.
            gms_server: GMS server URL when using live_sink mode.
            use_responses_library: If True, use responses library for HTTP replay
                instead of VCR.py. Useful for sources with VCR compatibility issues.
        """
        check_recording_dependencies()

        self.archive_path = archive_path
        self.password = password
        self.live_sink = live_sink
        self.gms_server = gms_server
        self.use_responses_library = use_responses_library

        # Internal state
        self._extracted_dir: Optional[Path] = None
        self._archive: Optional[RecordingArchive] = None
        self._manifest: Optional[ArchiveManifest] = None
        self._recipe: Optional[Dict[str, Any]] = None
        self._http_recorder: Optional[HTTPRecorder] = None
        self._http_replayer: Optional[HTTPReplayerForLiveSink] = None
        self._query_recorder: Optional[QueryRecorder] = None
        self._module_patcher: Optional[ModulePatcher] = None
        self._http_context: Optional[Any] = None

    def __enter__(self) -> "IngestionReplayer":
        """Start replay mode."""
        logger.info("Starting ingestion replay...")

        # Download from S3 if needed
        local_archive_path = self._ensure_local_archive()

        # Extract archive
        self._archive = RecordingArchive(self.password)
        self._extracted_dir = self._archive.extract(local_archive_path)

        # Load and verify manifest
        self._manifest = self._archive.read_manifest(local_archive_path)
        if not self._archive.verify_checksums(self._extracted_dir, self._manifest):
            logger.warning(
                "Archive checksum verification failed - data may be corrupted"
            )

        logger.info(
            f"Loaded recording: run_id={self._manifest.run_id}, "
            f"source={self._manifest.source_type}, "
            f"created={self._manifest.created_at}"
        )

        # Warn if the recording includes an exception
        if self._manifest.has_exception:
            exc_info = self._manifest.exception_info or {}
            logger.warning(
                f"Recording includes an exception - replay will reproduce the failure:\n"
                f"  Type: {exc_info.get('type', 'Unknown')}\n"
                f"  Message: {exc_info.get('message', 'N/A')}"
            )

        # Load and prepare recipe
        raw_recipe = self._archive.read_recipe(local_archive_path)
        self._recipe = prepare_recipe_for_replay(raw_recipe)

        # Fix sink configuration for replay
        if not self.live_sink:
            # Air-gapped mode: Replace sink with a file sink to avoid network connections
            # This handles cases where:
            # 1. Original sink was datahub-rest (different server URL now)
            # 2. Original sink was file with env vars (e.g., $INGESTION_ARTIFACT_DIR)
            original_sink = self._recipe.get("sink", {})
            replay_output = f"/tmp/datahub_replay_{self._manifest.run_id}.json"

            logger.info(
                f"Air-gapped mode: Replacing sink (type={original_sink.get('type')}) "
                f"with file sink to avoid network/path issues"
            )

            self._recipe["sink"] = {
                "type": "file",
                "config": {"filename": replay_output},
            }
            logger.info(f"Replay output will be written to: {replay_output}")

            # Disable stateful ingestion in air-gapped mode
            # Stateful ingestion requires a graph instance (from GMS connection)
            # which isn't available without a real datahub-rest sink
            source_config = self._recipe.get("source", {}).get("config", {})
            logger.info(
                "Air-gapped mode: Disabling stateful ingestion (no GMS connection)"
            )
            source_config["stateful_ingestion"] = {"enabled": False}
        elif self.gms_server:
            # Live sink mode with server override
            sink_config = self._recipe.get("sink", {}).get("config", {})
            logger.info(f"Live sink mode: Overriding GMS server to {self.gms_server}")
            sink_config["server"] = self.gms_server

        # Setup HTTP replay
        http_dir = self._extracted_dir / HTTP_DIR
        cassette_path = http_dir / "cassette.yaml"

        if self.live_sink:
            # Live sink mode - allow GMS calls through
            live_hosts = self._get_live_hosts()
            self._http_replayer = HTTPReplayerForLiveSink(cassette_path, live_hosts)
            self._http_context = self._http_replayer.replaying()
        else:
            # Full air-gapped mode
            self._http_recorder = HTTPRecorder(cassette_path)
            if self.use_responses_library:
                logger.info(
                    "Using responses library for HTTP replay (--use-responses-lib flag)"
                )
            self._http_context = self._http_recorder.replaying(
                use_responses_library=self.use_responses_library
            )

        # Setup database replay
        db_dir = self._extracted_dir / DB_DIR
        queries_path = db_dir / "queries.jsonl"
        self._query_recorder = QueryRecorder(queries_path)
        self._query_recorder.load_recordings()

        # Setup module patcher for replay mode
        self._module_patcher = ModulePatcher(
            recorder=self._query_recorder,
            is_replay=True,
        )

        # Enter all context managers
        self._http_context.__enter__()
        self._module_patcher.__enter__()

        logger.info(
            f"Replay mode active ({'live sink' if self.live_sink else 'air-gapped'})"
        )
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop replay mode and cleanup."""
        logger.info("Stopping ingestion replay...")

        # Exit context managers in reverse order
        if self._module_patcher:
            self._module_patcher.__exit__(exc_type, exc_val, exc_tb)

        if self._http_context:
            self._http_context.__exit__(exc_type, exc_val, exc_tb)

        # Cleanup extracted directory
        if self._extracted_dir and self._extracted_dir.exists():
            shutil.rmtree(self._extracted_dir, ignore_errors=True)

        logger.info("Replay complete")

    def _ensure_local_archive(self) -> Path:
        """Ensure archive is available locally (download from S3 if needed)."""
        archive_str = str(self.archive_path)
        if archive_str.startswith("s3://"):
            return self._download_from_s3(archive_str)
        return Path(self.archive_path)

    def _download_from_s3(self, s3_url: str) -> Path:
        """Download archive from S3."""
        logger.info(f"Downloading recording from {s3_url}")

        import boto3

        parsed = urlparse(s3_url)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        # Use NamedTemporaryFile with delete=False for secure, atomic file creation
        with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmpfile:
            local_path = Path(tmpfile.name)

        s3_client = boto3.client("s3")
        s3_client.download_file(bucket, key, str(local_path))

        logger.info(f"Downloaded recording to {local_path}")
        return local_path

    def _get_live_hosts(self) -> list:
        """Get list of hosts that should make real requests (for live sink)."""
        hosts = []

        # Use provided GMS server
        if self.gms_server:
            parsed = urlparse(self.gms_server)
            hosts.append(parsed.netloc)

        # Also check recipe for sink server
        if self._recipe:
            sink_config = self._recipe.get("sink", {}).get("config", {})
            server = sink_config.get("server")
            if server:
                parsed = urlparse(server)
                hosts.append(parsed.netloc)

        return hosts

    def get_recipe(self) -> Dict[str, Any]:
        """Get the replay-ready recipe with dummy secrets.

        The returned recipe has all secret values replaced with valid
        dummy strings that pass Pydantic validation. The actual values
        are never used since all data comes from recordings.

        Returns:
            Recipe dictionary ready for Pipeline.create()
        """
        if self._recipe is None:
            raise RuntimeError("Replayer not started - use within context manager")

        # Override sink if live_sink mode with specific server
        recipe = self._recipe.copy()

        if self.live_sink and self.gms_server:
            if "sink" not in recipe:
                recipe["sink"] = {"type": "datahub-rest", "config": {}}
            recipe["sink"]["config"]["server"] = self.gms_server

        # Remove recording config from recipe (not needed for replay)
        recipe.pop("recording", None)

        return recipe

    @property
    def manifest(self) -> Optional[ArchiveManifest]:
        """Get the archive manifest (available after entering context)."""
        return self._manifest

    @property
    def run_id(self) -> Optional[str]:
        """Get the original run_id from the recording."""
        return self._manifest.run_id if self._manifest else None


def create_replayer_from_config(config: ReplayConfig) -> IngestionReplayer:
    """Create an IngestionReplayer from a ReplayConfig.

    Args:
        config: Replay configuration.

    Returns:
        Configured IngestionReplayer instance.
    """
    return IngestionReplayer(
        archive_path=config.archive_path,
        password=config.password.get_secret_value(),
        live_sink=config.live_sink,
        gms_server=config.gms_server,
    )


def replay_ingestion(
    archive_path: str,
    password: str,
    live_sink: bool = False,
    gms_server: Optional[str] = None,
    report_to: Optional[str] = None,
) -> int:
    """Convenience function to replay a recorded ingestion run.

    This function handles the complete replay workflow:
    1. Extract and prepare the recording
    2. Create a pipeline from the recorded recipe
    3. Run the pipeline with data served from recordings
    4. Return the exit code

    Args:
        archive_path: Path to recording archive (local or s3://).
        password: Archive decryption password.
        live_sink: If True, emit to real GMS server.
        gms_server: GMS server URL (optional, defaults to recipe value).
        report_to: Optional path to write the report.

    Returns:
        Exit code (0 for success, non-zero for failure).
    """
    from datahub.ingestion.run.pipeline import Pipeline

    with IngestionReplayer(
        archive_path=archive_path,
        password=password,
        live_sink=live_sink,
        gms_server=gms_server,
    ) as replayer:
        recipe = replayer.get_recipe()

        logger.info(f"Running replay for recording: {replayer.run_id}")

        # Create and run pipeline
        pipeline = Pipeline.create(recipe)
        pipeline.run()

        # Optionally write report
        if report_to:
            pipeline.pretty_print_summary()

        # Return exit code: 0 for success, 1 for failures
        return 1 if pipeline.has_failures() else 0
