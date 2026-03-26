"""Main recording orchestrator that combines HTTP and database recording.

This module provides the IngestionRecorder class which is the main entry point
for recording ingestion runs. It coordinates HTTP recording (via VCR.py) and
database recording (via module patching) and packages everything into an
encrypted archive.

Usage:
    recorder = IngestionRecorder(
        run_id="my-run-id",
        password="secret",
        recipe=recipe_dict,
    )
    with recorder:
        pipeline.run()
    # Archive is automatically created and uploaded to S3
"""

import logging
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

from datahub.ingestion.recording.archive import (
    DB_DIR,
    HTTP_DIR,
    ArchiveManifest,
    RecordingArchive,
)
from datahub.ingestion.recording.config import (
    RecordingConfig,
    check_recording_dependencies,
)
from datahub.ingestion.recording.db_proxy import QueryRecorder
from datahub.ingestion.recording.http_recorder import HTTPRecorder
from datahub.ingestion.recording.patcher import ModulePatcher

logger = logging.getLogger(__name__)


class IngestionRecorder:
    """Main context manager for recording ingestion runs.

    Combines HTTP and database recording into a single interface.
    Creates an encrypted archive on exit and optionally uploads to S3.

    Usage:
        with IngestionRecorder(run_id, password, recipe) as recorder:
            # Run ingestion - all HTTP and DB calls are recorded
            pipeline.run()
        # Archive is created automatically on exit
    """

    def __init__(
        self,
        run_id: str,
        password: str,
        recipe: Dict[str, Any],
        config: Optional[RecordingConfig] = None,
        output_path: Optional[str] = None,
        s3_upload: bool = False,
        source_type: Optional[str] = None,
        sink_type: Optional[str] = None,
        redact_secrets: bool = True,
    ) -> None:
        """Initialize ingestion recorder.

        Args:
            run_id: Unique identifier for this ingestion run.
            password: Password for encrypting the archive.
            recipe: The ingestion recipe dictionary.
            config: Recording configuration (overrides other args if provided).
            output_path: Path to save the archive. Can be local path or S3 URL
                        (s3://bucket/path) when s3_upload=True.
            s3_upload: Upload directly to S3. When True, output_path must be S3 URL.
            source_type: Source type (for manifest metadata).
            sink_type: Sink type (for manifest metadata).
            redact_secrets: Whether to redact secrets in the stored recipe (default: True).
                          Set to False for local debugging to keep actual credentials.
        """
        check_recording_dependencies()

        self.run_id = run_id
        self.password = password
        self.recipe = recipe
        self.source_type = source_type
        self.sink_type = sink_type
        self.redact_secrets = redact_secrets

        # Apply config overrides if provided
        if config:
            if config.output_path:
                output_path = config.output_path
            s3_upload = config.s3_upload

        self.output_path = output_path
        self.s3_upload = s3_upload

        # Internal state
        self._temp_dir: Optional[Path] = None
        self._http_recorder: Optional[HTTPRecorder] = None
        self._query_recorder: Optional[QueryRecorder] = None
        self._module_patcher: Optional[ModulePatcher] = None
        self._http_context: Optional[Any] = None
        self._archive_path: Optional[Path] = None
        self._recording_start_time: Optional[str] = None

    def __enter__(self) -> "IngestionRecorder":
        """Start recording."""
        from datetime import datetime, timezone

        # Capture recording start time for timestamp freezing during replay
        self._recording_start_time = datetime.now(timezone.utc).isoformat()
        logger.info(f"Starting ingestion recording for run_id: {self.run_id}")
        logger.info(f"Recording start time: {self._recording_start_time}")

        # Create temp directory for recording files
        self._temp_dir = Path(tempfile.mkdtemp(prefix="datahub_recording_"))

        # Setup HTTP recording
        http_dir = self._temp_dir / HTTP_DIR
        http_dir.mkdir(parents=True, exist_ok=True)
        cassette_path = http_dir / "cassette.yaml"
        self._http_recorder = HTTPRecorder(cassette_path)

        # Setup database recording
        db_dir = self._temp_dir / DB_DIR
        db_dir.mkdir(parents=True, exist_ok=True)
        queries_path = db_dir / "queries.jsonl"
        self._query_recorder = QueryRecorder(queries_path)
        self._query_recorder.start_recording()

        # Setup module patcher for database connections
        self._module_patcher = ModulePatcher(
            recorder=self._query_recorder,
            is_replay=False,
        )

        # Enter all context managers
        self._http_context = self._http_recorder.recording()
        self._http_context.__enter__()
        self._module_patcher.__enter__()

        logger.info("Recording started for HTTP and database traffic")
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Stop recording and create archive.

        IMPORTANT: We ALWAYS create the archive, even when an exception occurred.
        This is critical for debugging - we need to capture the state up to the
        failure point so we can replay and reproduce the error.
        """
        logger.info("Stopping ingestion recording...")

        # Capture exception info for the manifest before doing anything else
        exception_info = None
        if exc_type is not None:
            import traceback

            exception_info = {
                "type": exc_type.__name__,
                "message": str(exc_val),
                "traceback": "".join(traceback.format_tb(exc_tb)),
            }
            logger.info(
                f"Recording will include exception info: {exc_type.__name__}: {exc_val}"
            )

        # Exit context managers in reverse order
        if self._module_patcher:
            self._module_patcher.__exit__(exc_type, exc_val, exc_tb)

        if self._http_context:
            self._http_context.__exit__(exc_type, exc_val, exc_tb)

        if self._query_recorder:
            self._query_recorder.stop_recording()

        # Validate recording completeness
        self._validate_recording()

        # ALWAYS create archive - this is a debug feature, we need to capture
        # the state even (especially!) when there's an exception
        try:
            self._create_archive(exception_info=exception_info)
        except Exception as e:
            # Don't let archive creation failure mask the original exception
            logger.error(f"Failed to create recording archive: {e}")
            if exc_type is None:
                # Only raise if there wasn't already an exception
                raise

        # Cleanup temp directory
        if self._temp_dir and self._temp_dir.exists():
            shutil.rmtree(self._temp_dir, ignore_errors=True)

    def _create_archive(self, exception_info: Optional[Dict[str, Any]] = None) -> None:
        """Create the encrypted archive and optionally upload to S3.

        Args:
            exception_info: If the ingestion failed, contains error details
                           (type, message, traceback) for debugging.
        """
        if not self._temp_dir:
            logger.warning("No temp directory, skipping archive creation")
            return

        # Determine local archive path
        # S3 upload uses temp file that gets uploaded then cleaned up
        if self.s3_upload:
            with tempfile.NamedTemporaryFile(suffix=".zip", delete=False) as tmpfile:
                archive_path = Path(tmpfile.name)
        elif self.output_path:
            output_path = Path(self.output_path)
            # If output_path is a directory, generate filename within it
            if output_path.is_dir():
                archive_path = output_path / f"recording-{self.run_id}.zip"
            else:
                # Explicit local output path provided - takes precedence
                archive_path = output_path
        else:
            # Check INGESTION_ARTIFACT_DIR env var, fallback to temp
            import os

            artifact_dir = os.getenv("INGESTION_ARTIFACT_DIR")
            if artifact_dir:
                artifact_path = Path(artifact_dir)
                artifact_path.mkdir(parents=True, exist_ok=True)
                archive_path = artifact_path / f"recording-{self.run_id}.zip"
                logger.info(
                    f"Saving recording to INGESTION_ARTIFACT_DIR: {archive_path}"
                )
            else:
                with tempfile.NamedTemporaryFile(
                    suffix=".zip", delete=False
                ) as tmpfile:
                    archive_path = Path(tmpfile.name)

        # Create manifest with exception info if present
        manifest = ArchiveManifest(
            run_id=self.run_id,
            source_type=self.source_type,
            sink_type=self.sink_type,
            datahub_cli_version=self._get_cli_version(),
            python_version=self._get_python_version(),
            exception_info=exception_info,
            recording_start_time=self._recording_start_time,
        )

        if exception_info:
            logger.info(
                f"Recording includes exception: {exception_info['type']}: "
                f"{exception_info['message'][:100]}..."
            )

        # Create archive
        archive = RecordingArchive(self.password)
        self._archive_path = archive.create(
            output_path=archive_path,
            temp_dir=self._temp_dir,
            manifest=manifest,
            recipe=self.recipe,
            redact_secrets_enabled=self.redact_secrets,
        )

        logger.info(f"Created recording archive: {self._archive_path}")

        # Upload to S3 if enabled
        if self.s3_upload:
            try:
                self._upload_to_s3()
            finally:
                # Clean up temp archive after S3 upload
                if self._archive_path and self._archive_path.exists():
                    self._archive_path.unlink()
        else:
            logger.info(f"Recording saved to: {self._archive_path}")

    def _upload_to_s3(self) -> None:
        """Upload archive to S3 URL specified in output_path."""
        if not self._archive_path or not self.output_path:
            return

        try:
            # Parse S3 URL from output_path (s3://bucket/key)
            s3_url = self.output_path
            if not s3_url.startswith("s3://"):
                logger.error(f"Invalid S3 URL: {s3_url}")
                return

            # Parse bucket and key from s3://bucket/key
            path_without_scheme = s3_url[5:]  # Remove "s3://"
            parts = path_without_scheme.split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else f"recording-{self.run_id}.zip"

            import boto3

            s3_client = boto3.client("s3")
            s3_client.upload_file(
                str(self._archive_path),
                bucket,
                key,
            )

            logger.info(f"Recording uploaded to {s3_url}")

        except Exception as e:
            logger.error(f"Failed to upload recording to S3: {e}")
            # Don't raise - S3 upload failure shouldn't fail the ingestion

    def _get_s3_bucket(self) -> Optional[str]:
        """Get S3 bucket for recordings.

        Tries to fetch from DataHub server configuration for consistency.
        Falls back to environment variable if server config unavailable.
        """
        import os

        # Try environment variable first (for testing/override)
        bucket = os.getenv("DATAHUB_S3_RECORDINGS_BUCKET")
        if bucket:
            return bucket

        # Try to get from DataHub server config
        # Note: Server system info API for S3 settings is not yet available
        # This could be extended to fetch bucket name from server config
        # when that API is exposed. For now, return None to indicate
        # S3 upload is not available without explicit bucket configuration.
        return None

    def _validate_recording(self) -> None:
        """Validate that the recording captured meaningful data.

        This helps catch issues where recording appears to succeed but didn't
        actually capture anything useful (e.g., VCR interference, connection
        failures that were swallowed, etc.).
        """
        query_count = (
            len(self._query_recorder._recordings) if self._query_recorder else 0
        )
        http_count = self._http_recorder.request_count if self._http_recorder else 0

        logger.info(
            f"📊 Recording summary: {query_count} database queries, {http_count} HTTP requests"
        )

        if query_count == 0 and http_count == 0:
            logger.error(
                "❌ Recording validation failed: No queries or HTTP requests recorded. "
                "The recording may be incomplete or the connection may have failed."
            )
        elif query_count == 0:
            logger.warning(
                f"⚠️ No database queries recorded (HTTP requests: {http_count}). "
                "This is expected for HTTP-only sources (Looker, PowerBI, etc.), "
                "but unusual for database sources (Snowflake, Databricks, etc.)."
            )
        else:
            logger.info(
                f"Recording validation passed: {query_count} database queries, "
                f"{http_count} HTTP requests recorded"
            )

    @staticmethod
    def _get_cli_version() -> Optional[str]:
        """Get the DataHub CLI version (user-friendly)."""
        try:
            from datahub._version import nice_version_name

            return nice_version_name()
        except Exception:
            return None

    @staticmethod
    def _get_python_version() -> Optional[str]:
        """Get the Python version."""
        try:
            import sys

            return f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        except Exception:
            return None

    @property
    def archive_path(self) -> Optional[Path]:
        """Path to the created archive (after recording completes)."""
        return self._archive_path


def create_recorder_from_config(
    config: RecordingConfig,
    run_id: str,
    recipe: Dict[str, Any],
    source_type: Optional[str] = None,
    sink_type: Optional[str] = None,
) -> Optional[IngestionRecorder]:
    """Create an IngestionRecorder from a RecordingConfig.

    Args:
        config: Recording configuration from the recipe.
        run_id: Unique run identifier.
        recipe: The full recipe dictionary.
        source_type: Source type for metadata.
        sink_type: Sink type for metadata.

    Returns:
        IngestionRecorder if recording is enabled, None otherwise.
    """
    if not config.enabled:
        return None

    if not config.password:
        raise ValueError("Recording password is required when recording is enabled")

    return IngestionRecorder(
        run_id=run_id,
        password=config.password.get_secret_value(),
        recipe=recipe,
        config=config,
        source_type=source_type,
        sink_type=sink_type,
    )
