"""
Ingestion Recording and Replay Module.

This module provides functionality to record and replay ingestion runs
for debugging purposes. It captures HTTP requests and database queries
during ingestion, packages them into encrypted archives, and allows
replaying the ingestion offline without network access.

For comprehensive documentation, see README.md in this directory.

Quick Start:
    pip install 'acryl-datahub[debug-recording]'

    # Record an ingestion run
    datahub ingest run -c recipe.yaml --record --record-password <pwd>

    # Replay a recording (air-gapped, no network needed)
    datahub ingest replay recording.zip --password <pwd>

    # Inspect a recording
    datahub recording info recording.zip --password <pwd>

Key Classes:
    - RecordingConfig: Configuration for recording settings
    - IngestionRecorder: Context manager for recording ingestion runs
    - IngestionReplayer: Context manager for replaying recordings

Limitations:
    - HTTP requests are serialized during recording (slower but complete)
    - Secrets are redacted in stored recipes
    - Database replay mocks connections entirely
"""

from datahub.ingestion.recording.config import RecordingConfig
from datahub.ingestion.recording.recorder import IngestionRecorder
from datahub.ingestion.recording.replay import IngestionReplayer

__all__ = [
    "RecordingConfig",
    "IngestionRecorder",
    "IngestionReplayer",
]
