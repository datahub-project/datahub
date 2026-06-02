"""Shared document import mode for context document ingestion sources."""

from enum import Enum


class DocumentImportMode(str, Enum):
    """Whether ingested documents are native (editable) or external (read-only references)."""

    NATIVE = "NATIVE"
    EXTERNAL = "EXTERNAL"
