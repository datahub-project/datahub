"""State classes for document chunking stateful ingestion."""

from typing import Dict

import pydantic

from datahub.ingestion.source.state.checkpoint import CheckpointStateBase


class DocumentChunkingCheckpointState(CheckpointStateBase):
    """
    Checkpoint state for document chunking.
    Stores document content hashes (batch mode) and event stream offsets (event mode).
    """

    # Document content tracking (used in batch mode)
    document_state: Dict[str, Dict[str, str]] = pydantic.Field(
        default_factory=dict,
        description="Document state mapping URN to content hash and last processed timestamp (batch mode)",
    )

    # Event stream offset tracking (used in event mode)
    event_offsets: Dict[str, str] = pydantic.Field(
        default_factory=dict,
        description="Maps topic to offset_id for event-driven mode",
    )
