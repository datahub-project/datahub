"""Typed accessors for the aspect inside a ``MetadataWorkUnit``."""

from typing import Any

from datahub.ingestion.api.workunit import MetadataWorkUnit


def aspect_of(workunit: MetadataWorkUnit) -> Any:
    """The workunit's aspect, without committing to a concrete type."""
    return workunit.metadata.aspect  # type: ignore[union-attr]
