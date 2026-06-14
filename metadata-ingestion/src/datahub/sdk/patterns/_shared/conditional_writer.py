"""Layer 1: CAS (Compare-And-Swap) mechanics over DataHub aspects.

Provides conditional read-then-write operations using the If-Version-Match
header, backed by GMS's ConditionalWriteValidator. Zero domain knowledge --
this module only knows about aspects, versions, and the DataHubGraph client.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional, Type

from datahub._codegen.aspect import _Aspect
from datahub.configuration.common import OperationalError
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.graph.client import DataHubGraph
from datahub.metadata.schema_classes import SystemMetadataClass

logger = logging.getLogger(__name__)

# Matches ConditionalWriteValidator.UNVERSIONED_ASPECT_VERSION on the server.
_UNVERSIONED = "-1"


@dataclass
class CASResult:
    """Outcome of a conditional write attempt."""

    success: bool
    new_version: Optional[str] = None  # version after write (if success)
    reason: Optional[str] = None  # "conflict", "error", etc. (if failure)


@dataclass
class VersionedAspect:
    """An aspect value paired with its version string.

    Returned by :meth:`ConditionalWriter.read_versioned_aspect` to combine
    both the version and the typed aspect in a single round trip.
    """

    version: str
    aspect: Optional[_Aspect] = None


class ConditionalWriter:
    """Thin wrapper around DataHubGraph for conditional aspect writes.

    All version strings use the same scheme as GMS: numeric strings for
    existing aspects, ``"-1"`` for aspects that do not yet exist.
    """

    def __init__(self, graph: DataHubGraph) -> None:
        self._graph = graph

    def read_version(self, entity_urn: str, aspect_name: str) -> str:
        """Read the current version of an aspect.

        Returns:
            Version string. ``"-1"`` if the aspect does not exist.
        """
        mcps = self._graph.get_entity_as_mcps(entity_urn, aspects=[aspect_name])
        for mcp in mcps:
            if (
                mcp.aspect is not None
                and mcp.aspectName == aspect_name
                and mcp.systemMetadata is not None
            ):
                return _extract_version(mcp.systemMetadata)
        return _UNVERSIONED

    def read_versioned_aspect(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_class: Type[_Aspect],
    ) -> VersionedAspect:
        """Read the version and typed aspect in a single GMS call.

        Returns:
            A :class:`VersionedAspect` with the version string and typed
            aspect (``None`` if the aspect is absent or doesn't match
            *aspect_class*).
        """
        mcps = self._graph.get_entity_as_mcps(entity_urn, aspects=[aspect_name])
        for mcp in mcps:
            if (
                mcp.aspect is not None
                and mcp.aspectName == aspect_name
                and mcp.systemMetadata is not None
            ):
                version = _extract_version(mcp.systemMetadata)
                aspect = mcp.aspect if isinstance(mcp.aspect, aspect_class) else None
                return VersionedAspect(version=version, aspect=aspect)
        return VersionedAspect(version=_UNVERSIONED)

    def read_aspect(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_class: Type[_Aspect],
    ) -> Optional[_Aspect]:
        """Read the current value of an aspect.

        Returns:
            The aspect instance, or ``None`` if absent.
        """
        return self._graph.get_aspect(
            entity_urn=entity_urn,
            aspect_type=aspect_class,
        )

    def write_if_version(
        self,
        entity_urn: str,
        aspect_name: str,
        aspect_value: _Aspect,
        expected_version: str,
        track_new_version: bool = True,
    ) -> CASResult:
        """Write an aspect only if the current version matches *expected_version*.

        Uses the ``If-Version-Match`` header so that GMS validates server-side:

        * Version matches -- write succeeds, returns the new version.
        * Version mismatch -- write rejected, returns a conflict result.

        When *track_new_version* is ``False`` the post-write read is skipped
        and ``new_version`` will be ``None``.  Use this when the caller does
        not need the updated version (e.g. release writes).

        All other exceptions propagate to the caller.
        """
        mcpw = MetadataChangeProposalWrapper(
            entityUrn=entity_urn,
            aspect=aspect_value,
            headers={"If-Version-Match": expected_version},
        )

        try:
            self._graph.emit(mcpw)
        except OperationalError as exc:
            if _is_precondition_failure(exc):
                logger.debug(
                    "CAS conflict for %s/%s (expected version %s): %s",
                    entity_urn,
                    aspect_name,
                    expected_version,
                    exc,
                )
                return CASResult(success=False, reason="conflict")
            raise

        if not track_new_version:
            return CASResult(success=True)

        # After a successful write, read the new version.
        new_version = self.read_version(entity_urn, aspect_name)
        return CASResult(success=True, new_version=new_version)


def _extract_version(system_metadata: SystemMetadataClass) -> str:
    """Extract the version string from system metadata.

    Mirrors the Java ``AspectWithMetadata.extractVersion`` logic:
    uses ``systemMetadata.version`` if present, falls back to ``"-1"``.
    """
    version = system_metadata.get("version")
    if version is not None:
        return str(version)
    return _UNVERSIONED


def _is_precondition_failure(exc: OperationalError) -> bool:
    """Detect a 412 Precondition Failed response from GMS.

    The rest emitter wraps HTTP errors in OperationalError. We check both
    the embedded info dict and the message string for precondition signals.
    """
    # Check for HTTP 412 status code in the info dict.
    status = exc.info.get("status")
    if status == 412:
        return True

    # Fallback: check message text for the precondition keyword.
    msg = (exc.message or "").lower()
    if "precondition" in msg or "expected version" in msg:
        return True

    return False
