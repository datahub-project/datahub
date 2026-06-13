"""Typed helpers for SAP Datasphere unit tests.

Workunit metadata is a union of MCE / MCP / MCPW classes, and the aspect itself
is ``Optional[_Aspect]``. These helpers narrow a workunit to its concrete
MetadataChangeProposalWrapper aspect so tests can access aspect fields without
tripping mypy's ``union-attr`` checks. The asserts encode invariants the tests
already rely on (every emitted workunit is an MCPW with a non-null aspect).
"""

from typing import Type, TypeVar

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import _Aspect

_AspectT = TypeVar("_AspectT", bound=_Aspect)


def aspect_of(wu: MetadataWorkUnit) -> _Aspect:
    """Narrow a workunit's metadata to its MCPW aspect (e.g. for class-name checks)."""
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert wu.metadata.aspect is not None
    return wu.metadata.aspect


def aspect_as(wu: MetadataWorkUnit, aspect_type: Type[_AspectT]) -> _AspectT:
    """Narrow a workunit's metadata to a specific aspect class (for field access)."""
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert isinstance(wu.metadata.aspect, aspect_type)
    return wu.metadata.aspect


def entity_urn_of(wu: MetadataWorkUnit) -> str:
    """Narrow a workunit's metadata to an MCPW and return its entityUrn."""
    assert isinstance(wu.metadata, MetadataChangeProposalWrapper)
    assert wu.metadata.entityUrn is not None
    return wu.metadata.entityUrn
