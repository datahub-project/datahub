"""Typed accessors for the aspect inside a ``MetadataWorkUnit``."""

from typing import Any, Iterable, List, Type, TypeVar

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import _Aspect

_AspectT = TypeVar("_AspectT", bound=_Aspect)


def aspect_of(workunit: MetadataWorkUnit) -> Any:
    """The workunit's aspect, without committing to a concrete type."""
    return workunit.metadata.aspect  # type: ignore[union-attr]


def entity_urn(workunit: MetadataWorkUnit) -> str:
    """The URN the workunit targets."""
    urn = workunit.metadata.entityUrn  # type: ignore[union-attr]
    assert urn is not None
    return urn


def aspects_of(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> List[_AspectT]:
    """Every aspect of ``aspect_type`` across ``workunits``, in order."""
    return [
        aspect
        for aspect in (aspect_of(workunit) for workunit in workunits)
        if isinstance(aspect, aspect_type)
    ]


def only_aspect(
    workunits: Iterable[MetadataWorkUnit], aspect_type: Type[_AspectT]
) -> _AspectT:
    """The single aspect of ``aspect_type``, asserting there is exactly one."""
    found = aspects_of(workunits, aspect_type)
    assert len(found) == 1, (
        f"expected exactly one {aspect_type.__name__}, found {len(found)}"
    )
    return found[0]
