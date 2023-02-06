from typing import Callable, Iterable, Optional, Set, Union

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.source import SourceReport
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalHandler,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
)
from datahub.utilities.urns.urn import guess_entity_type


def auto_workunit(
    stream: Iterable[Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]]
) -> Iterable[MetadataWorkUnit]:
    for item in stream:
        if isinstance(item, MetadataChangeEventClass):
            yield MetadataWorkUnit(id=f"{item.proposedSnapshot.urn}/mce", mce=item)
        else:
            yield item.as_workunit()


def auto_status_aspect(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """
    For all entities that don't have a status aspect, add one with removed set to false.
    """

    all_urns: Set[str] = set()
    status_urns: Set[str] = set()
    for wu in stream:
        urn = wu.get_urn()
        all_urns.add(urn)

        if not wu.is_primary_source:
            # If this is a non-primary source, we pretend like we've seen the status
            # aspect so that we don't try to emit a removal for it.
            status_urns.add(urn)
        elif isinstance(wu.metadata, MetadataChangeEventClass):
            if any(
                isinstance(aspect, StatusClass)
                for aspect in wu.metadata.proposedSnapshot.aspects
            ):
                status_urns.add(urn)
        elif isinstance(wu.metadata, MetadataChangeProposalWrapper):
            if isinstance(wu.metadata.aspect, StatusClass):
                status_urns.add(urn)
        elif isinstance(wu.metadata, MetadataChangeProposalClass):
            if wu.metadata.aspectName == StatusClass.ASPECT_NAME:
                status_urns.add(urn)
        else:
            raise ValueError(f"Unexpected type {type(wu.metadata)}")

        yield wu

    for urn in sorted(all_urns - status_urns):
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()


def _default_entity_type_fn(wu: MetadataWorkUnit) -> Optional[str]:
    urn = wu.get_urn()
    entity_type = guess_entity_type(urn)
    return entity_type


def auto_stale_entity_removal(
    stale_entity_removal_handler: StaleEntityRemovalHandler,
    stream: Iterable[MetadataWorkUnit],
    entity_type_fn: Callable[
        [MetadataWorkUnit], Optional[str]
    ] = _default_entity_type_fn,
) -> Iterable[MetadataWorkUnit]:
    """
    Record all entities that are found, and emit removals for any that disappeared in this run.
    """

    for wu in stream:
        urn = wu.get_urn()

        if wu.is_primary_source:
            entity_type = entity_type_fn(wu)
            if entity_type is not None:
                stale_entity_removal_handler.add_entity_to_state(entity_type, urn)
        else:
            stale_entity_removal_handler.add_urn_to_skip(urn)

        yield wu

    # Clean up stale entities.
    yield from stale_entity_removal_handler.gen_removed_entity_workunits()


def auto_workunit_reporter(
    report: SourceReport,
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """
    Calls report.report_workunit() on each workunit.
    """

    for wu in stream:
        report.report_workunit(wu)
        yield wu
