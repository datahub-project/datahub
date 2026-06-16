import logging
from dataclasses import dataclass
from typing import Iterable, Set

from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import entity_supports_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    StatusClass,
)
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


@dataclass
class AutoStatusAspectProcessorReport(WorkunitProcessorReport):
    """Report for AutoStatusAspectProcessor metrics."""

    num_errors: int = 0  # Unexpected metadata types
    num_status_aspects_emitted: int = 0  # Status aspects added to entities


class AutoStatusAspectProcessor(WorkunitProcessor[AutoStatusAspectProcessorReport]):
    """Add status aspect (removed=False) to entities that don't have one."""

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
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
                self.report.num_errors += 1
                raise ValueError(f"Unexpected type {type(wu.metadata)}")

            yield wu

        for urn in sorted(all_urns - status_urns):
            entity_type = guess_entity_type(urn)
            if not entity_supports_aspect(entity_type, StatusClass):
                # If any entity does not support aspect 'status' then skip that entity from adding status aspect.
                # Example like dataProcessInstance doesn't suppport status aspect.
                # If not skipped gives error: java.lang.RuntimeException: Unknown aspect status for entity dataProcessInstance
                continue
            self.report.num_status_aspects_emitted += 1
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=StatusClass(removed=False),
            ).as_workunit()
