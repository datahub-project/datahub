import itertools
from dataclasses import dataclass
from typing import Dict, Iterable

from datahub.configuration.env_vars import get_ingest_disable_dpi_first
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    DataPlatformInstanceClass,
    MetadataChangeProposalClass,
)


@dataclass
class EnsureDataPlatformInstanceFirstProcessorReport(WorkunitProcessorReport):
    """Report for EnsureDataPlatformInstanceFirstProcessor metrics."""

    num_runs_reordered: int = 0
    # dataPlatformInstance workunits in a later run of an entity whose first run had
    # none. The entity's earlier aspects are already on their way to the sink, so
    # this processor can't put the instance first; the connector has to. A later
    # re-emit for an entity whose first run did have one is harmless, not counted.
    num_dpi_after_first_run: int = 0


def _is_dpi(wu: MetadataWorkUnit) -> bool:
    # MCEs are not reordered: a dataPlatformInstance inside a snapshot is written
    # in the same request as the rest of the snapshot.
    md = wu.metadata
    return (
        isinstance(md, (MetadataChangeProposalWrapper, MetadataChangeProposalClass))
        and md.aspectName == DataPlatformInstanceClass.ASPECT_NAME
    )


class EnsureDataPlatformInstanceFirstProcessor(
    WorkunitProcessor[EnsureDataPlatformInstanceFirstProcessorReport]
):
    """Move an entity's dataPlatformInstance ahead of its other aspects.

    Policies scoped by platform instance match on the stored dataPlatformInstance.
    When another aspect creates the entity, the entity exists without an instance
    until the dataPlatformInstance write lands, and if those writes go out in
    separate requests the instance-scoped policy can't authorize the ones in
    between. Writing dataPlatformInstance first makes it the creating aspect.
    Stored metadata is unchanged for sources that emit one instance value per
    entity.

    Reorders only within the entity's first run of consecutive same-urn workunits,
    so the buffer holds one run. The map of seen urns grows with the number of
    entities, like AutoStatusAspectProcessor's. Like AutoBrowsePathV2Processor, a
    run is buffered, so if the source raises mid-run the buffered workunits of that
    run are not emitted.
    """

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return not get_ingest_disable_dpi_first()

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        # urn -> whether its first run contained a dataPlatformInstance
        first_run_had_dpi: Dict[str, bool] = {}
        for urn, group in itertools.groupby(stream, key=MetadataWorkUnit.get_urn):
            run = list(group)
            dpis = [wu for wu in run if _is_dpi(wu)]
            if urn in first_run_had_dpi:
                if not first_run_had_dpi[urn]:
                    self.report.num_dpi_after_first_run += len(dpis)
                yield from run
                continue
            first_run_had_dpi[urn] = bool(dpis)
            if any(not _is_dpi(wu) for wu in run[: len(dpis)]):
                self.report.num_runs_reordered += 1
                yield from dpis
                yield from (wu for wu in run if not _is_dpi(wu))
            else:
                yield from run
