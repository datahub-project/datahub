from dataclasses import dataclass
from typing import Iterable, List, Optional

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import MetadataChangeEventClass

_DATA_PLATFORM_INSTANCE = "dataPlatformInstance"


def _is_platform_instance(wu: MetadataWorkUnit) -> bool:
    return (
        not isinstance(wu.metadata, MetadataChangeEventClass)
        and wu.metadata.aspectName == _DATA_PLATFORM_INSTANCE
    )


@dataclass
class AutoPlatformInstanceFirstProcessorReport(WorkunitProcessorReport):
    """Report for AutoPlatformInstanceFirstProcessor metrics."""

    num_reordered: int = 0  # Entity runs whose dataPlatformInstance was moved up


class AutoPlatformInstanceFirstProcessor(
    WorkunitProcessor[AutoPlatformInstanceFirstProcessorReport]
):
    """Emit an entity's dataPlatformInstance before its other aspects.

    Policies scoped by platform instance read the stored dataPlatformInstance,
    and for entities whose URN carries no instance (containers, ML models, ...)
    that is the only thing they can match. If another aspect creates the entity
    first, a token that may create entities but only edit one instance is then
    denied the dataPlatformInstance write itself, and every write after it.

    Works on each run of consecutive workunits for the same URN: workunits are
    held until the run's dataPlatformInstance arrives (then emitted after it) or
    the run ends, so at most one run is buffered. Order between entities, and
    within a run that has no dataPlatformInstance, is unchanged.
    """

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        run_urn: Optional[str] = None
        held: List[MetadataWorkUnit] = []
        # Whether the current run has already let its dataPlatformInstance (or
        # its first workunit, when that was the instance) through.
        passing = False

        for wu in stream:
            urn = wu.get_urn()
            if urn != run_urn:
                yield from held
                held = []
                run_urn = urn
                passing = _is_platform_instance(wu)
                if passing:
                    yield wu
                    continue

            if passing:
                yield wu
            elif _is_platform_instance(wu):
                if held:
                    self.report.num_reordered += 1
                yield wu
                yield from held
                held = []
                passing = True
            else:
                held.append(wu)

        yield from held
