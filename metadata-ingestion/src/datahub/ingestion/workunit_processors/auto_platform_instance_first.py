from collections import OrderedDict
from dataclasses import dataclass
from typing import Iterable, List, Set

from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import MetadataChangeEventClass
from datahub.utilities.urns.urn import guess_entity_type

_DATA_PLATFORM_INSTANCE = "dataPlatformInstance"

# Entity types whose URN does not carry the platform instance, so policies scoped
# by instance can only match their stored dataPlatformInstance. Types whose URN
# carries it (datasets, charts, dashboards, ...) are matched by URN from their first
# write and pass through untouched; their aspects can also be large.
_INSTANCE_FROM_ASPECT_ENTITY_TYPES = frozenset(
    {
        "container",
        "mlModel",
        "mlModelGroup",
        "dataProcessInstance",
        "query",
        "assertion",
        "platformResource",
    }
)

# Bounds memory when an entity's dataPlatformInstance never arrives. Threaded
# sources interleave entities across a backpressure queue of a few hundred
# workunits, so an entity's instance normally arrives well within this window.
_MAX_HELD_WORKUNITS = 1000


def _is_platform_instance(wu: MetadataWorkUnit) -> bool:
    return (
        not isinstance(wu.metadata, MetadataChangeEventClass)
        and wu.metadata.aspectName == _DATA_PLATFORM_INSTANCE
    )


@dataclass
class AutoPlatformInstanceFirstProcessorReport(WorkunitProcessorReport):
    """Report for AutoPlatformInstanceFirstProcessor metrics."""

    num_reordered: int = 0  # Entities whose dataPlatformInstance was moved up
    # Entities released without a dataPlatformInstance, at the cap or stream end
    num_released_without_platform_instance: int = 0


class AutoPlatformInstanceFirstProcessor(
    WorkunitProcessor[AutoPlatformInstanceFirstProcessorReport]
):
    """Emit an entity's dataPlatformInstance before its other aspects.

    Policies scoped by platform instance read the stored dataPlatformInstance,
    and for entities whose URN carries no instance (containers, ML models, ...)
    that is the only thing they can match. If another aspect creates the entity
    first, a token that may create entities but only edit one instance is then
    denied the dataPlatformInstance write itself, and every write after it.

    Enabled only when the source config sets a platform instance. For the entity
    types above, an entity's workunits are held until its
    dataPlatformInstance arrives and are emitted right after it; later workunits
    for the entity pass through. Holding is per URN, so it also works when a
    threaded source interleaves entities. An entity whose instance hasn't arrived
    is released in its original order once more than _MAX_HELD_WORKUNITS
    workunits are held (oldest entity first), or at the end of the stream.
    """

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        # Without a platform instance there is nothing for an instance-scoped
        # policy to match, so the stream is left exactly as the source emits it.
        return bool(
            getattr(ctx.source_config, "platform_instance", None)
            or getattr(ctx.source_config, "include_data_platform_instance", False)
        )

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        held: "OrderedDict[str, List[MetadataWorkUnit]]" = OrderedDict()
        num_held = 0
        released: Set[str] = set()

        for wu in stream:
            urn = wu.get_urn()
            if urn in held:
                if _is_platform_instance(wu):
                    pending = held.pop(urn)
                    num_held -= len(pending)
                    released.add(urn)
                    self.report.num_reordered += 1
                    yield wu
                    yield from pending
                else:
                    held[urn].append(wu)
                    num_held += 1
            elif (
                urn in released
                or guess_entity_type(urn) not in _INSTANCE_FROM_ASPECT_ENTITY_TYPES
            ):
                yield wu
            elif _is_platform_instance(wu):
                released.add(urn)
                yield wu
            else:
                held[urn] = [wu]
                num_held += 1

            while num_held > _MAX_HELD_WORKUNITS:
                oldest, pending = held.popitem(last=False)
                num_held -= len(pending)
                released.add(oldest)
                self.report.num_released_without_platform_instance += 1
                yield from pending

        for pending in held.values():
            self.report.num_released_without_platform_instance += 1
            yield from pending
