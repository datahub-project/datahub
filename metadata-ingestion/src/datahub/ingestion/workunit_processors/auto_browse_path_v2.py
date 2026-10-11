import logging
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Tuple

from datahub.configuration.source_common import PlatformInstanceConfigMixin
from datahub.emitter.mce_builder import make_dataplatform_instance_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import entity_supports_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.api.workunit_processor import (
    WorkunitProcessor,
    WorkunitProcessorContext,
    WorkunitProcessorReport,
)
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ContainerClass,
)
from datahub.telemetry import telemetry
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


@dataclass
class AutoBrowsePathV2ProcessorReport(WorkunitProcessorReport):
    """Report for AutoBrowsePathV2Processor metrics."""

    # Invariant violations
    num_out_of_batch: int = 0  # URN seen in multiple batches
    num_out_of_order: int = 0  # Child container processed before parent

    # Browse path emission counters by source
    num_browse_path_v2_emitted: int = 0  # From source-generated BrowsePathsV2
    num_container_or_legacy_emitted: int = (
        0  # Derived from Container or legacy BrowsePaths
    )
    num_fallback_emitted: int = 0  # Fallback for root containers/dataFlow/dataJob


class AutoBrowsePathV2Processor(WorkunitProcessor[AutoBrowsePathV2ProcessorReport]):
    """Generate BrowsePathsV2 from Container and BrowsePaths aspects."""

    def __init__(self, ctx: WorkunitProcessorContext) -> None:
        super().__init__(ctx)
        flags = ctx.pipeline_context.flags
        self.dry_run: bool = flags.generate_browse_path_v2_dry_run

        config = ctx.source_config
        platform = ctx.source_platform or ctx.infer_platform()
        env = getattr(config, "env", None)
        drop_dirs_raw: List[Optional[str]] = [
            platform,
            platform.lower() if platform else None,
            env,
            env.lower() if env else None,
        ]
        self.drop_dirs: List[str] = [s for s in drop_dirs_raw if s is not None]
        self.platform = platform

        platform_instance: Optional[str] = None
        if isinstance(config, PlatformInstanceConfigMixin) and config.platform_instance:
            platform_instance = config.platform_instance
        self.platform_instance = platform_instance

    @classmethod
    def should_enable(cls, ctx: WorkunitProcessorContext) -> bool:
        return ctx.pipeline_context.flags.generate_browse_path_v2

    def process(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        """Generate BrowsePathsV2 from Container and BrowsePaths and BrowsePathsV2 aspects.

        Generates browse paths v2 on demand, rather than waiting for end of ingestion,
        for better UI experience while ingestion is running.

        To do this, assumes entities in container structure arrive in topological order
        and that all relevant aspects (Container, BrowsePaths, BrowsePathsV2) for an urn
        arrive together in a batch.

        Calculates the correct BrowsePathsV2 at end of workunit stream,
        and emits "corrections", i.e. a final BrowsePathsV2 for any urns that have changed.

        Source-generated original BrowsePathsV2 are assumed to be correct and are preferred
        over other aspects when generating BrowsePathsV2 of an entity or its children.
        This helper also prepends platform instance BrowsePathEntry to BrowsePathsV2 so the
        source need not include it in its browse paths v2.
        """

        # Set for all containers and urns with a Container aspect
        # Used to construct browse path v2 while iterating through stream
        # Assumes topological order of entities in stream, i.e. parent's
        # browse path/container is seen before child's browse path/container.
        paths: Dict[str, List[BrowsePathEntryClass]] = {}

        emitted_urns: Set[str] = set()
        containers_used_as_parent: Set[str] = set()
        for urn, batch in _batch_workunits_by_urn(stream):
            # Do not generate browse path v2 for entities that do not support it
            if not entity_supports_aspect(guess_entity_type(urn), BrowsePathsV2Class):
                yield from batch
                continue
            container_path: Optional[List[BrowsePathEntryClass]] = None
            legacy_path: Optional[List[BrowsePathEntryClass]] = None
            browse_path_v2: Optional[List[BrowsePathEntryClass]] = None

            for wu in batch:
                if not wu.is_primary_source:
                    yield wu
                    continue

                browse_path_v2_aspect = wu.get_aspect_of_type(BrowsePathsV2Class)
                if browse_path_v2_aspect is None:
                    yield wu
                else:
                    # This is browse path v2 aspect. We will process
                    # and emit it later with platform instance, as required.
                    browse_path_v2 = browse_path_v2_aspect.path
                    if guess_entity_type(urn) == "container":
                        paths[urn] = browse_path_v2

                container_aspect = wu.get_aspect_of_type(ContainerClass)
                if container_aspect:
                    parent_urn = container_aspect.container
                    containers_used_as_parent.add(parent_urn)
                    # If a container has both parent container and browsePathsV2
                    # emitted from source, prefer browsePathsV2, so using setdefault.
                    paths.setdefault(
                        urn,
                        [
                            *paths.setdefault(
                                parent_urn, []
                            ),  # Guess parent has no parents
                            BrowsePathEntryClass(id=parent_urn, urn=parent_urn),
                        ],
                    )
                    container_path = paths[urn]

                    if urn in containers_used_as_parent:
                        # Topological order invariant violated; we've used the previous value of paths[urn]
                        # TODO: Add sentry alert
                        self.report.num_out_of_order += 1

                browse_path_aspect = wu.get_aspect_of_type(BrowsePathsClass)
                if browse_path_aspect and browse_path_aspect.paths:
                    legacy_path = [
                        BrowsePathEntryClass(id=p.strip())
                        for p in browse_path_aspect.paths[0].strip("/").split("/")
                        if p.strip() and p.strip() not in self.drop_dirs
                    ]

            # Order of preference: browse path v2, container path, legacy browse path
            path = browse_path_v2 or container_path or legacy_path
            if path is not None and urn in emitted_urns:
                # Batch invariant violated
                # TODO: Add sentry alert
                self.report.num_out_of_batch += 1
            elif browse_path_v2 is not None:
                self.report.num_browse_path_v2_emitted += 1
                emitted_urns.add(urn)
                if not self.dry_run:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=BrowsePathsV2Class(
                            path=_prepend_platform_instance(
                                browse_path_v2, self.platform, self.platform_instance
                            )
                        ),
                    ).as_workunit()
                else:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=BrowsePathsV2Class(path=browse_path_v2),
                    ).as_workunit()
            elif path is not None:
                self.report.num_container_or_legacy_emitted += 1
                emitted_urns.add(urn)
                if not self.dry_run:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=BrowsePathsV2Class(
                            path=_prepend_platform_instance(
                                path, self.platform, self.platform_instance
                            )
                        ),
                    ).as_workunit()
            elif urn not in emitted_urns and (
                guess_entity_type(urn) == "container"
                or (
                    self.platform_instance
                    and guess_entity_type(urn) in ("dataFlow", "dataJob")
                )
            ):
                # Emit a browse path for:
                # - Root containers (no Container aspect, need empty path)
                # - DataFlow/DataJob when platform_instance is set, so they get
                #   grouped under their instance instead of the backend's catch-all
                #   "Default" folder.
                # TODO: This fallback should ideally apply to ALL entity types with
                # platform_instance (not just DataFlow/DataJob). However, entities
                # like Dataset often have their Container aspect emitted in a later
                # batch (due to _batch_workunits_by_urn grouping only consecutive
                # workunits). Emitting a fallback eagerly for those entities causes
                # OS-dependent golden file differences because filesystem enumeration
                # order determines which entities land in which batch. DataFlow and
                # DataJob are safe because they never have Container aspects.
                self.report.num_fallback_emitted += 1
                emitted_urns.add(urn)
                if not self.dry_run:
                    yield MetadataChangeProposalWrapper(
                        entityUrn=urn,
                        aspect=BrowsePathsV2Class(
                            path=_prepend_platform_instance(
                                [], self.platform, self.platform_instance
                            )
                        ),
                    ).as_workunit()

        # Send telemetry
        if self.report.num_out_of_batch or self.report.num_out_of_order:
            properties = {
                "platform": self.platform,
                "has_platform_instance": bool(self.platform_instance),
                "num_out_of_batch": self.report.num_out_of_batch,
                "num_out_of_order": self.report.num_out_of_order,
            }
            telemetry.telemetry_instance.ping("incorrect_browse_path_v2", properties)


def _batch_workunits_by_urn(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[Tuple[str, List[MetadataWorkUnit]]]:
    batch: List[MetadataWorkUnit] = []
    batch_urn: Optional[str] = None
    for wu in stream:
        if wu.get_urn() != batch_urn:
            if batch_urn is not None:
                yield batch_urn, batch
            batch = []

        batch.append(wu)
        batch_urn = wu.get_urn()

    if batch_urn is not None:
        yield batch_urn, batch


def _prepend_platform_instance(
    entries: List[BrowsePathEntryClass],
    platform: Optional[str],
    platform_instance: Optional[str],
) -> List[BrowsePathEntryClass]:
    if platform and platform_instance:
        urn = make_dataplatform_instance_urn(platform, platform_instance)
        # Check if platform instance is already the first entry to avoid duplication
        if entries and entries[0].urn == urn:
            return entries
        return [BrowsePathEntryClass(id=urn, urn=urn)] + entries

    return entries
