import logging
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    TypeVar,
    Union,
)

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import make_dataplatform_instance_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import entity_supports_aspect
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    BrowsePathEntryClass,
    BrowsePathsClass,
    BrowsePathsV2Class,
    ChangeTypeClass,
    ContainerClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnershipClass as Ownership,
    StatusClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DatasetUrn, GlossaryTermUrn, TagUrn, Urn
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.telemetry import telemetry
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns, lowercase_dataset_urns

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport
    from datahub.ingestion.source.state.stale_entity_removal_handler import (
        StaleEntityRemovalHandler,
    )

logger = logging.getLogger(__name__)


def auto_workunit(
    stream: Iterable[Union[MetadataChangeEventClass, MetadataChangeProposalWrapper]]
) -> Iterable[MetadataWorkUnit]:
    """Convert a stream of MCEs and MCPs to a stream of :class:`MetadataWorkUnit`s."""

    for item in stream:
        if isinstance(item, MetadataChangeEventClass):
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(item),
                mce=item,
            )
        else:
            yield item.as_workunit()


def create_dataset_props_patch_builder(
    dataset_urn: str,
    dataset_properties: DatasetPropertiesClass,
) -> DatasetPatchBuilder:
    """Creates a patch builder with a table's or view's attributes and dataset properties"""
    patch_builder = DatasetPatchBuilder(dataset_urn)
    patch_builder.set_display_name(dataset_properties.name)
    patch_builder.set_description(dataset_properties.description)
    patch_builder.set_created(dataset_properties.created)
    patch_builder.set_last_modified(dataset_properties.lastModified)
    patch_builder.set_qualified_name(dataset_properties.qualifiedName)
    patch_builder.add_custom_properties(dataset_properties.customProperties)

    return patch_builder


def create_dataset_owners_patch_builder(
    dataset_urn: str,
    ownership: Ownership,
) -> DatasetPatchBuilder:
    """Creates a patch builder with a dataset's owners"""
    patch_builder = DatasetPatchBuilder(dataset_urn)

    for owner in ownership.owners:
        patch_builder.add_owner(owner)

    return patch_builder


def auto_status_aspect(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """
    For all entities that don't have a status aspect, add one with removed set to false.
    """
    all_urns: Set[str] = set()
    status_urns: Set[str] = set()
    skip_urns: Set[str] = set()
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

        if not isinstance(
            wu.metadata, MetadataChangeEventClass
        ) and not entity_supports_aspect(wu.metadata.entityType, StatusClass):
            # If any entity does not support aspect 'status' then skip that entity from adding status aspect.
            # Example like dataProcessInstance doesn't suppport status aspect.
            # If not skipped gives error: java.lang.RuntimeException: Unknown aspect status for entity dataProcessInstance
            skip_urns.add(urn)

        yield wu

    for urn in sorted(all_urns - status_urns - skip_urns):
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()


def _default_entity_type_fn(wu: MetadataWorkUnit) -> Optional[str]:
    urn = wu.get_urn()
    entity_type = guess_entity_type(urn)
    return entity_type


def auto_stale_entity_removal(
    stale_entity_removal_handler: "StaleEntityRemovalHandler",
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


T = TypeVar("T", bound=MetadataWorkUnit)


def auto_workunit_reporter(report: "SourceReport", stream: Iterable[T]) -> Iterable[T]:
    """
    Calls report.report_workunit() on each workunit.
    """

    for wu in stream:
        report.report_workunit(wu)
        yield wu


def auto_materialize_referenced_tags_terms(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """For all references to tags/terms, emit a tag/term key aspect to ensure that the tag exists in our backend."""

    urn_entity_types = [TagUrn.ENTITY_TYPE, GlossaryTermUrn.ENTITY_TYPE]

    # Note: this code says "tags", but it applies to both tags and terms.

    referenced_tags = set()
    tags_with_aspects = set()

    for wu in stream:
        for urn in list_urns(wu.metadata):
            if guess_entity_type(urn) in urn_entity_types:
                referenced_tags.add(urn)

        urn = wu.get_urn()
        if guess_entity_type(urn) in urn_entity_types:
            tags_with_aspects.add(urn)

        yield wu

    for urn in sorted(referenced_tags - tags_with_aspects):
        urn_tp = Urn.from_string(urn)
        assert isinstance(urn_tp, (TagUrn, GlossaryTermUrn))

        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=urn_tp.to_key_aspect(),
        ).as_workunit()


def auto_lowercase_urns(
    stream: Iterable[MetadataWorkUnit],
) -> Iterable[MetadataWorkUnit]:
    """Lowercase all dataset urns"""

    for wu in stream:
        try:
            old_urn = wu.get_urn()
            lowercase_dataset_urns(wu.metadata)
            wu.id = wu.id.replace(old_urn, wu.get_urn())

            yield wu
        except Exception as e:
            logger.warning(f"Failed to lowercase urns for {wu}: {e}", exc_info=True)
            yield wu


def auto_browse_path_v2(
    stream: Iterable[MetadataWorkUnit],
    *,
    dry_run: bool = False,
    drop_dirs: Sequence[str] = (),
    platform: Optional[str] = None,
    platform_instance: Optional[str] = None,
) -> Iterable[MetadataWorkUnit]:
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

    # For telemetry, to see if our sources violate assumptions
    num_out_of_order = 0
    num_out_of_batch = 0

    # Set for all containers and urns with a Container aspect
    # Used to construct browse path v2 while iterating through stream
    # Assumes topological order of entities in stream, i.e. parent's
    # browse path/container is seen before child's browse path/container.
    paths: Dict[str, List[BrowsePathEntryClass]] = {}

    emitted_urns: Set[str] = set()
    containers_used_as_parent: Set[str] = set()
    for urn, batch in _batch_workunits_by_urn(stream):
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
                    num_out_of_order += 1

            browse_path_aspect = wu.get_aspect_of_type(BrowsePathsClass)
            if browse_path_aspect and browse_path_aspect.paths:
                legacy_path = [
                    BrowsePathEntryClass(id=p.strip())
                    for p in browse_path_aspect.paths[0].strip("/").split("/")
                    if p.strip() and p.strip() not in drop_dirs
                ]

        # Order of preference: browse path v2, container path, legacy browse path
        path = browse_path_v2 or container_path or legacy_path
        if path is not None and urn in emitted_urns:
            # Batch invariant violated
            # TODO: Add sentry alert
            num_out_of_batch += 1
        elif browse_path_v2 is not None:
            emitted_urns.add(urn)
            if not dry_run:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=BrowsePathsV2Class(
                        path=_prepend_platform_instance(
                            browse_path_v2, platform, platform_instance
                        )
                    ),
                ).as_workunit()
            else:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=BrowsePathsV2Class(path=browse_path_v2),
                ).as_workunit()
        elif path is not None:
            emitted_urns.add(urn)
            if not dry_run:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=BrowsePathsV2Class(
                        path=_prepend_platform_instance(
                            path, platform, platform_instance
                        )
                    ),
                ).as_workunit()
        elif urn not in emitted_urns and guess_entity_type(urn) == "container":
            # Root containers have no Container aspect, so they are not handled above
            emitted_urns.add(urn)
            if not dry_run:
                yield MetadataChangeProposalWrapper(
                    entityUrn=urn,
                    aspect=BrowsePathsV2Class(
                        path=_prepend_platform_instance([], platform, platform_instance)
                    ),
                ).as_workunit()

    if num_out_of_batch or num_out_of_order:
        properties = {
            "platform": platform,
            "has_platform_instance": bool(platform_instance),
            "num_out_of_batch": num_out_of_batch,
            "num_out_of_order": num_out_of_order,
        }
        telemetry.telemetry_instance.ping("incorrect_browse_path_v2", properties)


def auto_empty_dataset_usage_statistics(
    stream: Iterable[MetadataWorkUnit],
    *,
    dataset_urns: Set[str],
    config: BaseTimeWindowConfig,
    all_buckets: bool = False,  # TODO: Enable when CREATE changeType is supported for timeseries aspects
) -> Iterable[MetadataWorkUnit]:
    """Emit empty usage statistics aspect for all dataset_urns ingested with no usage."""
    buckets = config.buckets() if all_buckets else config.majority_buckets()
    bucket_timestamps = [int(bucket.timestamp() * 1000) for bucket in buckets]

    # Maps time bucket -> urns with usage statistics for that bucket
    usage_statistics_urns: Dict[int, Set[str]] = {ts: set() for ts in bucket_timestamps}
    invalid_timestamps = set()

    for wu in stream:
        yield wu
        if not wu.is_primary_source:
            continue

        urn = wu.get_urn()
        if guess_entity_type(urn) == DatasetUrn.ENTITY_TYPE:
            dataset_urns.add(urn)
            usage_aspect = wu.get_aspect_of_type(DatasetUsageStatisticsClass)
            if usage_aspect:
                if usage_aspect.timestampMillis in bucket_timestamps:
                    usage_statistics_urns[usage_aspect.timestampMillis].add(urn)
                elif all_buckets:
                    invalid_timestamps.add(usage_aspect.timestampMillis)

    if invalid_timestamps:
        logger.warning(
            f"Usage statistics with unexpected timestamps, bucket_duration={config.bucket_duration}:\n"
            ", ".join(
                str(datetime.fromtimestamp(ts / 1000, tz=timezone.utc))
                for ts in invalid_timestamps
            )
        )

    for bucket in bucket_timestamps:
        for urn in dataset_urns - usage_statistics_urns[bucket]:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=DatasetUsageStatisticsClass(
                    timestampMillis=bucket,
                    eventGranularity=TimeWindowSizeClass(unit=config.bucket_duration),
                    uniqueUserCount=0,
                    totalSqlQueries=0,
                    topSqlQueries=[],
                    userCounts=[],
                    fieldCounts=[],
                ),
                changeType=(
                    ChangeTypeClass.CREATE if all_buckets else ChangeTypeClass.UPSERT
                ),
            ).as_workunit()


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
        return [BrowsePathEntryClass(id=urn, urn=urn)] + entries

    return entries
