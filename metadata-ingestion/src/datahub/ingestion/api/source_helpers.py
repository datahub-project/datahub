import logging
from typing import (
    Dict,
    Iterable,
    Optional,
    Set,
    Union,
)

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import (
    get_sys_time,
    parse_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    MetadataChangeEventClass,
    OwnershipClass as Ownership,
    SystemMetadataClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DatasetUrn
from datahub.sdk.entity import Entity
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.utilities.urns.urn import guess_entity_type

logger = logging.getLogger(__name__)


def auto_workunit(
    stream: Iterable[
        Union[
            MetadataChangeEventClass,
            MetadataChangeProposalWrapper,
            MetadataWorkUnit,
            Entity,
        ]
    ],
) -> Iterable[MetadataWorkUnit]:
    """Convert a stream of MCEs and MCPs to a stream of :class:`MetadataWorkUnit`s."""

    for item in stream:
        if isinstance(item, MetadataChangeEventClass):
            yield MetadataWorkUnit(
                id=MetadataWorkUnit.generate_workunit_id(item),
                mce=item,
            )
        elif isinstance(item, MetadataChangeProposalWrapper):
            yield item.as_workunit()
        elif isinstance(item, Entity):
            yield from item.as_workunits()
        else:
            yield item


def create_dataset_props_patch_builder(
    dataset_urn: str,
    dataset_properties: DatasetPropertiesClass,
    system_metadata: Optional[SystemMetadataClass] = None,
) -> DatasetPatchBuilder:
    """Creates a patch builder with a table's or view's attributes and dataset properties"""
    patch_builder = DatasetPatchBuilder(dataset_urn, system_metadata)
    patch_builder.set_display_name(dataset_properties.name)
    patch_builder.set_description(dataset_properties.description)
    patch_builder.set_created(dataset_properties.created)
    patch_builder.set_last_modified(dataset_properties.lastModified)
    patch_builder.set_qualified_name(dataset_properties.qualifiedName)
    patch_builder.add_custom_properties(dataset_properties.customProperties)
    patch_builder.set_external_url(dataset_properties.externalUrl)

    return patch_builder


def create_dataset_owners_patch_builder(
    dataset_urn: str,
    ownership: Ownership,
    system_metadata: Optional[SystemMetadataClass] = None,
) -> DatasetPatchBuilder:
    """Creates a patch builder with a dataset's owners"""
    patch_builder = DatasetPatchBuilder(dataset_urn, system_metadata)

    for owner in ownership.owners:
        patch_builder.add_owner(owner)

    return patch_builder


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
            ", ".join(str(parse_ts_millis(ts)) for ts in invalid_timestamps)
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


class AutoSystemMetadata:
    def __init__(self, ctx: PipelineContext):
        self.ctx = ctx

    def stamp(self, stream: Iterable[MetadataWorkUnit]) -> Iterable[MetadataWorkUnit]:
        for wu in stream:
            yield self.stamp_wu(wu)

    def stamp_wu(self, wu: MetadataWorkUnit) -> MetadataWorkUnit:
        if self.ctx.flags.set_system_metadata:
            if not wu.metadata.systemMetadata:
                wu.metadata.systemMetadata = SystemMetadataClass()
            wu.metadata.systemMetadata.runId = self.ctx.run_id
            if not wu.metadata.systemMetadata.lastObserved:
                wu.metadata.systemMetadata.lastObserved = get_sys_time()
            if self.ctx.flags.set_system_metadata_pipeline_name:
                wu.metadata.systemMetadata.pipelineName = self.ctx.pipeline_name
        return wu
