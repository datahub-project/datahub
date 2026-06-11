import logging
from typing import (
    TYPE_CHECKING,
    Dict,
    Iterable,
    List,
    Optional,
    Set,
    TypeVar,
    Union,
)

from datahub.configuration.time_window_config import BaseTimeWindowConfig
from datahub.emitter.mce_builder import (
    get_sys_time,
    parse_ts_millis,
)
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import entity_supports_aspect
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.metadata.schema_classes import (
    ChangeTypeClass,
    DatasetPropertiesClass,
    DatasetUsageStatisticsClass,
    MetadataChangeEventClass,
    MetadataChangeProposalClass,
    OwnershipClass as Ownership,
    SchemaFieldClass,
    SchemaMetadataClass,
    StatusClass,
    SystemMetadataClass,
    TimeWindowSizeClass,
)
from datahub.metadata.urns import DatasetUrn, GlossaryTermUrn, TagUrn, Urn
from datahub.sdk.entity import Entity
from datahub.specific.dataset import DatasetPatchBuilder
from datahub.telemetry import telemetry
from datahub.utilities.urns.error import InvalidUrnError
from datahub.utilities.urns.urn import guess_entity_type
from datahub.utilities.urns.urn_iter import list_urns, lowercase_dataset_urns

if TYPE_CHECKING:
    from datahub.ingestion.api.source import SourceReport

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
        entity_type = guess_entity_type(urn)
        if not entity_supports_aspect(entity_type, StatusClass):
            # If any entity does not support aspect 'status' then skip that entity from adding status aspect.
            # Example like dataProcessInstance doesn't suppport status aspect.
            # If not skipped gives error: java.lang.RuntimeException: Unknown aspect status for entity dataProcessInstance
            continue
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=StatusClass(removed=False),
        ).as_workunit()


T = TypeVar("T", bound=MetadataWorkUnit)


def auto_workunit_reporter(report: "SourceReport", stream: Iterable[T]) -> Iterable[T]:
    """
    Calls report.report_workunit() on each workunit.
    """

    for wu in stream:
        report.report_workunit(wu)
        yield wu

    if report.event_not_produced_warn and report.events_produced == 0:
        report.warning(
            title="No metadata was produced by the source",
            message="Please check the source configuration, filters, and permissions.",
        )


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
        try:
            urn_tp = Urn.from_string(urn)
            assert isinstance(urn_tp, (TagUrn, GlossaryTermUrn))

            yield MetadataChangeProposalWrapper(
                entityUrn=urn,
                aspect=urn_tp.to_key_aspect(),
            ).as_workunit()
        except InvalidUrnError:
            logger.info(
                f"Source produced an invalid urn, so no key aspect will be generated: {urn}"
            )


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


def auto_fix_duplicate_schema_field_paths(
    stream: Iterable[MetadataWorkUnit],
    *,
    platform: Optional[str] = None,
) -> Iterable[MetadataWorkUnit]:
    """Count schema metadata aspects with duplicate field paths and emit telemetry."""

    total_schema_aspects = 0
    schemas_with_duplicates = 0
    duplicated_field_paths = 0

    for wu in stream:
        schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
        if schema_metadata:
            total_schema_aspects += 1

            seen_fields = set()
            dropped_fields = []
            updated_fields: List[SchemaFieldClass] = []
            for field in schema_metadata.fields:
                if field.fieldPath in seen_fields:
                    dropped_fields.append(field.fieldPath)
                else:
                    seen_fields.add(field.fieldPath)
                    updated_fields.append(field)

            if dropped_fields:
                logger.info(
                    f"Fixing duplicate field paths in schema aspect for {wu.get_urn()} by dropping fields: {dropped_fields}"
                )
                schema_metadata.fields = updated_fields
                schemas_with_duplicates += 1
                duplicated_field_paths += len(dropped_fields)

        yield wu

    if schemas_with_duplicates:
        properties = {
            "platform": platform,
            "total_schema_aspects": total_schema_aspects,
            "schemas_with_duplicates": schemas_with_duplicates,
            "duplicated_field_paths": duplicated_field_paths,
        }
        telemetry.telemetry_instance.ping(
            "ingestion_duplicate_schema_field_paths", properties
        )


def auto_fix_empty_field_paths(
    stream: Iterable[MetadataWorkUnit],
    *,
    platform: Optional[str] = None,
) -> Iterable[MetadataWorkUnit]:
    """Count schema metadata aspects with empty field paths and emit telemetry."""

    total_schema_aspects = 0
    schemas_with_empty_fields = 0
    empty_field_paths = 0

    for wu in stream:
        schema_metadata = wu.get_aspect_of_type(SchemaMetadataClass)
        if schema_metadata:
            total_schema_aspects += 1

            updated_fields: List[SchemaFieldClass] = []
            for field in schema_metadata.fields:
                if field.fieldPath:
                    updated_fields.append(field)
                else:
                    empty_field_paths += 1

            if empty_field_paths > 0:
                logger.info(
                    f"Fixing empty field paths in schema aspect for {wu.get_urn()} by dropping empty fields"
                )
                schema_metadata.fields = updated_fields
                schemas_with_empty_fields += 1

        yield wu

    if schemas_with_empty_fields > 0:
        properties = {
            "platform": platform,
            "total_schema_aspects": total_schema_aspects,
            "schemas_with_empty_fields": schemas_with_empty_fields,
            "empty_field_paths": empty_field_paths,
        }
        telemetry.telemetry_instance.ping(
            "ingestion_empty_schema_field_paths", properties
        )


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
