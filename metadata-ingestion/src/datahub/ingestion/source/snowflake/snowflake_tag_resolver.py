import threading
from typing import Dict, Iterable, List, Optional, Set, Type, TypeVar

from datahub.api.entities.common.serialized_value import SerializedResourceValue
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.mcp_builder import (
    StructuredPropertyValues,
    StructuredPropertyWriteMode,
)
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import SnowflakeTag
from datahub.ingestion.source.snowflake.snowflake_tag_entities import (
    SnowflakeTagPlatformResource,
    SnowflakeTagPlatformResourceId,
    SnowflakeTagPlatformResourceRepository,
    StoredResourceInfo,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import (
    GlobalTagsClass,
    StatusClass,
    StructuredPropertiesClass,
    TagAssociationClass,
    _Aspect,
)
from datahub.metadata.urns import StructuredPropertyUrn, TagUrn
from datahub.utilities.urns.urn import Urn

_A = TypeVar("_A", bound=_Aspect)


def _lower(value: Optional[str]) -> Optional[str]:
    return value.lower() if value is not None else None


class _AspectUnreadable(Exception):
    """DataHub's current aspect of an entity could not be read."""


class SnowflakeTagResolver:
    """Decides which Snowflake tags ingestion writes, and keeps DataHub's own.

    A Snowflake tag that DataHub sync-back created carries a PlatformResource
    record with `managed_by_datahub=True` that names the DataHub tag, glossary
    term or structured property it stands for. DataHub is the source of truth
    for those: ingestion never takes them from Snowflake, whose
    `tag_references` view lags by up to hours and would undo a recent change
    in DataHub. Where ingestion writes an aspect whole (dataset and container
    `GlobalTags`, `UPSERT` structured properties), it keeps the values DataHub
    currently holds for URNs that managed records link. Every other tag is
    native and resolves exactly as before; its record is written with
    `managed_by_datahub=False`.

    All records of this platform instance are loaded with one paged scroll on
    first use, so lookups cost nothing per tag. DataHub's current aspect is
    read only for entities about to get a whole aspect, and only when a managed
    record links a URN of that kind. Without a repository (no DataHub graph),
    or if loading fails, every tag is native.
    """

    def __init__(
        self,
        repository: Optional[SnowflakeTagPlatformResourceRepository],
        config: SnowflakeV2Config,
        report: SnowflakeV2Report,
        identifiers: SnowflakeIdentifierBuilder,
        platform_instance: Optional[str],
    ) -> None:
        self.repository = repository
        self.config = config
        self.report = report
        self.identifiers = identifiers
        self.platform_instance = platform_instance
        # Schemas are processed in parallel threads.
        self._lock = threading.RLock()
        self._records: Optional[Dict[str, SnowflakeTagPlatformResource]] = None
        self._new_keys: Set[str] = set()
        self._queued: Dict[str, SnowflakeTagPlatformResource] = {}
        self._emitted: Set[str] = set()
        # Filled by the load and emptied once their Status is emitted.
        self._managed_record_urns: List[str] = []
        # DataHub URNs that managed records link, and their entity types.
        self._managed_linked_urns: Set[str] = set()
        self._managed_linked_entity_types: Set[str] = set()
        self._load_failed = False

    def resolve_tag_urn(self, tag: SnowflakeTag) -> Optional[TagUrn]:
        """The native tag URN for `tag`, or None if DataHub manages it."""
        if not self.is_native(tag):
            self.report.num_tags_skipped_datahub_managed += 1
            return None
        native = TagUrn.from_string(
            make_tag_urn(self.identifiers.snowflake_identifier(tag.tag_identifier()))
        )
        self._use_native(tag, with_value=True, urn=native.urn())
        return native

    def is_native(self, tag: SnowflakeTag) -> bool:
        self._load()
        if self._load_failed:
            return False
        return (
            self._managed_urn(self._lookup(tag, with_value=True)) is None
            and self._managed_urn(self._lookup(tag, with_value=False)) is None
        )

    def global_tags(
        self, entity_urn: str, tags: List[SnowflakeTag]
    ) -> Optional[GlobalTagsClass]:
        """The whole `GlobalTags` aspect to write for an entity, or None for none.

        Native tags from Snowflake, plus the managed tags DataHub holds now.
        Nothing is written when there is no native tag (as before) or when
        DataHub's tags could not be read, since the write would drop them.
        """
        native = [
            tag_urn.urn()
            for tag_urn in (self.resolve_tag_urn(tag) for tag in tags)
            if tag_urn is not None
        ]
        if not native:
            return None
        try:
            current = self._current_aspect(
                entity_urn, TagUrn.ENTITY_TYPE, GlobalTagsClass
            )
        except _AspectUnreadable:
            return None
        associations = [TagAssociationClass(tag=urn) for urn in dict.fromkeys(native)]
        for association in current.tags if current else []:
            if (
                association.tag in self._managed_linked_urns
                and association.tag not in native
            ):
                associations.append(association)
                self.report.num_managed_tags_preserved_from_datahub += 1
        return GlobalTagsClass(tags=associations)

    def resolve_structured_properties(
        self,
        entity_urn: str,
        tags: List[SnowflakeTag],
        write_mode: StructuredPropertyWriteMode,
    ) -> Optional[Dict[StructuredPropertyUrn, StructuredPropertyValues]]:
        """The properties to write for an entity, or None to write none at all.

        Native properties from Snowflake. `UPSERT` replaces the aspect whole, so
        it also gets the values DataHub holds now for managed properties, and
        nothing is written if those could not be read.
        """
        resolved: Dict[StructuredPropertyUrn, StructuredPropertyValues] = {}
        for tag in tags:
            prop = self._native_structured_property(tag)
            if prop is not None:
                resolved[prop] = tag.value
        if not resolved:
            return None
        if write_mode != StructuredPropertyWriteMode.UPSERT:
            return resolved
        try:
            current = self._current_aspect(
                entity_urn, StructuredPropertyUrn.ENTITY_TYPE, StructuredPropertiesClass
            )
        except _AspectUnreadable:
            return None
        for assignment in current.properties if current else []:
            prop = StructuredPropertyUrn.from_string(assignment.propertyUrn)
            if (
                assignment.propertyUrn in self._managed_linked_urns
                and prop not in resolved
            ):
                resolved[prop] = list(assignment.values)
                self.report.num_managed_structured_properties_preserved_from_datahub += 1
        return resolved

    def platform_resource_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Records of native tags used since the last call, each once per run.

        Existing records are emitted again so stale-entity removal keeps them.
        A new record is re-read first and dropped if another writer (sync-back)
        created it since the run started, so it is never overwritten. Managed
        records get only a `Status`, once per run: never sync-back's content,
        but enough to keep them out of stale-entity removal.
        """
        if self.repository is not None and self.config.extract_tags != TagOption.skip:
            # Load even if no tag was seen, so managed records stay checkpointed.
            self._load()
        with self._lock:
            queued = list(self._queued.items())
            self._queued.clear()
            self._emitted.update(key for key, _ in queued)
            managed_urns = self._managed_record_urns
            self._managed_record_urns = []
        for urn in managed_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=StatusClass(removed=False)
            ).as_workunit()
        written_meanwhile = self._written_meanwhile(
            [key for key, _ in queued if key in self._new_keys]
        )
        for key, record in queued:
            if key in written_meanwhile:
                continue
            for mcp in record.as_platform_resource().to_mcps():
                yield mcp.as_workunit()

    def _native_structured_property(
        self, tag: SnowflakeTag
    ) -> Optional[StructuredPropertyUrn]:
        if not self.is_native(tag):
            self.report.num_structured_properties_skipped_datahub_managed += 1
            return None
        native = StructuredPropertyUrn(
            self.identifiers.snowflake_identifier(tag.structured_property_identifier())
        )
        self._use_native(tag, with_value=False, urn=native.urn())
        return native

    def _current_aspect(
        self, entity_urn: str, linked_entity_type: str, aspect_type: Type[_A]
    ) -> Optional[_A]:
        """DataHub's aspect of `entity_urn`, or None if no managed record links
        a URN of `linked_entity_type` (so nothing would be kept from it anyway)."""
        self._load()
        if (
            self.repository is None
            or linked_entity_type not in self._managed_linked_entity_types
        ):
            return None
        try:
            return self.repository.graph.get_aspect(entity_urn, aspect_type)
        except Exception as e:
            self.report.num_datahub_aspect_read_failures += 1
            self.report.warning(
                title="Failed to read an asset's current metadata from DataHub",
                message=f"Its {aspect_type.ASPECT_NAME} is left unchanged for this "
                "run, so tags or properties that DataHub manages are not lost.",
                context=entity_urn,
                exc=e,
            )
            raise _AspectUnreadable() from e

    def _load(self) -> Dict[str, SnowflakeTagPlatformResource]:
        with self._lock:
            if self._records is not None:
                return self._records
            records: Dict[str, SnowflakeTagPlatformResource] = {}
            if self.repository is not None:
                try:
                    stored = list(
                        self.repository.scroll_resource_infos(self.platform_instance)
                    )
                except Exception as e:
                    self.report.num_tag_platform_resource_lookup_failures += 1
                    # A failure, not a warning: it also keeps stale-entity removal
                    # from soft-deleting the records this run could not see.
                    self.report.failure(
                        title="Failed to load Snowflake tag platform resources",
                        message="Snowflake tags are skipped for this run, so tags "
                        "that DataHub sync-back created are neither duplicated nor "
                        "removed.",
                        exc=e,
                    )
                    # Without the records any tag may be sync-back's: treating it
                    # as native would duplicate it and, written whole, replace the
                    # DataHub tag. Skip every tag and write no record this run.
                    self._load_failed = True
                    self.repository = None
                    stored = []
                for resource in stored:
                    record = self._parse(resource)
                    if record is None or not self._is_this_instance(record):
                        continue
                    records[record.id.to_platform_resource_key().id] = record
                    if record.is_managed_by_datahub():
                        self._managed_record_urns.append(resource.urn)
                        for urn in record.datahub_linked_resources().urns:
                            self._managed_linked_urns.add(urn)
                            self._managed_linked_entity_types.add(
                                Urn.from_string(urn).entity_type
                            )
            self._records = records
            return records

    def _parse(
        self, resource: StoredResourceInfo
    ) -> Optional[SnowflakeTagPlatformResource]:
        if resource.info.value is None:
            return None
        try:
            return SerializedResourceValue.from_resource_value(
                resource.info.value
            ).as_pydantic_object(SnowflakeTagPlatformResource)
        except Exception as e:
            # One bad record must not turn the feature off for the whole run.
            self.report.num_tag_platform_resources_unparseable += 1
            self.report.warning(
                title="Skipped unreadable Snowflake tag platform resource",
                message="The Snowflake tag it stands for is treated as native.",
                context=resource.urn,
                exc=e,
            )
            return None

    def _is_this_instance(self, record: SnowflakeTagPlatformResource) -> bool:
        # The scroll only filters by instance when one is configured.
        return _lower(record.id.platform_instance) == _lower(self.platform_instance)

    def _lookup(
        self, tag: SnowflakeTag, with_value: bool
    ) -> Optional[SnowflakeTagPlatformResource]:
        return self._load().get(self._key(tag, with_value))

    def _key(self, tag: SnowflakeTag, with_value: bool) -> str:
        return (
            SnowflakeTagPlatformResourceId.from_snowflake_tag(
                tag, self.platform_instance, with_value=with_value
            )
            .to_platform_resource_key()
            .id
        )

    @staticmethod
    def _managed_urn(record: Optional[SnowflakeTagPlatformResource]) -> Optional[Urn]:
        if record is None or not record.is_managed_by_datahub():
            return None
        urns = record.datahub_linked_resources().urns
        if not urns:
            return None
        return Urn.from_string(urns[0])

    def _use_native(self, tag: SnowflakeTag, with_value: bool, urn: str) -> None:
        if self.repository is None:
            return
        key = self._key(tag, with_value)
        with self._lock:
            records = self._load()
            record = records.get(key)
            if record is None:
                record = SnowflakeTagPlatformResource.create_default(
                    SnowflakeTagPlatformResourceId.from_snowflake_tag(
                        tag, self.platform_instance, with_value=with_value
                    ),
                    managed_by_datahub=False,
                )
                records[key] = record
                self._new_keys.add(key)
            # A managed record without a URN was left half-written by sync-back;
            # it is sync-back's to finish, so ingestion never adopts it.
            if record.is_managed_by_datahub() or key in self._emitted:
                return
            if urn not in record.datahub_linked_resources().urns:
                try:
                    record.datahub_linked_resources().add(urn)
                except ValueError:
                    # The record already links a different entity type.
                    return
            self._queued[key] = record

    def _written_meanwhile(self, new_keys: List[str]) -> Set[str]:
        if not new_keys:
            return set()
        assert self.repository is not None
        try:
            return self.repository.existing_key_ids(new_keys)
        except Exception:
            # Unknown is treated as written: skipping a record only delays it.
            return set(new_keys)
