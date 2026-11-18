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
from datahub.ingestion.source.snowflake.snowflake_config import SnowflakeV2Config
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
    SchemaMetadataClass,
    StatusClass,
    StructuredPropertiesClass,
    TagAssociationClass,
    _Aspect,
)
from datahub.metadata.urns import PlatformResourceUrn, StructuredPropertyUrn, TagUrn
from datahub.utilities.urns.urn import Urn

_A = TypeVar("_A", bound=_Aspect)


def _tag_object(database: str, schema: str, tag_name: str) -> str:
    return f"{database}.{schema}.{tag_name}".lower()


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
    first use, so lookups cost nothing per tag. Sync-back may write a record
    after the load, so the keys of a tag the load found no record for are read
    directly, once each per run. DataHub's current aspect is read only for
    entities about to get a whole aspect, and only when a managed record links
    a URN of that kind. Without a repository (no DataHub graph) every tag is
    native; if loading fails, every tag is skipped.

    What DataHub owns stays out of this pipeline's stale-entity state: managed
    records and the DataHub tags carried over into a whole `GlobalTags` are
    emitted as non-primary, so a later run that no longer emits them never
    soft-deletes them.
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
        # Kept after a failed load, to carry column tags over from DataHub.
        self._graph = repository.graph if repository is not None else None
        # Schemas are processed in parallel threads.
        self._lock = threading.RLock()
        self._records: Optional[Dict[str, SnowflakeTagPlatformResource]] = None
        self._new_keys: Set[str] = set()
        # Keys the load had no record for that were read from DataHub since.
        self._read_since_load: Set[str] = set()
        self._queued: Dict[str, SnowflakeTagPlatformResource] = {}
        self._emitted: Set[str] = set()
        # Filled by the load and emptied once their Status is emitted.
        self._managed_record_urns: List[str] = []
        # DataHub URNs that managed records link, and their entity types.
        self._managed_linked_urns: Set[str] = set()
        self._managed_linked_entity_types: Set[str] = set()
        # Snowflake tag objects (lower-cased db.schema.tag) with a managed record.
        self._managed_tag_objects: Set[str] = set()
        # DataHub tags carried over into a whole GlobalTags this run, in order.
        self._carried_over_tag_urns: Dict[str, None] = {}
        self._carried_over_emitted: Set[str] = set()
        self._load_failed = False

    def resolve_tag_urn(self, tag: SnowflakeTag) -> Optional[TagUrn]:
        """The native tag URN for `tag`, or None if DataHub manages it."""
        if not self.is_native(tag):
            if not self._load_failed:
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
        with_value = self._lookup(tag, with_value=True)
        without_value = self._lookup(tag, with_value=False)
        if with_value is None and without_value is None:
            # Sync-back writes its record before it sets the tag, so a tag it
            # set during this run has a record the load did not see.
            self._read_since_load_once(
                [self._key(tag, with_value=True), self._key(tag, with_value=False)]
            )
            with_value = self._lookup(tag, with_value=True)
            without_value = self._lookup(tag, with_value=False)
        return not (self._is_managed(with_value) or self._is_managed(without_value))

    def is_datahub_tag_object(self, tag: SnowflakeTag) -> bool:
        """Whether DataHub owns the Snowflake tag object itself: a managed
        record exists for it, with any value or none.

        For what belongs to the tag object as a whole, such as its structured
        property definition; sync-back keys its records by value, so a lookup
        without a value would miss them. Per asset use `is_native`, where a
        record for one value does not claim the tag's other values. If the
        records could not be loaded, every tag object counts as DataHub's.
        """
        self._load()
        if self._load_failed:
            return True
        return _tag_object(tag.database, tag.schema, tag.name) in (
            self._managed_tag_objects
        )

    def global_tags(
        self, entity_urn: str, tags: List[SnowflakeTag]
    ) -> Optional[GlobalTagsClass]:
        """The whole `GlobalTags` aspect to write for an entity, or None for none.

        Native tags from Snowflake, plus the managed tags DataHub holds now. It
        is written whenever Snowflake lists tags for the entity, even if all of
        them are managed, so a native tag removed in Snowflake is dropped as
        before. Nothing is written when Snowflake lists none (as before), when
        the records could not be loaded, or when DataHub's tags could not be
        read, since the write would drop them.
        """
        if not tags or self._load_failed:
            return None
        native = [
            tag_urn.urn()
            for tag_urn in (self.resolve_tag_urn(tag) for tag in tags)
            if tag_urn is not None
        ]
        if self._load_failed:
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
                with self._lock:
                    self._carried_over_tag_urns[association.tag] = None
        return GlobalTagsClass(tags=associations)

    def resolve_structured_properties(
        self,
        entity_urn: str,
        tags: List[SnowflakeTag],
        write_mode: StructuredPropertyWriteMode,
    ) -> Optional[Dict[StructuredPropertyUrn, StructuredPropertyValues]]:
        """The properties to write for an entity, or None to write none at all.

        Native properties from Snowflake. `UPSERT` replaces the aspect whole, so
        it also gets the values DataHub holds now for managed properties, is
        written even when every tag is managed (dropping native properties
        removed in Snowflake, as before), and is not written if those values
        could not be read.
        """
        if not tags or self._load_failed:
            return None
        resolved: Dict[StructuredPropertyUrn, StructuredPropertyValues] = {}
        for tag in tags:
            prop = self._native_structured_property(tag)
            if prop is not None:
                resolved[prop] = tag.value
        if self._load_failed:
            return None
        if write_mode != StructuredPropertyWriteMode.UPSERT:
            return resolved or None
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
        Every record is re-read first so another writer's (sync-back's) is
        never overwritten: a new record is dropped if it was created since the
        run started, and an existing one if sync-back has taken it over (made
        it managed) since; that one, like any existing record whose state
        could not be read, gets only a non-primary `Status`.

        What DataHub owns goes out as non-primary, which keeps it out of this
        pipeline's stale-entity state and on its skip list: managed records get
        only a `Status`, once per run (never sync-back's content), and each
        DataHub tag carried over into a `GlobalTags` gets its key aspect, which
        is what auto-materialization would otherwise emit as this pipeline's.
        """
        if self.repository is not None:
            # Load even with tags skipped or none seen, so managed records are
            # protected in every run.
            self._load()
        with self._lock:
            queued = list(self._queued.items())
            self._queued.clear()
            self._emitted.update(key for key, _ in queued)
            managed_urns = self._managed_record_urns
            self._managed_record_urns = []
            carried_over = [
                urn
                for urn in self._carried_over_tag_urns
                if urn not in self._carried_over_emitted
            ]
            self._carried_over_emitted.update(carried_over)
        for urn in managed_urns:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=StatusClass(removed=False)
            ).as_workunit(is_primary_source=False)
        for urn in carried_over:
            yield MetadataChangeProposalWrapper(
                entityUrn=urn, aspect=TagUrn.from_string(urn).to_key_aspect()
            ).as_workunit(is_primary_source=False)
        stored = self._stored_now([key for key, _ in queued])
        for key, record in queued:
            if key in self._new_keys:
                # Unknown is treated as written: skipping a record only delays it.
                if stored is None or key in stored:
                    continue
            elif stored is None or self._taken_over(stored.get(key)):
                yield MetadataChangeProposalWrapper(
                    entityUrn=str(PlatformResourceUrn(key)),
                    aspect=StatusClass(removed=False),
                ).as_workunit(is_primary_source=False)
                continue
            for mcp in record.as_platform_resource().to_mcps():
                yield mcp.as_workunit()

    def _native_structured_property(
        self, tag: SnowflakeTag
    ) -> Optional[StructuredPropertyUrn]:
        if not self.is_native(tag):
            if not self._load_failed:
                self.report.num_structured_properties_skipped_datahub_managed += 1
            return None
        native = StructuredPropertyUrn(
            self.identifiers.snowflake_identifier(tag.structured_property_identifier())
        )
        self._use_native(tag, with_value=False, urn=native.urn())
        return native

    def kept_column_tags(
        self, dataset_urn: str
    ) -> Optional[Dict[str, GlobalTagsClass]]:
        """Column tags to keep, by field path, when the records could not be
        loaded; None when they loaded and column tags resolve as usual.

        Column tags are written inside `schemaMetadata`, whole, so a run that
        skips tags would otherwise clear every column's tags. The tags DataHub
        holds now are carried over instead, as dataset tags are left alone.
        """
        self._load()
        if not self._load_failed:
            return None
        if self._graph is None:
            return {}
        try:
            current = self._graph.get_aspect(dataset_urn, SchemaMetadataClass)
        except Exception as e:
            self.report.num_datahub_aspect_read_failures += 1
            self.report.warning(
                title="Failed to read an asset's current metadata from DataHub",
                message="Its column tags could not be kept while Snowflake tags "
                "are skipped, and are cleared for this run.",
                context=dataset_urn,
                exc=e,
            )
            return {}
        return {
            field.fieldPath: field.globalTags
            for field in (current.fields if current else [])
            if field.globalTags is not None and field.globalTags.tags
        }

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
                    self._add_record(records, resource)
            self._records = records
            return records

    def _add_record(
        self,
        records: Dict[str, SnowflakeTagPlatformResource],
        resource: StoredResourceInfo,
    ) -> None:
        record = self._parse(resource)
        if record is None or not self._is_this_instance(record):
            return
        self._register(records, resource.urn, record)
        if record.is_managed_by_datahub():
            self._managed_record_urns.append(resource.urn)

    def _register(
        self,
        records: Dict[str, SnowflakeTagPlatformResource],
        resource_urn: str,
        record: SnowflakeTagPlatformResource,
    ) -> None:
        records[record.id.to_platform_resource_key().id] = record
        linked = self._valid_linked_urns(resource_urn, record)
        if record.is_managed_by_datahub():
            self._managed_tag_objects.add(
                _tag_object(
                    record.id.database, record.id.schema_name, record.id.tag_name
                )
            )
            for urn in linked:
                self._managed_linked_urns.add(urn.urn())
                self._managed_linked_entity_types.add(urn.entity_type)

    def _read_since_load_once(self, keys: List[str]) -> None:
        """Reads the records stored now for `keys` the load had none for, each
        key at most once per run, and adds those found."""
        with self._lock:
            records = self._load()
            missing = [
                key
                for key in dict.fromkeys(keys)
                if key not in records and key not in self._read_since_load
            ]
            if not missing or self.repository is None:
                return
            self._read_since_load.update(missing)
            # Read under the lock so no thread sees the key as checked before
            # its record is added. Each key is read once per run.
            try:
                stored = self.repository.read_resource_infos(missing)
            except Exception as e:
                self.report.num_tag_platform_resource_lookup_failures += 1
                self.report.warning(
                    title="Failed to read Snowflake tag platform resources",
                    message="The Snowflake tag is resolved from the records "
                    "loaded at the start of the run.",
                    context=", ".join(missing),
                    exc=e,
                )
                return
            for resource in stored.values():
                self._add_record(records, resource)

    def _stored_now(self, keys: List[str]) -> Optional[Dict[str, StoredResourceInfo]]:
        """The records stored now for `keys`, or None if they could not be read."""
        if not keys:
            return {}
        assert self.repository is not None
        try:
            return self.repository.read_resource_infos(keys)
        except Exception as e:
            self.report.num_tag_platform_resource_lookup_failures += 1
            self.report.warning(
                title="Failed to read Snowflake tag platform resources",
                message="Records of native Snowflake tags are not written this "
                "run, so none written meanwhile by DataHub is overwritten.",
                exc=e,
            )
            return None

    def _taken_over(self, stored: Optional[StoredResourceInfo]) -> bool:
        """Whether an existing native record is no longer ingestion's to write."""
        if stored is None:
            return False
        record = self._parse(stored)
        if record is None:
            # Rewritten by another writer in a form ingestion can't read.
            return True
        if not record.is_managed_by_datahub():
            return False
        with self._lock:
            self._register(self._load(), stored.urn, record)
        return True

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

    def _valid_linked_urns(
        self, resource_urn: str, record: SnowflakeTagPlatformResource
    ) -> List[Urn]:
        """The record's linked URNs; any that don't parse are dropped from it.

        Another tool may have written the record: one bad URN must neither stop
        the load nor make the record's tag native.
        """
        links = record.datahub_linked_resources().urns
        valid: List[Urn] = []
        for urn in list(links):
            try:
                valid.append(Urn.from_string(urn))
            except Exception as e:
                links.remove(urn)
                self.report.num_tag_platform_resource_invalid_urns += 1
                self.report.warning(
                    title="Ignored an invalid URN in a Snowflake tag platform resource",
                    message="The rest of the record is still used.",
                    context=f"{resource_urn}: {urn}",
                    exc=e,
                )
        return valid

    def _is_this_instance(self, record: SnowflakeTagPlatformResource) -> bool:
        # The scroll only filters by instance when one is configured. Instances
        # are case-sensitive, as dataPlatformInstance URNs are.
        return record.id.platform_instance == self.platform_instance

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
    def _is_managed(record: Optional[SnowflakeTagPlatformResource]) -> bool:
        # Linked or not: sync-back writes the record before it sets the tag, so
        # a managed record it has yet to link still means the tag is DataHub's.
        return record is not None and record.is_managed_by_datahub()

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
