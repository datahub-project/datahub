import math
import threading
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Set, Union

from datahub.api.entities.common.serialized_value import SerializedResourceValue
from datahub.emitter.mce_builder import make_tag_urn
from datahub.emitter.mcp_builder import StructuredPropertyWriteMode
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
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass
from datahub.metadata.urns import DataTypeUrn, StructuredPropertyUrn, TagUrn
from datahub.utilities.urns.urn import Urn

_NUMBER_TYPE_URN = DataTypeUrn("datahub.number").urn()


def _lower(value: Optional[str]) -> Optional[str]:
    return value.lower() if value is not None else None


@dataclass(frozen=True)
class ResolvedStructuredProperty:
    urn: StructuredPropertyUrn
    value: Union[str, float]


class _UnresolvedProperty(Exception):
    """A DataHub-owned property whose value could not be written back."""


class SnowflakeTagResolver:
    """Decides which DataHub URN a Snowflake tag becomes, or whether it is skipped.

    A Snowflake tag that DataHub sync-back created carries a PlatformResource
    record with `managed_by_datahub=True` that names the DataHub tag, glossary
    term or structured property it stands for. Such a tag maps back to that URN
    where the aspect being written replaces the user's copy whole (dataset and
    container `GlobalTags`, `UPSERT` structured properties): leaving it out would
    delete it. Everywhere else it is skipped, because DataHub already holds it.
    Every other tag is native and resolves exactly as before; its record is
    written with `managed_by_datahub=False`.

    All records of this platform instance are loaded with one paged scroll on
    first use, so lookups cost nothing per tag. Without a repository (no DataHub
    graph), or if loading fails, every tag is native.
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
        self._value_types: Dict[str, str] = {}
        self._unreadable_definitions: Set[str] = set()

    def resolve_tag_urn(
        self, tag: SnowflakeTag, map_back: bool = True
    ) -> Optional[TagUrn]:
        """The tag URN to write, or None to leave the tag out.

        `map_back=False` is for aspects that are ingestion's own copy (column
        tags in schemaMetadata), where a DataHub-owned tag is simply skipped.
        """
        value_record = self._lookup(tag, with_value=True)
        managed = self._managed_urn(value_record)
        if managed is not None:
            if map_back and managed.entity_type == TagUrn.ENTITY_TYPE:
                self.report.num_tags_mapped_to_datahub_urn += 1
                return TagUrn.from_string(managed.urn())
            self.report.num_tags_skipped_datahub_managed += 1
            return None
        if self._managed_urn(self._lookup(tag, with_value=False)) is not None:
            self.report.num_tags_skipped_datahub_managed += 1
            return None

        native = TagUrn.from_string(
            make_tag_urn(self.identifiers.snowflake_identifier(tag.tag_identifier()))
        )
        self._use_native(tag, with_value=True, urn=native.urn())
        return native

    def is_native(self, tag: SnowflakeTag) -> bool:
        return (
            self._managed_urn(self._lookup(tag, with_value=True)) is None
            and self._managed_urn(self._lookup(tag, with_value=False)) is None
        )

    def resolve_structured_properties(
        self, tags: List[SnowflakeTag]
    ) -> Optional[Dict[StructuredPropertyUrn, Union[str, float]]]:
        """The properties to write for one entity, or None to write none at all.

        In `UPSERT` mode the aspect is replaced whole, so a DataHub-owned
        property that cannot be written back blocks the entity's aspect rather
        than being dropped from it (which would delete the user's value).
        """
        resolved: Dict[StructuredPropertyUrn, Union[str, float]] = {}
        for tag in tags:
            try:
                prop = self._resolve_structured_property(tag)
            except _UnresolvedProperty:
                self.report.num_structured_properties_unresolved += 1
                return None
            if prop is not None:
                resolved[prop.urn] = prop.value
        return resolved

    def platform_resource_workunits(self) -> Iterable[MetadataWorkUnit]:
        """Records of native tags used since the last call, each once per run.

        Existing records are emitted again so stale-entity removal keeps them.
        A new record is re-read first and dropped if another writer (sync-back)
        created it since the run started, so it is never overwritten.
        """
        with self._lock:
            queued = list(self._queued.items())
            self._queued.clear()
            self._emitted.update(key for key, _ in queued)
        written_meanwhile = self._written_meanwhile(
            [key for key, _ in queued if key in self._new_keys]
        )
        for key, record in queued:
            if key in written_meanwhile:
                continue
            for mcp in record.as_platform_resource().to_mcps():
                yield mcp.as_workunit()

    def _resolve_structured_property(
        self, tag: SnowflakeTag
    ) -> Optional[ResolvedStructuredProperty]:
        if self._managed_urn(self._lookup(tag, with_value=True)) is not None:
            self.report.num_structured_properties_skipped_datahub_managed += 1
            return None

        managed = self._managed_urn(self._lookup(tag, with_value=False))
        if managed is not None:
            if (
                managed.entity_type != StructuredPropertyUrn.ENTITY_TYPE
                or self.config.structured_properties_write_mode
                == StructuredPropertyWriteMode.PATCH
            ):
                self.report.num_structured_properties_skipped_datahub_managed += 1
                return None
            self.report.num_structured_properties_mapped_to_datahub_urn += 1
            return ResolvedStructuredProperty(
                urn=StructuredPropertyUrn.from_string(managed.urn()),
                value=self._cast(managed.urn(), tag),
            )

        native = StructuredPropertyUrn(
            self.identifiers.snowflake_identifier(tag.structured_property_identifier())
        )
        self._use_native(tag, with_value=False, urn=native.urn())
        return ResolvedStructuredProperty(urn=native, value=tag.value)

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
                    self.report.warning(
                        title="Failed to load Snowflake tag platform resources",
                        message="Snowflake tags are ingested as native tags for this "
                        "run. Tags that DataHub sync-back created may show up as "
                        "duplicates.",
                        exc=e,
                    )
                    # Writing records without knowing what exists could overwrite
                    # sync-back's, so this run writes none.
                    self.repository = None
                    stored = []
                for resource in stored:
                    record = self._parse(resource)
                    if record is not None and self._is_this_instance(record):
                        records[record.id.to_platform_resource_key().id] = record
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

    def _cast(self, property_urn: str, tag: SnowflakeTag) -> Union[str, float]:
        value_type = self._value_type(property_urn)
        if value_type != _NUMBER_TYPE_URN:
            return tag.value
        try:
            number = float(tag.value)
        except ValueError:
            number = math.nan
        # float() also accepts "nan" and "inf", which DataHub cannot store.
        if not math.isfinite(number):
            self.report.warning(
                title="Snowflake tag value does not match its structured property type",
                message="Structured properties of this asset are left unchanged.",
                context=f"{tag.tag_identifier()} -> {property_urn}",
            )
            raise _UnresolvedProperty()
        return number

    def _value_type(self, property_urn: str) -> str:
        with self._lock:
            if property_urn in self._unreadable_definitions:
                raise _UnresolvedProperty()
            if property_urn in self._value_types:
                return self._value_types[property_urn]
        assert self.repository is not None
        try:
            definition = self.repository.graph.get_aspect(
                property_urn, StructuredPropertyDefinitionClass
            )
        except Exception as e:
            with self._lock:
                self._unreadable_definitions.add(property_urn)
            self.report.warning(
                title="Failed to read structured property definition",
                message="Structured properties of assets using it are left unchanged.",
                context=property_urn,
                exc=e,
            )
            raise _UnresolvedProperty() from e
        if definition is None:
            # A value for a property that no longer exists would fail the write.
            with self._lock:
                self._unreadable_definitions.add(property_urn)
            self.report.warning(
                title="Structured property definition not found",
                message="Structured properties of assets using it are left unchanged.",
                context=property_urn,
            )
            raise _UnresolvedProperty()
        with self._lock:
            self._value_types[property_urn] = definition.valueType
        return definition.valueType
