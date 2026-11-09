from dataclasses import dataclass
from typing import Dict, Iterable, Optional, Set, Union

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
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.schema_classes import StructuredPropertyDefinitionClass
from datahub.metadata.urns import DataTypeUrn, StructuredPropertyUrn, TagUrn
from datahub.utilities.urns.urn import Urn

_NUMBER_TYPE_URN = DataTypeUrn("datahub.number").urn()


@dataclass(frozen=True)
class ResolvedStructuredProperty:
    urn: StructuredPropertyUrn
    value: Union[str, float]


class SnowflakeTagResolver:
    """Decides which DataHub URN a Snowflake tag becomes, or whether it is skipped.

    A Snowflake tag that DataHub sync-back created carries a PlatformResource
    record with `managed_by_datahub=True` that names the DataHub tag, glossary
    term or structured property it stands for. Such a tag maps back to that
    URN where the aspect being written replaces the whole aspect (dataset and
    container `GlobalTags`, `UPSERT` structured properties): leaving it out
    would delete the user's copy. Everywhere else it is skipped, because DataHub
    already holds it. Every other tag is native and resolves exactly as before;
    its record is written with `managed_by_datahub=False`. Without a repository
    (no DataHub graph) every tag is native.
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
        self._queued: Dict[str, SnowflakeTagPlatformResource] = {}
        self._failed_lookups: Set[str] = set()
        self._value_types: Dict[str, Optional[str]] = {}

    def resolve_tag_urn(self, tag: SnowflakeTag) -> Optional[TagUrn]:
        value_record = self._lookup(tag, with_value=True)
        managed = self._managed_urn(value_record)
        if managed is not None:
            if managed.entity_type == TagUrn.ENTITY_TYPE:
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
        self._link(value_record, native.urn())
        return native

    def is_native(self, tag: SnowflakeTag) -> bool:
        return (
            self._managed_urn(self._lookup(tag, with_value=True)) is None
            and self._managed_urn(self._lookup(tag, with_value=False)) is None
        )

    def resolve_structured_property(
        self, tag: SnowflakeTag
    ) -> Optional[ResolvedStructuredProperty]:
        if self._managed_urn(self._lookup(tag, with_value=True)) is not None:
            self.report.num_structured_properties_skipped_datahub_managed += 1
            return None

        definition_record = self._lookup(tag, with_value=False)
        managed = self._managed_urn(definition_record)
        if managed is not None:
            if (
                managed.entity_type != StructuredPropertyUrn.ENTITY_TYPE
                or self.config.structured_properties_write_mode
                == StructuredPropertyWriteMode.PATCH
            ):
                self.report.num_structured_properties_skipped_datahub_managed += 1
                return None
            value = self._cast(managed.urn(), tag)
            if value is None:
                self.report.num_structured_properties_skipped_datahub_managed += 1
                return None
            self.report.num_structured_properties_mapped_to_datahub_urn += 1
            return ResolvedStructuredProperty(
                urn=StructuredPropertyUrn.from_string(managed.urn()), value=value
            )

        native = StructuredPropertyUrn(
            self.identifiers.snowflake_identifier(tag.structured_property_identifier())
        )
        self._link(definition_record, native.urn())
        return ResolvedStructuredProperty(urn=native, value=tag.value)

    def platform_resource_workunits(self) -> Iterable[MetadataWorkUnit]:
        queued = list(self._queued.values())
        self._queued.clear()
        for record in queued:
            for mcp in record.as_platform_resource().to_mcps():
                yield mcp.as_workunit()

    def _lookup(
        self, tag: SnowflakeTag, with_value: bool
    ) -> Optional[SnowflakeTagPlatformResource]:
        if self.repository is None:
            return None
        entity_id = SnowflakeTagPlatformResourceId.from_snowflake_tag(
            tag, self.platform_instance, with_value=with_value
        )
        # One failed lookup stops both lookups for that tag, so it is reported once.
        failure_key = tag.tag_identifier()
        if failure_key in self._failed_lookups:
            return None
        try:
            return self.repository.get_entity_from_datahub(
                entity_id, managed_by_datahub=False
            )
        except Exception as e:
            self._failed_lookups.add(failure_key)
            self.report.num_tag_platform_resource_lookup_failures += 1
            self.report.warning(
                title="Failed to look up Snowflake tag platform resource",
                message="The tag is ingested as a native Snowflake tag. If DataHub "
                "sync-back created it, it may show up as a duplicate tag.",
                context=entity_id.to_platform_resource_key().primary_key,
                exc=e,
            )
            return None

    @staticmethod
    def _managed_urn(record: Optional[SnowflakeTagPlatformResource]) -> Optional[Urn]:
        if record is None or not record.is_managed_by_datahub():
            return None
        urns = record.datahub_linked_resources().urns
        if not urns:
            return None
        return Urn.from_string(urns[0])

    def _link(self, record: Optional[SnowflakeTagPlatformResource], urn: str) -> None:
        # A managed record without a URN was left half-written by sync-back; it is
        # sync-back's to finish, so ingestion never adopts it.
        if (
            record is None
            or record.is_managed_by_datahub()
            or urn in record.datahub_linked_resources().urns
        ):
            return
        try:
            record.datahub_linked_resources().add(urn)
        except ValueError:
            # The record already links a different entity type; leave it as is.
            return
        self._queued[record.id.to_platform_resource_key().id] = record

    def _cast(
        self, property_urn: str, tag: SnowflakeTag
    ) -> Optional[Union[str, float]]:
        try:
            value_type = self._value_type(property_urn)
        except Exception as e:
            self.report.warning(
                title="Failed to read structured property definition",
                message="The structured property is left unchanged for this asset.",
                context=property_urn,
                exc=e,
            )
            return None
        if value_type != _NUMBER_TYPE_URN:
            return tag.value
        try:
            return float(tag.value)
        except ValueError:
            self.report.warning(
                title="Snowflake tag value does not match its structured property type",
                message="The structured property is left unchanged for this asset.",
                context=f"{tag.tag_identifier()} -> {property_urn}",
            )
            return None

    def _value_type(self, property_urn: str) -> Optional[str]:
        if property_urn not in self._value_types:
            definition = (
                self.repository.graph.get_aspect(
                    property_urn, StructuredPropertyDefinitionClass
                )
                if self.repository is not None
                else None
            )
            self._value_types[property_urn] = (
                definition.valueType if definition is not None else None
            )
        return self._value_types[property_urn]
