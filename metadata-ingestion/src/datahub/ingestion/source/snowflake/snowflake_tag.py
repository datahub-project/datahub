import logging
from typing import Dict, Iterable, List, Optional

from datahub.emitter.mce_builder import get_sys_time
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.ingestion.api.workunit import MetadataWorkUnit
from datahub.ingestion.source.snowflake.constants import SnowflakeObjectDomain
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeV2Config,
    TagOption,
)
from datahub.ingestion.source.snowflake.snowflake_report import SnowflakeV2Report
from datahub.ingestion.source.snowflake.snowflake_schema import (
    SnowflakeDataDictionary,
    SnowflakeTag,
    _SnowflakeTagCache,
)
from datahub.ingestion.source.snowflake.snowflake_utils import (
    SnowflakeCommonMixin,
    SnowflakeIdentifierBuilder,
)
from datahub.metadata.com.linkedin.pegasus2avro.common import AuditStamp
from datahub.metadata.com.linkedin.pegasus2avro.structured import (
    StructuredPropertyDefinition,
)
from datahub.metadata.schema_classes import ChangeTypeClass
from datahub.metadata.urns import (
    ContainerUrn,
    DatasetUrn,
    DataTypeUrn,
    EntityTypeUrn,
    SchemaFieldUrn,
    StructuredPropertyUrn,
)

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeTagExtractor(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        data_dictionary: SnowflakeDataDictionary,
        report: SnowflakeV2Report,
        snowflake_identifiers: SnowflakeIdentifierBuilder,
    ) -> None:
        self.config = config
        self.data_dictionary = data_dictionary
        self.report = report
        self.snowflake_identifiers = snowflake_identifiers
        self.tag_cache: Dict[str, _SnowflakeTagCache] = {}

    def _get_tags_on_object_without_propagation(
        self,
        domain: str,
        db_name: str,
        schema_name: Optional[str],
        table_name: Optional[str],
    ) -> List[SnowflakeTag]:
        if db_name not in self.tag_cache:
            self.tag_cache[db_name] = (
                self.data_dictionary.get_tags_for_database_without_propagation(db_name)
            )

        if domain == SnowflakeObjectDomain.DATABASE:
            return self.tag_cache[db_name].get_database_tags(db_name)
        elif domain == SnowflakeObjectDomain.SCHEMA:
            assert schema_name is not None
            tags = self.tag_cache[db_name].get_schema_tags(schema_name, db_name)
        elif (
            domain == SnowflakeObjectDomain.TABLE
        ):  # Views belong to this domain as well.
            assert schema_name is not None
            assert table_name is not None
            tags = self.tag_cache[db_name].get_table_tags(
                table_name, schema_name, db_name
            )
        else:
            raise ValueError(f"Unknown domain {domain}")
        return tags

    def create_structured_property_templates(self) -> Iterable[MetadataWorkUnit]:
        for tag in self.data_dictionary.get_all_tags():
            if not self.config.structured_property_pattern.allowed(
                tag._id_prefix_as_str()
            ):
                continue
            if self.config.extract_tags_as_structured_properties:
                self.report.num_structured_property_templates_created += 1
                yield from self.gen_tag_as_structured_property_workunits(tag)

    def gen_tag_as_structured_property_workunits(
        self, tag: SnowflakeTag
    ) -> Iterable[MetadataWorkUnit]:
        identifier = self.snowflake_identifiers.snowflake_identifier(
            tag.structured_property_identifier()
        )
        urn = StructuredPropertyUrn(identifier).urn()
        aspect = StructuredPropertyDefinition(
            qualifiedName=identifier,
            displayName=tag.name,
            valueType=DataTypeUrn("datahub.string").urn(),
            entityTypes=[
                EntityTypeUrn(f"datahub.{ContainerUrn.ENTITY_TYPE}").urn(),
                EntityTypeUrn(f"datahub.{DatasetUrn.ENTITY_TYPE}").urn(),
                EntityTypeUrn(f"datahub.{SchemaFieldUrn.ENTITY_TYPE}").urn(),
            ],
            lastModified=AuditStamp(
                time=get_sys_time(), actor="urn:li:corpuser:datahub"
            ),
        )
        yield MetadataChangeProposalWrapper(
            entityUrn=urn,
            aspect=aspect,
            changeType=ChangeTypeClass.CREATE,
            headers={"If-None-Match": "*"},
        ).as_workunit()

    def _get_tags_on_object_with_propagation(
        self,
        domain: str,
        db_name: str,
        schema_name: Optional[str],
        table_name: Optional[str],
    ) -> List[SnowflakeTag]:
        identifier = ""
        if domain == SnowflakeObjectDomain.DATABASE:
            identifier = self.identifiers.get_quoted_identifier_for_database(db_name)
        elif domain == SnowflakeObjectDomain.SCHEMA:
            assert schema_name is not None
            identifier = self.identifiers.get_quoted_identifier_for_schema(
                db_name, schema_name
            )
        elif (
            domain == SnowflakeObjectDomain.TABLE
        ):  # Views belong to this domain as well.
            assert schema_name is not None
            assert table_name is not None
            identifier = self.identifiers.get_quoted_identifier_for_table(
                db_name, schema_name, table_name
            )
        else:
            raise ValueError(f"Unknown domain {domain}")
        assert identifier

        self.report.num_get_tags_for_object_queries += 1
        tags = self.data_dictionary.get_tags_for_object_with_propagation(
            domain=domain, quoted_identifier=identifier, db_name=db_name
        )
        return tags

    def get_tags_on_object(
        self,
        domain: str,
        db_name: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SnowflakeTag]:
        if self.config.extract_tags == TagOption.without_lineage:
            tags = self._get_tags_on_object_without_propagation(
                domain=domain,
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
            )

        elif self.config.extract_tags == TagOption.with_lineage:
            tags = self._get_tags_on_object_with_propagation(
                domain=domain,
                db_name=db_name,
                schema_name=schema_name,
                table_name=table_name,
            )
        else:
            tags = []

        allowed_tags = self._filter_tags(tags)

        return allowed_tags if allowed_tags else []

    def get_column_tags_for_table(
        self,
        table_name: str,
        schema_name: str,
        db_name: str,
    ) -> Dict[str, List[SnowflakeTag]]:
        temp_column_tags: Dict[str, List[SnowflakeTag]] = {}
        if self.config.extract_tags == TagOption.without_lineage:
            if db_name not in self.tag_cache:
                self.tag_cache[db_name] = (
                    self.data_dictionary.get_tags_for_database_without_propagation(
                        db_name
                    )
                )
            temp_column_tags = self.tag_cache[db_name].get_column_tags_for_table(
                table_name, schema_name, db_name
            )
        elif self.config.extract_tags == TagOption.with_lineage:
            self.report.num_get_tags_on_columns_for_table_queries += 1
            temp_column_tags = self.data_dictionary.get_tags_on_columns_for_table(
                quoted_table_name=self.identifiers.get_quoted_identifier_for_table(
                    db_name, schema_name, table_name
                ),
                db_name=db_name,
            )

        column_tags: Dict[str, List[SnowflakeTag]] = {}

        for column_name in temp_column_tags:
            tags = temp_column_tags[column_name]
            allowed_tags = self._filter_tags(tags)
            if allowed_tags:
                column_tags[column_name] = allowed_tags

        return column_tags

    def _filter_tags(
        self, tags: Optional[List[SnowflakeTag]]
    ) -> Optional[List[SnowflakeTag]]:
        if tags is None:
            return tags

        allowed_tags = []
        for tag in tags:
            identifier = (
                tag._id_prefix_as_str()
                if self.config.extract_tags_as_structured_properties
                else tag.tag_identifier()
            )
            self.report.report_entity_scanned(identifier, "tag")

            pattern = (
                self.config.structured_property_pattern
                if self.config.extract_tags_as_structured_properties
                else self.config.tag_pattern
            )
            if not pattern.allowed(identifier):
                self.report.report_dropped(identifier)
            else:
                allowed_tags.append(tag)
        return allowed_tags
