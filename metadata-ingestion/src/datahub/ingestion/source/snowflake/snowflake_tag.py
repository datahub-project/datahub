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

    def _ensure_cache_loaded(self, db_name: str) -> None:
        if db_name not in self.tag_cache:
            try:
                self.tag_cache[db_name] = (
                    self.data_dictionary.get_tags_for_database_without_propagation(
                        db_name
                    )
                )
            except Exception as e:
                logger.warning(
                    f"Failed to load tag cache for database {db_name}: {e}",
                    exc_info=True,
                )
                self.report.warning(
                    title="Failed to load tags for database",
                    message="Tag extraction will be skipped for this database. "
                    "Check that the ingestion role has access to "
                    "SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES.",
                    context=db_name,
                    exc=e,
                )
                # Insert empty cache so we don't retry on every object
                self.tag_cache[db_name] = _SnowflakeTagCache()

    def _get_tags_on_object(
        self,
        domain: SnowflakeObjectDomain,
        db_name: str,
        schema_name: Optional[str],
        table_name: Optional[str],
        with_inheritance: bool,
    ) -> List[SnowflakeTag]:
        self._ensure_cache_loaded(db_name)
        cache = self.tag_cache[db_name]

        if domain == SnowflakeObjectDomain.DATABASE:
            return cache.get_database_tags(db_name)
        elif domain == SnowflakeObjectDomain.SCHEMA:
            if schema_name is None:
                raise ValueError(
                    f"schema_name is required for domain {domain} (db_name={db_name})"
                )
            if with_inheritance:
                return cache.get_schema_tags_with_inheritance(schema_name, db_name)
            return cache.get_schema_tags(schema_name, db_name)
        elif domain == SnowflakeObjectDomain.TABLE:
            # Views belong to this domain as well.
            if schema_name is None or table_name is None:
                raise ValueError(
                    f"schema_name and table_name are required for domain {domain} "
                    f"(db_name={db_name}, schema_name={schema_name}, table_name={table_name})"
                )
            if with_inheritance:
                return cache.get_table_tags_with_inheritance(
                    table_name, schema_name, db_name
                )
            return cache.get_table_tags(table_name, schema_name, db_name)
        else:
            raise ValueError(
                f"Tag extraction is not supported for domain {domain!r}. "
                f"Supported domains: DATABASE, SCHEMA, TABLE."
            )

    def create_structured_property_templates(self) -> Iterable[MetadataWorkUnit]:
        try:
            all_tags = list(self.data_dictionary.get_all_tags())
        except Exception as e:
            self.report.warning(
                title="Failed to create structured property templates",
                message="Could not fetch tag definitions from Snowflake. "
                "Structured property templates will not be created.",
                context="get_all_tags",
                exc=e,
            )
            return

        for tag in all_tags:
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

    def get_tags_on_object(
        self,
        domain: SnowflakeObjectDomain,
        db_name: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SnowflakeTag]:
        if self.config.extract_tags == TagOption.skip:
            return []

        tags = self._get_tags_on_object(
            domain=domain,
            db_name=db_name,
            schema_name=schema_name,
            table_name=table_name,
            with_inheritance=self.config.extract_tags == TagOption.with_lineage,
        )

        return self._filter_tags(tags)

    def get_column_tags_for_table(
        self,
        table_name: str,
        schema_name: str,
        db_name: str,
    ) -> Dict[str, List[SnowflakeTag]]:
        if self.config.extract_tags == TagOption.skip:
            return {}

        self._ensure_cache_loaded(db_name)
        cache = self.tag_cache[db_name]

        if self.config.extract_tags == TagOption.with_lineage:
            temp_column_tags = cache.get_column_tags_for_table_with_inheritance(
                table_name, schema_name, db_name
            )
        else:
            temp_column_tags = cache.get_column_tags_for_table(
                table_name, schema_name, db_name
            )

        column_tags: Dict[str, List[SnowflakeTag]] = {}
        for column_name, tags in temp_column_tags.items():
            allowed_tags = self._filter_tags(tags)
            if allowed_tags:
                column_tags[column_name] = allowed_tags

        return column_tags

    def _filter_tags(self, tags: List[SnowflakeTag]) -> List[SnowflakeTag]:
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
