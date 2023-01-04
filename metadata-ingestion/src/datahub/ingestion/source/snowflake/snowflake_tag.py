import logging
from typing import Dict, List, Optional

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
from datahub.ingestion.source.snowflake.snowflake_utils import SnowflakeCommonMixin

logger: logging.Logger = logging.getLogger(__name__)


class SnowflakeTagExtractor(SnowflakeCommonMixin):
    def __init__(
        self,
        config: SnowflakeV2Config,
        data_dictionary: SnowflakeDataDictionary,
        report: SnowflakeV2Report,
    ) -> None:
        self.config = config
        self.data_dictionary = data_dictionary
        self.report = report
        self.logger = logger

        self.tag_cache: Optional[_SnowflakeTagCache] = None

    def invalidate_cache(self):
        self.tag_cache = None

    def get_tags_on_object(
        self,
        domain: str,
        db_name: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
    ) -> List[SnowflakeTag]:
        if self.config.extract_tags == TagOption.without_lineage:
            if self.tag_cache is None:
                self.tag_cache = (
                    self.data_dictionary.get_tags_for_database_without_propagation(
                        db_name
                    )
                )

            if domain == "database":
                return self.tag_cache.get_database_tags(db_name)
            elif domain == "schema":
                assert schema_name is not None
                tags = self.tag_cache.get_schema_tags(schema_name, db_name)
            elif domain == "table":  # Views belong to this domain as well.
                assert schema_name is not None
                assert table_name is not None
                tags = self.tag_cache.get_table_tags(table_name, schema_name, db_name)
            else:
                raise ValueError(f"Unknown domain {domain}")
        elif self.config.extract_tags == TagOption.with_lineage:
            identifier = ""
            if domain == "database":
                identifier = self.get_quoted_identifier_for_database(db_name)
            elif domain == "schema":
                assert schema_name is not None
                identifier = self.get_quoted_identifier_for_schema(db_name, schema_name)
            elif domain == "table":  # Views belong to this domain as well.
                assert schema_name is not None
                assert table_name is not None
                identifier = self.get_quoted_identifier_for_table(
                    db_name, schema_name, table_name
                )
            else:
                raise ValueError(f"Unknown domain {domain}")
            assert identifier

            self.report.num_get_tags_for_object_queries += 1
            tags = self.data_dictionary.get_tags_for_object(
                domain=domain, quoted_identifier=identifier, db_name=db_name
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
            if self.tag_cache is None:
                self.tag_cache = (
                    self.data_dictionary.get_tags_for_database_without_propagation(
                        db_name
                    )
                )
            temp_column_tags = self.tag_cache.get_column_tags_for_table(
                table_name, schema_name, db_name
            )
        elif self.config.extract_tags == TagOption.with_lineage:
            self.report.num_get_tags_on_columns_for_table_queries += 1
            temp_column_tags = self.data_dictionary.get_tags_on_columns_for_table(
                quoted_table_name=self.get_quoted_identifier_for_table(
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
            tag_identifier = tag.identifier()
            self.report.report_entity_scanned(tag_identifier, "tag")
            if not self.config.tag_pattern.allowed(tag_identifier):
                self.report.report_dropped(tag_identifier)
            allowed_tags.append(tag)
        return allowed_tags
