import re
from typing import List

import sqlparse

from datahub.utilities.sql_parser import SqlLineageSQLParser, SQLParser


class BigQuerySQLParser(SQLParser):
    parser: SQLParser

    def __init__(self, sql_query: str) -> None:
        super().__init__(sql_query)

        self._parsed_sql_query = self.parse_sql_query(sql_query)
        self.parser = SqlLineageSQLParser(self._parsed_sql_query)

    def parse_sql_query(self, sql_query: str) -> str:
        sql_query = BigQuerySQLParser._parse_bigquery_comment_sign(sql_query)
        sql_query = BigQuerySQLParser._escape_keyword_from_as_field_name(sql_query)
        sql_query = BigQuerySQLParser._escape_cte_name_after_keyword_with(sql_query)

        sql_query = sqlparse.format(
            sql_query.strip(),
            reindent_aligned=True,
            strip_comments=True,
        )

        sql_query = BigQuerySQLParser._escape_table_or_view_name_at_create_statement(
            sql_query
        )
        sql_query = BigQuerySQLParser._escape_object_name_after_keyword_from(sql_query)

        return sql_query

    @staticmethod
    def _parse_bigquery_comment_sign(sql_query: str) -> str:
        return re.sub(r"#(.*)", r"-- \1", sql_query, flags=re.IGNORECASE)

    @staticmethod
    def _escape_keyword_from_as_field_name(sql_query: str) -> str:
        return re.sub(r"(\w*\.from)", r"`\1`", sql_query, flags=re.IGNORECASE)

    @staticmethod
    def _escape_cte_name_after_keyword_with(sql_query: str) -> str:
        """
        Escape the first cte name in case it is one of reserved words
        """
        return re.sub(r"(with\s)([^`\s()]+)", r"\1`\2`", sql_query, flags=re.IGNORECASE)

    @staticmethod
    def _escape_table_or_view_name_at_create_statement(sql_query: str) -> str:
        """
        Reason: in case table name contains hyphens which breaks sqllineage later on
        """
        return re.sub(
            r"(create.*\s)(table\s|view\s)([^`\s()]+)(?=\sas)",
            r"\1\2`\3`",
            sql_query,
            flags=re.IGNORECASE,
        )

    @staticmethod
    def _escape_object_name_after_keyword_from(sql_query: str) -> str:
        """
        Reason: in case table name contains hyphens which breaks sqllineage later on
        Note: ignore cases of having keyword FROM as part of datetime function EXTRACT
        """
        return re.sub(
            r"(?<!day\s)(?<!(date|time|hour|week|year)\s)(?<!month\s)(?<!(second|minute)\s)(?<!quarter\s)(?<!\.)(from\s)([^`\s()]+)",
            r"\3`\4`",
            sql_query,
            flags=re.IGNORECASE,
        )

    def get_tables(self) -> List[str]:
        return self.parser.get_tables()

    def get_columns(self) -> List[str]:
        return self.parser.get_columns()
