import logging
import re
import unittest
import unittest.mock
from typing import Dict, List, Set

from sqllineage.core.holders import Column, SQLLineageHolder
from sqllineage.exceptions import SQLLineageException

try:
    import sqlparse
    from networkx import DiGraph
    from sqllineage.core import LineageAnalyzer

    import datahub.utilities.sqllineage_patch
except ImportError:
    pass

logger = logging.getLogger(__name__)


class SqlLineageSQLParserImpl:
    _DATE_SWAP_TOKEN = "__d_a_t_e"
    _HOUR_SWAP_TOKEN = "__h_o_u_r"
    _TIMESTAMP_SWAP_TOKEN = "__t_i_m_e_s_t_a_m_p"
    _DATA_SWAP_TOKEN = "__d_a_t_a"
    _ADMIN_SWAP_TOKEN = "__a_d_m_i_n"
    _MYVIEW_SQL_TABLE_NAME_TOKEN = "__my_view__.__sql_table_name__"
    _MYVIEW_LOOKER_TOKEN = "my_view.SQL_TABLE_NAME"

    def __init__(self, sql_query: str) -> None:
        original_sql_query = sql_query

        # SqlLineageParser makes mistakes on lateral flatten queries, use the prefix
        if "lateral flatten" in sql_query:
            sql_query = sql_query[: sql_query.find("lateral flatten")]

        # BigQuery can use # as a comment symbol and that may break the parsing
        # if the comment starts without a space sign after #
        # and the comment is written inside SQL DDL statement (e.g., after CREATE...AS)
        sql_query = re.sub(r"#([^ ])", r"# \1", sql_query, flags=re.IGNORECASE)

        # Wrap calls to field '<table_alias>.from' in ` so it would not be taken as a reserved keyword
        sql_query = re.sub(r"(\w*\.from)", r"`\1`", sql_query, flags=re.IGNORECASE)

        # Apply sqlparser formatting to get rid of comments and reindent keywords
        # which should remove some potential inconsistencies in parsing output
        sql_query = sqlparse.format(
            sql_query.strip(),
            reindent_aligned=True,
            strip_comments=True,
        )

        # SqlLineageParser does not allow table/view names not being wrapped in quotes or backticks
        # Add ` signs before and after supposed object name that comes right after reserved word FROM
        # note 1: this excludes date/time/datetime extract functions like EXTRACT DATE FROM...
        # note 2: this includes adding ` signs to CTE aliases
        sql_query = re.sub(
            r"(?<!day\s)(?<!(date|time|hour|week|year)\s)(?<!month\s)(?<!(second|minute)\s)(?<!quarter\s)(?<!\.)(from\s)([^`\s()]+)",
            r"\3`\4`",
            sql_query,
            flags=re.IGNORECASE,
        )

        # Add ` signs before and after table/view name at the beginning of SQL DDL statement (e.g. CREATE/CREATE OR REPLACE...AS)
        sql_query = re.sub(
            r"(create.*\s)(table\s|view\s)([^`\s()]+)(?=\sas)",
            r"\1\2`\3`",
            sql_query,
            flags=re.IGNORECASE,
        )

        # Add ` signs before and after CTE alias name at WITH
        sql_query = re.sub(
            r"(with\s)([^`\s()]+)", r"\1`\2`", sql_query, flags=re.IGNORECASE
        )

        # Replace reserved words that break SqlLineageParser
        self.token_to_original: Dict[str, str] = {
            self._DATE_SWAP_TOKEN: "date",
            self._HOUR_SWAP_TOKEN: "hour",
            self._TIMESTAMP_SWAP_TOKEN: "timestamp",
            self._DATA_SWAP_TOKEN: "data",
            self._ADMIN_SWAP_TOKEN: "admin",
        }
        for replacement, original in self.token_to_original.items():
            sql_query = re.sub(
                rf"(\b{original}\b)", rf"{replacement}", sql_query, flags=re.IGNORECASE
            )

        # SqlLineageParser lowercarese tablenames and we need to replace Looker specific token which should be uppercased
        sql_query = re.sub(
            rf"(\${{{self._MYVIEW_LOOKER_TOKEN}}})",
            rf"{self._MYVIEW_SQL_TABLE_NAME_TOKEN}",
            sql_query,
        )

        # SqlLineageParser does not handle "encode" directives well. Remove them
        sql_query = re.sub(r"\sencode [a-zA-Z]*", "", sql_query, flags=re.IGNORECASE)

        # Replace lookml templates with the variable otherwise sqlparse can't parse ${
        sql_query = re.sub(r"(\${)(.+)(})", r"\2", sql_query)
        if sql_query != original_sql_query:
            logger.debug(f"Rewrote original query {original_sql_query} as {sql_query}")

        self._sql = sql_query

        try:
            self._stmt = [
                s
                for s in sqlparse.parse(
                    sqlparse.format(
                        self._sql.strip(),
                        use_space_around_operators=True,
                    ),
                )
                if s.token_first(skip_cm=True)
            ]

            with unittest.mock.patch(
                "sqllineage.core.handlers.source.SourceHandler.end_of_query_cleanup",
                datahub.utilities.sqllineage_patch.end_of_query_cleanup_patch,
            ):
                with unittest.mock.patch(
                    "sqllineage.core.holders.SubQueryLineageHolder.add_column_lineage",
                    datahub.utilities.sqllineage_patch.add_column_lineage_patch,
                ):
                    self._stmt_holders = [
                        LineageAnalyzer().analyze(stmt) for stmt in self._stmt
                    ]
                    self._sql_holder = SQLLineageHolder.of(*self._stmt_holders)
        except SQLLineageException as e:
            logger.error(f"SQL lineage analyzer error '{e}' for query: '{self._sql}")

    def get_tables(self) -> List[str]:
        result: List[str] = list()
        for table in self._sql_holder.source_tables:
            table_normalized = re.sub(r"^<default>.", "", str(table))
            result.append(str(table_normalized))

        # We need to revert TOKEN replacements
        for token, replacement in self.token_to_original.items():
            result = [replacement if c == token else c for c in result]
        result = [
            self._MYVIEW_LOOKER_TOKEN if c == self._MYVIEW_SQL_TABLE_NAME_TOKEN else c
            for c in result
        ]

        # Sort tables to make the list deterministic
        result.sort()

        return result

    def get_columns(self) -> List[str]:
        graph: DiGraph = self._sql_holder.graph  # For mypy attribute checking
        column_nodes = [n for n in graph.nodes if isinstance(n, Column)]
        column_graph = graph.subgraph(column_nodes)

        target_columns = {column for column, deg in column_graph.out_degree if deg == 0}

        result: Set[str] = set()
        for column in target_columns:
            # Let's drop all the count(*) and similard columns which are expression actually if it does not have an alias
            if not any(ele in column.raw_name for ele in ["*", "(", ")"]):
                result.add(str(column.raw_name))

        # Reverting back all the previously renamed words which confuses the parser
        result = set(["date" if c == self._DATE_SWAP_TOKEN else c for c in result])
        result = set(
            [
                "timestamp" if c == self._TIMESTAMP_SWAP_TOKEN else c
                for c in list(result)
            ]
        )
        # swap back renamed date column
        return list(result)
