import logging
import re
import unittest
import unittest.mock
from typing import Dict, List, Set

from sqllineage.core.holders import Column, SQLLineageHolder

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
    _TIMESTAMP_SWAP_TOKEN = "__t_i_m_e_s_t_a_m_p"
    _DATA_SWAP_TOKEN = "__d_a_t_a"
    _MYVIEW_SQL_TABLE_NAME_TOKEN = "__my_view__.__sql_table_name__"
    _MYVIEW_LOOKER_TOKEN = "my_view.SQL_TABLE_NAME"

    def __init__(self, sql_query: str) -> None:
        original_sql_query = sql_query

        # SqlLineageParser makes mistakes on lateral flatten queries, use the prefix
        if "lateral flatten" in sql_query:
            sql_query = sql_query[: sql_query.find("lateral flatten")]

        self.token_to_original: Dict[str, str] = {
            self._DATE_SWAP_TOKEN: "date",
            self._TIMESTAMP_SWAP_TOKEN: "timestamp",
            self._DATA_SWAP_TOKEN: "data",
        }
        for replacement, original in self.token_to_original.items():
            sql_query = re.sub(
                rf"(\b{original}\b)", rf"{replacement}", sql_query, flags=re.IGNORECASE
            )

        sql_query = re.sub(r"#([^ ])", r"# \1", sql_query)

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
            logger.debug(f"rewrote original query {original_sql_query} as {sql_query}")

        self._sql = sql_query

        self._stmt = [
            s
            for s in sqlparse.parse(
                # first apply sqlparser formatting just to get rid of comments, which cause
                # inconsistencies in parsing output
                sqlparse.format(
                    self._sql.strip(),
                    strip_comments=True,
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
