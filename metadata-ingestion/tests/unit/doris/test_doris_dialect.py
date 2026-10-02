"""Unit tests for Doris SQLAlchemy dialect."""

from typing import Any, Callable, Dict, List, Optional, Sequence
from unittest.mock import Mock, patch

import pytest
from sqlalchemy.dialects.mysql.base import MySQLDialect
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.sql import sqltypes

from datahub.ingestion.source.sql.doris.doris_dialect import (
    AGG_STATE,
    BITMAP,
    DORIS_ARRAY,
    DORIS_JSONB,
    DORIS_MAP,
    DORIS_STRUCT,
    HLL,
    IPV4,
    IPV6,
    LARGEINT,
    QUANTILE_STATE,
    VARIANT,
    DorisDialect,
    ReflectionFallback,
    _doris_type_map,
    _parse_doris_type,
)


def _execute_by_query(
    describe_rows: Sequence[Sequence[Any]] = (),
    column_comment_rows: Sequence[Sequence[Any]] = (),
    table_comment_rows: Sequence[Sequence[Any]] = (),
) -> Callable[..., List[Sequence[Any]]]:
    """Answer each statement the dialect issues with its own rows. Requires `text`
    to be patched to pass the SQL string through unchanged."""

    def execute(
        statement: str, params: Optional[Dict[str, Any]] = None
    ) -> List[Sequence[Any]]:
        if statement.startswith("DESCRIBE"):
            return list(describe_rows)
        if "information_schema.COLUMNS" in statement:
            return list(column_comment_rows)
        if "information_schema.TABLES" in statement:
            return list(table_comment_rows)
        raise AssertionError(f"Unexpected statement: {statement}")

    return execute


def _count_queries(mock_connection: Mock, fragment: str) -> int:
    return sum(
        1 for call in mock_connection.execute.call_args_list if fragment in call.args[0]
    )


class TestParseDorisType:
    """Test _parse_doris_type() function with various inputs."""

    @pytest.mark.parametrize(
        "type_str,expected_type",
        [
            # Known Doris types
            ("hll", HLL),
            ("HLL", HLL),
            ("  hll  ", HLL),
            ("bitmap", BITMAP),
            ("BITMAP", BITMAP),
            ("quantile_state", QUANTILE_STATE),
            ("QUANTILE_STATE", QUANTILE_STATE),
            ("agg_state", AGG_STATE),
            ("AGG_STATE", AGG_STATE),
            ("array", DORIS_ARRAY),
            ("ARRAY", DORIS_ARRAY),
            ("array<int>", DORIS_ARRAY),
            ("map", DORIS_MAP),
            ("MAP", DORIS_MAP),
            ("map<string,int>", DORIS_MAP),
            ("struct", DORIS_STRUCT),
            ("STRUCT", DORIS_STRUCT),
            ("struct<id:int,name:string>", DORIS_STRUCT),
            ("jsonb", DORIS_JSONB),
            ("JSONB", DORIS_JSONB),
        ],
    )
    def test_known_types(self, type_str, expected_type):
        """Test that known Doris types are correctly parsed."""
        result = _parse_doris_type(type_str, _doris_type_map)
        assert isinstance(result, expected_type)

    @pytest.mark.parametrize(
        "type_str",
        [
            # Unknown types should return NULLTYPE (will fall back to MySQL)
            "varchar",
            "int",
            "decimal",
            "datetime",
            "unknown_type",
            "custom_type",
        ],
    )
    def test_unknown_types_return_nulltype(self, type_str):
        """Test that unknown types return NULLTYPE for MySQL fallback."""
        result = _parse_doris_type(type_str, _doris_type_map)
        assert result is sqltypes.NULLTYPE

    @pytest.mark.parametrize(
        "type_str",
        [
            # Invalid/malformed type strings
            "",
            "   ",
            "\t\n",
            "123invalid",
            "!@#$",
        ],
    )
    def test_invalid_types_return_nulltype(self, type_str):
        """Test that invalid type strings return NULLTYPE."""
        result = _parse_doris_type(type_str, _doris_type_map)
        assert result is sqltypes.NULLTYPE

    def test_case_insensitive_parsing(self):
        """Test that type parsing is case-insensitive."""
        assert isinstance(_parse_doris_type("HLL", _doris_type_map), HLL)
        assert isinstance(_parse_doris_type("hll", _doris_type_map), HLL)
        assert isinstance(_parse_doris_type("Hll", _doris_type_map), HLL)
        assert isinstance(_parse_doris_type("hLL", _doris_type_map), HLL)

    def test_whitespace_handling(self):
        """Test that leading/trailing whitespace is handled correctly."""
        assert isinstance(_parse_doris_type("  bitmap  ", _doris_type_map), BITMAP)
        assert isinstance(_parse_doris_type("\tarray\t", _doris_type_map), DORIS_ARRAY)
        assert isinstance(_parse_doris_type("\njsonb\n", _doris_type_map), DORIS_JSONB)


class TestDorisDialect:
    """Test DorisDialect class."""

    def test_dialect_initialization(self):
        """Test that DorisDialect initializes with correct type mappings."""
        dialect = DorisDialect()

        assert dialect.name == "doris"
        assert dialect.supports_statement_cache is False

        # Verify custom types are registered
        assert "hll" in dialect.ischema_names
        assert "bitmap" in dialect.ischema_names
        assert "quantile_state" in dialect.ischema_names
        assert "agg_state" in dialect.ischema_names
        assert "array" in dialect.ischema_names
        assert "map" in dialect.ischema_names
        assert "struct" in dialect.ischema_names
        assert "jsonb" in dialect.ischema_names

        assert dialect.ischema_names["hll"] == HLL
        assert dialect.ischema_names["bitmap"] == BITMAP
        assert dialect.ischema_names["array"] == DORIS_ARRAY
        assert dialect.ischema_names["jsonb"] == DORIS_JSONB

    @patch("datahub.ingestion.source.sql.doris.doris_dialect.text")
    def test_get_columns_success(self, mock_text):
        """Test get_columns() successfully fetches and parses DESCRIBE output."""
        dialect = DorisDialect()

        # Mock connection and result
        mock_connection = Mock()
        mock_connection.engine.url.database = "testdb"

        # Mock DESCRIBE result
        mock_describe_result = [
            ("customer_id", "INT", "NO", "", "0", ""),
            ("customer_name", "VARCHAR(100)", "YES", "", None, ""),
            ("tags", "ARRAY<VARCHAR(50)>", "YES", "", None, ""),
            ("metrics", "JSONB", "YES", "", None, ""),
            ("sketch", "HLL", "YES", "", None, ""),
        ]
        mock_connection.execute.return_value = mock_describe_result

        # Mock parent get_columns result (MySQL fallback)
        with patch.object(
            dialect.__class__.__bases__[0],
            "get_columns",
            return_value=[
                {"name": "customer_id", "type": sqltypes.INTEGER()},
                {"name": "customer_name", "type": sqltypes.VARCHAR(100)},
                {"name": "tags", "type": sqltypes.TEXT()},
                {"name": "metrics", "type": sqltypes.JSON()},
                {"name": "sketch", "type": sqltypes.BINARY()},
            ],
        ):
            columns = dialect.get_columns(mock_connection, "customers", schema="testdb")

        # Verify full_type was set. "full_type" is a non-standard key the Doris
        # dialect adds to ReflectedColumn for SQLAlchemySource, so mypy rejects it.
        assert columns[0]["full_type"] == "INT"  # type: ignore[typeddict-item]
        assert columns[2]["full_type"] == "ARRAY<VARCHAR(50)>"  # type: ignore[typeddict-item]
        assert columns[3]["full_type"] == "JSONB"  # type: ignore[typeddict-item]
        assert columns[4]["full_type"] == "HLL"  # type: ignore[typeddict-item]

        # Verify custom types were parsed
        assert isinstance(columns[2]["type"], DORIS_ARRAY)
        assert isinstance(columns[3]["type"], DORIS_JSONB)
        assert isinstance(columns[4]["type"], HLL)

    def test_get_columns_records_overlay_failure_for_the_report(self):
        """A failed DESCRIBE overlay keeps MySQL's columns but silently downgrades
        Doris-specific types, so it has to reach the report, not just the log."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "testdb"
        mock_connection.execute.side_effect = SQLAlchemyError("Connection lost")

        with patch.object(
            dialect.__class__.__bases__[0],
            "get_columns",
            return_value=[{"name": "col1", "type": sqltypes.INTEGER()}],
        ):
            columns = dialect.get_columns(
                mock_connection, "test_table", schema="testdb"
            )

        # Should fall back to MySQL reflection rather than losing the table.
        assert len(columns) == 1
        assert columns[0]["name"] == "col1"

        assert list(dialect.type_overlay_failures) == ["`testdb`.`test_table`"]
        assert (
            "Connection lost" in dialect.type_overlay_failures["`testdb`.`test_table`"]
        )

    def test_get_columns_does_not_swallow_unexpected_errors(self):
        """The overlay catch is scoped to failures DESCRIBE can actually produce, so a
        MemoryError surfaces instead of being downgraded to a type warning."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "testdb"
        mock_connection.execute.side_effect = MemoryError("out of memory")

        with patch.object(
            dialect.__class__.__bases__[0],
            "get_columns",
            return_value=[{"name": "col1", "type": sqltypes.INTEGER()}],
        ):
            with pytest.raises(MemoryError):
                dialect.get_columns(mock_connection, "test_table", schema="testdb")

        assert dialect.type_overlay_failures == {}

    def test_get_columns_no_schema(self):
        """Test get_columns() returns MySQL columns when no schema available."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = None

        with patch.object(
            dialect.__class__.__bases__[0],
            "get_columns",
            return_value=[{"name": "col1", "type": sqltypes.INTEGER()}],
        ):
            columns = dialect.get_columns(mock_connection, "test_table")

        # Should return parent result without executing DESCRIBE
        assert len(columns) == 1
        mock_connection.execute.assert_not_called()

    @patch("datahub.ingestion.source.sql.doris.doris_dialect.text")
    def test_reflection_survives_async_materialized_view(self, mock_text):
        """Doris rejects SHOW CREATE TABLE for async MVs.

        Every reflection method sql_common's _process_table calls reads the state
        parsed from that statement, so all of them have to degrade together —
        surviving get_columns alone still loses the table.
        """
        dialect = DorisDialect()
        # Normally set by dialect.initialize() against a live server.
        dialect._needs_correct_for_88718_96365 = False  # type: ignore[attr-defined]

        mock_text.side_effect = lambda sql: sql
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.side_effect = _execute_by_query(
            describe_rows=[
                ("col_a", "LARGEINT", "NO", "true", None, ""),
                ("col_b", "DECIMALV3(20,6)", "YES", "false", None, ""),
                ("col_c", "VARIANT", "YES", "false", None, ""),
            ],
            column_comment_rows=[
                ("my_async_mv", "col_a", "Primary identifier"),
                ("my_async_mv", "col_b", ""),
            ],
            table_comment_rows=[("my_async_mv", "Hourly rollup")],
        )

        # The Inspector shares one info_cache across a table's reflection calls.
        kw: Dict[str, Any] = {"schema": "my_db", "info_cache": {}}
        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=SQLAlchemyError(
                "not support async materialized view, please use "
                "`show create materialized view`"
            ),
        ):
            columns = dialect.get_columns(mock_connection, "my_async_mv", **kw)
            pk_constraint = dialect.get_pk_constraint(
                mock_connection, "my_async_mv", **kw
            )
            foreign_keys = dialect.get_foreign_keys(
                mock_connection, "my_async_mv", **kw
            )
            table_comment = dialect.get_table_comment(
                mock_connection, "my_async_mv", **kw
            )
            indexes = dialect.get_indexes(mock_connection, "my_async_mv", **kw)

        assert [col["name"] for col in columns] == ["col_a", "col_b", "col_c"]
        assert isinstance(columns[0]["type"], LARGEINT)
        assert isinstance(columns[1]["type"], sqltypes.DECIMAL)
        assert isinstance(columns[2]["type"], VARIANT)
        assert columns[0]["nullable"] is False
        assert columns[1]["nullable"] is True
        assert columns[1]["full_type"] == "DECIMALV3(20,6)"  # type: ignore[typeddict-item]

        # DESCRIBE carries no comments, so they come from information_schema.
        assert columns[0]["comment"] == "Primary identifier"
        assert columns[1]["comment"] is None
        assert table_comment["text"] == "Hourly rollup"

        # No DDL to parse, so these degrade to empty rather than raising.
        assert pk_constraint == {"constrained_columns": [], "name": None}
        assert foreign_keys == []
        assert indexes == []

        # One DESCRIBE for the whole table: the fallback state is cached for the four
        # later calls, and get_columns skips its type overlay on a fallback table.
        assert _count_queries(mock_connection, "DESCRIBE") == 1

        # The source drains this into the ingestion report, so the degraded table is
        # visible to operators rather than only in the logs.
        assert list(dialect.reflection_fallbacks) == ["`my_db`.`my_async_mv`"]
        fallback = dialect.reflection_fallbacks["`my_db`.`my_async_mv`"]
        assert "async materialized view" in fallback.error
        assert fallback.expected is True

    def test_reflection_does_not_degrade_on_unexpected_error(self):
        """Only Doris' two known refusals fall back; other errors stay fatal."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"

        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=MemoryError("out of memory"),
        ):
            with pytest.raises(MemoryError):
                dialect.get_columns(
                    mock_connection, "my_table", schema="my_db", info_cache={}
                )

        assert dialect.reflection_fallbacks == {}

    def test_registering_types_does_not_leak_into_other_mysql_dialects(self):
        """ischema_names is a class attribute shared by every MySQL-family dialect, so
        updating it in place would teach MySQL/MariaDB/TiDB about Doris types."""
        DorisDialect()

        assert "largeint" not in MySQLDialect.ischema_names
        assert "variant" not in MySQLDialect.ischema_names
        assert "hll" not in MySQLDialect_pymysql().ischema_names

    def test_fallback_not_recorded_when_describe_also_fails(self):
        """A table whose DESCRIBE fails too is dropped by the caller, so it must not
        also be reported as reflected-but-degraded."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.side_effect = SQLAlchemyError("SELECT command denied")

        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=SQLAlchemyError("not support async materialized view"),
        ):
            with pytest.raises(SQLAlchemyError):
                dialect.get_columns(
                    mock_connection, "my_async_mv", schema="my_db", info_cache={}
                )

        assert dialect.reflection_fallbacks == {}

    def test_unexpected_reflection_error_is_flagged_as_unexpected(self):
        """A missing grant degrades the same way an async MV does, but must not be
        reported under the same benign heading."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.return_value = [
            ("col_a", "INT", "NO", "true", None, "")
        ]

        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=SQLAlchemyError("SHOW command denied to user 'svc'"),
        ):
            columns = dialect.get_columns(
                mock_connection, "my_table", schema="my_db", info_cache={}
            )

        assert [col["name"] for col in columns] == ["col_a"]
        assert dialect.reflection_fallbacks["`my_db`.`my_table`"].expected is False

    def test_type_error_from_ddl_parser_falls_back(self):
        """The MySQL DDL parser raises TypeError (not SQLAlchemyError) building
        NullType(*args) for a type it cannot model, so that branch must degrade too."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.return_value = [
            ("col_a", "LARGEINT", "NO", "true", None, "")
        ]

        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=TypeError("NullType() takes no arguments"),
        ):
            columns = dialect.get_columns(
                mock_connection, "my_table", schema="my_db", info_cache={}
            )

        assert isinstance(columns[0]["type"], LARGEINT)
        assert dialect.reflection_fallbacks["`my_db`.`my_table`"].expected is True

    def test_unrelated_type_error_is_not_classified_as_expected(self):
        """The NullType anchor keeps an unrelated 'takes no arguments' TypeError from
        being waved through under the benign heading."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.return_value = [
            ("col_a", "INT", "NO", "true", None, "")
        ]

        with patch.object(
            dialect.__class__.__bases__[0],
            "_setup_parser",
            side_effect=TypeError("SomeOtherThing() takes no arguments"),
        ):
            dialect.get_columns(
                mock_connection, "my_table", schema="my_db", info_cache={}
            )

        assert dialect.reflection_fallbacks["`my_db`.`my_table`"].expected is False

    def test_pop_reflection_fallbacks_drains(self):
        """Draining has to empty the dialect, or a second database re-reports the
        first one's tables."""
        dialect = DorisDialect()
        dialect.reflection_fallbacks["`db`.`t`"] = ReflectionFallback(
            error="boom", expected=True
        )
        dialect.type_overlay_failures["`db`.`u`"] = "bang"

        assert list(dialect.pop_reflection_fallbacks()) == ["`db`.`t`"]
        assert list(dialect.pop_type_overlay_failures()) == ["`db`.`u`"]
        assert dialect.reflection_fallbacks == {}
        assert dialect.type_overlay_failures == {}
        assert dialect.pop_reflection_fallbacks() == {}

    @pytest.mark.parametrize(
        "type_str,expected_type",
        [
            ("ipv4", IPV4),
            ("ipv6", IPV6),
            ("largeint(40)", LARGEINT),
            ("variant", VARIANT),
        ],
    )
    def test_describe_columns_builds_doris_only_types(self, type_str, expected_type):
        """DESCRIBE is the only type source on a fallback table, so these have to
        instantiate end to end, not merely be registered."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.execute.return_value = [
            ("col_a", type_str, "YES", "", None, "")
        ]

        columns = dialect._describe_columns(mock_connection, "`db`.`t`")

        assert isinstance(columns[0]["type"], expected_type)
        assert columns[0]["full_type"] == type_str

    def test_describe_row_tolerates_short_rows(self):
        """Doris external catalogs do not always return the internal catalog's full
        six-column DESCRIBE shape."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.execute.return_value = [("col_a", "INT")]

        columns = dialect._describe_columns(mock_connection, "`db`.`t`")

        assert columns[0]["name"] == "col_a"
        assert columns[0]["nullable"] is True
        assert columns[0]["default"] is None

    def test_largeint_does_not_fall_back_to_nulltype(self):
        """NullType(*args) raises TypeError, so Doris-only types must be registered."""
        dialect = DorisDialect()

        for type_name in ("largeint", "variant", "ipv4", "ipv6", "string"):
            assert dialect.ischema_names[type_name] is not sqltypes.NullType

    def test_doris_types_parse_from_show_create_table(self):
        """The DDL parser builds NullType(40) for `largeint(40)` unless the Doris type
        names are registered, and NullType takes no arguments."""
        dialect = DorisDialect()

        state = dialect._tabledef_parser.parse(  # type: ignore[attr-defined]
            "CREATE TABLE `my_table` (\n"
            "  `col_a` largeint(40) NOT NULL,\n"
            "  `col_b` decimalv3(20,6) NULL,\n"
            "  `col_c` datetimev2(3) NULL,\n"
            "  `col_d` string NULL\n"
            ") ENGINE=OLAP",
            "utf8",
        )

        types = {col["name"]: col["type"] for col in state.columns}
        assert isinstance(types["col_a"], LARGEINT)
        assert isinstance(types["col_b"], sqltypes.DECIMAL)
        assert isinstance(types["col_c"], sqltypes.DATETIME)
        assert isinstance(types["col_d"], sqltypes.TEXT)

    @patch("datahub.ingestion.source.sql.doris.doris_dialect.text")
    def test_get_schema_names(self, mock_text):
        """Test get_schema_names() uses SHOW SCHEMAS."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.execute.return_value = [
            ("information_schema",),
            ("mysql",),
            ("test_db",),
            ("analytics",),
        ]

        schemas = dialect.get_schema_names(mock_connection)

        assert schemas == ["information_schema", "mysql", "test_db", "analytics"]
        mock_text.assert_called_once_with("SHOW SCHEMAS")


@patch(
    "datahub.ingestion.source.sql.doris.doris_dialect.text",
    side_effect=lambda sql: sql,
)
class TestDorisComments:
    """SHOW CREATE TABLE's comments are lost to the MySQL DDL parser, so they are
    read from information_schema instead."""

    @staticmethod
    def _mysql_columns() -> List[Dict[str, Any]]:
        # What MySQL reflection yields from Doris DDL: double-quoted, aggregate-column
        # and complex-type comments all come back as None. Only a single-quoted
        # comment on a plain column survives the parse.
        return [
            {"name": "col_a", "type": sqltypes.INTEGER(), "comment": None},
            {"name": "col_b", "type": sqltypes.TEXT(), "comment": None},
            {"name": "col_c", "type": sqltypes.TEXT(), "comment": "Parsed from DDL"},
        ]

    def _get_columns(
        self, dialect: DorisDialect, connection: Mock, table_name: str, **kw: Any
    ) -> List[Any]:
        with patch.object(
            MySQLDialect, "get_columns", return_value=self._mysql_columns()
        ):
            return dialect.get_columns(connection, table_name, **kw)

    def test_column_comments_come_from_information_schema(self, mock_text):
        dialect = DorisDialect()
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.side_effect = _execute_by_query(
            column_comment_rows=[
                ("my_table", "col_a", 'Amount in "minor" units, 金额'),
                # Doris reports a column without a comment as an empty string.
                ("my_table", "col_b", ""),
                ("other_table", "col_c", "Belongs to another table"),
            ],
        )

        columns = self._get_columns(
            dialect, mock_connection, "my_table", schema="my_db", info_cache={}
        )

        comments = {col["name"]: col["comment"] for col in columns}
        assert comments == {
            "col_a": 'Amount in "minor" units, 金额',
            "col_b": None,
            "col_c": "Parsed from DDL",
        }
        assert dialect.comment_lookup_failures == {}

    def test_comments_are_fetched_once_per_database(self, mock_text):
        """Two queries cover every table in a database, rather than two per table."""
        dialect = DorisDialect()
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.side_effect = _execute_by_query(
            column_comment_rows=[
                ("table_1", "col_a", "First"),
                ("table_2", "col_a", "Second"),
            ],
            table_comment_rows=[("table_1", "Table one"), ("table_2", "Table two")],
        )
        kw: Dict[str, Any] = {"schema": "my_db", "info_cache": {}}

        with patch.object(
            MySQLDialect, "get_table_comment", return_value={"text": None}
        ):
            first = self._get_columns(dialect, mock_connection, "table_1", **kw)
            second = self._get_columns(dialect, mock_connection, "table_2", **kw)
            first_comment = dialect.get_table_comment(mock_connection, "table_1", **kw)
            second_comment = dialect.get_table_comment(mock_connection, "table_2", **kw)

        assert first[0]["comment"] == "First"
        assert second[0]["comment"] == "Second"
        assert first_comment == {"text": "Table one"}
        assert second_comment == {"text": "Table two"}
        assert _count_queries(mock_connection, "information_schema.COLUMNS") == 1
        assert _count_queries(mock_connection, "information_schema.TABLES") == 1

    def test_table_comment_keeps_parsed_value_when_information_schema_has_none(
        self, mock_text
    ):
        dialect = DorisDialect()
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        mock_connection.execute.side_effect = _execute_by_query(
            table_comment_rows=[("my_table", "")],
        )

        with patch.object(
            MySQLDialect, "get_table_comment", return_value={"text": "Parsed"}
        ):
            table_comment = dialect.get_table_comment(
                mock_connection, "my_table", schema="my_db", info_cache={}
            )

        assert table_comment == {"text": "Parsed"}

    @pytest.mark.parametrize("table_schema", ["my_db", "my_catalog.my_db"])
    def test_external_catalog_matches_bare_and_qualified_database_name(
        self, mock_text, table_schema
    ):
        """An external catalog's information_schema rows carry the bare database
        name, or `catalog.database` when show_full_dbname_in_info_schema_db is on."""
        dialect = DorisDialect()
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_catalog.my_db"
        describe = _execute_by_query()

        # Like Doris, return a row only when its TABLE_SCHEMA is one of the bound values.
        def execute(
            statement: str, params: Optional[Dict[str, Any]] = None
        ) -> List[Sequence[Any]]:
            if "information_schema.COLUMNS" not in statement:
                return describe(statement, params)
            assert params is not None
            if table_schema not in params.values():
                return []
            return [("my_table", "col_a", "From the catalog")]

        mock_connection.execute.side_effect = execute

        columns = self._get_columns(dialect, mock_connection, "my_table")

        assert columns[0]["comment"] == "From the catalog"

    def test_failed_lookup_keeps_columns_and_is_recorded_once(self, mock_text):
        """A missing information_schema grant costs descriptions, not tables, and
        is not retried for every table in the database."""
        dialect = DorisDialect()
        mock_connection = Mock()
        mock_connection.engine.url.database = "my_db"
        describe = _execute_by_query()

        def execute(
            statement: str, params: Optional[Dict[str, Any]] = None
        ) -> List[Sequence[Any]]:
            if "information_schema" in statement:
                raise SQLAlchemyError("Access denied")
            return describe(statement, params)

        mock_connection.execute.side_effect = execute
        kw: Dict[str, Any] = {"schema": "my_db", "info_cache": {}}

        first = self._get_columns(dialect, mock_connection, "table_1", **kw)
        self._get_columns(dialect, mock_connection, "table_2", **kw)

        assert [col["name"] for col in first] == ["col_a", "col_b", "col_c"]
        assert first[2]["comment"] == "Parsed from DDL"
        assert _count_queries(mock_connection, "information_schema") == 1
        failures = dialect.pop_comment_lookup_failures()
        assert list(failures) == ["my_db"]
        assert "Access denied" in failures["my_db"]
        assert dialect.comment_lookup_failures == {}
