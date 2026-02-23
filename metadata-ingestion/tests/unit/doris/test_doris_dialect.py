"""Unit tests for Doris SQLAlchemy dialect."""

from unittest.mock import Mock, patch

import pytest
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
    QUANTILE_STATE,
    DorisDialect,
    _parse_doris_type,
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
        result = _parse_doris_type(type_str)
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
        result = _parse_doris_type(type_str)
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
        result = _parse_doris_type(type_str)
        assert result is sqltypes.NULLTYPE

    def test_case_insensitive_parsing(self):
        """Test that type parsing is case-insensitive."""
        assert isinstance(_parse_doris_type("HLL"), HLL)
        assert isinstance(_parse_doris_type("hll"), HLL)
        assert isinstance(_parse_doris_type("Hll"), HLL)
        assert isinstance(_parse_doris_type("hLL"), HLL)

    def test_whitespace_handling(self):
        """Test that leading/trailing whitespace is handled correctly."""
        assert isinstance(_parse_doris_type("  bitmap  "), BITMAP)
        assert isinstance(_parse_doris_type("\tarray\t"), DORIS_ARRAY)
        assert isinstance(_parse_doris_type("\njsonb\n"), DORIS_JSONB)


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

        # Verify full_type was set
        assert columns[0]["full_type"] == "INT"
        assert columns[2]["full_type"] == "ARRAY<VARCHAR(50)>"
        assert columns[3]["full_type"] == "JSONB"
        assert columns[4]["full_type"] == "HLL"

        # Verify custom types were parsed
        assert isinstance(columns[2]["type"], DORIS_ARRAY)
        assert isinstance(columns[3]["type"], DORIS_JSONB)
        assert isinstance(columns[4]["type"], HLL)

    @patch("datahub.ingestion.source.sql.doris.doris_dialect.logger")
    def test_get_columns_sqlalchemy_error(self, mock_logger):
        """Test get_columns() handles SQLAlchemyError gracefully."""
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

        # Should fall back to MySQL reflection
        assert len(columns) == 1
        assert columns[0]["name"] == "col1"

        # Verify debug log was created (SQLAlchemyError is expected, so debug level)
        mock_logger.debug.assert_called_once()
        assert "DESCRIBE failed" in str(mock_logger.debug.call_args)

    @patch("datahub.ingestion.source.sql.doris.doris_dialect.logger")
    def test_get_columns_unexpected_error(self, mock_logger):
        """Test get_columns() handles unexpected exceptions gracefully."""
        dialect = DorisDialect()

        mock_connection = Mock()
        mock_connection.engine.url.database = "testdb"
        mock_connection.execute.side_effect = RuntimeError("Unexpected error")

        with patch.object(
            dialect.__class__.__bases__[0],
            "get_columns",
            return_value=[{"name": "col1", "type": sqltypes.INTEGER()}],
        ):
            columns = dialect.get_columns(
                mock_connection, "test_table", schema="testdb"
            )

        # Should fall back to MySQL reflection
        assert len(columns) == 1
        assert columns[0]["name"] == "col1"

        # Verify warning was logged
        mock_logger.warning.assert_called_once()
        assert "Unexpected error in DESCRIBE" in str(mock_logger.warning.call_args)
        assert "Falling back" in str(mock_logger.warning.call_args)

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
