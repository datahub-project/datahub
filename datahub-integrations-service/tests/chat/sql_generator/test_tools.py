"""Unit tests for sql_generator tools."""

from unittest.mock import Mock, patch

import pytest

from datahub_integrations.chat.sql_generator.models import (
    BaseTable,
    LogicalTable,
    SemanticModel,
)
from datahub_integrations.chat.sql_generator.tools import (
    _extract_tool_response,
    _get_platform_dialect_hints,
    _validate_sql_safety,
    generate_sql,
)


class TestValidateSqlSafety:
    """Tests for SQL safety validation."""

    def test_allows_select_query(self) -> None:
        """Test that SELECT queries are allowed."""
        sql = "SELECT customer_id, SUM(order_total) FROM orders GROUP BY customer_id"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) == 0

    def test_blocks_insert(self) -> None:
        """Test that INSERT is blocked."""
        sql = "INSERT INTO orders VALUES (1, 'test')"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0
        assert any("INSERT" in w for w in warnings)

    def test_blocks_update(self) -> None:
        """Test that UPDATE is blocked."""
        sql = "UPDATE orders SET status = 'cancelled'"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_blocks_delete(self) -> None:
        """Test that DELETE is blocked."""
        sql = "DELETE FROM orders WHERE id = 1"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_blocks_drop_table(self) -> None:
        """Test that DROP TABLE is blocked."""
        sql = "DROP TABLE orders"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_blocks_create_table(self) -> None:
        """Test that CREATE TABLE is blocked."""
        sql = "CREATE TABLE new_orders (id INT)"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_blocks_alter_table(self) -> None:
        """Test that ALTER TABLE is blocked."""
        sql = "ALTER TABLE orders ADD COLUMN new_col VARCHAR(100)"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_blocks_truncate(self) -> None:
        """Test that TRUNCATE is blocked."""
        sql = "TRUNCATE TABLE orders"
        warnings = _validate_sql_safety(sql)
        assert len(warnings) > 0

    def test_allows_cte_with_select(self) -> None:
        """Test that CTEs with SELECT are allowed."""
        sql = """
        WITH order_totals AS (
            SELECT customer_id, SUM(total) as total
            FROM orders
            GROUP BY customer_id
        )
        SELECT * FROM order_totals WHERE total > 100
        """
        warnings = _validate_sql_safety(sql)
        assert len(warnings) == 0


class TestGetPlatformDialectHints:
    """Tests for platform dialect hints."""

    def test_snowflake_hints(self) -> None:
        """Test Snowflake hints are returned."""
        hints = _get_platform_dialect_hints("snowflake")
        assert "Snowflake" in hints
        assert "ILIKE" in hints

    def test_bigquery_hints(self) -> None:
        """Test BigQuery hints are returned."""
        hints = _get_platform_dialect_hints("bigquery")
        assert "BigQuery" in hints
        assert "backticks" in hints.lower()

    def test_postgres_hints(self) -> None:
        """Test PostgreSQL hints are returned."""
        hints = _get_platform_dialect_hints("postgres")
        assert "PostgreSQL" in hints

    def test_generic_hints(self) -> None:
        """Test generic hints are returned for unknown platforms."""
        hints = _get_platform_dialect_hints("generic")
        assert "standard sql" in hints.lower()

    def test_unknown_platform_returns_generic(self) -> None:
        """Test unknown platform returns generic hints."""
        hints = _get_platform_dialect_hints("unknown_db")
        assert "standard sql" in hints.lower()


class TestExtractToolResponse:
    """Tests for extracting tool response from LLM output."""

    def test_extracts_tool_use_block(self) -> None:
        """Test extracting response from tool use block."""
        response = {
            "output": {
                "message": {
                    "content": [
                        {
                            "toolUse": {
                                "name": "test_tool",
                                "input": {"key": "value"},
                            }
                        }
                    ]
                }
            }
        }
        result = _extract_tool_response(response)
        assert result == {"key": "value"}

    def test_raises_error_when_no_tool_use(self) -> None:
        """Test raises error when no tool use block found."""
        response = {
            "output": {"message": {"content": [{"text": "Some text response"}]}}
        }
        # Text response that's not JSON should raise
        with pytest.raises(ValueError, match="No tool use block found"):
            _extract_tool_response(response)


class TestGenerateSql:
    """Tests for generate_sql function."""

    @patch("datahub_integrations.chat.sql_generator.tools.get_datahub_client")
    @patch("datahub_integrations.chat.sql_generator.tools.SemanticModelBuilder")
    @patch("datahub_integrations.chat.sql_generator.tools._generate_sql_from_context")
    def test_generates_sql_successfully(
        self, mock_generate, mock_builder_class, mock_get_client
    ) -> None:
        """Test successful SQL generation."""
        # Setup mocks
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_builder = Mock()
        mock_builder._joins = {}
        mock_builder.build.return_value = SemanticModel(
            name="test",
            tables=[
                LogicalTable(
                    name="orders",
                    base_table=BaseTable(
                        database="prod", schema_name="sales", table="orders"
                    ),
                )
            ],
        )
        mock_builder_class.return_value = mock_builder

        mock_generate.return_value = {
            "sql": "SELECT * FROM orders",
            "explanation": "Gets all orders",
            "confidence": "high",
            "assumptions": [],
            "ambiguities": [],
            "suggested_clarifications": [],
        }

        # Call generate_sql
        result = generate_sql(
            natural_language_query="Show me all orders",
            table_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
            ],
            platform="snowflake",
        )

        # Verify result
        assert result["sql"] == "SELECT * FROM orders"
        assert result["confidence"] == "high"
        assert result["platform"] == "snowflake"
        assert "semantic_model_summary" in result

    def test_raises_error_for_invalid_urn(self) -> None:
        """Test raises error for invalid URN."""
        with pytest.raises(ValueError, match="Invalid URN"):
            generate_sql(
                natural_language_query="Show me data",
                table_urns=["not-a-valid-urn"],
                platform="generic",
            )

    def test_raises_error_for_empty_urns(self) -> None:
        """Test raises error when no URNs provided."""
        with pytest.raises(ValueError, match="At least one table URN"):
            generate_sql(
                natural_language_query="Show me data",
                table_urns=[],
                platform="generic",
            )

    def test_raises_error_for_non_dataset_urn(self) -> None:
        """Test raises error for non-dataset URN."""
        with pytest.raises(ValueError, match="not a dataset URN"):
            generate_sql(
                natural_language_query="Show me data",
                table_urns=["urn:li:dashboard:(looker,dashboard123)"],
                platform="generic",
            )

    def test_raises_error_for_non_sql_platform(self) -> None:
        """Test raises error for non-SQL platforms like elasticsearch."""
        with pytest.raises(
            ValueError, match="SQL generation is only supported for SQL databases"
        ):
            generate_sql(
                natural_language_query="Show me data",
                table_urns=[
                    "urn:li:dataset:(urn:li:dataPlatform:elasticsearch,my_index,PROD)"
                ],
                platform="generic",
            )

    def test_raises_error_for_kafka_platform(self) -> None:
        """Test raises error for Kafka (streaming, not SQL)."""
        with pytest.raises(
            ValueError, match="SQL generation is only supported for SQL databases"
        ):
            generate_sql(
                natural_language_query="Show me data",
                table_urns=["urn:li:dataset:(urn:li:dataPlatform:kafka,my_topic,PROD)"],
                platform="generic",
            )

    @patch("datahub_integrations.chat.sql_generator.tools.get_datahub_client")
    @patch("datahub_integrations.chat.sql_generator.tools.SemanticModelBuilder")
    @patch("datahub_integrations.chat.sql_generator.tools._generate_sql_from_context")
    def test_detects_unsafe_sql(
        self, mock_generate, mock_builder_class, mock_get_client
    ) -> None:
        """Test that unsafe SQL is flagged."""
        # Setup mocks
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_builder = Mock()
        mock_builder._joins = {}
        mock_builder.build.return_value = SemanticModel(
            name="test",
            tables=[
                LogicalTable(
                    name="orders",
                    base_table=BaseTable(
                        database="prod", schema_name="sales", table="orders"
                    ),
                )
            ],
        )
        mock_builder_class.return_value = mock_builder

        # LLM returns dangerous SQL (shouldn't happen, but test safety check)
        mock_generate.return_value = {
            "sql": "DROP TABLE orders",
            "explanation": "Dangerous!",
            "confidence": "high",
            "assumptions": [],
            "ambiguities": [],
            "suggested_clarifications": [],
        }

        result = generate_sql(
            natural_language_query="Delete all orders",
            table_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
            ],
            platform="snowflake",
        )

        # Confidence should be lowered and ambiguity added
        assert result["confidence"] == "low"
        assert len(result["ambiguities"]) > 0
        assert any("dangerous" in a.lower() for a in result["ambiguities"])

    @patch("datahub_integrations.chat.sql_generator.tools.get_datahub_client")
    @patch("datahub_integrations.chat.sql_generator.tools.SemanticModelBuilder")
    @patch("datahub_integrations.chat.sql_generator.tools._generate_sql_from_context")
    def test_includes_additional_context(
        self, mock_generate, mock_builder_class, mock_get_client
    ) -> None:
        """Test that additional context is passed to generation."""
        # Setup mocks
        mock_client = Mock()
        mock_get_client.return_value = mock_client

        mock_builder = Mock()
        mock_builder._joins = {}
        mock_builder.build.return_value = SemanticModel(
            name="test",
            tables=[
                LogicalTable(
                    name="orders",
                    base_table=BaseTable(
                        database="prod", schema_name="sales", table="orders"
                    ),
                )
            ],
        )
        mock_builder_class.return_value = mock_builder

        mock_generate.return_value = {
            "sql": "SELECT * FROM orders WHERE date > '2024-01-01'",
            "explanation": "Orders after Jan 1",
            "confidence": "high",
            "assumptions": ["Used provided date filter"],
            "ambiguities": [],
            "suggested_clarifications": [],
        }

        generate_sql(
            natural_language_query="Show recent orders",
            table_urns=[
                "urn:li:dataset:(urn:li:dataPlatform:snowflake,prod.sales.orders,PROD)"
            ],
            platform="snowflake",
            additional_context={"date_filter": "2024-01-01"},
        )

        # Verify context was passed
        mock_generate.assert_called_once()
        call_args = mock_generate.call_args
        assert call_args[1]["additional_context"] == {"date_filter": "2024-01-01"}
