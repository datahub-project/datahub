"""Unit tests for sql_generator models."""

from datahub_integrations.chat.sql_generator.models import (
    BaseTable,
    Dimension,
    Fact,
    GenerateSqlResponse,
    LogicalTable,
    PrimaryKey,
    Relationship,
    RelationshipColumn,
    SemanticModel,
    SemanticModelSummary,
    TimeDimension,
)


class TestBaseTable:
    """Tests for BaseTable model."""

    def test_create_base_table(self) -> None:
        """Test creating a BaseTable."""
        table = BaseTable(
            database="mydb",
            schema_name="myschema",
            table="mytable",
        )
        assert table.database == "mydb"
        assert table.schema_name == "myschema"
        assert table.table == "mytable"

    def test_serialization_uses_alias(self) -> None:
        """Test that schema_name serializes as 'schema'."""
        table = BaseTable(
            database="db",
            schema_name="schema",
            table="table",
        )
        dumped = table.model_dump(by_alias=True)
        assert "schema" in dumped
        assert dumped["schema"] == "schema"


class TestDimension:
    """Tests for Dimension model."""

    def test_create_dimension(self) -> None:
        """Test creating a Dimension."""
        dim = Dimension(
            name="customer_name",
            expr="customer_name",
            data_type="VARCHAR",
            description="Name of the customer",
        )
        assert dim.name == "customer_name"
        assert dim.data_type == "VARCHAR"
        assert dim.description == "Name of the customer"

    def test_optional_fields(self) -> None:
        """Test that optional fields default to None."""
        dim = Dimension(
            name="status",
            expr="status",
            data_type="VARCHAR",
        )
        assert dim.synonyms is None
        assert dim.sample_values is None
        assert dim.is_enum is None


class TestTimeDimension:
    """Tests for TimeDimension model."""

    def test_create_time_dimension(self) -> None:
        """Test creating a TimeDimension."""
        td = TimeDimension(
            name="created_at",
            expr="created_at",
            data_type="TIMESTAMP",
        )
        assert td.name == "created_at"
        assert td.data_type == "TIMESTAMP"


class TestFact:
    """Tests for Fact model."""

    def test_create_fact(self) -> None:
        """Test creating a Fact."""
        fact = Fact(
            name="order_total",
            expr="order_total",
            data_type="DECIMAL(10,2)",
        )
        assert fact.name == "order_total"
        assert fact.data_type == "DECIMAL(10,2)"


class TestRelationship:
    """Tests for Relationship model."""

    def test_create_relationship(self) -> None:
        """Test creating a Relationship."""
        rel = Relationship(
            name="orders -> customers",
            left_table="orders",
            right_table="customers",
            relationship_columns=[
                RelationshipColumn(
                    left_column="customer_id",
                    right_column="id",
                )
            ],
            join_type="left_outer",
            relationship_type="many_to_one",
        )
        assert rel.left_table == "orders"
        assert rel.right_table == "customers"
        assert len(rel.relationship_columns) == 1
        assert rel.join_type == "left_outer"
        assert rel.relationship_type == "many_to_one"


class TestLogicalTable:
    """Tests for LogicalTable model."""

    def test_create_logical_table(self) -> None:
        """Test creating a LogicalTable."""
        table = LogicalTable(
            name="orders",
            description="Order transactions",
            base_table=BaseTable(
                database="prod",
                schema_name="sales",
                table="orders",
            ),
            primary_key=PrimaryKey(columns=["order_id"]),
            dimensions=[
                Dimension(
                    name="status",
                    expr="status",
                    data_type="VARCHAR",
                )
            ],
            facts=[
                Fact(
                    name="total",
                    expr="total",
                    data_type="DECIMAL",
                )
            ],
        )
        assert table.name == "orders"
        assert table.base_table.database == "prod"
        assert table.primary_key is not None
        assert len(table.primary_key.columns) == 1
        assert len(table.dimensions or []) == 1
        assert len(table.facts or []) == 1


class TestSemanticModel:
    """Tests for SemanticModel model."""

    def test_create_semantic_model(self) -> None:
        """Test creating a SemanticModel."""
        model = SemanticModel(
            name="sales_model",
            description="Sales semantic model",
            tables=[
                LogicalTable(
                    name="orders",
                    base_table=BaseTable(
                        database="prod",
                        schema_name="sales",
                        table="orders",
                    ),
                )
            ],
        )
        assert model.name == "sales_model"
        assert len(model.tables) == 1
        assert model.relationships is None


class TestGenerateSqlResponse:
    """Tests for GenerateSqlResponse model."""

    def test_create_response(self) -> None:
        """Test creating a GenerateSqlResponse."""
        response = GenerateSqlResponse(
            sql="SELECT * FROM orders",
            explanation="Gets all orders",
            platform="snowflake",
            confidence="high",
            tables_used=["urn:li:dataset:(urn:li:dataPlatform:snowflake,orders,PROD)"],
            assumptions=["Assumed last 30 days"],
            ambiguities=[],
            suggested_clarifications=[],
            semantic_model_summary=SemanticModelSummary(
                tables_analyzed=1,
                relationships_found=0,
                query_patterns_analyzed=5,
            ),
        )
        assert response.sql == "SELECT * FROM orders"
        assert response.confidence == "high"
        assert len(response.assumptions) == 1

    def test_default_lists(self) -> None:
        """Test that list fields default to empty."""
        response = GenerateSqlResponse(
            sql="SELECT 1",
            explanation="Test",
            platform="generic",
            confidence="medium",
            tables_used=[],
            semantic_model_summary=SemanticModelSummary(
                tables_analyzed=0,
                relationships_found=0,
                query_patterns_analyzed=0,
            ),
        )
        assert response.assumptions == []
        assert response.ambiguities == []
        assert response.suggested_clarifications == []
