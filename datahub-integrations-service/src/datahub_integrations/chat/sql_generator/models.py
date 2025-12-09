"""
Pydantic models for semantic model representation.

These models represent the semantic understanding of database tables and their
relationships, used to generate SQL queries from natural language.

Ported and adapted from datahub-semantic-agent/src/datahub_semantic_agent/semantic_model.py
"""

from typing import Any, List, Literal, Optional

from pydantic import BaseModel, Field


class BaseTable(BaseModel):
    """
    Represents the physical table location in the database.

    Attributes:
        database: The database name.
        schema_name: The schema name within the database.
        table: The table name.
    """

    database: str
    schema_name: str = Field(serialization_alias="schema")
    table: str


class PrimaryKey(BaseModel):
    """
    Represents the primary key of a table.

    Attributes:
        columns: List of column names that form the primary key.
    """

    columns: List[str]


class Dimension(BaseModel):
    """
    Represents a dimension column (categorical/descriptive data).

    Dimensions are typically used for grouping, filtering, and labeling.
    Examples: customer_name, product_category, region.

    Attributes:
        name: Column name.
        expr: SQL expression to reference the column.
        data_type: Native data type of the column.
        synonyms: Alternative names for this column.
        description: Human-readable description.
        unique: Whether values are unique.
        sample_values: Example values from the column.
        is_enum: Whether this is an enumeration type.
    """

    name: str
    expr: str
    data_type: str
    synonyms: Optional[List[str]] = None
    description: Optional[str] = None
    unique: Optional[bool] = None
    sample_values: Optional[List[Any]] = None
    is_enum: Optional[bool] = None


class TimeDimension(BaseModel):
    """
    Represents a time-based dimension column.

    Time dimensions are special dimensions used for time-based analysis.
    Examples: created_at, order_date, updated_timestamp.

    Attributes:
        name: Column name.
        expr: SQL expression to reference the column.
        data_type: Native data type of the column.
        synonyms: Alternative names for this column.
        description: Human-readable description.
        unique: Whether values are unique.
        sample_values: Example values from the column.
    """

    name: str
    expr: str
    data_type: str
    synonyms: Optional[List[str]] = None
    description: Optional[str] = None
    unique: Optional[bool] = None
    sample_values: Optional[List[Any]] = None


class Fact(BaseModel):
    """
    Represents a fact/measure column (numeric/quantitative data).

    Facts are typically used for aggregations and calculations.
    Examples: order_amount, quantity, revenue.

    Attributes:
        name: Column name.
        expr: SQL expression to reference the column.
        data_type: Native data type of the column.
        synonyms: Alternative names for this column.
        description: Human-readable description.
        unique: Whether values are unique.
        sample_values: Example values from the column.
    """

    name: str
    expr: str
    data_type: str
    synonyms: Optional[List[str]] = None
    description: Optional[str] = None
    unique: Optional[bool] = None
    sample_values: Optional[List[Any]] = None


class Filter(BaseModel):
    """
    Represents a predefined filter expression.

    Filters are commonly used WHERE clause conditions.
    Examples: "is_active = true", "status = 'completed'".

    Attributes:
        name: Filter name.
        expr: SQL expression for the filter.
        synonyms: Alternative names for this filter.
        description: Human-readable description.
    """

    name: str
    expr: str
    synonyms: Optional[List[str]] = None
    description: Optional[str] = None


class Metric(BaseModel):
    """
    Represents a derived metric/calculation.

    Metrics are computed values based on facts.
    Examples: total_revenue, average_order_value.

    Attributes:
        name: Metric name.
        expr: SQL expression for the metric.
        data_type: Resulting data type.
        synonyms: Alternative names for this metric.
        description: Human-readable description.
        sample_values: Example computed values.
    """

    name: str
    expr: str
    data_type: str
    synonyms: Optional[List[str]] = None
    description: Optional[str] = None
    sample_values: Optional[List[Any]] = None


class RelationshipColumn(BaseModel):
    """
    Represents a column pair used in a relationship join.

    Attributes:
        left_column: Column name from the left table.
        right_column: Column name from the right table.
    """

    left_column: str
    right_column: str


class Relationship(BaseModel):
    """
    Represents a relationship between two tables.

    Describes how tables should be joined together.

    Attributes:
        name: Descriptive name for the relationship.
        left_table: Name of the left table in the join.
        right_table: Name of the right table in the join.
        relationship_columns: Column pairs used for joining.
        join_type: Type of SQL join (left_outer or inner).
        relationship_type: Cardinality (many_to_one or one_to_one).
    """

    name: str
    left_table: str
    right_table: str
    relationship_columns: List[RelationshipColumn]
    join_type: Literal["left_outer", "inner"]
    relationship_type: Literal["many_to_one", "one_to_one"]


class LogicalTable(BaseModel):
    """
    Represents a logical table with semantic annotations.

    A logical table combines physical table information with semantic
    understanding of columns (dimensions, facts, time dimensions).

    Attributes:
        name: Display name for the table.
        description: Human-readable description.
        base_table: Physical table location.
        primary_key: Primary key columns.
        dimensions: Categorical/descriptive columns.
        time_dimensions: Time-based columns.
        facts: Numeric/measure columns.
        metrics: Derived calculations.
        filters: Predefined filter expressions.
    """

    name: str
    description: Optional[str] = None
    base_table: BaseTable
    primary_key: Optional[PrimaryKey] = None
    dimensions: Optional[List[Dimension]] = None
    time_dimensions: Optional[List[TimeDimension]] = None
    facts: Optional[List[Fact]] = None
    metrics: Optional[List[Metric]] = None
    filters: Optional[List[Filter]] = None


class SemanticModel(BaseModel):
    """
    Complete semantic model containing tables and relationships.

    This is the top-level container that holds all semantic information
    needed for SQL generation.

    Attributes:
        name: Name of the semantic model.
        description: Human-readable description.
        tables: List of logical tables.
        relationships: List of relationships between tables.
    """

    name: str
    description: Optional[str] = None
    tables: List[LogicalTable]
    relationships: Optional[List[Relationship]] = None


# Response models for the generate_sql tool


class SemanticModelSummary(BaseModel):
    """
    Summary of the semantic model used for SQL generation.

    Attributes:
        tables_analyzed: Number of tables analyzed.
        relationships_found: Number of relationships discovered.
        query_patterns_analyzed: Number of historical queries analyzed.
    """

    tables_analyzed: int
    relationships_found: int
    query_patterns_analyzed: int


class GenerateSqlResponse(BaseModel):
    """
    Response from the generate_sql tool.

    Attributes:
        sql: The generated SQL query.
        explanation: Human-readable explanation of the query.
        platform: Target SQL platform/dialect.
        confidence: Confidence level in the generated SQL.
        tables_used: URNs of tables used in the query.
        assumptions: Assumptions made during generation.
        ambiguities: Ambiguities encountered that could affect results.
        suggested_clarifications: Questions to ask the user for better results.
        semantic_model_summary: Summary of the semantic model used.
    """

    sql: str
    explanation: str
    platform: str
    confidence: Literal["high", "medium", "low"]
    tables_used: List[str]
    assumptions: List[str] = Field(default_factory=list)
    ambiguities: List[str] = Field(default_factory=list)
    suggested_clarifications: List[str] = Field(default_factory=list)
    semantic_model_summary: SemanticModelSummary
