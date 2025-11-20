from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator


class DataformColumn(BaseModel):
    """Represents a column in a Dataform table."""

    name: str = Field(description="Column name")
    type: str = Field(description="Column data type")
    description: Optional[str] = Field(None, description="Column description")
    tags: List[str] = Field(default_factory=list, description="Column tags")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate column name is not empty."""
        if not v or not v.strip():
            raise ValueError("Column name cannot be empty")
        return v.strip()

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate column type is not empty."""
        if not v or not v.strip():
            raise ValueError("Column type cannot be empty")
        return v.strip()


class DataformTable(BaseModel):
    """Represents a Dataform table (materialized view or table)."""

    name: str = Field(description="Table name")
    database: Optional[str] = Field(None, description="Database name")
    schema_name: str = Field(description="Schema name")
    type: str = Field(description="Table type: table, view, or incremental")
    description: Optional[str] = Field(None, description="Table description")
    columns: List[DataformColumn] = Field(
        default_factory=list, description="Table columns"
    )
    tags: List[str] = Field(default_factory=list, description="Table tags")
    dependencies: List[str] = Field(
        default_factory=list, description="Upstream dependencies"
    )
    sql_query: Optional[str] = Field(None, description="SQL query defining the table")
    materialization_type: Optional[str] = Field(
        None, description="Materialization type"
    )
    file_path: Optional[str] = Field(None, description="Source file path")
    custom_properties: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict, description="Custom properties"
    )

    # Performance and execution tracking
    execution_performances: List["DataformModelPerformance"] = Field(
        default_factory=list, description="Model execution performance metrics"
    )

    # Column-level lineage
    upstream_cll: List["DataformColumnLineageInfo"] = Field(
        default_factory=list, description="Column-level lineage information"
    )

    # Test results
    test_results: List["DataformTestResult"] = Field(
        default_factory=list, description="Test execution results"
    )

    # Compiled code
    compiled_code: Optional[str] = Field(None, description="Compiled SQL code")

    # Owner information
    owner: Optional[str] = Field(None, description="Table owner")

    # Metadata
    meta: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict, description="Metadata properties"
    )

    @field_validator("type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        """Validate table type."""
        valid_types = {"table", "view", "incremental", "snapshot"}
        if v.lower() not in valid_types:
            raise ValueError(f"Table type must be one of {valid_types}, got: {v}")
        return v.lower()

    @field_validator("name", "schema_name")
    @classmethod
    def validate_name_fields(cls, v: str) -> str:
        """Validate name fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Name and schema fields cannot be empty")
        return v.strip()


class DataformAssertion(BaseModel):
    """Represents a Dataform assertion (data quality test)."""

    name: str = Field(description="Assertion name")
    database: Optional[str] = Field(None, description="Database name")
    schema_name: str = Field(description="Schema name")
    table: str = Field(description="Target table name")
    description: Optional[str] = Field(None, description="Assertion description")
    tags: List[str] = Field(default_factory=list, description="Assertion tags")
    dependencies: List[str] = Field(
        default_factory=list, description="Assertion dependencies"
    )
    sql_query: Optional[str] = Field(None, description="Assertion SQL query")
    file_path: Optional[str] = Field(None, description="Source file path")

    @field_validator("name", "schema_name", "table")
    @classmethod
    def validate_name_fields(cls, v: str) -> str:
        """Validate name fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Name, schema, and table fields cannot be empty")
        return v.strip()


class DataformOperation(BaseModel):
    """Represents a Dataform operation (custom SQL operation)."""

    name: str = Field(description="Operation name")
    description: Optional[str] = Field(None, description="Operation description")
    tags: List[str] = Field(default_factory=list, description="Operation tags")
    dependencies: List[str] = Field(
        default_factory=list, description="Operation dependencies"
    )
    sql_query: str = Field(description="Operation SQL query")
    file_path: Optional[str] = Field(None, description="Source file path")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v: str) -> str:
        """Validate name is not empty."""
        if not v or not v.strip():
            raise ValueError("Operation name cannot be empty")
        return v.strip()

    @field_validator("sql_query")
    @classmethod
    def validate_sql_query(cls, v: str) -> str:
        """Validate SQL query is not empty."""
        if not v or not v.strip():
            raise ValueError("SQL query cannot be empty")
        return v.strip()


class DataformDeclaration(BaseModel):
    """Represents a Dataform declaration (external data source)."""

    name: str = Field(description="Declaration name")
    database: Optional[str] = Field(None, description="Database name")
    schema_name: str = Field(description="Schema name")
    description: Optional[str] = Field(None, description="Declaration description")
    columns: List[DataformColumn] = Field(
        default_factory=list, description="Declaration columns"
    )
    tags: List[str] = Field(default_factory=list, description="Declaration tags")

    @field_validator("name", "schema_name")
    @classmethod
    def validate_name_fields(cls, v: str) -> str:
        """Validate name fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Name and schema fields cannot be empty")
        return v.strip()


class DataformEntities(BaseModel):
    """Container for all Dataform entities."""

    tables: List[DataformTable] = Field(
        default_factory=list, description="Dataform tables"
    )
    assertions: List[DataformAssertion] = Field(
        default_factory=list, description="Dataform assertions"
    )
    operations: List[DataformOperation] = Field(
        default_factory=list, description="Dataform operations"
    )
    declarations: List[DataformDeclaration] = Field(
        default_factory=list, description="Dataform declarations"
    )

    def get_all_entities(self) -> List[BaseModel]:
        """Get all entities as a flat list."""
        return [
            *self.tables,
            *self.assertions,
            *self.operations,
            *self.declarations,
        ]

    def get_entity_count(self) -> Dict[str, int]:
        """Get count of each entity type."""
        return {
            "tables": len(self.tables),
            "assertions": len(self.assertions),
            "operations": len(self.operations),
            "declarations": len(self.declarations),
        }


class DataformCompilationResult(BaseModel):
    """Represents a Dataform compilation result."""

    compiled_graph: Dict[str, Any] = Field(
        default_factory=dict, description="Compiled graph data"
    )
    targets: List[Dict[str, Union[str, int, float, bool]]] = Field(
        default_factory=list, description="Compilation targets"
    )

    @field_validator("compiled_graph")
    @classmethod
    def validate_compiled_graph(cls, v: Dict[str, Any]) -> Dict[str, Any]:
        """Validate compiled graph is not empty."""
        if not v:
            raise ValueError("Compiled graph cannot be empty")
        return v


class DataformWorkflowConfig(BaseModel):
    """Represents Dataform workflow configuration."""

    default_database: Optional[str] = Field(None, description="Default database")
    default_schema: Optional[str] = Field(None, description="Default schema")
    default_location: Optional[str] = Field(None, description="Default location")
    assertion_schema: Optional[str] = Field(None, description="Schema for assertions")
    vars: Dict[str, Union[str, int, float, bool]] = Field(
        default_factory=dict, description="Workflow variables"
    )

    model_config = ConfigDict(extra="allow")


class DataformRepositoryInfo(BaseModel):
    """Represents Dataform repository information."""

    project_id: str = Field(description="GCP project ID")
    repository_id: str = Field(description="Dataform repository ID")
    region: str = Field(default="us-central1", description="GCP region")
    workspace_id: Optional[str] = Field(None, description="Workspace ID")
    git_remote_settings: Optional[Dict[str, Union[str, int, bool]]] = Field(
        None, description="Git remote settings"
    )

    @field_validator("project_id", "repository_id")
    @classmethod
    def validate_required_fields(cls, v: str) -> str:
        """Validate required fields are not empty."""
        if not v or not v.strip():
            raise ValueError("Project ID and repository ID cannot be empty")
        return v.strip()


@dataclass
class DataformModelPerformance:
    """Performance metrics for Dataform model execution."""

    # This is specifically for table/view builds
    execution_time: Optional[float]  # in seconds
    status: str  # "success", "error", "skipped", etc.
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    thread_id: Optional[str]
    message: Optional[str]

    def is_success(self) -> bool:
        """Check if the model execution was successful."""
        return self.status == "success"


@dataclass
class DataformColumnLineageInfo:
    """Column-level lineage information for Dataform entities."""

    downstream_column_name: str
    upstream_column_names: List[str] = field(default_factory=list)
    confidence_score: float = 0.0  # 0.0 to 1.0


@dataclass
class DataformTestResult:
    """Results from Dataform test execution."""

    test_name: str
    status: str  # "pass", "fail", "error", "skip"
    execution_time: Optional[float]  # in seconds
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    message: Optional[str]

    def is_success(self) -> bool:
        """Check if the test passed."""
        return self.status == "pass"
