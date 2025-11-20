import logging
from dataclasses import dataclass, field
from typing import Dict, Generator, List, Optional, Type, Union

from datahub.ingestion.source.dataform.dataform_api import DataformColumn
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StaleEntityRemovalSourceReport,
)
from datahub.metadata.com.linkedin.pegasus2avro.schema import (
    SchemaField,
)
from datahub.metadata.schema_classes import (
    ArrayTypeClass,
    BooleanTypeClass,
    BytesTypeClass,
    DateTypeClass,
    NullTypeClass,
    NumberTypeClass,
    RecordTypeClass,
    SchemaFieldDataTypeClass,
    StringTypeClass,
    TimeTypeClass,
)
from datahub.utilities.lossy_collections import LossyList

logger = logging.getLogger(__name__)

PLATFORM_NAME_IN_DATAHUB = "dataform"

# Platform name for server-side hooks recognition
DATAFORM_PLATFORM = "dataform"


@dataclass
class DataformSourceReport(StaleEntityRemovalSourceReport):
    """Report for the Dataform source."""

    num_tables_failed: int = 0
    num_assertions_failed: int = 0
    num_operations_failed: int = 0
    num_declarations_failed: int = 0

    tables_scanned: int = 0
    assertions_scanned: int = 0
    operations_scanned: int = 0
    declarations_scanned: int = 0

    entities_profiled: int = 0
    filtered: LossyList[str] = field(default_factory=LossyList)

    compilation_errors: LossyList[str] = field(default_factory=LossyList)

    def report_entity_scanned(self, name: str, ent_type: str = "Table") -> None:
        """Report that an entity has been scanned."""
        if ent_type == "Table":
            self.tables_scanned += 1
        elif ent_type == "Assertion":
            self.assertions_scanned += 1
        elif ent_type == "Operation":
            self.operations_scanned += 1
        elif ent_type == "Declaration":
            self.declarations_scanned += 1

    def report_entity_failure(self, name: str, ent_type: str = "Table") -> None:
        """Report that an entity processing failed."""
        if ent_type == "Table":
            self.num_tables_failed += 1
        elif ent_type == "Assertion":
            self.num_assertions_failed += 1
        elif ent_type == "Operation":
            self.num_operations_failed += 1
        elif ent_type == "Declaration":
            self.num_declarations_failed += 1

    def report_dropped(self, ent_name: str) -> None:
        """Report that an entity was dropped due to filtering."""
        self.filtered.append(ent_name)


class DataformToSchemaFieldConverter:
    """Converts Dataform column information to DataHub schema fields."""

    # Mapping from Dataform/SQL types to DataHub types
    _TYPE_MAPPING: Dict[str, Type] = {
        # String types
        "STRING": StringTypeClass,
        "TEXT": StringTypeClass,
        "VARCHAR": StringTypeClass,
        "CHAR": StringTypeClass,
        # Numeric types
        "INT64": NumberTypeClass,
        "INTEGER": NumberTypeClass,
        "BIGINT": NumberTypeClass,
        "SMALLINT": NumberTypeClass,
        "TINYINT": NumberTypeClass,
        "FLOAT64": NumberTypeClass,
        "FLOAT": NumberTypeClass,
        "DOUBLE": NumberTypeClass,
        "DECIMAL": NumberTypeClass,
        "NUMERIC": NumberTypeClass,
        # Boolean types
        "BOOL": BooleanTypeClass,
        "BOOLEAN": BooleanTypeClass,
        # Date/time types
        "DATE": DateTypeClass,
        "TIME": TimeTypeClass,
        "DATETIME": TimeTypeClass,
        "TIMESTAMP": TimeTypeClass,
        # Binary types
        "BYTES": BytesTypeClass,
        "BINARY": BytesTypeClass,
        "VARBINARY": BytesTypeClass,
        # Array types
        "ARRAY": ArrayTypeClass,
        "REPEATED": ArrayTypeClass,
        # Struct/Record types
        "STRUCT": RecordTypeClass,
        "RECORD": RecordTypeClass,
        # JSON types
        "JSON": StringTypeClass,
        "JSONB": StringTypeClass,
    }

    @classmethod
    def get_schema_fields(
        cls, column_infos: List[DataformColumn]
    ) -> Generator[SchemaField, None, None]:
        """Convert Dataform columns to DataHub schema fields."""
        for column in column_infos:
            yield cls._get_schema_field_for_column(column)

    @classmethod
    def _get_schema_field_for_column(cls, column: DataformColumn) -> SchemaField:
        """Convert a single Dataform column to a DataHub schema field."""
        field_type = cls._get_column_type(column.type)

        return SchemaField(
            fieldPath=column.name,
            type=SchemaFieldDataTypeClass(type=field_type),
            nativeDataType=column.type,
            description=column.description,
            nullable=True,  # Default to nullable unless specified otherwise
            recursive=False,
        )

    @classmethod
    def _get_column_type(
        cls, dataform_type: str
    ) -> Union[
        BooleanTypeClass,
        StringTypeClass,
        NumberTypeClass,
        BytesTypeClass,
        DateTypeClass,
        TimeTypeClass,
        ArrayTypeClass,
        RecordTypeClass,
        NullTypeClass,
    ]:
        """Map a Dataform column type to a DataHub type."""
        if not dataform_type:
            return NullTypeClass()

        # Normalize the type string
        normalized_type = dataform_type.upper().strip()

        # Handle array types like ARRAY<STRING>
        if normalized_type.startswith("ARRAY<") or normalized_type.startswith(
            "REPEATED "
        ):
            return ArrayTypeClass()

        # Handle struct types like STRUCT<field1 STRING, field2 INT64>
        if normalized_type.startswith("STRUCT<") or normalized_type.startswith(
            "RECORD<"
        ):
            return RecordTypeClass()

        # Extract base type for parameterized types like VARCHAR(255)
        base_type = normalized_type.split("(")[0].split("<")[0].strip()

        type_class = cls._TYPE_MAPPING.get(base_type, StringTypeClass)
        return type_class()


def get_dataform_platform_for_target(target_platform: str) -> str:
    """Get the appropriate platform name for the target platform."""
    # Map common target platforms to their DataHub platform names
    platform_mapping = {
        "bigquery": "bigquery",
        "postgres": "postgres",
        "postgresql": "postgres",
        "redshift": "redshift",
        "snowflake": "snowflake",
        "mysql": "mysql",
        "mssql": "mssql",
        "sqlserver": "mssql",
        "oracle": "oracle",
        "databricks": "databricks",
        "spark": "spark",
        "hive": "hive",
    }

    return platform_mapping.get(target_platform.lower(), target_platform.lower())


def get_dataform_entity_name(
    schema: str, name: str, database: Optional[str] = None
) -> str:
    """Generate a fully qualified entity name for Dataform entities."""
    parts = []

    if database:
        parts.append(database)

    if schema:
        parts.append(schema)

    parts.append(name)

    return ".".join(parts)


def extract_sql_tables_from_query(sql_query: str) -> List[str]:
    """Extract table references from a SQL query (simplified implementation)."""
    # This is a very basic implementation
    # In practice, you might want to use a proper SQL parser like sqlparse

    import re

    # Pattern to match table references (simplified)
    # This won't catch all cases but provides basic functionality
    patterns = [
        r'\bFROM\s+([`"]?[\w\.]+[`"]?)',
        r'\bJOIN\s+([`"]?[\w\.]+[`"]?)',
        r'\bINTO\s+([`"]?[\w\.]+[`"]?)',
        r'\bUPDATE\s+([`"]?[\w\.]+[`"]?)',
    ]

    tables = []
    for pattern in patterns:
        matches = re.findall(pattern, sql_query, re.IGNORECASE)
        for match in matches:
            # Clean up the table name
            table_name = match.strip('`"')
            if table_name and table_name not in tables:
                tables.append(table_name)

    return tables


def parse_dataform_file_path(file_path: str) -> Dict[str, str]:
    """Parse a Dataform file path to extract useful metadata."""
    import os

    result = {
        "filename": os.path.basename(file_path),
        "directory": os.path.dirname(file_path),
        "extension": os.path.splitext(file_path)[1],
    }

    # Extract relative path components for better organization
    path_parts = file_path.split(os.sep)
    if "definitions" in path_parts:
        # Find the definitions directory and get the relative path from there
        definitions_index = path_parts.index("definitions")
        if definitions_index < len(path_parts) - 1:
            relative_parts = path_parts[definitions_index + 1 :]
            result["relative_path"] = os.sep.join(relative_parts)

            # Determine the type based on directory structure
            if relative_parts:
                first_dir = relative_parts[0]
                if first_dir in [
                    "tables",
                    "views",
                    "sources",
                    "assertions",
                    "operations",
                ]:
                    result["entity_type"] = first_dir

    return result


def validate_dataform_config(config_dict: Dict) -> List[str]:
    """Validate Dataform configuration and return a list of errors."""
    errors = []

    # Check that either cloud_config or core_config is provided
    if not config_dict.get("cloud_config") and not config_dict.get("core_config"):
        errors.append("Either 'cloud_config' or 'core_config' must be provided")

    if config_dict.get("cloud_config") and config_dict.get("core_config"):
        errors.append("Only one of 'cloud_config' or 'core_config' can be provided")

    # Validate cloud config
    cloud_config = config_dict.get("cloud_config")
    if cloud_config:
        required_fields = ["project_id", "repository_id"]
        for field in required_fields:
            if not cloud_config.get(field):
                errors.append(f"cloud_config.{field} is required")

        # Check authentication
        if not cloud_config.get("service_account_key_file") and not cloud_config.get(
            "credentials_json"
        ):
            errors.append(
                "Either 'service_account_key_file' or 'credentials_json' must be provided for cloud_config"
            )

    # Validate core config
    core_config = config_dict.get("core_config")
    if core_config:
        if not core_config.get("project_path"):
            errors.append("core_config.project_path is required")

    # Check target platform
    if not config_dict.get("target_platform"):
        errors.append("target_platform is required")

    return errors
