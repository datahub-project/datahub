"""Column type detection and mapping for the custom SQLAlchemy profiler."""

import re
from enum import Enum

import sqlalchemy.types as sa_types

from datahub.ingestion.source.sql.sql_types import resolve_sql_type
from datahub.metadata.com.linkedin.pegasus2avro.schema import NumberType

logger = __import__("logging").getLogger(__name__)

# Pattern to normalize SQL type names (removes array/struct modifiers)
NORMALIZE_TYPE_PATTERN = re.compile(r"^(.*?)(?:[\[<(].*)?$")


class ProfilerDataType(Enum):
    """Profiler data type enumeration."""

    INT = "INT"
    FLOAT = "FLOAT"
    STRING = "STRING"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"
    UNKNOWN = "UNKNOWN"


def get_column_profiler_type(
    col_type: sa_types.TypeEngine, dialect_name: str
) -> ProfilerDataType:
    """Map SQLAlchemy type to profiler type."""

    # Integer types
    if isinstance(
        col_type,
        (
            sa_types.Integer,
            sa_types.BigInteger,
            sa_types.SmallInteger,
        ),
    ):
        return ProfilerDataType.INT

    # Float types
    if isinstance(
        col_type,
        (
            sa_types.Float,
            sa_types.Numeric,
            sa_types.DECIMAL,
        ),
    ):
        return ProfilerDataType.FLOAT

    # String types
    if isinstance(
        col_type,
        (
            sa_types.String,
            sa_types.Text,
            sa_types.VARCHAR,
            sa_types.CHAR,
            sa_types.Unicode,
            sa_types.UnicodeText,
        ),
    ):
        return ProfilerDataType.STRING

    # DateTime types
    if isinstance(
        col_type,
        (
            sa_types.DateTime,
            sa_types.Date,
            sa_types.Time,
            sa_types.TIMESTAMP,
            sa_types.Interval,
        ),
    ):
        return ProfilerDataType.DATETIME

    # Boolean types
    if isinstance(col_type, sa_types.Boolean):
        return ProfilerDataType.BOOLEAN

    return ProfilerDataType.UNKNOWN


def resolve_profiler_type_with_fallback(
    col_type: sa_types.TypeEngine,
    dialect_name: str,
    column_type_str: str,
) -> ProfilerDataType:
    """Get profiler type with fallback to resolve_sql_type for UNKNOWN."""
    profiler_type = get_column_profiler_type(col_type, dialect_name)

    if profiler_type == ProfilerDataType.UNKNOWN:
        try:
            datahub_field_type = resolve_sql_type(column_type_str, dialect_name.lower())
            if isinstance(datahub_field_type, NumberType):
                # Determine if int or float based on type
                if "int" in str(datahub_field_type).lower():
                    return ProfilerDataType.INT
                return ProfilerDataType.FLOAT
        except Exception as e:
            logger.debug(
                f"Error resolving sql type {column_type_str}: {type(e).__name__}: {str(e)}",
            )

    return profiler_type


def should_profile_column(
    col_type: sa_types.TypeEngine,
    dialect_name: str,
    column_name: str,
    profile_nested_fields: bool = False,
) -> bool:
    """Determine if column should be profiled based on type and name."""
    # Exclude nested fields if not enabled
    if not profile_nested_fields and "." in column_name:
        return False

    # Exclude complex types
    if isinstance(
        col_type,
        (
            sa_types.ARRAY,
            sa_types.JSON,
            # Add dialect-specific types to exclude
        ),
    ):
        return False

    return get_column_profiler_type(col_type, dialect_name) != ProfilerDataType.UNKNOWN


def _get_column_types_to_ignore(dialect_name: str) -> list[str]:
    """
    Get list of column types to ignore for profiling.

    Returns uppercase type names for case-insensitive matching.
    """
    dialect_lower = dialect_name.lower()
    if dialect_lower == "postgresql":
        return ["JSON"]
    elif dialect_lower == "bigquery":
        # GEOGRAPHY doesn't support aggregate functions like APPROX_COUNT_DISTINCT
        return ["ARRAY", "STRUCT", "GEOGRAPHY", "JSON", "INTERVAL"]
    elif dialect_lower == "snowflake":
        # GEOGRAPHY and GEOMETRY were skipped in GE profiler by registering as NullType
        # OBJECT and ARRAY are complex types that don't support standard profiling operations
        # (same behavior as GE profiler which maps OBJECT to NullType and ARRAY to ArrayType)
        return ["GEOGRAPHY", "GEOMETRY", "OBJECT", "ARRAY"]

    return []
