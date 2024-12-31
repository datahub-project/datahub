import enum
from typing import Optional

from typing_extensions import TypedDict

from datahub.metadata.schema_classes import OperationTypeClass

PLATFORMS_WITH_CASE_SENSITIVE_TABLES = {
    "bigquery",
}

DIALECTS_WITH_CASE_INSENSITIVE_COLS = {
    # Column identifiers are case-insensitive in BigQuery, so we need to
    # do a normalization step beforehand to make sure it's resolved correctly.
    "bigquery",
    # Our snowflake source lowercases column identifiers, so we are forced
    # to do fuzzy (case-insensitive) resolution instead of exact resolution.
    "snowflake",
    # Teradata column names are case-insensitive.
    # A name, even when enclosed in double quotation marks, is not case sensitive. For example, CUSTOMER and Customer are the same.
    # See more below:
    # https://documentation.sas.com/doc/en/pgmsascdc/9.4_3.5/acreldb/n0ejgx4895bofnn14rlguktfx5r3.htm
    "teradata",
    # For SQL server, the default collation rules mean that all identifiers (schema, table, column names)
    # are case preserving but case insensitive.
    "mssql",
}
DIALECTS_WITH_DEFAULT_UPPERCASE_COLS = {
    # In some dialects, column identifiers are effectively case insensitive
    # because they are automatically converted to uppercase. Most other systems
    # automatically lowercase unquoted identifiers.
    "snowflake",
}
assert DIALECTS_WITH_DEFAULT_UPPERCASE_COLS.issubset(
    DIALECTS_WITH_CASE_INSENSITIVE_COLS
)


class QueryType(enum.Enum):
    UNKNOWN = "UNKNOWN"

    CREATE_DDL = "CREATE_DDL"
    CREATE_VIEW = "CREATE_VIEW"
    CREATE_TABLE_AS_SELECT = "CREATE_TABLE_AS_SELECT"
    CREATE_OTHER = "CREATE_OTHER"

    SELECT = "SELECT"
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"
    MERGE = "MERGE"

    def is_create(self) -> bool:
        return self in {
            QueryType.CREATE_DDL,
            QueryType.CREATE_VIEW,
            QueryType.CREATE_TABLE_AS_SELECT,
            QueryType.CREATE_OTHER,
        }

    def to_operation_type(self) -> Optional[str]:
        if self.is_create():
            return OperationTypeClass.CREATE

        query_to_operation_mapping = {
            QueryType.SELECT: None,
            QueryType.INSERT: OperationTypeClass.INSERT,
            QueryType.UPDATE: OperationTypeClass.UPDATE,
            QueryType.DELETE: OperationTypeClass.DELETE,
            QueryType.MERGE: OperationTypeClass.UPDATE,
        }
        return query_to_operation_mapping.get(self, OperationTypeClass.UNKNOWN)


class QueryTypeProps(TypedDict, total=False):
    kind: str  # used for create statements
    temporary: bool
