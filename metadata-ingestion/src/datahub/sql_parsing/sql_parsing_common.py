import enum
from typing import Optional

from typing_extensions import TypedDict

from datahub.metadata.schema_classes import OperationTypeClass

PLATFORMS_WITH_CASE_SENSITIVE_TABLES = {
    "bigquery",
    "db2",
}


def get_dialect_str(platform: str) -> str:
    """Map DataHub platform names to sqlglot dialect names.

    Most DataHub platform names map directly to sqlglot dialect names,
    but some platforms require translation.

    Args:
        platform: The DataHub platform name (e.g., "snowflake", "mssql")

    Returns:
        The corresponding sqlglot dialect string
    """
    platform_lower = platform.lower()

    if platform_lower == "presto-on-hive":
        return "hive"
    elif platform_lower == "mssql":
        return "tsql"
    elif platform_lower == "athena":
        return "trino"
    elif platform_lower == "salesforce":
        # TODO: define SalesForce SOQL dialect
        # Temporary workaround is to treat SOQL as databricks dialect
        # At least it allows to parse simple SQL queries and build lineage for them
        return "databricks"
    elif platform_lower in {"mysql", "mariadb"}:
        # In sqlglot v20+, MySQL is now case-sensitive by default, which is the
        # default behavior on Linux. However, MySQL's default case sensitivity
        # actually depends on the underlying OS.
        # For us, it's simpler to just assume that it's case-insensitive, and
        # let the fuzzy resolution logic handle it.
        # MariaDB is a fork of MySQL, so we reuse the same dialect.
        return "mysql, normalization_strategy = lowercase"
    else:
        return platform_lower


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
    # Oracle automatically converts unquoted identifiers to uppercase.
    # https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html#GUID-3C59E44A-5140-4BCA-B9E1-3039C8050C49
    # In our Oracle connector, we then normalize column names to lowercase. This behavior
    # actually comes from the underlying Oracle sqlalchemy dialect.
    # https://github.com/sqlalchemy/sqlalchemy/blob/d9b4d8ff3aae504402d324f3ebf0b8faff78f5dc/lib/sqlalchemy/dialects/oracle/base.py#L2579
    "oracle",
}
DIALECTS_WITH_DEFAULT_UPPERCASE_COLS = {
    # In some dialects, column identifiers are effectively case insensitive
    # because they are automatically converted to uppercase. Most other systems
    # automatically lowercase unquoted identifiers.
    "snowflake",
    "oracle",
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
