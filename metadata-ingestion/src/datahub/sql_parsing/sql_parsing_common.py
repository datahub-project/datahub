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
    elif platform_lower == "fabric-onelake":
        # Fabric SQL Analytics Endpoint speaks T-SQL.
        return "tsql"
    elif platform_lower == "fabricspark":
        # dbt-fabricspark (Fabric Lakehouse via Spark/Livy) speaks Spark 3+ SQL.
        # Use sqlglot's "spark" (Spark 3), not "spark2". Do not map to "fabric"
        # — that dialect is T-SQL (warehouse), not Lakehouse Spark.
        return "spark"
    elif platform_lower in {"athena", "glue"}:
        # Glue catalog views are Athena/Presto views, which speak Trino SQL.
        return "trino"
    elif platform_lower == "hana":
        # sqlglot does not ship a dedicated SAP HANA dialect. HANA SQL is
        # closest to PostgreSQL among the available dialects (window
        # functions, ANSI joins, double-quoted identifiers, schema-qualified
        # names); using "postgres" lets the parser resolve table references
        # in calculation-view SQL fragments without forcing every query to
        # be treated as opaque. Known mis-parses: CALL, NCLOB, MERGE,
        # SQLScript-only constructs (WITH PARAMETERS, type-cast literals,
        # table variables) — table-ref extraction restricts itself to
        # quoted schema-qualified identifiers in `hana_script_lineage.py`
        # to sidestep them.
        return "postgres"
    elif platform_lower == "informix":
        # sqlglot has no Informix dialect. Informix view text (sysviews.viewtext)
        # is typically ANSI-ish with double-quoted schema-qualified identifiers
        # and aliased joins; postgres is the closest available dialect for
        # Tableau / query lineage / SqlParsingAggregator. Known mis-parses:
        # MATCHES / NOT MATCHES, FIRST / SKIP, native OUTER joins,
        # DATETIME ... YEAR TO DAY.
        return "postgres"
    elif platform_lower == "salesforce":
        # TODO: define SalesForce SOQL dialect
        # Temporary workaround is to treat SOQL as databricks dialect
        # At least it allows to parse simple SQL queries and build lineage for them
        return "databricks"
    elif platform_lower in {"mysql", "mariadb", "tidb"}:
        # MySQL's identifier case sensitivity depends on the underlying OS
        # (lower_case_table_names), so we treat identifiers as case-insensitive
        # and let the fuzzy resolution logic handle the mismatch. That fuzzing
        # deliberately happens *after* parsing: SchemaResolver probes the
        # exact-cased, lowercased and mixed-cased URNs, and column names are
        # matched case-insensitively via DIALECTS_WITH_CASE_INSENSITIVE_COLS.
        #
        # We must therefore keep sqlglot's case-preserving normalization here.
        # Forcing "normalization_strategy = lowercase" folds the real casing away
        # while parsing, before the resolver ever sees it, so all three URN
        # candidates collapse to the lowercased one and a table stored in
        # DataHub as `my_schema.MixedCaseTable` can never be matched.
        #
        # MariaDB and TiDB are MySQL-compatible (TiDB speaks the MySQL wire
        # protocol and presents as MySQL 8.0), so we reuse the same dialect.
        # Without this, sqlglot has no "tidb" dialect and view/query lineage
        # parsing silently produces nothing.
        return "mysql"
    elif platform == "timescaledb":
        return "postgres"
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
    # Fabric SQL Analytics Endpoint inherits SQL Server's case-insensitive collation.
    "fabric-onelake",
    # MySQL column names are always case-insensitive, independent of the
    # lower_case_table_names setting that governs table and database names.
    # https://dev.mysql.com/doc/refman/8.0/en/identifier-case-sensitivity.html
    # This also covers the MySQL-derived sqlglot dialects (MariaDB, TiDB, Doris,
    # StarRocks, SingleStore), since is_dialect_instance matches on the sqlglot
    # dialect class rather than on the platform name.
    "mysql",
    # Oracle automatically converts unquoted identifiers to uppercase.
    # https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Database-Object-Names-and-Qualifiers.html#GUID-3C59E44A-5140-4BCA-B9E1-3039C8050C49
    # In our Oracle connector, we then normalize column names to lowercase. This behavior
    # actually comes from the underlying Oracle sqlalchemy dialect.
    # https://github.com/sqlalchemy/sqlalchemy/blob/d9b4d8ff3aae504402d324f3ebf0b8faff78f5dc/lib/sqlalchemy/dialects/oracle/base.py#L2579
    "oracle",
    # NOTE: SAP HANA and Informix also fold unquoted identifiers to uppercase
    # and are therefore semantically case-insensitive, but we deliberately do
    # *not* add "hana" or "informix" here. ``is_dialect_instance`` resolves
    # both through ``get_dialect_str`` to the Postgres sqlglot dialect (neither
    # has a dedicated dialect upstream), so any membership check would also
    # match every other Postgres-dialect parse — flipping Postgres lineage
    # to lowercase column names and breaking unrelated golden files. Column-case
    # normalisation for these platforms is handled at the connector level.
}
DIALECTS_WITH_CASE_PRESERVING_COL_URNS = {
    # When no schema is available we cannot know a column's real casing, so we have
    # to guess. For most case-insensitive dialects the guess is lowercase, because
    # their sources lowercase column names when building URNs (Snowflake and Oracle
    # explicitly, see DIALECTS_WITH_DEFAULT_UPPERCASE_COLS).
    #
    # MySQL is the exception: its source emits the casing the catalog reports, so a
    # lowercased guess produces a schemaField URN that does not exist. The spelling
    # written in the query is the better guess there - an identifier that is not in
    # the engine's default case has to be quoted to work at all, so a quoted alias
    # like `MyAlias` is the author telling us the exact casing.
    "mysql",
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
