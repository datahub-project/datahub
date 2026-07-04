import re

MICROSTRATEGY_PLATFORM = "microstrategy"

# REST endpoint paths that are referenced in more than one place -- the request
# plus its response-shape/error check, or the re-auth guard -- are centralized
# so the two uses cannot drift apart. Single-use paths stay inline.
MSTR_API_AUTH_PREFIX = "/api/auth/"
MSTR_API_AUTH_LOGIN = "/api/auth/login"
MSTR_API_AUTH_LOGOUT = "/api/auth/logout"
MSTR_API_PROJECTS = "/api/projects"
MSTR_API_OBJECT = "/api/objects/{object_id}"
MSTR_API_METADATA_SEARCHES = "/api/metadataSearches/results"
MSTR_API_SEARCHES = "/api/searches/results"

MSTR_LOGIN_MODE_STANDARD = 1
MSTR_LOGIN_MODE_GUEST = 8

MSTR_OBJECT_TYPE_DASHBOARD = 55
MSTR_OBJECT_TYPE_REPORT = 3
# Documents share object type 55 with dossiers; the subtype distinguishes them.
MSTR_OBJECT_SUBTYPE_DOCUMENT = "14081"

MEASURE_TAG_URN = "urn:li:tag:Measure"
DIMENSION_TAG_URN = "urn:li:tag:Dimension"
TEMPORAL_TAG_URN = "urn:li:tag:Temporal"

# Entity kinds usage buckets can attach to (dashboards get
# DashboardUsageStatistics, report charts get ChartUsageStatistics).
USAGE_TARGET_DASHBOARD = "dashboard"
USAGE_TARGET_CHART = "chart"

# Sentinel MicroStrategy returns for "no object", which must never be emitted as
# a real object id.
MSTR_NULL_OBJECT_ID = "00000000000000000000000000000000"

# API response field-name fallbacks. MicroStrategy carries the same logical
# field under different keys across server versions, so every lookup tries an
# ordered list of aliases. Declaring each alias set once here keeps the model
# normalizers that share them from drifting apart.
MSTR_KEYS_ID = ("id", "objectId")
MSTR_KEYS_NAME = ("name", "title")
MSTR_KEYS_KEY_ID = ("key", "id")
MSTR_KEYS_DISPLAY_NAME = ("name", "title", "displayName")
MSTR_KEYS_OWNER = ("username", "name", "id")
MSTR_KEYS_PROJECT_ID = ("id", "projectId", "project_id")
MSTR_KEYS_PROJECT_NAME = ("name", "projectName")
MSTR_KEYS_DATASET_OBJECT_ID = ("id", "objectId", "datasetId")
MSTR_KEYS_DATASET_ID = ("id", "objectId", "datasetId", "dataSetId")
MSTR_KEYS_DATASOURCE_ID = ("id", "objectId", "datasourceId")
MSTR_KEYS_DRIVER_TYPE = ("driverType", "driver")
MSTR_KEYS_DATASOURCE_TYPE = ("datasourceType", "sourceType", "type")
MSTR_KEYS_DBMS_NAME = ("name", "type")
MSTR_KEYS_DATABASE_TYPE = ("type", "databaseType")
MSTR_KEYS_DATABASE_VERSION = ("version", "databaseVersion")
MSTR_KEYS_DATABASE_NAME = ("databaseName", "database", "catalog")
MSTR_KEYS_DATABASE_NAME_NESTED = ("name", "databaseName", "catalog")
MSTR_KEYS_SCHEMA_NAME = ("schemaName", "schema", "databaseSchema")
MSTR_KEYS_SCHEMA_NAME_NESTED = ("schema", "schemaName", "databaseSchema")
MSTR_KEYS_VISUALIZATION_KEY = (
    "key",
    "id",
    "objectId",
    "visualizationKey",
    "nodeKey",
    "definitionKey",
)
MSTR_KEYS_VISUALIZATION_TYPE = ("type", "visualizationType")
MSTR_KEYS_SOURCE_OBJECT = (
    "dataSource",
    "datasource",
    "source",
    "sourceObject",
    "cube",
    "dataset",
)
MSTR_KEYS_SOURCE_OBJECT_ID = (
    "id",
    "objectId",
    "sourceId",
    "dataSourceId",
    "datasetId",
    "cubeId",
)
MSTR_KEYS_SOURCE_ID = (
    "sourceId",
    "dataSourceId",
    "datasourceId",
    "datasetId",
    "cubeId",
)
MSTR_KEYS_SOURCE_NAME = (
    "sourceName",
    "dataSourceName",
    "datasourceName",
    "datasetName",
    "cubeName",
)
MSTR_KEYS_DATASOURCE_REFERENCE = (
    "sourceWarehouse",
    "warehouse",
    "sourceDatasource",
    "sourceDataSource",
    "dataSource",
    "datasource",
)
MSTR_KEYS_LIST_CONTAINERS = ("items", "objects", "prompts")
MSTR_KEYS_FOLDER_PATH = ("path", "name")

# Lowercased container keys whose children are dataset references.
MSTR_DATASET_CONTAINER_KEYS = frozenset(
    {
        "dataset",
        "datasets",
        "datasetid",
        "datasetids",
        "datasetkey",
        "datasources",
        "datasource",
    }
)
# Lowercased parent keys / object types that mark metric & attribute object ids.
# templateMetrics is a container key in dossier definitions, not an object type.
MSTR_OBJECT_ID_PARENT_KEYS = frozenset(
    {"metric", "metrics", "attribute", "attributes", "templatemetrics"}
)
MSTR_OBJECT_TYPES = frozenset({"metric", "attribute"})

# Pre-compiled regexes. Matched case-insensitively; declared once so the
# hot-path normalizers reuse a single compiled pattern.
MSTR_DATASET_KEY_RE = re.compile("dataset", re.IGNORECASE)


def _compile_connection_param_re(*param_names: str) -> re.Pattern[str]:
    alternatives = "|".join(re.escape(name) for name in param_names)
    # Delimiters cover ODBC-style (;KEY=value) and JDBC URL query-string style
    # (?db=value&schema=value) connection strings.
    return re.compile(
        rf"(?:^|[;,&?\s])(?:{alternatives})\s*=\s*([^;&,\s}}]+)",
        re.IGNORECASE,
    )


MSTR_DATABASE_PARAM_RE = _compile_connection_param_re(
    "DATABASE", "databaseName", "db", "catalog"
)
MSTR_SCHEMA_PARAM_RE = _compile_connection_param_re(
    "schema", "currentSchema", "CURRENT_SCHEMA", "searchpath", "search_path"
)

# Generic BI vocabulary excluded from the name-token overlap heuristic used to
# infer visualization -> dataset lineage. Only genuinely non-discriminating
# words belong here; business terms (e.g. "SALES") must NOT be added because
# they legitimately distinguish datasets on real tenants.
MSTR_LINEAGE_STOP_WORDS = frozenset(
    {
        "AND",
        "DASHBOARD",
        "DATA",
        "DATASET",
        "REPORT",
        "TOTAL",
        "VISUALIZATION",
    }
)

# MicroStrategy SQL views append non-SQL commentary after the final pass
# ("[Analytical engine calculation steps: ...]", "with parameters: 1") and
# include DROP statements for its volatile tables. None of these carry lineage,
# so they are skipped instead of counted as parse failures. The "with
# parameters" match requires the trailing colon so a legitimate CTE named
# "parameters" is never skipped.
MSTR_LINEAGE_IRRELEVANT_STATEMENT_RE = re.compile(
    r"^\s*(drop\s|\[|with\s+parameters\s*:)",
    re.IGNORECASE,
)
MSTR_CREATE_STATEMENT_RE = re.compile(r"^\s*create\b", re.IGNORECASE)
# Captures the target name of a CREATE ... TABLE statement, tolerating any
# modifiers between CREATE and TABLE (VOLATILE, MULTISET, SET, GLOBAL TEMPORARY,
# TEMP, ...). Group 1 is the (optionally dotted/quoted) created table name; its
# leaf identifies the intermediate temp table to collapse for SQL-view lineage.
MSTR_CREATE_TABLE_NAME_RE = re.compile(
    r"^\s*create\b[\s\S]*?\btable\s+(?:if\s+not\s+exists\s+)?([A-Za-z0-9_$#.\"]+)",
    re.IGNORECASE,
)
# Matches a statement that returns rows (bare SELECT or a CTE-led SELECT). Only
# these can define a SQL-view dataset's output columns.
MSTR_SELECT_STATEMENT_RE = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)
# Non-emitted sentinel output table. Wrapping a SQL view's final SELECT as
# `CREATE TABLE <sentinel> AS <select>` gives the parser a concrete downstream,
# so it surfaces the final projection's column lineage (temp tables collapsed).
MSTR_SQL_VIEW_OUTPUT_SENTINEL = "__datahub_mstr_sql_view_output__"

# Alphanumeric run used to tokenize object names for the lineage overlap
# heuristic.
MSTR_NAME_TOKEN_RE = re.compile(r"[A-Za-z0-9]+")
# Collapses non-alphanumeric runs to a single underscore when normalizing a
# datasource source-type into a comparable key.
MSTR_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")
# SQL identifier (column/table token) extractor for expression parsing.
MSTR_SQL_IDENTIFIER_RE = re.compile(r"[A-Za-z_][\w$#]*")
# A 32-char hex string is MicroStrategy's canonical object-id form.
MSTR_HEX_OBJECT_ID_RE = re.compile(r"[0-9A-Fa-f]{32}")
# Collapses internal whitespace runs to a single space.
MSTR_WHITESPACE_RE = re.compile(r"\s+")
# Collapses repeated dots when normalizing a qualified table name.
MSTR_DOT_COLLAPSE_RE = re.compile(r"\.+")

# Maps normalized MicroStrategy datasource/database type tokens (see
# _normalize_source_type) to DataHub platform names. Keyed by the normalized
# form so both "SQL Server" and "sql_server" resolve to "mssql".
MSTR_SOURCE_TYPE_TO_DATAHUB_PLATFORM = {
    "athena": "athena",
    "big_query": "bigquery",
    "bigquery": "bigquery",
    "db2": "db2",
    "google_bigquery": "bigquery",
    "microsoft_sql_server": "mssql",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgre_sql": "postgres",
    "postgres": "postgres",
    "postgresql": "postgres",
    "red_shift": "redshift",
    "redshift": "redshift",
    "snow_flake": "snowflake",
    "snowflake": "snowflake",
    "sql_server": "mssql",
    "sqlserver": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
}

# Number of parts a warehouse table's fully-qualified name carries on each
# platform, so model-derived table urns match the native connector's. Most
# warehouses use database.schema.table (3); MySQL/MariaDB use database.table (2)
# because they have no schema layer. Platforms absent here default to 3.
MSTR_PLATFORM_QUALIFIED_NAME_PARTS = {
    "mysql": 2,
    "mariadb": 2,
}
MSTR_DEFAULT_QUALIFIED_NAME_PARTS = 3

# Keywords and function names excluded when extracting column identifiers from
# model expression text (e.g. `SUM(QTY_SOLD * UNIT_PRICE)` must yield only the
# column names, never the function tokens).
MSTR_SQL_EXPRESSION_KEYWORDS = frozenset(
    {
        "abs",
        "and",
        "as",
        "avg",
        "between",
        "case",
        "cast",
        "coalesce",
        "count",
        "cross",
        "delete",
        "distinct",
        "else",
        "end",
        "first",
        "full",
        "group",
        "having",
        "if",
        "in",
        "inner",
        "into",
        "is",
        "join",
        "last",
        "left",
        "max",
        "median",
        "min",
        "not",
        "null",
        "nullif",
        "on",
        "or",
        "order",
        "outer",
        "right",
        "round",
        "select",
        "set",
        "stdev",
        "sum",
        "then",
        "trunc",
        "update",
        "var",
        "when",
        "where",
        "with",
    }
)
