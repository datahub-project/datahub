import re
from typing import Dict, Optional, Pattern, Type, Union

from datahub.metadata.schema_classes import (
    BooleanTypeClass,
    BytesTypeClass,
    ChartTypeClass,
    DateTypeClass,
    EnumTypeClass,
    NumberTypeClass,
    StringTypeClass,
    TimeTypeClass,
)

_CARD_TYPE_MODEL = "model"

_QUERY_TYPE_NATIVE = "native"
_QUERY_TYPE_QUERY = "query"

_MBQL_REF_FIELD = "field"
_MBQL_REF_EXPRESSION = "expression"
_MBQL_REF_AGGREGATION = "aggregation"

# Source-table value prefix when referencing another card
_CARD_REF_PREFIX = "card__"

_SPECIAL_CHARS_PATTERN: Pattern[str] = re.compile(r"[^a-zA-Z0-9_]")
_MULTIPLE_UNDERSCORES_PATTERN: Pattern[str] = re.compile(r"_+")

# Metabase template expression patterns stripped before SQL parsing.
_OPTIONAL_CLAUSE_PATTERN: Pattern[str] = re.compile(r"\[\[.+?\]\]")
_TEMPLATE_VARIABLE_PATTERN: Pattern[str] = re.compile(r"\{\{.+?\}\}")

# JDBC connection-string constants used when sanitizing the `db` detail field
_JDBC_URI_SCHEMES = ("file:", "mem:")
_JDBC_DB_EXTENSION = ".db"

DATASOURCE_URN_RECURSION_LIMIT = 5  # prevent stack overflow on circular card refs

# Metabase engine names that differ from their DataHub platform identifier.
# Engines not listed here are used as-is (e.g. "postgres" → "postgres").
# Reference: metadata-service/configuration/src/main/resources/bootstrap_mcps/data-platforms.yaml
METABASE_ENGINE_TO_DATAHUB_PLATFORM: Dict[str, str] = {
    "sparksql": "spark",
    "mongo": "mongodb",
    "presto-jdbc": "presto",
    "sqlserver": "mssql",
    "bigquery-cloud-sdk": "bigquery",
}

# Maps each Metabase engine to the MetabaseDatabaseDetails attribute that holds
# the logical database name. Engines mapped to "_clean_db" go through JDBC
# sanitization; all others return the named field directly.
_ENGINE_TO_DB_DETAIL_FIELD: Dict[str, str] = {
    "athena": "catalog",
    "bigquery": "dataset_id",
    "bigquery-cloud-sdk": "project_id",
    "clickhouse": "dbname",
    "databricks": "catalog",
    "druid": "dbname",
    "h2": "_clean_db",
    "mongo": "dbname",
    "mysql": "dbname",
    "oracle": "service_name",
    "postgres": "dbname",
    "presto": "catalog",
    "presto-jdbc": "catalog",
    "redshift": "_clean_db",
    "snowflake": "_clean_db",
    "sparksql": "dbname",
    "sqlserver": "_clean_db",
    "trino": "catalog",
    "vertica": "database",
}

# Platforms that use a two-tier naming scheme (database.table) rather than
# three-tier (database.schema.table).  For these platforms the schema component
# returned by Metabase's /api/table endpoint is omitted from the dataset URN so
# that lineage URNs match those produced by the corresponding DataHub connector.
_TWO_TIER_PLATFORMS: frozenset = frozenset[str](
    {
        "mysql",  # MySQL has no schema layer; "schema" == database
        "mongodb",  # MongoDB: database + collection only
        "druid",  # Druid: datasource only, no schema concept
        "h2",  # H2: file-based embedded DB, no schema layer
    }
)

# Union of all engines we explicitly handle. The "unrecognised platform" warning
# is suppressed for these — they either translate via METABASE_ENGINE_TO_DATAHUB_PLATFORM
# or map 1:1 and are covered by _ENGINE_TO_DB_DETAIL_FIELD.
_KNOWN_METABASE_ENGINES: frozenset = frozenset[str](
    {*METABASE_ENGINE_TO_DATAHUB_PLATFORM, *_ENGINE_TO_DB_DETAIL_FIELD}
)

METABASE_CHART_DISPLAY_TYPE_MAP: Dict[str, Optional[str]] = {
    "table": ChartTypeClass.TABLE,
    "bar": ChartTypeClass.BAR,
    "line": ChartTypeClass.LINE,
    "row": ChartTypeClass.BAR,
    "area": ChartTypeClass.AREA,
    "pie": ChartTypeClass.PIE,
    "funnel": ChartTypeClass.BAR,
    "scatter": ChartTypeClass.SCATTER,
    "scalar": ChartTypeClass.TEXT,
    "smartscalar": ChartTypeClass.TEXT,
    "pivot": ChartTypeClass.TABLE,
    "waterfall": ChartTypeClass.BAR,
    "combo": None,
    "gauge": None,
    "map": None,
    "object": None,
    "progress": None,
    "sankey": None,
}

METABASE_TYPE_TO_DATAHUB_TYPE: Dict[
    str,
    Type[
        Union[
            NumberTypeClass,
            StringTypeClass,
            BooleanTypeClass,
            EnumTypeClass,
            BytesTypeClass,
            DateTypeClass,
            TimeTypeClass,
        ]
    ],
] = {
    "Integer": NumberTypeClass,
    "BigInteger": NumberTypeClass,
    "Float": NumberTypeClass,
    "Decimal": NumberTypeClass,
    "Number": NumberTypeClass,
    "Text": StringTypeClass,
    "String": StringTypeClass,
    "UUID": StringTypeClass,
    "Array": StringTypeClass,  # Metabase serializes as text
    "JSON": StringTypeClass,  # Displayed as text unless unfolded
    "Enum": EnumTypeClass,  # PostgresEnum, MySQLEnum
    "Boolean": BooleanTypeClass,
    "JSONB": BytesTypeClass,
    "Blob": BytesTypeClass,
    "Bytes": BytesTypeClass,
    "Date": DateTypeClass,
    "DateTime": DateTypeClass,
    "DateTimeWithTZ": DateTypeClass,
    "Time": TimeTypeClass,
}
