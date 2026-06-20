from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from datahub.configuration.env_vars import get_trace_powerbi_mquery_parser
from datahub.ingestion.source.powerbi.config import DataPlatformPair
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo

TRACE_POWERBI_MQUERY_PARSER: bool = get_trace_powerbi_mquery_parser()


@dataclass
class IdentifierAccessor:
    """
    statement
        public_order_date = Source{[Schema="public",Item="order_date"]}[Data]
    will be converted to IdentifierAccessor instance
    where:

        "Source" is identifier

        "[Schema="public",Item="order_date"]" is "items" in ItemSelector. Data of items varies as per DataSource

        "public_order_date" is in "next" of ItemSelector. The "next" will be None if this identifier is leaf i.e., table

    """

    identifier: str
    items: Dict[str, Any]
    next: Optional["IdentifierAccessor"]


@dataclass
class DataAccessFunctionDetail:
    arg_list: dict  # InvokeExpression node dict from NodeIdMap
    data_access_function_name: (
        str  # matches FunctionName.value (e.g. "Snowflake.Databases")
    )
    identifier_accessor: Optional[IdentifierAccessor]
    node_map: Dict[int, dict]  # full NodeIdMap for ast_utils navigation
    parameters: Dict[str, str] = field(default_factory=dict)


@dataclass
class ReferencedTable:
    warehouse: str
    catalog: Optional[str]
    database: str
    schema: str
    table: str


@dataclass
class DataPlatformTable:
    data_platform_pair: DataPlatformPair
    urn: str


@dataclass
class Lineage:
    # External warehouse/catalog tables from recognized M data-access functions
    # (e.g. Sql.Database); each entry includes platform pair and dataset URN.
    upstreams: List[DataPlatformTable]
    # Column-level lineage from SQL / Value.NativeQuery parsing (upstream columns
    # per downstream column).
    column_lineage: List[ColumnLineageInfo]
    # Names of other tables in the same Power BI dataset referenced by M (unresolved
    # identifiers) or DAX; mapper resolves names to sibling dataset URNs as TRANSFORMED.
    powerbi_table_upstreams: List[str] = field(default_factory=list)

    @staticmethod
    def empty() -> "Lineage":
        # Same as default field values: no external upstreams, CLL, or sibling refs.
        return Lineage(upstreams=[], column_lineage=[])


class FunctionName(Enum):
    NATIVE_QUERY = "Value.NativeQuery"
    POSTGRESQL_DATA_ACCESS = "PostgreSQL.Database"
    ORACLE_DATA_ACCESS = "Oracle.Database"
    SNOWFLAKE_DATA_ACCESS = "Snowflake.Databases"
    MSSQL_DATA_ACCESS = "Sql.Database"
    MSSQL_MULTI_DATABASE_DATA_ACCESS = "Sql.Databases"
    DATABRICK_DATA_ACCESS = "Databricks.Catalogs"
    GOOGLE_BIGQUERY_DATA_ACCESS = "GoogleBigQuery.Database"
    AMAZON_ATHENA_DATA_ACCESS = "AmazonAthena.Databases"
    AMAZON_REDSHIFT_DATA_ACCESS = "AmazonRedshift.Database"
    DATABRICK_MULTI_CLOUD_DATA_ACCESS = "DatabricksMultiCloud.Catalogs"
    MYSQL_DATA_ACCESS = "MySQL.Database"
    ODBC_DATA_ACCESS = "Odbc.DataSource"
    ODBC_QUERY = "Odbc.Query"
