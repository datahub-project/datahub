import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional

from lark import Tree

from datahub.ingestion.source.powerbi.config import DataPlatformPair
from datahub.sql_parsing.sqlglot_lineage import ColumnLineageInfo

TRACE_POWERBI_MQUERY_PARSER = os.getenv("DATAHUB_TRACE_POWERBI_MQUERY_PARSER", False)


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
    arg_list: Tree
    data_access_function_name: str
    identifier_accessor: Optional[IdentifierAccessor]


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
    upstreams: List[DataPlatformTable]
    column_lineage: List[ColumnLineageInfo]

    @staticmethod
    def empty() -> "Lineage":
        return Lineage(upstreams=[], column_lineage=[])


class FunctionName(Enum):
    NATIVE_QUERY = "Value.NativeQuery"
    POSTGRESQL_DATA_ACCESS = "PostgreSQL.Database"
    ORACLE_DATA_ACCESS = "Oracle.Database"
    SNOWFLAKE_DATA_ACCESS = "Snowflake.Databases"
    MSSQL_DATA_ACCESS = "Sql.Database"
    DATABRICK_DATA_ACCESS = "Databricks.Catalogs"
    GOOGLE_BIGQUERY_DATA_ACCESS = "GoogleBigQuery.Database"
    AMAZON_REDSHIFT_DATA_ACCESS = "AmazonRedshift.Database"
    DATABRICK_MULTI_CLOUD_DATA_ACCESS = "DatabricksMultiCloud.Catalogs"
