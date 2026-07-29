from dataclasses import dataclass
from enum import Enum
from typing import Optional

import pydantic
from pydantic import field_validator, model_validator

from datahub.configuration.source_common import PlatformDetail

# Literal M-Query value emitted when a source argument resolves to null. Kept as a
# constant because it is compared against parsed argument strings, not Python None.
M_QUERY_NULL = '"null"'

# Marker attached to an upstream URN when native SQL parsing of an M-Query
# NativeQuery expression fails; downstream code surfaces it in the report.
SQL_PARSING_FAILURE = "SQL Parsing Failure"


@dataclass
class DataPlatformPair:
    datahub_data_platform_name: str
    powerbi_data_platform_name: str


@dataclass
class PowerBIPlatformDetail:
    data_platform_pair: DataPlatformPair
    data_platform_server: str


class SupportedDataPlatform(Enum):
    POSTGRES_SQL = DataPlatformPair(
        powerbi_data_platform_name="PostgreSQL", datahub_data_platform_name="postgres"
    )

    ORACLE = DataPlatformPair(
        powerbi_data_platform_name="Oracle", datahub_data_platform_name="oracle"
    )

    SNOWFLAKE = DataPlatformPair(
        powerbi_data_platform_name="Snowflake", datahub_data_platform_name="snowflake"
    )

    MS_SQL = DataPlatformPair(
        powerbi_data_platform_name="Sql", datahub_data_platform_name="mssql"
    )

    GOOGLE_BIGQUERY = DataPlatformPair(
        powerbi_data_platform_name="GoogleBigQuery",
        datahub_data_platform_name="bigquery",
    )

    AMAZON_ATHENA = DataPlatformPair(
        powerbi_data_platform_name="Amazon Athena",
        datahub_data_platform_name="athena",
    )

    AMAZON_REDSHIFT = DataPlatformPair(
        powerbi_data_platform_name="AmazonRedshift",
        datahub_data_platform_name="redshift",
    )

    DATABRICKS_SQL = DataPlatformPair(
        powerbi_data_platform_name="Databricks", datahub_data_platform_name="databricks"
    )

    DatabricksMultiCloud_SQL = DataPlatformPair(
        powerbi_data_platform_name="DatabricksMultiCloud",
        datahub_data_platform_name="databricks",
    )

    MYSQL = DataPlatformPair(
        powerbi_data_platform_name="MySQL",
        datahub_data_platform_name="mysql",
    )

    HIVE = DataPlatformPair(
        powerbi_data_platform_name="Hive",
        datahub_data_platform_name="hive",
    )

    ODBC = DataPlatformPair(
        powerbi_data_platform_name="Odbc",
        datahub_data_platform_name="odbc",
    )

    # Fabric OneLake for DirectLake lineage (Lakehouse/Warehouse tables)
    FABRIC_ONELAKE = DataPlatformPair(
        powerbi_data_platform_name="FabricOneLake",
        datahub_data_platform_name="fabric-onelake",
    )


class DataBricksPlatformDetail(PlatformDetail):
    """
    metastore is an additional field used in Databricks connector to generate the dataset urn
    """

    metastore: str = pydantic.Field(
        description="Databricks Unity Catalog metastore name.",
    )


class OraclePlatformDetail(PlatformDetail):
    default_schema: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Owner/schema applied to unqualified table references inside "
            '``Oracle.Database(…, Query="…")`` inline native SQL, so they resolve '
            "to your ingested Oracle datasets. Not used by hierarchical navigation."
        ),
    )
    default_database: Optional[str] = pydantic.Field(
        default=None,
        description=(
            "Database segment prepended to the table name when the "
            "``Oracle.Database`` connection is a bare TNS alias or descriptor "
            "(which carries no database). Set this to match the database segment "
            "your Oracle ingestion uses, only when that ingestion emits 3-part "
            "``database.schema.table`` URNs (``add_database_name_to_urn: true``); "
            "leave unset for the default 2-part URNs and for EZ-Connect "
            "``host:port/service`` connections."
        ),
    )

    @field_validator("default_schema", "default_database")
    @classmethod
    def _strip_and_reject_blank(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            raise ValueError("must not be empty or whitespace")
        return stripped

    # Requires at least one knob. This is also relied on to disambiguate
    # OraclePlatformDetail from a plain PlatformDetail in the
    # server_to_platform_instance Union: a plain {platform_instance} entry fails
    # this check, so it is never a valid OraclePlatformDetail candidate —
    # independent of pydantic's union-resolution order.
    @model_validator(mode="after")
    def _require_at_least_one_default(self) -> "OraclePlatformDetail":
        if self.default_schema is None and self.default_database is None:
            raise ValueError(
                "OraclePlatformDetail requires 'default_schema' and/or "
                "'default_database'; use a plain platform-instance mapping if "
                "you need neither."
            )
        return self
