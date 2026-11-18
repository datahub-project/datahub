from dataclasses import dataclass
from enum import Enum
from typing import Final, Optional

import pydantic
from pydantic import field_validator, model_validator
from typing_extensions import LiteralString

from datahub.configuration.source_common import PlatformDetail

# Literal M-Query value emitted when a source argument resolves to null. Kept as a
# constant because it is compared against parsed argument strings, not Python None.
M_QUERY_NULL = '"null"'

# Marker attached to an upstream URN when native SQL parsing of an M-Query
# NativeQuery expression fails; downstream code surfaces it in the report.
SQL_PARSING_FAILURE: Final[LiteralString] = "SQL Parsing Failure"
EXTERNAL_QUERY_NOT_MAPPED: Final[LiteralString] = (
    "BigQuery EXTERNAL_QUERY connection not mapped"
)
M_QUERY_NAVIGATION_UNRESOLVED: Final[LiteralString] = (
    "M-Query navigation not resolved to a table"
)


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

    # Starburst's Power BI connectors target Trino-protocol clusters, ingested
    # under the "trino" platform (there is no separate "starburst" platform).
    STARBURST_TRINO = DataPlatformPair(
        powerbi_data_platform_name="Starburst",
        datahub_data_platform_name="trino",
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


def _strip_and_reject_blank(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    stripped = value.strip()
    if not stripped:
        raise ValueError("must not be empty or whitespace")
    return stripped


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
    def _validate_optional_str(cls, value: Optional[str]) -> Optional[str]:
        return _strip_and_reject_blank(value)

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


class BigQueryExternalQueryPlatformDetail(PlatformDetail):
    """Maps a BigQuery ``EXTERNAL_QUERY`` connection to the external source it federates to.

    BigQuery federation (``EXTERNAL_QUERY(connection, sql)``) runs the inner SQL on an
    external engine such as Cloud SQL or AlloyDB (which expose MySQL/PostgreSQL). The
    connection id (``project.region.connection``) does not reveal the external platform,
    so this mapping supplies it, letting PowerBI lineage point at the real upstream table
    on that platform instead of failing to resolve a URN. ``platform`` must be a
    recognized DataHub platform (see ``SupportedDataPlatform``).
    """

    platform: str = pydantic.Field(
        min_length=1,
        description="Target DataHub platform of the external source the EXTERNAL_QUERY "
        "federates to (e.g. 'postgres', 'mysql', 'snowflake').",
    )
    default_database: Optional[str] = pydantic.Field(
        default=None,
        description="Database prepended to unqualified or 2-part table names in the "
        "federated (inner) SQL. The EXTERNAL_QUERY connection id does not carry the "
        "external database name, so set this when your external source's ingestion emits "
        "3-part database.schema.table URNs so the lineage URNs match.",
    )
    default_schema: Optional[str] = pydantic.Field(
        default=None,
        description="Schema applied to unqualified table references in the federated "
        "(inner) SQL.",
    )

    @field_validator("platform")
    @classmethod
    def _validate_known_platform(cls, value: str) -> str:
        known_platforms = {
            item.value.datahub_data_platform_name for item in SupportedDataPlatform
        }
        if value not in known_platforms:
            raise ValueError(
                f"platform '{value}' is not a recognized DataHub platform. "
                f"Known platforms: {sorted(known_platforms)}."
            )
        return value

    @field_validator("default_schema", "default_database")
    @classmethod
    def _validate_optional_str(cls, value: Optional[str]) -> Optional[str]:
        return _strip_and_reject_blank(value)
