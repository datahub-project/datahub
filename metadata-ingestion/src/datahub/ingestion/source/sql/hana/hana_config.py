from pydantic import Field, PositiveInt

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

# Default deny patterns cover the SAP-managed schemas present on every tenant.
# ``_SYS_BIC`` stays *allowed* — it exposes activated calculation views and is
# the entry point for calc-view lineage.
_DEFAULT_DENY_SCHEMAS = [
    r"^SYS$",
    r"^_SYS_AUDIT$",
    r"^_SYS_BI$",
    r"^_SYS_BIC_CDS$",
    r"^_SYS_DATA_ANONYMIZATION$",
    r"^_SYS_EPM$",
    r"^_SYS_PLAN_STABILITY$",
    r"^_SYS_REPO$",
    r"^_SYS_RT$",
    r"^_SYS_SECURITY$",
    r"^_SYS_SQL_ANALYZER$",
    r"^_SYS_STATISTICS$",
    r"^_SYS_TASK$",
    r"^_SYS_TELEMETRY$",
    r"^_SYS_WORKLOAD_REPLAY$",
    r"^_SYS_XS$",
    r"^HANA_XS_BASE$",
]


class HanaConfig(BasicSQLAlchemyConfig, BaseUsageConfig):
    """Configuration for the SAP HANA DataHub source."""

    scheme: HiddenFromDocs[str] = Field(
        default="hana+hdbcli",
        description="SQLAlchemy scheme. Defaults to the official SAP `hdbcli` driver.",
    )
    host_port: str = Field(
        default="localhost:39041",
        description="SAP HANA host and port, e.g. `myhost.example.com:30015`.",
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=_DEFAULT_DENY_SCHEMAS),
        description=(
            "Regex patterns for schemas to filter in ingestion. By default "
            "every `_SYS_*` schema (except `_SYS_BIC`, which exposes activated "
            "calculation views) is excluded. Override `deny` to disable this "
            "default filtering."
        ),
    )
    include_calculation_views: bool = Field(
        default=False,
        description=(
            "If true, extract SAP HANA calculation views from "
            "`_SYS_REPO.ACTIVE_OBJECT` and emit column-level lineage from "
            "their XML definitions. Requires `SELECT` on "
            "`_SYS_REPO.ACTIVE_OBJECT` and `SYS.VIEW_COLUMNS`. Off by default "
            "because the calculation-view repository requires SAP HANA XS "
            "classic / repository content, which is not present on every "
            "deployment."
        ),
    )
    include_stored_procedures: bool = Field(
        default=True,
        description=(
            "If true, ingest SAP HANA stored procedures as `DataJob` entities "
            "(grouped by schema under a `DataFlow`) and parse their bodies "
            "for table-level and procedure-to-procedure lineage."
        ),
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description=(
            "Regex patterns matched against `<schema>.<procedure_name>` to "
            "filter which stored procedures are ingested."
        ),
    )
    include_query_usage: bool = Field(
        default=False,
        description=(
            "If true, mine query history from "
            "`_SYS_STATISTICS.HOST_SQL_PLAN_CACHE` and feed it through the "
            "SQL parsing aggregator. Requires the statistics service to be "
            "running and the ingestion user to have the `MONITORING` role "
            "(or `CATALOG READ` system privilege). Off by default because "
            "neither prerequisite is universally available."
        ),
    )
    include_usage_stats: bool = Field(
        default=False,
        description=(
            "If true, emit `DatasetUsageStatistics` aspects rolled up from "
            "the observed queries extracted via `include_query_usage`. "
            "Has no effect unless `include_query_usage` is also enabled."
        ),
    )
    usage_max_queries: PositiveInt = Field(
        default=10000,
        description=(
            "Maximum number of distinct `(statement_hash, "
            "last_execution_timestamp)` rows pulled from "
            "`HOST_SQL_PLAN_CACHE` per ingestion run."
        ),
    )
