"""Configuration model for the SAP HANA ingestion source."""

from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig

# Default deny patterns cover the SAP-managed schemas that are present on every
# tenant database. `_SYS_BIC` intentionally stays *allowed* because it stores
# the activated calculation views and is the entry point for calc-view lineage.
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


class HanaConfig(BasicSQLAlchemyConfig):
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
            "Regex patterns for schemas to filter in ingestion. By default, "
            "every `_SYS_*` schema (except `_SYS_BIC`, which exposes activated "
            "calculation views) is excluded. Override `deny` to disable this "
            "default filtering."
        ),
    )
    include_calculation_views: bool = Field(
        default=False,
        description=(
            "If true, extract SAP HANA calculation views from "
            "`_SYS_REPO.ACTIVE_OBJECT` and emit column-level lineage from their "
            "XML definitions. Disabled by default because the calculation-view "
            "repository is not present on every deployment (it requires SAP "
            "HANA XS classic / repository content). When enabled, the connecting "
            "user must have `SELECT` on `_SYS_REPO.ACTIVE_OBJECT` and "
            "`SYS.VIEW_COLUMNS`."
        ),
    )
