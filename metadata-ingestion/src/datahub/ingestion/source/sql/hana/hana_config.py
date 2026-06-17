from pydantic import Field, PositiveInt, model_validator

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.source.sql.hana.constants import DEFAULT_DENY_SCHEMAS
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig


class HanaConfig(BasicSQLAlchemyConfig, BaseUsageConfig):
    scheme: HiddenFromDocs[str] = Field(
        default="hana+hdbcli",
        description="SQLAlchemy scheme. Defaults to the official SAP `hdbcli` driver.",
    )
    host_port: str = Field(
        default="localhost:39041",
        description=(
            "SAP HANA host and port, e.g. `myhost.example.com:30015`. "
            "HANA Cloud requires TLS — pass `encrypt=true` via "
            "`options.connect_args` if the driver does not enable it."
        ),
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=DEFAULT_DENY_SCHEMAS),
        description="Regex patterns for schemas to filter in ingestion. SAP-managed `_SYS_*` schemas (except `_SYS_BIC`) are denied by default.",
    )
    include_calculation_views: bool = Field(
        default=False,
        description="If true, ingest SAP HANA calculation views from `_SYS_REPO.ACTIVE_OBJECT` with column-level lineage parsed from their XML. On-premise / self-managed HANA only.",
    )
    calculation_view_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against `<package_id>.<view_name>` (e.g. `acme.analytics.SalesOverview`) to filter calculation views. Use this instead of `view_pattern` for calculation views.",
    )
    include_stored_procedures: bool = Field(
        default=True,
        description="If true, ingest stored procedures as `DataJob` entities with table-level lineage parsed from their bodies.",
    )
    procedure_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns matched against `<schema>.<procedure_name>` to filter stored procedures.",
    )
    include_query_usage: bool = Field(
        default=False,
        description="If true, mine query history from `_SYS_STATISTICS.HOST_SQL_PLAN_CACHE`. Requires the `MONITORING` role or `CATALOG READ` system privilege.",
    )
    include_usage_stats: bool = Field(
        default=False,
        description=(
            "If true, emit `DatasetUsageStatistics` aspects. Requires "
            "`include_query_usage` and the same `MONITORING` / "
            "`CATALOG READ` privilege."
        ),
    )
    usage_max_queries: PositiveInt = Field(
        default=10000,
        description="Maximum number of distinct `(statement_hash, last_execution_timestamp)` rows pulled from `HOST_SQL_PLAN_CACHE` per ingestion run. Distinct from `top_n_queries`, which caps the per-bucket rollup.",
    )

    @model_validator(mode="after")
    def _usage_stats_requires_query_usage(self) -> "HanaConfig":
        if self.include_usage_stats and not self.include_query_usage:
            raise ValueError(
                "`include_usage_stats=true` requires `include_query_usage=true`; "
                "usage statistics are computed from mined queries."
            )
        return self
