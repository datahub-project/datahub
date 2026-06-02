import logging
from dataclasses import dataclass
from typing import Dict, Optional, Protocol, Tuple

from datahub.emitter.mce_builder import (
    make_dataset_urn_with_platform_instance,
    make_schema_field_urn,
)
from datahub.ingestion.source.montecarlo.client import ResolvedTable
from datahub.ingestion.source.montecarlo.config import MonteCarloSourceConfig
from datahub.ingestion.source.montecarlo.report import MonteCarloSourceReport

logger = logging.getLogger(__name__)

# Monte Carlo connection/warehouse types -> DataHub platform names.
CONNECTION_TYPE_TO_PLATFORM: Dict[str, str] = {
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "redshift": "redshift",
    "databricks": "databricks",
    "databricks-metastore": "databricks",
    "databricks-sql": "databricks",
    "spark": "spark",
    "presto": "presto",
    "hive": "hive",
    "glue": "glue",
    "athena": "athena",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "sql-server": "mssql",
    "synapse": "mssql",
    "teradata": "teradata",
    "transactional-db": "postgres",
}


class TableResolverClient(Protocol):
    """The subset of the Monte Carlo client the resolver depends on."""

    def get_table(self, mcon: str) -> Optional[ResolvedTable]: ...


@dataclass(frozen=True)
class ParsedMcon:
    account_id: str
    resource_id: str
    object_type: str
    object_id: str


def parse_mcon(mcon: str) -> Optional[ParsedMcon]:
    """Parse ``MCON++{account}++{resource}++{object_type}++{object_id}``."""
    if not mcon or not mcon.startswith("MCON++"):
        return None
    parts = mcon.split("++")
    if len(parts) != 5:
        return None
    _, account_id, resource_id, object_type, object_id = parts
    return ParsedMcon(
        account_id=account_id,
        resource_id=resource_id,
        object_type=object_type,
        object_id=object_id,
    )


class MconResolver:
    """Resolves Monte Carlo MCONs into DataHub dataset (and field) URNs.

    Each MCON is resolved via ``getTable`` to obtain the full table path and the
    warehouse connection type, which is then mapped to a DataHub platform. Results
    are cached per MCON to avoid repeated API calls.
    """

    def __init__(
        self,
        config: MonteCarloSourceConfig,
        client: TableResolverClient,
        report: MonteCarloSourceReport,
    ) -> None:
        self.config = config
        self.client = client
        self.report = report
        self._cache: Dict[str, Optional[str]] = {}

    def _platform_detail(
        self, resource_id: str, connection_type: Optional[str]
    ) -> Optional[Tuple[str, Optional[str], str]]:
        mapped = self.config.connection_to_platform_map.get(resource_id)
        if mapped is not None:
            return mapped.platform, mapped.platform_instance, mapped.env
        platform: Optional[str] = None
        if connection_type:
            platform = CONNECTION_TYPE_TO_PLATFORM.get(connection_type.lower())
        platform = platform or self.config.default_platform
        if platform is None:
            return None
        return platform, self.config.platform_instance, self.config.env

    def dataset_urn_for_mcon(self, mcon: str) -> Optional[str]:
        if mcon in self._cache:
            return self._cache[mcon]

        urn: Optional[str] = None
        try:
            resolved = self.client.get_table(mcon)
            parsed = parse_mcon(mcon)
            if resolved is None or parsed is None:
                self.report.report_mcon_resolution_failed()
                self.report.warning(
                    title="Unresolvable Monte Carlo asset",
                    message="Could not resolve MCON to a warehouse table; skipping.",
                    context=mcon,
                )
            else:
                detail = self._platform_detail(
                    parsed.resource_id, resolved.connection_type
                )
                if detail is None:
                    self.report.mcons_unmapped_platform.append(mcon)
                    self.report.warning(
                        title="Unmapped Monte Carlo warehouse",
                        message="No platform mapping for this warehouse connection type. "
                        "Add it to connection_to_platform_map or set default_platform.",
                        context=f"{mcon} (connection_type={resolved.connection_type})",
                    )
                else:
                    platform, platform_instance, env = detail
                    urn = make_dataset_urn_with_platform_instance(
                        platform=platform,
                        name=resolved.full_table_id,
                        platform_instance=platform_instance,
                        env=env,
                    )
                    self.report.report_mcon_resolved()
        except Exception as e:
            self.report.report_mcon_resolution_failed()
            self.report.warning(
                title="Error resolving Monte Carlo asset",
                message="Failed to resolve MCON to a dataset URN; skipping.",
                context=mcon,
                exc=e,
            )

        self._cache[mcon] = urn
        return urn

    def field_urn_for_mcon(self, mcon: str, field: str) -> Optional[str]:
        dataset_urn = self.dataset_urn_for_mcon(mcon)
        if dataset_urn is None:
            return None
        return make_schema_field_urn(dataset_urn, field)
