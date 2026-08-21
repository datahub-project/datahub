from typing import Optional

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.connection import create_workspace_client
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport


class UnityCatalogConnectionTest:
    def __init__(self, config: UnityCatalogSourceConfig):
        self.config = config
        self.report = UnityCatalogReport()
        self.proxy = UnityCatalogApiProxy(
            create_workspace_client(self.config),
            report=self.report,
            databricks_api_page_size=self.config.databricks_api_page_size,
        )

    def get_connection_test(self) -> TestConnectionReport:
        capability_report = {
            SourceCapability.USAGE_STATS: self.usage_connectivity(),
            SourceCapability.DATA_PROFILING: self.profiling_connectivity(),
        }
        return TestConnectionReport(
            basic_connectivity=self.basic_connectivity(),
            capability_report={
                k: v for k, v in capability_report.items() if v is not None
            },
        )

    def basic_connectivity(self) -> CapabilityReport:
        try:
            return CapabilityReport(capable=self.proxy.check_basic_connectivity())
        except Exception as e:
            return CapabilityReport(capable=False, failure_reason=str(e))

    def usage_connectivity(self) -> Optional[CapabilityReport]:
        if not self.config.include_usage_statistics:
            return None
        try:
            query_history = self.proxy.query_history(
                self.config.start_time, self.config.end_time
            )
            first = next(iter(query_history))
            if first.is_query_text_redacted:
                # Only the system-tables preparsed path can still emit table-level
                # usage from redacted queries (via system.access.table_lineage,
                # which doesn't need SQL text). The REST-API path and the column
                # usage stats path both need SQL text, so redacted queries are
                # dropped entirely and usage is fully degraded.
                preparsed_path_available = (
                    self.config.usage_uses_system_tables(self.config.warehouse_id)
                    and not self.config.include_column_usage_stats
                )
                if preparsed_path_available:
                    return CapabilityReport(
                        capable=True,
                        mitigation_message=(
                            "Query text is redacted (<REDACTED>). Table-level "
                            "usage statistics are still emitted via "
                            "system.access.table_lineage on the system-tables "
                            "path, but Query entities, column-level usage "
                            "statistics, and the top-SQL sample are incomplete. "
                            "Add the ingestion principal to the account-level "
                            "group databricks_pii_access to restore them."
                        ),
                    )
                return CapabilityReport(
                    capable=False,
                    failure_reason=(
                        "Query text is redacted (<REDACTED>) and the configured "
                        "usage path cannot process it: redacted queries need "
                        "system.access.table_lineage (system-tables path with "
                        "include_column_usage_stats=false) to contribute usage "
                        "statistics. Add the ingestion principal to the "
                        "account-level group databricks_pii_access to read "
                        "unmasked SQL statement text."
                    ),
                )
            return CapabilityReport(capable=True)
        except StopIteration:
            return CapabilityReport(
                capable=False,
                failure_reason=(
                    "No query history found. "
                    "Do you have CAN_MANAGE permissions on the specified SQL warehouse?"
                ),
            )
        except Exception as e:
            return CapabilityReport(capable=False, failure_reason=str(e))

    def profiling_connectivity(self) -> Optional[CapabilityReport]:
        if not self.config.is_profiling_enabled():
            return None
        try:
            return CapabilityReport(capable=self.proxy.check_profiling_connectivity())
        except Exception as e:
            return CapabilityReport(capable=False, failure_reason=str(e))
