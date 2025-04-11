import logging
from typing import Optional

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport

try:
    from databricks import sql  # type: ignore

    DATABRICKS_SQL_AVAILABLE = True
except ImportError:
    DATABRICKS_SQL_AVAILABLE = False

logger = logging.getLogger(__name__)

# Databricks resource ID for Azure
DATABRICKS_RESOURCE_ID = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


class UnityCatalogConnectionTest:
    def __init__(self, config: UnityCatalogSourceConfig):
        self.config = config
        self.report = UnityCatalogReport()

        token = self.config.get_token()
        self.proxy = UnityCatalogApiProxy(
            self.config.workspace_url,
            token,
            self.config.profiling.warehouse_id,
            report=self.report,
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
            _ = next(iter(query_history))
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

    @staticmethod
    def test_connection(config_dict: dict) -> TestConnectionReport:
        try:
            config = UnityCatalogSourceConfig.parse_obj(config_dict)

            # Test authentication
            try:
                token = config.get_token()
            except Exception as e:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason=f"Failed to get token: {str(e)}",
                    )
                )

            # Test connection to Databricks
            if not DATABRICKS_SQL_AVAILABLE:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False,
                        failure_reason="databricks-sql-connector package is required. Please install it with: pip install databricks-sql-connector",
                    )
                )

            try:
                with sql.connect(
                    server_hostname=config.workspace_url,
                    http_path=f"/sql/1.0/warehouses/{config.warehouse_id}",
                    access_token=token,
                ) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        result = cursor.fetchone()
                        if not result:
                            return TestConnectionReport(
                                basic_connectivity=CapabilityReport(
                                    capable=False,
                                    failure_reason="Failed to execute test query",
                                )
                            )
            except Exception as e:
                return TestConnectionReport(
                    basic_connectivity=CapabilityReport(
                        capable=False, failure_reason=str(e)
                    )
                )

            return TestConnectionReport(
                basic_connectivity=CapabilityReport(capable=True)
            )

        except Exception as e:
            return TestConnectionReport(
                basic_connectivity=CapabilityReport(
                    capable=False, failure_reason=str(e)
                )
            )
