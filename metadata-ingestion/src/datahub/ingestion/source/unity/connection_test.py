# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Optional

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig
from datahub.ingestion.source.unity.proxy import UnityCatalogApiProxy
from datahub.ingestion.source.unity.report import UnityCatalogReport


class UnityCatalogConnectionTest:
    def __init__(self, config: UnityCatalogSourceConfig):
        self.config = config
        self.report = UnityCatalogReport()
        self.proxy = UnityCatalogApiProxy(
            self.config.workspace_url,
            self.config.profiling.warehouse_id,
            report=self.report,
            databricks_api_page_size=self.config.databricks_api_page_size,
            personal_access_token=self.config.token,
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
