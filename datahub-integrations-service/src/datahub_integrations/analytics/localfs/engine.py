from typing import Any, Dict, Tuple

from datahub_integrations.analytics.config import AnalyticsConfig
from datahub_integrations.analytics.duckdb.engine import (
    DuckDBAnalyticsEngine,
    DuckDBEngineConfig,
)


class LocalFSAnalyticsEngine(DuckDBAnalyticsEngine):
    def __init__(self) -> None:
        super().__init__(
            DuckDBEngineConfig(
                local_cache_path=AnalyticsConfig.get_instance().cache_dir,
                memory_backed=False,
            )
        )

    def get_location_and_credential_query_fragment(
        self, params: Dict[str, Any]
    ) -> Tuple[str, str]:
        return (params["path"], "")
