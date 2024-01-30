import enum
from typing import Callable, Optional

from datahub.ingestion.source.usage.usage_common import BaseUsageConfig
from datahub.metadata._urns.urn_defs import DataPlatformUrn


class StoreQueriesSetting(enum.Enum):
    DISABLED = "DISABLED"
    STORE_ALL = "STORE_ALL"
    STORE_FAILED = "STORE_FAILED"


class SqlParsingAggregator:
    platform: DataPlatformUrn
    platform_instance: Optional[str]
    env: str

    generate_view_lineage: bool = True
    generate_observed_lineage: bool = True
    generate_queries: bool = True  # only respected if generate_lineage
    generate_usage_statistics: bool = False
    generate_operations: bool = False

    usage_config: Optional[BaseUsageConfig] = None

    # can be used by BQ where we have a "temp_table_dataset_prefix"
    is_temp_table: Optional[Callable[[str], bool]] = None

    store_queries: StoreQueriesSetting = StoreQueriesSetting.DISABLED
