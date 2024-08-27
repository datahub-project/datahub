import logging
from typing import Optional

import pydantic

from datahub.ingestion.source.usage.usage_common import BaseUsageConfig

logger = logging.getLogger(__name__)


class SnowflakeUsageConfig(BaseUsageConfig):
    email_domain: Optional[str] = pydantic.Field(
        default=None,
        description="Email domain of your organization so users can be displayed on UI appropriately.",
    )
    apply_view_usage_to_tables: bool = pydantic.Field(
        default=False,
        description="Whether to apply view's usage to its base tables. If set to True, usage is applied to base tables only.",
    )
