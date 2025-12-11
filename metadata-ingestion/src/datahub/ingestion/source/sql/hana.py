# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from typing import Dict

import pydantic

from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.sql_common import SQLAlchemySource
from datahub.ingestion.source.sql.sql_config import BasicSQLAlchemyConfig


class HanaConfig(BasicSQLAlchemyConfig):
    # Override defaults
    host_port: str = pydantic.Field(default="localhost:39041")
    scheme: str = pydantic.Field(default="hana+hdbcli")


@platform_name("SAP HANA", id="hana")
@config_class(HanaConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
@capability(
    SourceCapability.DELETION_DETECTION, "Enabled by default via stateful ingestion"
)
class HanaSource(SQLAlchemySource):
    def __init__(self, config: HanaConfig, ctx: PipelineContext):
        super().__init__(config, ctx, "hana")

    @classmethod
    def create(cls, config_dict: Dict, ctx: PipelineContext) -> "HanaSource":
        config = HanaConfig.model_validate(config_dict)
        return cls(config, ctx)
