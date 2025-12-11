# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


class CockroachDBConfig(PostgresConfig):
    scheme: HiddenFromDocs[str] = Field(
        default="cockroachdb+psycopg2", description="database scheme"
    )
    schema_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern(deny=["information_schema", "crdb_internal"])
    )


@platform_name("CockroachDB")
@config_class(CockroachDBConfig)
@support_status(SupportStatus.TESTING)
@capability(SourceCapability.PLATFORM_INSTANCE, "Enabled by default")
@capability(SourceCapability.DOMAINS, "Supported via the `domain` config field")
@capability(SourceCapability.DATA_PROFILING, "Optionally enabled via configuration")
class CockroachDBSource(PostgresSource):
    config: CockroachDBConfig

    def __init__(self, config: CockroachDBConfig, ctx: PipelineContext):
        super().__init__(config, ctx)

    def get_platform(self):
        return "cockroachdb"

    @classmethod
    def create(cls, config_dict, ctx):
        config = CockroachDBConfig.model_validate(config_dict)
        return cls(config, ctx)
