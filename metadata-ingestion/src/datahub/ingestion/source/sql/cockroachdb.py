from typing import Annotated

from pydantic.fields import Field

from datahub.configuration.common import AllowDenyPattern, Filters, HiddenFromDocs
from datahub.ingestion.api.common import PipelineContext
from datahub.ingestion.api.decorators import (
    SourceCapability,
    SupportStatus,
    capability,
    config_class,
    platform_name,
    support_status,
)
from datahub.ingestion.source.common.subtypes import DatasetContainerSubTypes
from datahub.ingestion.source.sql.postgres import PostgresConfig, PostgresSource


class CockroachDBConfig(PostgresConfig):
    scheme: HiddenFromDocs[str] = Field(
        default="cockroachdb+psycopg2", description="database scheme"
    )
    # Annotated, not a bare redeclaration: pydantic v2 replaces the annotation
    # wholesale, so restating an inherited field silently drops the Filters(...)
    # the parent attached. Nothing failed when it did -- the `<kind>_pattern`
    # name convention covered for it -- which is how BigQuery came to resolve
    # to a deprecated alias and report wrong verdicts.
    schema_pattern: Annotated[
        AllowDenyPattern, Filters(DatasetContainerSubTypes.SCHEMA)
    ] = Field(default=AllowDenyPattern(deny=["information_schema", "crdb_internal"]))


@platform_name("CockroachDB")
@config_class(CockroachDBConfig)
@support_status(SupportStatus.ALPHA)
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
