"""Configuration models for the Convex source."""

from typing import List

from pydantic import Field, PositiveInt, SecretStr

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import EnvConfigMixin


class ConvexDeploymentConfig(ConfigModel):
    name: str = Field(
        description="Human-readable deployment name. Used in dataset URNs and as the container name."
    )
    url: str = Field(
        description="Deployment URL, e.g. `https://happy-animal-123.convex.cloud`."
    )
    deploy_key: SecretStr = Field(
        description="Deploy key for the deployment. A read-only key (scope `deployment:data:view`) is sufficient."
    )


class ConvexSourceConfig(EnvConfigMixin):
    deployments: List[ConvexDeploymentConfig] = Field(
        description="Convex deployments to ingest. Each deployment becomes a container."
    )
    table_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for tables to filter in ingestion. Matched against `<deployment name>.<table name>`.",
    )
    include_row_counts: bool = Field(
        default=True,
        description="Count rows per table by paging the streaming export snapshot, and emit a `datasetProfile` aspect.",
    )
    max_count_pages: PositiveInt = Field(
        default=200,
        description="Safety cap on the number of snapshot pages (roughly 1024 rows each) read per table when counting rows.",
    )
