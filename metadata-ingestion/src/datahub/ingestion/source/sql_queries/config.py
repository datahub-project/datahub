import re
from abc import abstractmethod
from functools import cached_property
from typing import Any, Dict, List, Literal, Optional

from pydantic import Field, field_validator, model_validator

from datahub.configuration.common import ConfigModel, HiddenFromDocs
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.configuration.validate_field_removal import pydantic_removed_field
from datahub.ingestion.api.incremental_lineage_helper import (
    IncrementalLineageConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.aws.s3_util import is_s3_uri
from datahub.ingestion.source.elastic_search_common import (
    ElasticsearchConnectionConfig,
)
from datahub.ingestion.source.usage.usage_common import BaseUsageConfig


class QueryFieldMappings(ConfigModel):
    """Where each query-record field is found in a backend record.

    Only query is required; an unmapped optional field is left empty.
    """

    query: str = Field("query", description="Field holding the SQL query text.")
    timestamp: Optional[str] = Field(
        None, description="Field holding the query timestamp."
    )
    user: Optional[str] = Field(None, description="Field holding the executing user.")
    session_id: Optional[str] = Field(
        None,
        description="Field holding the session/connection id, used to resolve "
        "temporary tables across statements.",
    )
    default_db: Optional[str] = Field(
        None,
        description="Field holding the default database for unqualified table names.",
    )
    default_schema: Optional[str] = Field(
        None,
        description="Field holding the default schema for unqualified table names.",
    )

    def mapped_optional_fields(self) -> Dict[str, str]:
        """Optional field name -> configured path, for the fields that are mapped."""
        paths = {
            "timestamp": self.timestamp,
            "user": self.user,
            "session_id": self.session_id,
            "default_db": self.default_db,
            "default_schema": self.default_schema,
        }
        return {name: path for name, path in paths.items() if path}


class MappedQuerySourceConfig(ConfigModel):
    """Settings shared by backends whose records are mapped onto query fields."""

    unescape_sql: bool = Field(
        False,
        description="Unescape backslash-escaped quotes in the SQL text. "
        "Audit logs commonly store SQL with escaped quotes (\\' and \\\").",
    )
    email_domain: Optional[str] = Field(
        None,
        description="If set, bare user values are turned into CorpUser URNs by "
        "appending '@' and this domain (e.g. a user value of 'alice' with "
        "email_domain 'example.com' becomes alice@example.com).",
    )

    field_mappings: QueryFieldMappings = Field(
        default_factory=QueryFieldMappings,
        description="Maps query-record fields to backend record fields.",
    )

    @property
    @abstractmethod
    def label(self) -> str:
        """Short description of where records are read from, for reports."""
        raise NotImplementedError


class ElasticsearchQueryFieldMappings(QueryFieldMappings):
    """Maps query-record fields to document fields.

    Dotted paths reach into nested objects; a document key that itself contains
    dots is matched first. A path through an array takes its first element.
    """

    timestamp: Optional[str] = Field(
        "@timestamp", description="Document field holding the query timestamp."
    )


class ElasticsearchQuerySourceConfig(MappedQuerySourceConfig):
    type: Literal["elasticsearch"] = Field(
        description="Selects the Elasticsearch/OpenSearch backend."
    )
    connection: ElasticsearchConnectionConfig = Field(
        default_factory=ElasticsearchConnectionConfig,
        description="Elasticsearch/OpenSearch connection settings, including AWS IAM "
        "(SigV4) authentication via connection.aws_config. That is separate from the "
        "top-level aws_config, which is only used to read an s3:// query_file.",
    )
    index: str = Field(
        description="Index, alias, or index pattern to read query records from."
    )
    field_mappings: ElasticsearchQueryFieldMappings = Field(
        default_factory=ElasticsearchQueryFieldMappings,
        description="Maps query-record fields to document fields.",
    )
    query_filter: Optional[Dict[str, Any]] = Field(
        None,
        description="Additional Elasticsearch query DSL added to the bool filter. "
        "This backend has no time window of its own, so use a range filter here "
        "to bound which records are read.",
    )
    unescape_sql: bool = Field(
        True,
        description="Unescape backslash-escaped quotes in the SQL text. "
        "Audit logs commonly store SQL with escaped quotes (\\' and \\\").",
    )
    batch_size: int = Field(
        1000,
        gt=0,
        description="Scroll size, i.e. the number of hits fetched per shard per "
        "scroll request (standard Elasticsearch scroll semantics). Against a "
        "multi-shard index or an index pattern, a scroll page holds up to "
        "batch_size times the number of matching shards, so size scroll_keepalive "
        "for that effective page rather than for batch_size alone.",
    )
    scroll_keepalive: str = Field(
        "5m",
        description="How long each scroll page is kept alive server-side. Raise it "
        "if parsing a scroll page downstream can take longer than this, otherwise "
        "the scroll can expire mid-run. See batch_size for how shard count affects "
        "the page size this must cover.",
    )
    request_timeout_seconds: int = Field(
        60,
        gt=0,
        description="Timeout for each search and scroll request. A sorted first "
        "page over a large index pattern can take well over the client default.",
    )
    max_retries: int = Field(
        3,
        ge=0,
        description="How many times a request is retried after a timeout or a "
        "connection error before the run fails.",
    )

    @property
    def label(self) -> str:
        return f"elasticsearch:{self.index}"


class SqlQueriesSourceConfig(
    PlatformInstanceConfigMixin, EnvConfigMixin, IncrementalLineageConfigMixin
):
    query_file: Optional[str] = Field(
        None,
        description="Path to a newline-delimited JSON file of query records "
        "(local path or s3:// URI). Exactly one of query_file and query_source "
        "must be set.",
    )
    query_source: Optional[ElasticsearchQuerySourceConfig] = Field(
        None,
        description="A non-file backend to read query records from. Exactly one "
        "of query_file and query_source must be set.",
    )

    platform: str = Field(
        description="The platform for which to generate data, e.g. snowflake"
    )

    usage: BaseUsageConfig = Field(
        description="The usage config to use when generating usage statistics",
        default=BaseUsageConfig(),
    )

    use_schema_resolver: HiddenFromDocs[bool] = Field(
        True,
        description="Read SchemaMetadata aspects from DataHub to aid in SQL parsing. Turn off only for testing.",
    )
    default_db: Optional[str] = Field(
        None,
        description="The default database to use for unqualified table names",
    )
    default_schema: Optional[str] = Field(
        None,
        description="The default schema to use for unqualified table names",
    )
    override_dialect: Optional[str] = Field(
        None,
        description="The SQL dialect to use when parsing queries. Overrides automatic dialect detection.",
    )
    temp_table_patterns: List[str] = Field(
        description="Regex patterns for temporary tables to filter in lineage ingestion. "
        "Patterns are start-anchored (re.match, like AllowDenyPattern). "
        "For example, 'temp_' matches any table starting with 'temp_'. "
        "This is useful for platforms like Athena "
        "that don't have native temp tables but use naming patterns for fake temp tables.",
        default=[],
    )

    aws_config: Optional[AwsConnectionConfig] = Field(
        default=None,
        description="AWS configuration for S3 access. Required when query_file is an S3 URI (s3://).",
    )

    _enable_lazy_schema_loading_removed = pydantic_removed_field(
        "enable_lazy_schema_loading", "August", 2026
    )

    @field_validator("temp_table_patterns")
    @classmethod
    def validate_temp_table_patterns(cls, v: List[str]) -> List[str]:
        for pattern in v:
            try:
                re.compile(pattern)
            except re.error as e:
                raise ValueError(
                    f"Invalid regex in temp_table_patterns: '{pattern}': {e}"
                ) from e
        return v

    @model_validator(mode="after")
    def validate_query_source(self) -> "SqlQueriesSourceConfig":
        if bool(self.query_file) == bool(self.query_source):
            raise ValueError("Exactly one of query_file or query_source must be set.")
        return self

    @model_validator(mode="after")
    def validate_s3_config(self) -> "SqlQueriesSourceConfig":
        if self.query_file and is_s3_uri(self.query_file) and not self.aws_config:
            raise ValueError(
                "aws_config is required when query_file is an S3 URI (s3://)"
            )
        return self

    @cached_property
    def _compiled_temp_table_patterns(self) -> List["re.Pattern[str]"]:
        return [
            re.compile(pattern, re.IGNORECASE) for pattern in self.temp_table_patterns
        ]
