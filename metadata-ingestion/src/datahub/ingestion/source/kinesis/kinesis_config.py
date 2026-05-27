from typing import Any, Dict, Literal, Optional

import pydantic
from pydantic import Field

from datahub.configuration.common import AllowDenyPattern, ConfigModel
from datahub.configuration.source_common import (
    EnvConfigMixin,
    PlatformInstanceConfigMixin,
)
from datahub.ingestion.source.aws.aws_common import AwsConnectionConfig
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)
from datahub.metadata.schema_classes import FabricTypeClass

# Closed set of URN-target platforms our Firehose extractor produces lineage edges
# to. Used as the key type for `destination_platform_map` (pydantic v2 enforces the
# closed set at parse time) AND as the ClassVar on `DestinationHandler` subclasses
# in `kinesis_firehose_destinations`. "glue" is included for Firehose's
# DataFormatConversionConfiguration.SchemaConfiguration — it references a Glue
# table that defines the Parquet/ORC output schema; not a Firehose destination
# type itself, but an override target for URN construction.
DestinationPlatform = Literal[
    "s3", "redshift", "elasticsearch", "snowflake", "iceberg", "mongodb", "glue"
]

# Closed set of AWS service names that `make_client` knows how to configure
# (endpoint override + retry config). Narrowing this from `str` catches
# typos at typecheck time: `make_client(session, "firhose")` fails the
# Literal check instead of returning a misconfigured boto3 client whose
# first API call surfaces an unhelpful botocore error. "sts" intentionally
# omitted — `_resolve_account_id` constructs it directly via
# `session.client("sts")` because it runs before the rest of the config is
# loaded.
AwsService = Literal["kinesis", "firehose", "glue"]


class DestinationPlatformDetail(ConfigModel):
    """Per-destination-platform override for cross-platform lineage URN construction.

    Used by `KinesisSourceConfig.destination_platform_map` so Firehose lineage edges
    to S3 / Redshift / Snowflake / etc. resolve to the same URNs those platforms'
    own DataHub ingestion sources emit.
    """

    platform_instance: Optional[str] = Field(
        default=None,
        description=(
            "platform_instance of the destination platform (must match the "
            "ingestion of that platform). If unset, the downstream URN has no "
            "platform_instance slot."
        ),
    )
    env: Optional[str] = Field(
        default=None,
        description=(
            "env (e.g., PROD, DEV) of the destination. If unset, inherits from "
            "this source's env."
        ),
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description=(
            "Whether to lowercase the URN name (`<db>.<schema>.<table>` etc.) "
            "before emitting it. Mirrors the same-named field on platforms' "
            "own DataHub source configs (notably Snowflake). Defaults to "
            "`True`, matching the Snowflake source's default behavior. Set "
            "to `False` when your destination's source recipe set "
            "`convert_urns_to_lowercase=False` and your identifiers are "
            "case-sensitive (Iceberg, MongoDB, or quoted-identifier "
            "Snowflake / Redshift)."
        ),
    )

    @pydantic.field_validator("env")
    @classmethod
    def validate_env(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v
        # Validate against FabricType enum values (PROD, DEV, QA, etc.)
        valid = {
            k
            for k, val in vars(FabricTypeClass).items()
            if not k.startswith("_") and isinstance(val, str)
        }
        if v not in valid:
            raise ValueError(
                f"env must be a valid DataHub FabricType (one of {sorted(valid)}); got {v!r}"
            )
        return v


class KinesisGlueSchemaRegistryConfig(ConfigModel):
    """Glue Schema Registry attachment for Kinesis streams.

    Follows the Kafka source's `topic_subject_map` pattern (see
    `confluent_schema_registry.py:81-112`). AWS doesn't store stream→schema
    associations server-side; we resolve via explicit map OR naming convention.
    """

    enabled: bool = Field(
        default=False,
        description=(
            "When True, fetch Glue Schema Registry schemas for streams. "
            "Requires the additional `glue:Get*` / `glue:List*` IAM "
            "permissions listed in the connector prerequisites."
        ),
    )
    registry_name: str = Field(
        default="default-registry",
        description="Glue Schema Registry registry name to query.",
    )
    stream_schema_map: Dict[str, str] = Field(
        default_factory=dict,
        description=(
            "Explicit override map: stream name → schema name within "
            "registry_name. Wins over use_naming_convention."
        ),
    )
    use_naming_convention: bool = Field(
        default=False,
        description=(
            "Heuristic fallback: when `stream_schema_map` has no entry for a stream, "
            "look up a Glue schema whose name matches the stream name. Disabled by "
            "default — unlike Kafka + Confluent Schema Registry (which has a "
            "documented `TopicNameStrategy`), AWS Glue Schema Registry has NO "
            "AWS-defined relationship between a Kinesis Data Stream and a schema: "
            "schemas are chosen per-record by producers, and one stream may carry "
            "multiple schemas. Enable only if your organization has adopted "
            "schema-name == stream-name as an internal convention."
        ),
    )

    @pydantic.model_validator(mode="after")
    def warn_if_configured_but_disabled(self) -> "KinesisGlueSchemaRegistryConfig":
        """Surface configured-but-disabled state as a config error.

        Setting stream_schema_map without enabled=True is silently inert —
        user almost certainly meant to enable GSR.
        """
        if not self.enabled and self.stream_schema_map:
            raise ValueError(
                "glue_schema_registry has stream_schema_map set but enabled=False. "
                "Set enabled=True to use Glue Schema Registry, OR remove the "
                "stream_schema_map field."
            )
        return self


class KinesisSourceConfig(
    StatefulIngestionConfigBase,
    PlatformInstanceConfigMixin,
    EnvConfigMixin,
):
    """Top-level Kinesis source config.

    Composes AwsConnectionConfig (creds + region + endpoint override) with
    feature toggles, filters, and cross-platform lineage overrides.
    """

    aws_config: AwsConnectionConfig = Field(
        default_factory=AwsConnectionConfig,
        description=(
            "Standard DataHub AWS connection config (creds, region, role assumption, "
            "endpoint override). Optional — when omitted, boto3 resolves everything "
            "from the standard credential chain (env vars, shared credentials file, "
            "IAM role, SSO profile)."
        ),
    )

    include_streams: bool = Field(
        default=True,
        description="Extract Kinesis Data Streams (KDS).",
    )
    stream_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns for which Kinesis streams to include.",
    )

    include_firehose: bool = Field(
        default=True,
        description="Extract Kinesis Data Firehose (KDF) delivery streams.",
    )
    delivery_stream_pattern: AllowDenyPattern = Field(
        default_factory=AllowDenyPattern.allow_all,
        description="Regex patterns for which Firehose delivery streams to include.",
    )

    include_table_lineage: bool = Field(
        default=True,
        description="Emit Firehose-stream → destination lineage edges.",
    )

    extract_tags: bool = Field(
        default=True,
        description="Extract AWS resource tags as DataHub globalTags.",
    )
    extract_owners: bool = Field(
        default=True,
        description=(
            "Extract owner information from AWS resource tags. The tag key is "
            "configurable via `owner_tag_key`."
        ),
    )
    owner_tag_key: str = Field(
        default="owner",
        description="Tag key whose value identifies the resource owner.",
    )

    glue_schema_registry: KinesisGlueSchemaRegistryConfig = Field(
        default_factory=KinesisGlueSchemaRegistryConfig,
        description="Glue Schema Registry integration (disabled by default).",
    )

    destination_platform_map: Dict[DestinationPlatform, DestinationPlatformDetail] = (
        Field(
            default_factory=dict,
            description=(
                "Map destination platform name (one of 's3', 'redshift', 'snowflake', "
                "'iceberg', 'mongodb', 'elasticsearch') to platform_instance + env "
                "overrides. REQUIRED when the destination platform was ingested with "
                "a platform_instance — without this, lineage edges reference "
                "non-existent URNs (silent failure mode). Unknown platform keys are "
                "rejected at parse time by the DestinationPlatform Literal."
            ),
        )
    )

    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None,
        description="Stateful ingestion configuration for stale entity removal.",
    )

    def make_client(self, session: Any, service: AwsService) -> Any:
        """Construct a boto3 client with endpoint override + retry config wired.

        Centralizes the ``endpoint_url=`` + ``config=`` boilerplate that's otherwise
        repeated at every ``session.client(<svc>)`` call site (six in total —
        kinesis_stream, kinesis_firehose, glue in KinesisSource.__init__, plus
        three in test_connection). Returns ``Any`` because boto3 clients lack
        proper type stubs.
        """
        return session.client(
            service,
            endpoint_url=self.aws_config.aws_endpoint_url,
            config=self.aws_config._aws_config(),
        )
