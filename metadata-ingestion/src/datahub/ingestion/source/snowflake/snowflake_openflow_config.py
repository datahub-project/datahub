from typing import Optional

from pydantic import Field, model_validator

from datahub.configuration.common import AllowDenyPattern
from datahub.configuration.source_common import (
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
)
from datahub.emitter.mce_builder import ALL_ENV_TYPES
from datahub.ingestion.source.snowflake.snowflake_config import (
    SnowflakeIdentifierConfig,
)
from datahub.ingestion.source.snowflake.snowflake_connection import (
    SnowflakeConnectionConfig,
)
from datahub.ingestion.source.snowflake.snowflake_openflow_urns import (
    MAX_PLATFORM_INSTANCE_BYTES,
    encoded_urn_len,
)
from datahub.ingestion.source.state.stale_entity_removal_handler import (
    StatefulStaleMetadataRemovalConfig,
)
from datahub.ingestion.source.state.stateful_ingestion_base import (
    StatefulIngestionConfigBase,
)


def _resolved_env(value: Optional[str], default: str, field: str) -> str:
    """A foreign-platform env field, defaulted and normalised like `env` itself.

    Shared by snowflake_env and source_env so the two cannot drift. Both fold
    case exactly as EnvConfigMixin.env_must_be_one_of does: without it,
    `env: prod` is accepted and normalised while `snowflake_env: prod` -- the
    same word, in an adjacent field documented as needing to match -- is
    rejected outright. Both also validate eagerly, so a typo fails at
    recipe-load time rather than deep inside lineage emission.
    """
    if value is None:
        return default
    normalised = value.upper()
    if normalised not in ALL_ENV_TYPES:
        raise ValueError(f"{field} must be one of {ALL_ENV_TYPES}, found {value}")
    return normalised


class SnowflakeOpenflowSourceConfig(
    StatefulIngestionConfigBase,
    DatasetSourceConfigMixin,
    LowerCaseDatasetUrnConfigMixin,
):
    """Recipe configuration for the Snowflake Openflow source.

    Connection details are reused unchanged from the ``snowflake`` family. The
    three allow/deny patterns filter the deployment, runtime and connector
    objects that become Containers, DataFlows and DataJobs.

    The remaining fields exist because lineage here is derived from each
    connector's configuration rather than from observed queries: the URNs on
    both ends of an edge are constructed, not looked up, so they only line up
    with the recipes that actually own those tables if this config repeats
    those recipes' coordinates. ``snowflake_platform_instance`` /
    ``snowflake_env`` / ``convert_urns_to_lowercase`` must match the
    ``snowflake`` recipe covering the destination account, and
    ``source_platform_instance`` / ``source_env`` the recipe covering the
    upstream system. A mismatch produces well-formed lineage pointing at
    datasets that do not exist, which nothing downstream reports as an error.

    ``convert_urns_to_lowercase`` is deliberately one-sided: it folds the
    destination Snowflake identifiers only. Upstream identifiers are emitted
    exactly as the connector configuration spells them, because
    ``postgres`` / ``mysql`` / ``mssql`` all preserve case by default and a
    folded upstream URN would join to nothing. See
    ``SnowflakeOpenflowSource.get_excluded_workunit_processors``.
    """

    connection: SnowflakeConnectionConfig = Field(
        description="Snowflake connection details. Reused unchanged from the snowflake "
        "family, so key-pair and OAuth authentication behave identically.",
    )

    # StatefulIngestionConfigBase leaves this unparameterized, so without this
    # override it types as the plain StatefulIngestionConfig, which has no
    # remove_stale_metadata / fail_safe_threshold fields. The stale entity
    # removal handler reads both; without this override, stale removal is
    # silently a no-op and mypy does not catch it (the framework's call site
    # carries a `# type: ignore[arg-type]`).
    stateful_ingestion: Optional[StatefulStaleMetadataRemovalConfig] = Field(
        default=None, description="Stateful ingestion config."
    )

    deployment_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Openflow deployments to filter in ingestion.",
    )
    runtime_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Openflow runtimes to filter in ingestion.",
    )
    connector_pattern: AllowDenyPattern = Field(
        default=AllowDenyPattern.allow_all(),
        description="Regex patterns for Openflow connectors to filter in ingestion.",
    )

    snowflake_platform_instance: Optional[str] = Field(
        default=None,
        description="The `platform_instance` of the Snowflake ingestion that owns the "
        "tables Openflow writes to. Must match that recipe exactly. A mismatch "
        "produces well-formed lineage pointing at datasets that do not exist, which "
        "fails silently.",
    )
    snowflake_env: Optional[str] = Field(
        default=None,
        description="The `env` of the Snowflake ingestion that owns the destination "
        "tables. Defaults to this source's own `env`.",
    )
    source_platform_instance: Optional[str] = Field(
        default=None,
        description="The `platform_instance` of the ingestion that owns the upstream "
        "tables Openflow reads from (e.g. the Postgres recipe behind a CDC "
        "connector). Must match that recipe exactly. A mismatch produces "
        "well-formed lineage pointing at datasets that do not exist, which fails "
        "silently.",
    )
    source_env: Optional[str] = Field(
        default=None,
        description="The `env` of the ingestion that owns the upstream tables "
        "Openflow reads from. Defaults to this source's own `env`.",
    )
    convert_urns_to_lowercase: bool = Field(
        default=True,
        description="Whether to lowercase the destination Snowflake dataset URNs. Must "
        "match the `snowflake` recipe pointed at the same account, or the URNs will "
        "not line up. Inherited from LowerCaseDatasetUrnConfigMixin with default "
        "overridden to True for compatibility with Snowflake identifiers. Applies to "
        "the destination side only: upstream (Postgres/MySQL/SQL Server) identifiers "
        "are always emitted verbatim, matching those sources' case-preserving "
        "default.",
    )

    include_table_lineage: bool = Field(
        default=True,
        description="Emit table-level lineage from each connector's configuration to "
        "the Snowflake tables it writes.",
    )

    include_connector_external_url: Optional[bool] = Field(
        default=None,
        description=(
            "Emit each connector's NiFi canvas URL as the DataFlow's external "
            "link. The URL is per-runtime, so it costs one DESCRIBE OPENFLOW "
            "CONNECTOR per distinct runtime, not per connector -- SHOW does not "
            "return it. `null` (the default) fetches the links unless the "
            "account has more than 500 distinct runtimes, where even that "
            "per-runtime round trip would dominate the run; "
            "`true` always fetches them; `false` never does. Tri-state rather "
            "than a bool whose meaning depends on whether it appears in the "
            "recipe -- that distinction does not survive a recipe round-trip."
        ),
    )

    @model_validator(mode="after")
    def platform_instance_must_leave_room_in_the_urn(
        self,
    ) -> "SnowflakeOpenflowSourceConfig":
        for field, value in (
            ("platform_instance", self.platform_instance),
            ("snowflake_platform_instance", self.snowflake_platform_instance),
            ("source_platform_instance", self.source_platform_instance),
        ):
            if value is None:
                continue
            encoded = encoded_urn_len(value)
            if encoded > MAX_PLATFORM_INSTANCE_BYTES:
                raise ValueError(
                    f"{field} is {encoded} bytes once URL-encoded, over the "
                    f"{MAX_PLATFORM_INSTANCE_BYTES}-byte limit this source "
                    "allows. It appears in every urn and cannot be shortened "
                    "the way a connector name can, so a longer one would make "
                    "urns that DataHub rejects. Non-ASCII characters cost three "
                    "bytes each once encoded."
                )
        return self

    @model_validator(mode="after")
    def default_snowflake_env_to_env(self) -> "SnowflakeOpenflowSourceConfig":
        self.snowflake_env = _resolved_env(
            self.snowflake_env, self.env, "snowflake_env"
        )
        return self

    source_convert_urns_to_lowercase: bool = Field(
        default=False,
        description="Whether the upstream system's own ingestion lowercases its "
        "dataset URNs, i.e. whether its recipe sets `convert_urns_to_lowercase` "
        "explicitly. When enabled this folds the upstream identifier AND "
        "`source_platform_instance`, because the pipeline-level pass that recipe "
        "engages folds the whole URN name, prefix included. "
        "Defaults to False because `postgres`, `mysql` and `mssql` all preserve "
        "identifier case by default — but DataHub's own MSSQL source warns "
        "operators to enable it for lineage, so an operator who followed that "
        "advice needs this set to True or the upstream URNs this connector emits "
        "will not match the ones that recipe wrote.",
    )

    @model_validator(mode="after")
    def default_source_env_to_env(self) -> "SnowflakeOpenflowSourceConfig":
        self.source_env = _resolved_env(self.source_env, self.env, "source_env")
        return self

    def get_snowflake_identifier_config(self) -> SnowflakeIdentifierConfig:
        return SnowflakeIdentifierConfig(
            platform_instance=self.snowflake_platform_instance,
            env=self.snowflake_env,
            convert_urns_to_lowercase=self.convert_urns_to_lowercase,
        )
