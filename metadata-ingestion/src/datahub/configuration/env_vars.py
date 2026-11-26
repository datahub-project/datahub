# ABOUTME: Central registry for all environment variables used in metadata-ingestion.
# ABOUTME: All environment variable reads should go through this module for discoverability and maintainability.

import logging
import os
from typing import Optional

from datahub.configuration.env_var_registry import (
    EnvVarAudience,
    EnvVarGroup,
    EnvVarSpec,
    EnvVarTier,
    env_var,
    register_env_var,
)

logger = logging.getLogger(__name__)

# Sink types that a no-sink recipe can default to. These match the YAML `sink:`
# type names (datahub-rest / datahub-kafka), not shorthand.
DEFAULT_SINK_REST = "datahub-rest"
DEFAULT_SINK_KAFKA = "datahub-kafka"

# ============================================================================
# Core DataHub Configuration
# ============================================================================


@env_var(
    name="DATAHUB_GMS_URL",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
)
def get_gms_url() -> Optional[str]:
    """Complete GMS URL (takes precedence over separate host/port)."""
    return os.getenv("DATAHUB_GMS_URL")


@env_var(
    name="DATAHUB_GMS_HOST",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    deprecated=True,
    replaced_by="DATAHUB_GMS_URL",
)
def get_gms_host() -> Optional[str]:
    """Host name of the GMS instance, combined with DATAHUB_GMS_PORT and DATAHUB_GMS_PROTOCOL."""
    return os.getenv("DATAHUB_GMS_HOST")


@env_var(
    name="DATAHUB_GMS_PORT",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    deprecated=True,
    replaced_by="DATAHUB_GMS_URL",
)
def get_gms_port() -> Optional[str]:
    """Port of the GMS instance, used with DATAHUB_GMS_HOST."""
    return os.getenv("DATAHUB_GMS_PORT")


@env_var(
    name="DATAHUB_GMS_PROTOCOL",
    type=str,
    group=EnvVarGroup.CONNECTION,
    default="http",
    tier=EnvVarTier.BASIC,
    choices=["http", "https"],
    deprecated=True,
    replaced_by="DATAHUB_GMS_URL",
)
def get_gms_protocol() -> str:
    """Scheme used with DATAHUB_GMS_HOST and DATAHUB_GMS_PORT."""
    return os.getenv("DATAHUB_GMS_PROTOCOL", "http")


@env_var(
    name="DATAHUB_GMS_TOKEN",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    secret=True,
)
def get_gms_token() -> Optional[str]:
    """Authentication token for GMS."""
    return os.getenv("DATAHUB_GMS_TOKEN")


@env_var(
    name="DATAHUB_USERNAME",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
)
def get_username() -> Optional[str]:
    """Username for generating access tokens."""
    return os.getenv("DATAHUB_USERNAME")


@env_var(
    name="DATAHUB_PASSWORD",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    secret=True,
)
def get_password() -> Optional[str]:
    """Password for generating access tokens."""
    return os.getenv("DATAHUB_PASSWORD")


@env_var(
    name="DATAHUB_SYSTEM_CLIENT_ID",
    type=str,
    group=EnvVarGroup.CONNECTION,
)
def get_system_client_id() -> Optional[str]:
    """System client id, for service-to-service authentication against GMS."""
    return os.getenv("DATAHUB_SYSTEM_CLIENT_ID")


@env_var(
    name="DATAHUB_SYSTEM_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.CONNECTION,
    secret=True,
)
def get_system_client_secret() -> Optional[str]:
    """System client secret paired with DATAHUB_SYSTEM_CLIENT_ID."""
    return os.getenv("DATAHUB_SYSTEM_CLIENT_SECRET")


@env_var(
    name="DATAHUB_SKIP_CONFIG",
    risky=True,
    type=bool,
    group=EnvVarGroup.CONNECTION,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true", "false"],
)
def get_skip_config() -> bool:
    """Skip loading saved connection files, so connection settings come from the environment only.

    Cannot be combined with `--profile`, and `DATAHUB_PROFILE` is ignored when it is set.
    """
    return os.getenv("DATAHUB_SKIP_CONFIG", "").lower() == "true"


@env_var(
    name="DATAHUB_GMS_BASE_PATH",
    type=str,
    group=EnvVarGroup.CONNECTION,
    default="",
)
def get_gms_base_path() -> str:
    """Path prefix all GMS API calls are made under, for deployments served from a sub-path."""
    return os.getenv("DATAHUB_GMS_BASE_PATH", "")


# ============================================================================
# Ingestion Sink Configuration
# ============================================================================


@env_var(
    name="DATAHUB_INGESTION_DEFAULT_SINK",
    type=str,
    group=EnvVarGroup.SINK,
    default=DEFAULT_SINK_REST,
    tier=EnvVarTier.BASIC,
    choices=["datahub-rest", "datahub-kafka"],
)
def get_ingestion_default_sink() -> str:
    """Sink type for a recipe that does not specify one.

    Returns one of ``datahub-rest`` (the default) or ``datahub-kafka``. An
    unset, blank, or unrecognized ``DATAHUB_INGESTION_DEFAULT_SINK`` resolves to
    ``datahub-rest`` -- unrecognized values also log a warning -- so the historical
    REST behavior is always the safe fallback.
    """
    raw = os.getenv("DATAHUB_INGESTION_DEFAULT_SINK", DEFAULT_SINK_REST).strip().lower()
    if raw not in (DEFAULT_SINK_REST, DEFAULT_SINK_KAFKA):
        logger.warning(
            "Unrecognized DATAHUB_INGESTION_DEFAULT_SINK=%r; falling back to %s. "
            "Accepted values: %s or %s.",
            raw,
            DEFAULT_SINK_REST,
            DEFAULT_SINK_REST,
            DEFAULT_SINK_KAFKA,
        )
        return DEFAULT_SINK_REST
    return raw


@env_var(
    name="KAFKA_BOOTSTRAP_SERVER",
    type=str,
    group=EnvVarGroup.SINK,
)
def get_kafka_sink_bootstrap() -> Optional[str]:
    """Kafka bootstrap servers for the default Kafka ingestion sink.

    Reuses the DataHub-wide KAFKA_BOOTSTRAP_SERVER convention (same broker GMS /
    consumers use) rather than a sink-specific var, so ingestion targets the
    same cluster as the rest of the platform.
    """
    return os.getenv("KAFKA_BOOTSTRAP_SERVER")


def _get_int_env(var: str, default: int) -> int:
    """Parse an int env var, raising a clear error naming the offending variable.

    Avoids a bare ValueError ("invalid literal for int()...") that operators
    can't trace back to a specific env var / ConfigMap key.
    """
    raw = os.getenv(var)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError:
        raise ValueError(
            f"Environment variable {var}={raw!r} is not a valid integer."
        ) from None


@env_var(
    name="DATAHUB_KAFKA_SINK_QUEUE_MAX_KBYTES",
    type=int,
    group=EnvVarGroup.SINK,
    default=131072,
)
def get_kafka_sink_queue_max_kbytes() -> int:
    """Per-producer local queue size cap (KiB) for the default Kafka ingestion sink.

    Bounds producer buffer memory so backpressure engages before OOM. Peak
    buffer memory ~= num_producers * this value. Default 128 MiB.
    """
    return _get_int_env("DATAHUB_KAFKA_SINK_QUEUE_MAX_KBYTES", 131072)


@env_var(
    name="DATAHUB_KAFKA_SINK_QUEUE_MAX_MESSAGES",
    type=int,
    group=EnvVarGroup.SINK,
    default=20000,
)
def get_kafka_sink_queue_max_messages() -> int:
    """Per-producer local queue size cap (msg count) for the default Kafka ingestion sink."""
    return _get_int_env("DATAHUB_KAFKA_SINK_QUEUE_MAX_MESSAGES", 20000)


@env_var(
    name="DATAHUB_KAFKA_SINK_LINGER_MS",
    type=int,
    group=EnvVarGroup.SINK,
    default=100,
)
def get_kafka_sink_linger_ms() -> int:
    """Producer linger.ms (send batching window) for the default Kafka ingestion sink."""
    return _get_int_env("DATAHUB_KAFKA_SINK_LINGER_MS", 100)


@env_var(
    name="DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES",
    type=int,
    group=EnvVarGroup.SINK,
    default=5242880,
)
def get_kafka_sink_max_message_bytes() -> int:
    """Max serialized message size (bytes) the default Kafka ingestion sink will send.

    librdkafka's own default is ~1 MiB -- stricter than GMS's aspect limit, so
    without this an aspect REST would accept could be rejected over Kafka.
    Defaults to 5 MiB to match DataHub's default MCP topic ``max.message.bytes``.
    Must be <= the broker/topic ``max.message.bytes`` or the broker rejects the
    produce. An oversized aspect degrades to the sink's REST fallback when one is
    configured (parity with the REST sink's ~16 MiB cap); with no fallback it
    fails the record (reported), never silent.
    """
    return _get_int_env("DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES", 5242880)


@env_var(
    name="DATAHUB_KAFKA_SINK_INIT_PROBE_TIMEOUT",
    type=int,
    group=EnvVarGroup.SINK,
    default=10,
)
def get_kafka_sink_init_probe_timeout() -> int:
    """Per-check timeout (seconds) for the Kafka ingestion sink's init reachability probe.

    The probe runs whenever the Kafka default sink is selected
    (DATAHUB_INGESTION_DEFAULT_SINK=kafka) -- no separate on/off flag. Bounds
    both the broker metadata fetch and the schema-registry request so a
    misconfigured endpoint dies in seconds at startup. Default 10s.
    """
    return _get_int_env("DATAHUB_KAFKA_SINK_INIT_PROBE_TIMEOUT", 10)


# Env-configured OAuth (DATAHUB_AUTH_TYPE)
# Parsed into an AuthConfig by datahub.ingestion.auth.env.
# ============================================================================


@env_var(
    name="DATAHUB_AUTH_TYPE",
    type=str,
    group=EnvVarGroup.OAUTH,
    choices=["k8s_oidc", "azure_entra", "oidc_client_credentials"],
)
def get_auth_type() -> Optional[str]:
    """Env-configured OAuth provider type (k8s_oidc / azure_entra / oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_TYPE")


@env_var(
    name="DATAHUB_AUTH_TOKEN_FILE",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_token_file() -> Optional[str]:
    """Path to the projected service-account token file (k8s_oidc)."""
    return os.getenv("DATAHUB_AUTH_TOKEN_FILE")


@env_var(
    name="DATAHUB_AUTH_AUDIENCE",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_audience() -> Optional[str]:
    """Expected token audience (k8s_oidc, oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_AUDIENCE")


@env_var(
    name="DATAHUB_AUTH_SCOPE",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_scope() -> Optional[str]:
    """OAuth scope (oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_SCOPE")


@env_var(
    name="DATAHUB_AUTH_TOKEN_ENDPOINT",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_token_endpoint() -> Optional[str]:
    """IdP token endpoint URL (oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_TOKEN_ENDPOINT")


@env_var(
    name="DATAHUB_AUTH_CLIENT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_client_id() -> Optional[str]:
    """OAuth client id (oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_CLIENT_ID")


@env_var(
    name="DATAHUB_AUTH_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.OAUTH,
    secret=True,
)
def get_auth_client_secret() -> Optional[str]:
    """OAuth client secret (oidc_client_credentials)."""
    return os.getenv("DATAHUB_AUTH_CLIENT_SECRET")


@env_var(
    name="DATAHUB_AUTH_AZURE_TENANT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_azure_tenant_id() -> Optional[str]:
    """Azure Entra tenant id (azure_entra)."""
    return os.getenv("DATAHUB_AUTH_AZURE_TENANT_ID")


@env_var(
    name="DATAHUB_AUTH_AZURE_CLIENT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_azure_client_id() -> Optional[str]:
    """Azure Entra client id (azure_entra)."""
    return os.getenv("DATAHUB_AUTH_AZURE_CLIENT_ID")


@env_var(
    name="DATAHUB_AUTH_AZURE_SCOPE",
    type=str,
    group=EnvVarGroup.OAUTH,
)
def get_auth_azure_scope() -> Optional[str]:
    """Azure Entra token scope (azure_entra)."""
    return os.getenv("DATAHUB_AUTH_AZURE_SCOPE")


@env_var(
    name="DATAHUB_AUTH_AZURE_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.OAUTH,
    secret=True,
)
def get_auth_azure_client_secret() -> Optional[str]:
    """Azure Entra client secret; omit for workload identity (azure_entra)."""
    return os.getenv("DATAHUB_AUTH_AZURE_CLIENT_SECRET")


# ============================================================================
# REST Emitter Configuration
# ============================================================================


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES",
    type=str,
    group=EnvVarGroup.EMITTER,
    default="4",
)
def get_rest_emitter_default_retry_max_times() -> str:
    """Times a failed request to GMS is retried before the emit fails."""
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES", "4")


@env_var(
    name="DATAHUB_REST_EMITTER_429_RETRY_MULTIPLIER",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=2,
)
def get_rest_emitter_429_retry_multiplier() -> int:
    """Multiplier applied to the retry budget when GMS replies 429 (rate limited).

    Effective retries are this value multiplied by DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES.
    """
    return int(os.getenv("DATAHUB_REST_EMITTER_429_RETRY_MULTIPLIER", "2")) or 1


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_POOL_CONNECTIONS",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=100,
)
def get_rest_emitter_default_pool_connections() -> int:
    """Max number of different hosts to cache connection pools for."""
    return int(os.getenv("DATAHUB_REST_EMITTER_DEFAULT_POOL_CONNECTIONS", "100"))


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_POOL_MAXSIZE",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=100,
)
def get_rest_emitter_default_pool_maxsize() -> int:
    """Max connections per host in each connection pool."""
    return int(os.getenv("DATAHUB_REST_EMITTER_DEFAULT_POOL_MAXSIZE", "100"))


@env_var(
    name="DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_BYTES",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=15 * 1024 * 1024,
)
def get_rest_emitter_batch_max_payload_bytes() -> int:
    """Maximum payload size in bytes for batch operations."""
    return int(
        os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_BYTES", str(15 * 1024 * 1024))
    )


@env_var(
    name="DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=200,
)
def get_rest_emitter_batch_max_payload_length() -> int:
    """Maximum number of MCPs per batch."""
    return int(os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH", "200"))


@env_var(
    name="DATAHUB_EMIT_MODE",
    type=str,
    group=EnvVarGroup.EMITTER,
    choices=["SYNC_PRIMARY", "SYNC_WAIT", "ASYNC", "ASYNC_WAIT"],
)
def get_emit_mode() -> Optional[str]:
    """Emission mode (SYNC_PRIMARY, SYNC_WAIT, ASYNC, ASYNC_WAIT)."""
    return os.getenv("DATAHUB_EMIT_MODE")


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT",
    type=str,
    group=EnvVarGroup.EMITTER,
    choices=["RESTLI", "OPENAPI"],
)
def get_rest_emitter_default_endpoint() -> Optional[str]:
    """REST endpoint type (RESTLI or OPENAPI)."""
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT")


@env_var(
    name="DATAHUB_EMITTER_TRACE",
    type=bool,
    group=EnvVarGroup.EMITTER,
    default=False,
    choices=["true"],
)
def get_emitter_trace() -> bool:
    """Enable detailed emitter tracing."""
    return os.getenv("DATAHUB_EMITTER_TRACE", "").lower() == "true"


# ============================================================================
# REST Sink Configuration
# ============================================================================


@env_var(
    name="DATAHUB_REST_SINK_DEFAULT_MAX_THREADS",
    type=int,
    group=EnvVarGroup.REST_SINK,
    default=15,
    recipe_field="max_threads",
)
def get_rest_sink_default_max_threads() -> int:
    """Threads the REST sink uses to emit in parallel. Raise for throughput, lower to ease load on GMS."""
    return int(os.getenv("DATAHUB_REST_SINK_DEFAULT_MAX_THREADS", "15"))


@env_var(
    name="DATAHUB_REST_SINK_DEFAULT_MODE",
    type=str,
    group=EnvVarGroup.REST_SINK,
    choices=["SYNC", "ASYNC", "ASYNC_BATCH"],
    recipe_field="mode",
)
def get_rest_sink_default_mode() -> Optional[str]:
    """Sink mode (SYNC, ASYNC, ASYNC_BATCH)."""
    return os.getenv("DATAHUB_REST_SINK_DEFAULT_MODE")


@env_var(
    name="DATAHUB_REST_SINK_DEFAULT_TCP_KEEPALIVE",
    type=bool,
    group=EnvVarGroup.REST_SINK,
    default=False,
    choices=["true"],
    recipe_field="tcp_keepalive",
)
def get_rest_sink_default_tcp_keepalive() -> bool:
    """Default value for tcp_keepalive on the REST sink / DataHub client"""
    return os.getenv("DATAHUB_REST_SINK_DEFAULT_TCP_KEEPALIVE", "").lower() == "true"


# ============================================================================
# Telemetry & Monitoring
# ============================================================================


@env_var(
    name="DATAHUB_TELEMETRY_TIMEOUT",
    type=str,
    group=EnvVarGroup.TELEMETRY,
    default="10",
    tier=EnvVarTier.BASIC,
)
def get_telemetry_timeout() -> str:
    """Seconds to wait when sending anonymous usage telemetry before giving up."""
    return os.getenv("DATAHUB_TELEMETRY_TIMEOUT", "10")


@env_var(
    name="SENTRY_DSN",
    type=str,
    group=EnvVarGroup.TELEMETRY,
    secret=True,
)
def get_sentry_dsn() -> Optional[str]:
    """Sentry error tracking DSN."""
    return os.getenv("SENTRY_DSN")


@env_var(
    name="SENTRY_ENVIRONMENT",
    type=str,
    group=EnvVarGroup.TELEMETRY,
    default="dev",
)
def get_sentry_environment() -> str:
    """Sentry environment (dev/prod)."""
    return os.getenv("SENTRY_ENVIRONMENT", "dev")


# ============================================================================
# Report Configuration
# ============================================================================


@env_var(
    name="DATAHUB_REPORT_FAILURE_SAMPLE_SIZE",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="report_failure_sample_size",
)
def get_report_failure_sample_size() -> int:
    """Maximum number of failure entries to include in the report."""
    return int(os.getenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_REPORT_WARNING_SAMPLE_SIZE",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="report_warning_sample_size",
)
def get_report_warning_sample_size() -> int:
    """Maximum number of warning entries to include in the report."""
    return int(os.getenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_REPORT_INFO_SAMPLE_SIZE",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="report_info_sample_size",
)
def get_report_info_sample_size() -> int:
    """Maximum number of info entries to include in the report."""
    return int(os.getenv("DATAHUB_REPORT_INFO_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_FAILURES",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_failures",
)
def get_progress_report_max_failures() -> int:
    """Maximum number of failure entries in intermediate progress reports."""
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_FAILURES", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_WARNINGS",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_warnings",
)
def get_progress_report_max_warnings() -> int:
    """Maximum number of warning entries in intermediate progress reports."""
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_WARNINGS", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_INFOS",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_infos",
)
def get_progress_report_max_infos() -> int:
    """Maximum number of info entries in intermediate progress reports."""
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_INFOS", "10"))


# ============================================================================
# Logging & Debug Configuration
# ============================================================================


@env_var(
    name="DATAHUB_SUPPRESS_LOGGING_MANAGER",
    type=str,
    group=EnvVarGroup.LOGGING,
    audience=EnvVarAudience.INTERNAL,
)
def get_suppress_logging_manager() -> Optional[str]:
    """Suppress DataHub logging manager initialization."""
    return os.getenv("DATAHUB_SUPPRESS_LOGGING_MANAGER")


@env_var(
    name="NO_COLOR",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true"],
)
def get_no_color() -> bool:
    """Disable ANSI colour in CLI output. Follows the no-color.org convention."""
    return os.getenv("NO_COLOR", "").lower() == "true"


@env_var(
    name="DATAHUB_TEST_MODE",
    type=str,
    group=EnvVarGroup.LOGGING,
    audience=EnvVarAudience.INTERNAL,
)
def get_test_mode() -> Optional[str]:
    """Indicates running in test context."""
    return os.getenv("DATAHUB_TEST_MODE")


@env_var(
    name="DATAHUB_DEBUG",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true"],
    risky=True,
)
def get_debug() -> bool:
    """Enable debug logging, the same as passing `--debug`.

    Debug logs expose sensitive information. Avoid it on production instances, especially with
    UI ingestion, where run logs are readable by anyone who can see the run.
    """
    return os.getenv("DATAHUB_DEBUG", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_DISABLE_SECRET_MASKING",
    risky=True,
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
    description=(
        "Disable secret masking in logs, for debugging. Credentials then appear in run output, "
        "which is visible to anyone who can see the ingestion run. Development only."
    ),
)
def get_disable_secret_masking() -> bool:
    """
    Disable secret masking for debugging purposes.

    WARNING: Only use this in development/debugging scenarios.
    Disabling secret masking will expose sensitive information in logs.
    """
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_DISABLE_RAW_ACCESS",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
)
def get_disable_agent_probe_raw_access() -> bool:
    """
    Refuse the recipe probe's raw passthrough commands (`sql`, `api`).

    For an operator who does not want an agent issuing its own queries or API
    calls against a source at all. It withholds only the commands that take a
    caller-supplied query or path.

    On most connectors the typed listings keep working, so recipe diagnosis
    still functions -- this said so unconditionally and was wrong about the
    two that matter most. Snowflake exposes `sql` as its ONLY probe command, so
    setting this withholds its probe entirely. BigQuery also declares
    `projects`, which takes no caller-supplied query or path, so its probe is
    narrowed rather than withheld: `projects` still authenticates and lists
    what the recipe would consider. The
    refusal message names what is left for the connector in hand rather than
    promising something in general.

    An environment variable rather than a recipe field because the agent authors
    the recipe: a field there would let it grant itself the access. Set it where
    the probe runs (the ingestion executor).
    """
    return os.getenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_DISABLED",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
)
def get_probe_disabled() -> bool:
    """Whether the probe may open a connection to the source at all.

    The wider switch. DATAHUB_PROBE_DISABLE_RAW_ACCESS withholds the
    caller-supplied `sql` and `api` passthroughs and leaves every typed
    listing live -- which is the right granularity for "no arbitrary
    queries" and the wrong one for "this agent does not touch my source".
    Those listings still authenticate and still return metadata.

    This refuses every `probe run <command>`, typed listing as much as
    passthrough. Enforced in run_probe_method, the one function every probe
    command funnels through, so it covers commands that do not exist yet.

    SCOPE, stated precisely because a security control that promises more
    than it delivers is worse than none: `recipe test-connection` is NOT
    gated. It opens a connection and authenticates, but it is not a probe
    command -- it predates the probe group and verifying that a recipe's
    credentials work is a different act from reading the source's metadata.
    An earlier version of this docstring listed it as covered, which was
    left behind when the gate on it was removed. If the requirement is
    "this process must not reach the source at all", this switch alone does
    not give you that.

    Deliberately not everything. `recipe describe`, `recipe scaffold`,
    `recipe validate`, `probe methods` and `probe filter` need no
    connection -- they read the connector's own declarations and judge names
    the caller already has. Disabling those too would stop an agent learning
    what a recipe needs or checking one it has written, which is work that
    never reaches the source. The line is the connection, not the feature.

    An environment variable rather than a recipe field, for the same reason
    as the switch above: the agent authors the recipe, so a field there
    would let it grant itself the access. Set it where the probe runs (the
    ingestion executor).
    """
    return os.getenv("DATAHUB_PROBE_DISABLED", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_VERBOSE_LOGS",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
)
def get_probe_verbose_logs() -> bool:
    """The local-debugging switch for probes: reused code's logs as logged,
    and the withheld text of its errors.

    Off by default: during a probe, every log record the framework did not
    write is scrubbed and loses its traceback, and a provider's silenced
    loggers are dropped (see agent.log_guard); an error the framework does
    not trust is named by class and code only (see agent.error_policy),
    because both quote connection strings and request URLs. When on, logs
    pass through and each withheld error text is appended to its label,
    still scrubbed. For a person debugging a connector, not for an agent.
    """
    return os.getenv("DATAHUB_PROBE_VERBOSE_LOGS", "").lower() in ("true", "1")


# ============================================================================
# Data Processing Configuration
# ============================================================================


def get_disable_executor_task_secret_scope() -> bool:
    """Turn off per-task secret scopes in the executor (on by default).

    An operational rollback, not a tuning knob. With scopes off, every task's
    secrets go to the one process-global registry, as they did before scopes
    existed: masking then over-masks across tasks -- a later task's output
    redacted against an earlier task's secret, with that task's variable
    name in the marker -- but never under-masks. Read per task, so a
    restart with the variable set is enough to switch.
    """
    return os.getenv("DATAHUB_DISABLE_EXECUTOR_TASK_SECRET_SCOPE", "").lower() in (
        "true",
        "1",
    )


@env_var(
    name="DATAHUB_SQL_AGG_QUERY_LOG",
    type=str,
    group=EnvVarGroup.SQL,
    default="DISABLED",
)
def get_sql_agg_query_log() -> str:
    """Whether the SQL aggregator logs the queries it parses. DISABLED, or ENABLED to log every statement."""
    return os.getenv("DATAHUB_SQL_AGG_QUERY_LOG", "DISABLED")


@env_var(
    name="DATAHUB_SQL_AGG_SKIP_JOINS",
    type=bool,
    group=EnvVarGroup.SQL,
    default=False,
    choices=["true"],
)
def get_sql_agg_skip_joins() -> bool:
    """Skip join processing in SQL aggregator (default: False)."""
    return os.getenv("DATAHUB_SQL_AGG_SKIP_JOINS", "").lower() == "true"


@env_var(
    name="DATAHUB_SQL_AGG_MAX_QUERIES_PER_DOWNSTREAM",
    risky=True,
    type=int,
    group=EnvVarGroup.SQL,
    default=0,
)
def get_sql_agg_max_queries_per_downstream() -> int:
    """Optional cap on the queries considered for one downstream table's lineage.

    0 (the default) means no cap: every query is considered, most recent first.
    Setting it keeps only the newest N parsed queries per downstream, which can drop
    an upstream that only an older query references; use it as a safety valve for
    runtime, not as the default.
    """
    var = "DATAHUB_SQL_AGG_MAX_QUERIES_PER_DOWNSTREAM"
    limit = _get_int_env(var, 0)
    if limit < 0:
        raise ValueError(
            f"Environment variable {var}={limit} must be 0 (no cap) or more."
        )
    return limit


@env_var(
    name="DATAHUB_SQL_PARSE_CACHE_SIZE",
    type=int,
    group=EnvVarGroup.SQL,
    default=1000,
)
def get_sql_parse_cache_size() -> int:
    """SQL parse result cache size (number of entries)."""
    return int(os.getenv("DATAHUB_SQL_PARSE_CACHE_SIZE", "1000"))


@env_var(
    name="SQL_LINEAGE_TIMEOUT_SECONDS",
    type=int,
    group=EnvVarGroup.SQL,
    default=10,
)
def get_sql_lineage_timeout_seconds() -> int:
    """Budget for generating column-level lineage for one query, in seconds.

    Measured on the wall clock, so a host that stalls under load spends the budget
    without the parser doing any work. Raise it where lineage is being dropped on
    queries that are merely slow to parse rather than pathological.
    """
    return _get_int_env("SQL_LINEAGE_TIMEOUT_SECONDS", 10)


@env_var(
    name="DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH",
    type=int,
    group=EnvVarGroup.SQL,
    risky=True,
)
def get_max_transform_operation_length() -> Optional[int]:
    """Truncate `transformOperation` in fine-grained lineage to N characters, for the rare case where column-level lineage aspects grow too large.

    Set it to at least 1; a value is kept unchanged when it is at most N + 3 characters, since
    truncating would not make it shorter. Unset means no truncation.

    With `incremental_lineage`, changing this leaves entries already stored under the old value
    in place -- they are only rewritten by a run with `incremental_lineage: false`.
    """
    var = "DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH"
    if not os.getenv(var, "").strip():
        return None
    length = _get_int_env(var, 1)
    if length < 1:
        raise ValueError(f"Environment variable {var}={length} must be at least 1.")
    return length


@env_var(
    name="DATAHUB_DATASET_URN_TO_LOWER",
    risky=True,
    type=str,
    group=EnvVarGroup.SQL,
    default="false",
    choices=["true", "false"],
)
def get_dataset_urn_to_lower() -> str:
    """Lowercase dataset names when building URNs. Changing this on an existing deployment creates duplicate entities."""
    return os.getenv("DATAHUB_DATASET_URN_TO_LOWER", "false")


# ============================================================================
# Integration-Specific Configuration
# ============================================================================


@env_var(
    name="KAFKA_SCHEMAREGISTRY_URL",
    type=str,
    group=EnvVarGroup.CONNECTORS,
)
def get_kafka_schema_registry_url() -> Optional[str]:
    """Schema registry the Kafka connector and Kafka sink use when none is set in the recipe."""
    return os.getenv("KAFKA_SCHEMAREGISTRY_URL")


@env_var(
    name="DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS",
    risky=True,
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=True,
    choices=["true", "false"],
)
def get_kafka_auto_register_schemas() -> bool:
    """Auto-register MCE/MCP schemas with the Kafka Schema Registry (default: True).

    Set to "false" when the ingestion user only has read access to the Schema
    Registry; the schemas must then be pre-registered.
    """
    return os.getenv("DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS", "true").lower() == "true"


@env_var(
    name="SPARK_VERSION",
    type=str,
    group=EnvVarGroup.CONNECTORS,
)
def get_spark_version() -> Optional[str]:
    """Spark version (for S3 source)."""
    return os.getenv("SPARK_VERSION")


@env_var(
    name="DATAHUB_VERTEXAI_DISABLE_PARALLELISM",
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=False,
    choices=["true", "1"],
)
def get_vertexai_disable_parallelism() -> bool:
    """Disable parallel resource fetching in Vertex AI connector."""
    return os.getenv("DATAHUB_VERTEXAI_DISABLE_PARALLELISM", "").lower() in (
        "1",
        "true",
    )


@env_var(
    name="DATAHUB_BIGQUERY_SCHEMA_PARALLELISM",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=20,
)
def get_bigquery_schema_parallelism() -> int:
    """Parallelism level for BigQuery schema extraction."""
    return int(os.getenv("DATAHUB_BIGQUERY_SCHEMA_PARALLELISM", "20"))


@env_var(
    name="DATAHUB_SNOWFLAKE_SCHEMA_PARALLELISM",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=20,
)
def get_snowflake_schema_parallelism() -> int:
    """Parallelism level for Snowflake schema extraction."""
    return int(os.getenv("DATAHUB_SNOWFLAKE_SCHEMA_PARALLELISM", "20"))


@env_var(
    name="DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=60,
)
def get_powerbi_m_query_parse_timeout() -> int:
    """Timeout for PowerBI M query parsing."""
    return int(os.getenv("DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT", "60"))


@env_var(
    name="DATAHUB_TRACE_POWERBI_MQUERY_PARSER",
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=False,
    choices=["true"],
)
def get_trace_powerbi_mquery_parser() -> bool:
    """Enable PowerBI M query parser tracing."""
    return os.getenv("DATAHUB_TRACE_POWERBI_MQUERY_PARSER", "").lower() == "true"


@env_var(
    name="DATAHUB_LOOKML_GIT_TEST_SSH_KEY",
    type=str,
    group=EnvVarGroup.CONNECTORS,
    audience=EnvVarAudience.INTERNAL,
    secret=True,
)
def get_lookml_git_test_ssh_key() -> Optional[str]:
    """SSH key for LookML Git tests."""
    return os.getenv("DATAHUB_LOOKML_GIT_TEST_SSH_KEY")


# ============================================================================
# AWS/Cloud Configuration
# ============================================================================


@env_var(
    name="AWS_LAMBDA_FUNCTION_NAME",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_aws_lambda_function_name() -> Optional[str]:
    """Indicates running in AWS Lambda."""
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME")


@env_var(
    name="AWS_EXECUTION_ENV",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_aws_execution_env() -> Optional[str]:
    """AWS execution environment."""
    return os.getenv("AWS_EXECUTION_ENV")


@env_var(
    name="AWS_WEB_IDENTITY_TOKEN_FILE",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_aws_web_identity_token_file() -> Optional[str]:
    """OIDC token file path."""
    return os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")


@env_var(
    name="AWS_ROLE_ARN",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_aws_role_arn() -> Optional[str]:
    """AWS role ARN for OIDC."""
    return os.getenv("AWS_ROLE_ARN")


@env_var(
    name="AWS_APP_RUNNER_SERVICE_ID",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_aws_app_runner_service_id() -> Optional[str]:
    """AWS App Runner service ID."""
    return os.getenv("AWS_APP_RUNNER_SERVICE_ID")


@env_var(
    name="ECS_CONTAINER_METADATA_URI_V4",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_ecs_container_metadata_uri_v4() -> Optional[str]:
    """ECS metadata endpoint v4."""
    return os.getenv("ECS_CONTAINER_METADATA_URI_V4")


@env_var(
    name="ECS_CONTAINER_METADATA_URI",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_ecs_container_metadata_uri() -> Optional[str]:
    """ECS metadata endpoint v3."""
    return os.getenv("ECS_CONTAINER_METADATA_URI")


@env_var(
    name="ELASTIC_BEANSTALK_ENVIRONMENT_NAME",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_elastic_beanstalk_environment_name() -> Optional[str]:
    """Elastic Beanstalk environment."""
    return os.getenv("ELASTIC_BEANSTALK_ENVIRONMENT_NAME")


# ============================================================================
# Docker & Local Development
# ============================================================================


@env_var(
    name="DATAHUB_COMPOSE_PROJECT_NAME",
    type=str,
    group=EnvVarGroup.DOCKER,
    default="datahub",
)
def get_compose_project_name() -> str:
    """Docker Compose project name."""
    return os.getenv("DATAHUB_COMPOSE_PROJECT_NAME", "datahub")


@env_var(
    name="DOCKER_COMPOSE_BASE",
    type=str,
    group=EnvVarGroup.DOCKER,
)
def get_docker_compose_base() -> Optional[str]:
    """Base path for Docker Compose files."""
    return os.getenv("DOCKER_COMPOSE_BASE")


@env_var(
    name="DATAHUB_VERSION",
    type=str,
    group=EnvVarGroup.DOCKER,
    description=(
        "Docker image tag quickstart runs: `quickstart`, a release `v*`, or a commit pin "
        "`sha-<short_sha>`. Unset here; `datahub docker quickstart` resolves it to `quickstart` "
        "and exports it for the compose stack."
    ),
)
def get_datahub_version() -> Optional[str]:
    """DataHub version (set during docker init)."""
    return os.getenv("DATAHUB_VERSION")


@env_var(
    name="DATAHUB_MAPPED_MYSQL_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_mapped_mysql_port() -> Optional[str]:
    """MySQL port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_MYSQL_PORT")


@env_var(
    name="DATAHUB_MAPPED_KAFKA_BROKER_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_mapped_kafka_broker_port() -> Optional[str]:
    """Kafka broker port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_KAFKA_BROKER_PORT")


@env_var(
    name="DATAHUB_MAPPED_ELASTIC_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_mapped_elastic_port() -> Optional[str]:
    """Elasticsearch port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_ELASTIC_PORT")


@env_var(
    name="METADATA_SERVICE_AUTH_ENABLED",
    type=str,
    group=EnvVarGroup.DOCKER,
    default="false",
    choices=["true", "false"],
)
def get_metadata_service_auth_enabled() -> str:
    """Enable/disable auth in Docker."""
    return os.getenv("METADATA_SERVICE_AUTH_ENABLED", "false")


@env_var(
    name="UI_INGESTION_DEFAULT_CLI_VERSION",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_ui_ingestion_default_cli_version() -> Optional[str]:
    """CLI version for UI ingestion (set during init)."""
    return os.getenv("UI_INGESTION_DEFAULT_CLI_VERSION")


# ============================================================================
# Utility & Helper Configuration
# ============================================================================


@env_var(
    name="DATAHUB_COMPONENT",
    type=str,
    group=EnvVarGroup.MISC,
    default="datahub",
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_datahub_component() -> str:
    """Component label reported in the User-Agent header, identifying which DataHub component made the call."""
    return os.getenv("DATAHUB_COMPONENT", "datahub")


@env_var(
    name="FORCE_LOCAL_QUICKSTART_MAPPING",
    type=str,
    group=EnvVarGroup.MISC,
    default="",
    audience=EnvVarAudience.INTERNAL,
)
def get_force_local_quickstart_mapping() -> str:
    """Force local quickstart mapping file."""
    return os.getenv("FORCE_LOCAL_QUICKSTART_MAPPING", "")


@env_var(
    name="DATAHUB_DATAPRODUCT_EXTERNAL_URL",
    type=str,
    group=EnvVarGroup.MISC,
)
def get_dataproduct_external_url() -> Optional[str]:
    """External URL for data products."""
    return os.getenv("DATAHUB_DATAPRODUCT_EXTERNAL_URL")


@env_var(
    name="OVERRIDE_SQLITE_VERSION_REQ",
    type=str,
    group=EnvVarGroup.MISC,
    default="",
    audience=EnvVarAudience.INTERNAL,
)
def get_override_sqlite_version_req() -> str:
    """Override SQLite version requirement."""
    return os.getenv("OVERRIDE_SQLITE_VERSION_REQ", "")


@env_var(
    name="UPDATE_ENTITY_REGISTRY",
    type=str,
    group=EnvVarGroup.MISC,
    default="false",
    audience=EnvVarAudience.INTERNAL,
)
def get_update_entity_registry() -> str:
    """Update entity registry during tests."""
    return os.getenv("UPDATE_ENTITY_REGISTRY", "false")


# Declared here rather than at its read site: _force_pure_python_sqlglot runs at
# interpreter startup from a .pth file, before this package (and pydantic) can be
# imported. See _force_pure_python_sqlglot._sqlglot_c_disabled.
register_env_var(
    EnvVarSpec(
        name="DATAHUB_SQLGLOT_DISABLE_C",
        type=bool,
        group=EnvVarGroup.SQL,
        default=False,
        choices=("true", "1", "yes", "false", "0", "no"),
        description=(
            "Force the pure-Python sqlglot tokenizer instead of the compiled extension. "
            "A workaround for platforms where the native tokenizer segfaults or is "
            "unavailable; SQL parsing gets slower."
        ),
    )
)


# Read by is_ci() below, which has no accessor of its own for it.
register_env_var(
    EnvVarSpec(
        name="GITHUB_ACTIONS",
        type=str,
        group=EnvVarGroup.MISC,
        audience=EnvVarAudience.AUTO_DETECTED,
        description=(
            "Set to `true` by GitHub Actions. Read as a fallback for CI detection on custom "
            "runners that do not set `CI`."
        ),
    )
)


@env_var(
    name="CI",
    type=str,
    group=EnvVarGroup.MISC,
    audience=EnvVarAudience.AUTO_DETECTED,
)
def get_ci() -> Optional[str]:
    """Set by most CI systems. Read to adjust output and to disable telemetry."""
    return os.getenv("CI")


def is_ci() -> bool:
    """Check if running in a CI environment.

    Returns True if running in a CI environment.

    Checks multiple indicators:
    - CI environment variable (set by most CI systems like GitHub Actions, GitLab CI, Travis CI, CircleCI, etc.)
    - GITHUB_ACTIONS (GitHub Actions specific, always set even on custom runners)

    This handles various CI value formats (true, 1, yes) and provides fallback
    detection for GitHub Actions workflows using custom runners that might not set
    the standard CI variable.
    """
    # Check standard CI variable (set by most CI systems)
    ci_value = os.getenv("CI", "").lower()
    if ci_value in ("true", "1", "yes"):
        return True

    # Fallback: GitHub Actions always sets GITHUB_ACTIONS=true
    # This handles Depot runners and other custom GitHub Actions runners
    return os.getenv("GITHUB_ACTIONS") == "true"
