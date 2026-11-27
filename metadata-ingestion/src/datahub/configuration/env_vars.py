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
    description=(
        "Full URL of the GMS instance, including scheme and any port. `DATAHUB_GMS_PORT` wins "
        "over this: when it is set, the URL is rebuilt from `DATAHUB_GMS_HOST`, "
        "`DATAHUB_GMS_PORT` and `DATAHUB_GMS_PROTOCOL` and this value is ignored. Leave those "
        "unset when using this one."
    ),
)
def get_gms_url() -> Optional[str]:
    return os.getenv("DATAHUB_GMS_URL")


@env_var(
    name="DATAHUB_GMS_HOST",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    deprecated=True,
    replaced_by="DATAHUB_GMS_URL",
    description=(
        "Host name of the GMS instance, combined with DATAHUB_GMS_PORT and "
        "DATAHUB_GMS_PROTOCOL."
    ),
)
def get_gms_host() -> Optional[str]:
    return os.getenv("DATAHUB_GMS_HOST")


@env_var(
    name="DATAHUB_GMS_PORT",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    deprecated=True,
    replaced_by="DATAHUB_GMS_URL",
    description=(
        "Port of the GMS instance, used with DATAHUB_GMS_HOST. Setting it overrides "
        "DATAHUB_GMS_URL entirely, which is rarely what you want."
    ),
)
def get_gms_port() -> Optional[str]:
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
    description="Scheme used with DATAHUB_GMS_HOST and DATAHUB_GMS_PORT.",
)
def get_gms_protocol() -> str:
    return os.getenv("DATAHUB_GMS_PROTOCOL", "http")


@env_var(
    name="DATAHUB_GMS_TOKEN",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    secret=True,
    description="Authentication token for GMS.",
)
def get_gms_token() -> Optional[str]:
    return os.getenv("DATAHUB_GMS_TOKEN")


@env_var(
    name="DATAHUB_USERNAME",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    description="Username for generating access tokens.",
)
def get_username() -> Optional[str]:
    return os.getenv("DATAHUB_USERNAME")


@env_var(
    name="DATAHUB_PASSWORD",
    type=str,
    group=EnvVarGroup.CONNECTION,
    tier=EnvVarTier.BASIC,
    secret=True,
    description="Password for generating access tokens.",
)
def get_password() -> Optional[str]:
    return os.getenv("DATAHUB_PASSWORD")


@env_var(
    name="DATAHUB_SYSTEM_CLIENT_ID",
    type=str,
    group=EnvVarGroup.CONNECTION,
    description="System client id, for service-to-service authentication against GMS.",
)
def get_system_client_id() -> Optional[str]:
    return os.getenv("DATAHUB_SYSTEM_CLIENT_ID")


@env_var(
    name="DATAHUB_SYSTEM_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.CONNECTION,
    secret=True,
    description="System client secret paired with DATAHUB_SYSTEM_CLIENT_ID.",
)
def get_system_client_secret() -> Optional[str]:
    return os.getenv("DATAHUB_SYSTEM_CLIENT_SECRET")


@env_var(
    name="DATAHUB_SKIP_CONFIG",
    risky=True,
    type=bool,
    group=EnvVarGroup.CONNECTION,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true", "false"],
    description=(
        "Skip loading saved connection files, so connection settings come from the "
        "environment only. Cannot be combined with `--profile`, and `DATAHUB_PROFILE` "
        "is ignored when it is set."
    ),
)
def get_skip_config() -> bool:
    return os.getenv("DATAHUB_SKIP_CONFIG", "").lower() == "true"


@env_var(
    name="DATAHUB_GMS_BASE_PATH",
    type=str,
    group=EnvVarGroup.CONNECTION,
    default="",
    description=(
        "Path prefix inserted into the derived Kafka schema registry URL, for deployments "
        "served from a sub-path. Its one consumer builds that URL against localhost:8080, not "
        "the configured GMS address, and it does not prefix GMS API calls at all -- set "
        "DATAHUB_GMS_URL to the full path for those."
    ),
)
def get_gms_base_path() -> str:
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
    description=(
        "Sink type for a recipe that does not specify one. Returns one of "
        "``datahub-rest`` (the default) or ``datahub-kafka``. An unset, blank, or "
        "unrecognized ``DATAHUB_INGESTION_DEFAULT_SINK`` resolves to ``datahub-rest`` "
        "-- unrecognized values also log a warning -- so the historical REST behavior "
        "is always the safe fallback."
    ),
)
def get_ingestion_default_sink() -> str:
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
    description=(
        "Kafka bootstrap servers for the default Kafka ingestion sink. Reuses the "
        "DataHub-wide KAFKA_BOOTSTRAP_SERVER convention (same broker GMS / consumers "
        "use) rather than a sink-specific var, so ingestion targets the same cluster "
        "as the rest of the platform."
    ),
)
def get_kafka_sink_bootstrap() -> Optional[str]:
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
    description=(
        "Per-producer local queue size cap (KiB) for the default Kafka ingestion "
        "sink. Bounds producer buffer memory so backpressure engages before OOM. Peak "
        "buffer memory ~= num_producers * this value. Default 128 MiB."
    ),
)
def get_kafka_sink_queue_max_kbytes() -> int:
    return _get_int_env("DATAHUB_KAFKA_SINK_QUEUE_MAX_KBYTES", 131072)


@env_var(
    name="DATAHUB_KAFKA_SINK_QUEUE_MAX_MESSAGES",
    type=int,
    group=EnvVarGroup.SINK,
    default=20000,
    description=(
        "Per-producer local queue size cap (msg count) for the default Kafka "
        "ingestion sink."
    ),
)
def get_kafka_sink_queue_max_messages() -> int:
    return _get_int_env("DATAHUB_KAFKA_SINK_QUEUE_MAX_MESSAGES", 20000)


@env_var(
    name="DATAHUB_KAFKA_SINK_LINGER_MS",
    type=int,
    group=EnvVarGroup.SINK,
    default=100,
    description=(
        "Producer linger.ms (send batching window) for the default Kafka ingestion "
        "sink."
    ),
)
def get_kafka_sink_linger_ms() -> int:
    return _get_int_env("DATAHUB_KAFKA_SINK_LINGER_MS", 100)


@env_var(
    name="DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES",
    type=int,
    group=EnvVarGroup.SINK,
    default=5242880,
    description=(
        "Max serialized message size (bytes) the default Kafka ingestion sink will "
        "send. librdkafka's own default is ~1 MiB -- stricter than GMS's aspect "
        "limit, so without this an aspect REST would accept could be rejected over "
        "Kafka. Defaults to 5 MiB to match DataHub's default MCP topic "
        "``max.message.bytes``. Must be <= the broker/topic ``max.message.bytes`` or "
        "the broker rejects the produce. An oversized aspect degrades to the sink's "
        "REST fallback when one is configured (parity with the REST sink's ~16 MiB "
        "cap); with no fallback it fails the record (reported), never silent."
    ),
)
def get_kafka_sink_max_message_bytes() -> int:
    return _get_int_env("DATAHUB_KAFKA_SINK_MAX_MESSAGE_BYTES", 5242880)


@env_var(
    name="DATAHUB_KAFKA_SINK_INIT_PROBE_TIMEOUT",
    type=int,
    group=EnvVarGroup.SINK,
    default=10,
    description=(
        "Per-check timeout (seconds) for the Kafka ingestion sink's init reachability "
        "probe. The probe runs whenever the Kafka default sink is selected "
        "(DATAHUB_INGESTION_DEFAULT_SINK=datahub-kafka) -- no separate on/off flag. Bounds "
        "both the broker metadata fetch and the schema-registry request so a "
        "misconfigured endpoint dies in seconds at startup. Default 10s."
    ),
)
def get_kafka_sink_init_probe_timeout() -> int:
    return _get_int_env("DATAHUB_KAFKA_SINK_INIT_PROBE_TIMEOUT", 10)


# Env-configured OAuth (DATAHUB_AUTH_TYPE)
# Parsed into an AuthConfig by datahub.ingestion.auth.env.
# ============================================================================


@env_var(
    name="DATAHUB_AUTH_TYPE",
    type=str,
    group=EnvVarGroup.OAUTH,
    choices=["k8s_oidc", "azure_entra", "oidc_client_credentials"],
    description=(
        "Env-configured OAuth provider type (k8s_oidc / azure_entra / "
        "oidc_client_credentials)."
    ),
)
def get_auth_type() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_TYPE")


@env_var(
    name="DATAHUB_AUTH_TOKEN_FILE",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="Path to the projected service-account token file (k8s_oidc).",
)
def get_auth_token_file() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_TOKEN_FILE")


@env_var(
    name="DATAHUB_AUTH_AUDIENCE",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="Expected token audience (k8s_oidc, oidc_client_credentials).",
)
def get_auth_audience() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_AUDIENCE")


@env_var(
    name="DATAHUB_AUTH_SCOPE",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="OAuth scope (oidc_client_credentials).",
)
def get_auth_scope() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_SCOPE")


@env_var(
    name="DATAHUB_AUTH_TOKEN_ENDPOINT",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="IdP token endpoint URL (oidc_client_credentials).",
)
def get_auth_token_endpoint() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_TOKEN_ENDPOINT")


@env_var(
    name="DATAHUB_AUTH_CLIENT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="OAuth client id (oidc_client_credentials).",
)
def get_auth_client_id() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_CLIENT_ID")


@env_var(
    name="DATAHUB_AUTH_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.OAUTH,
    secret=True,
    description="OAuth client secret (oidc_client_credentials).",
)
def get_auth_client_secret() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_CLIENT_SECRET")


@env_var(
    name="DATAHUB_AUTH_AZURE_TENANT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="Azure Entra tenant id (azure_entra).",
)
def get_auth_azure_tenant_id() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_AZURE_TENANT_ID")


@env_var(
    name="DATAHUB_AUTH_AZURE_CLIENT_ID",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="Azure Entra client id (azure_entra).",
)
def get_auth_azure_client_id() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_AZURE_CLIENT_ID")


@env_var(
    name="DATAHUB_AUTH_AZURE_SCOPE",
    type=str,
    group=EnvVarGroup.OAUTH,
    description="Azure Entra token scope (azure_entra).",
)
def get_auth_azure_scope() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_AZURE_SCOPE")


@env_var(
    name="DATAHUB_AUTH_AZURE_CLIENT_SECRET",
    type=str,
    group=EnvVarGroup.OAUTH,
    secret=True,
    description="Azure Entra client secret; omit for workload identity (azure_entra).",
)
def get_auth_azure_client_secret() -> Optional[str]:
    return os.getenv("DATAHUB_AUTH_AZURE_CLIENT_SECRET")


# ============================================================================
# REST Emitter Configuration
# ============================================================================


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES",
    type=str,
    group=EnvVarGroup.EMITTER,
    default="4",
    description="Times a failed request to GMS is retried before the emit fails.",
)
def get_rest_emitter_default_retry_max_times() -> str:
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES", "4")


@env_var(
    name="DATAHUB_REST_EMITTER_429_RETRY_MULTIPLIER",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=2,
    description=(
        "Multiplier applied to the retry budget when GMS replies 429 (rate limited). "
        "Effective retries are this value multiplied by "
        "DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES."
    ),
)
def get_rest_emitter_429_retry_multiplier() -> int:
    return int(os.getenv("DATAHUB_REST_EMITTER_429_RETRY_MULTIPLIER", "2")) or 1


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_POOL_CONNECTIONS",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=100,
    description="Max number of different hosts to cache connection pools for.",
)
def get_rest_emitter_default_pool_connections() -> int:
    return int(os.getenv("DATAHUB_REST_EMITTER_DEFAULT_POOL_CONNECTIONS", "100"))


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_POOL_MAXSIZE",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=100,
    description="Max connections per host in each connection pool.",
)
def get_rest_emitter_default_pool_maxsize() -> int:
    return int(os.getenv("DATAHUB_REST_EMITTER_DEFAULT_POOL_MAXSIZE", "100"))


@env_var(
    name="DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_BYTES",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=15 * 1024 * 1024,
    description="Maximum payload size in bytes for batch operations.",
)
def get_rest_emitter_batch_max_payload_bytes() -> int:
    return int(
        os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_BYTES", str(15 * 1024 * 1024))
    )


@env_var(
    name="DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH",
    type=int,
    group=EnvVarGroup.EMITTER,
    default=200,
    description="Maximum number of MCPs per batch.",
)
def get_rest_emitter_batch_max_payload_length() -> int:
    return int(os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH", "200"))


@env_var(
    name="DATAHUB_EMIT_MODE",
    type=str,
    group=EnvVarGroup.EMITTER,
    choices=["SYNC_PRIMARY", "SYNC_WAIT", "ASYNC", "ASYNC_WAIT"],
    description=(
        "How the emitter waits for a write to be durable. Unset means SYNC_PRIMARY, which "
        "returns once the primary store has the change but updates search asynchronously. "
        "SYNC_WAIT also waits for search. ASYNC queues the change and returns immediately, so "
        "a read straight afterwards may not see it; ASYNC_WAIT queues it but still blocks "
        "until it is persisted."
    ),
)
def get_emit_mode() -> Optional[str]:
    return os.getenv("DATAHUB_EMIT_MODE")


@env_var(
    name="DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT",
    type=str,
    group=EnvVarGroup.EMITTER,
    choices=["RESTLI", "OPENAPI"],
    description=(
        "Which REST API the emitter targets. Unset does not simply mean RESTLI: an SDK client "
        "negotiates, using OpenAPI when the server advertises support for it and RESTLI "
        "otherwise. Setting this pins the choice and disables that negotiation."
    ),
)
def get_rest_emitter_default_endpoint() -> Optional[str]:
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT")


@env_var(
    name="DATAHUB_EMITTER_TRACE",
    type=bool,
    group=EnvVarGroup.EMITTER,
    default=False,
    choices=["true"],
    description="Enable detailed emitter tracing.",
)
def get_emitter_trace() -> bool:
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
    description=(
        "Threads the REST sink uses to emit in parallel. Raise for throughput, lower "
        "to ease load on GMS."
    ),
)
def get_rest_sink_default_max_threads() -> int:
    return int(os.getenv("DATAHUB_REST_SINK_DEFAULT_MAX_THREADS", "15"))


@env_var(
    name="DATAHUB_REST_SINK_DEFAULT_MODE",
    type=str,
    group=EnvVarGroup.REST_SINK,
    choices=["SYNC", "ASYNC", "ASYNC_BATCH"],
    recipe_field="mode",
    description=(
        "How the REST sink submits records. Unset means ASYNC_BATCH. Any recipe `mode` takes "
        "precedence over this."
    ),
)
def get_rest_sink_default_mode() -> Optional[str]:
    return os.getenv("DATAHUB_REST_SINK_DEFAULT_MODE")


@env_var(
    name="DATAHUB_REST_SINK_DEFAULT_TCP_KEEPALIVE",
    type=bool,
    group=EnvVarGroup.REST_SINK,
    default=False,
    choices=["true"],
    recipe_field="tcp_keepalive",
    description="Default value for tcp_keepalive on the REST sink / DataHub client",
)
def get_rest_sink_default_tcp_keepalive() -> bool:
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
    description=(
        "Seconds to wait when sending anonymous usage telemetry before giving up."
    ),
)
def get_telemetry_timeout() -> str:
    return os.getenv("DATAHUB_TELEMETRY_TIMEOUT", "10")


@env_var(
    name="SENTRY_DSN",
    type=str,
    group=EnvVarGroup.TELEMETRY,
    secret=True,
    description="Sentry error tracking DSN.",
)
def get_sentry_dsn() -> Optional[str]:
    return os.getenv("SENTRY_DSN")


@env_var(
    name="SENTRY_ENVIRONMENT",
    type=str,
    group=EnvVarGroup.TELEMETRY,
    default="dev",
    description="Sentry environment (dev/prod).",
)
def get_sentry_environment() -> str:
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
    description="Maximum number of failure entries to include in the report.",
)
def get_report_failure_sample_size() -> int:
    return int(os.getenv("DATAHUB_REPORT_FAILURE_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_REPORT_WARNING_SAMPLE_SIZE",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="report_warning_sample_size",
    description="Maximum number of warning entries to include in the report.",
)
def get_report_warning_sample_size() -> int:
    return int(os.getenv("DATAHUB_REPORT_WARNING_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_REPORT_INFO_SAMPLE_SIZE",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="report_info_sample_size",
    description="Maximum number of info entries to include in the report.",
)
def get_report_info_sample_size() -> int:
    return int(os.getenv("DATAHUB_REPORT_INFO_SAMPLE_SIZE", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_FAILURES",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_failures",
    description="Maximum number of failure entries in intermediate progress reports.",
)
def get_progress_report_max_failures() -> int:
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_FAILURES", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_WARNINGS",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_warnings",
    description="Maximum number of warning entries in intermediate progress reports.",
)
def get_progress_report_max_warnings() -> int:
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_WARNINGS", "10"))


@env_var(
    name="DATAHUB_PROGRESS_REPORT_MAX_INFOS",
    type=int,
    group=EnvVarGroup.REPORTING,
    default=10,
    recipe_field="progress_report_max_infos",
    description="Maximum number of info entries in intermediate progress reports.",
)
def get_progress_report_max_infos() -> int:
    return int(os.getenv("DATAHUB_PROGRESS_REPORT_MAX_INFOS", "10"))


# ============================================================================
# Logging & Debug Configuration
# ============================================================================


@env_var(
    name="DATAHUB_SUPPRESS_LOGGING_MANAGER",
    type=str,
    group=EnvVarGroup.LOGGING,
    audience=EnvVarAudience.INTERNAL,
    description="Suppress DataHub logging manager initialization.",
)
def get_suppress_logging_manager() -> Optional[str]:
    return os.getenv("DATAHUB_SUPPRESS_LOGGING_MANAGER")


@env_var(
    name="NO_COLOR",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true"],
    description=(
        "Disable ANSI colour in CLI output. DataHub reads it as a strict boolean and accepts "
        "only `true`, unlike the no-color.org convention where any non-empty value disables "
        "colour."
    ),
)
def get_no_color() -> bool:
    return os.getenv("NO_COLOR", "").lower() == "true"


@env_var(
    name="DATAHUB_TEST_MODE",
    type=str,
    group=EnvVarGroup.LOGGING,
    audience=EnvVarAudience.INTERNAL,
    description="Indicates running in test context.",
)
def get_test_mode() -> Optional[str]:
    return os.getenv("DATAHUB_TEST_MODE")


@env_var(
    name="DATAHUB_DEBUG",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    tier=EnvVarTier.BASIC,
    choices=["true", "1"],
    risky=True,
    description=(
        "Enable debug logging, the same as passing `--debug`. Debug logs expose "
        "sensitive information. Avoid it on production instances, especially with UI "
        "ingestion, where run logs are readable by anyone who can see the run."
    ),
)
def get_debug() -> bool:
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
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_DISABLE_RAW_ACCESS",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
    description=(
        "Refuse the probe's raw passthrough commands, `sql` and `api`, so an agent cannot run "
        "its own queries or API calls against a source. Typed listings keep working on most "
        "connectors, so recipe diagnosis still functions -- but not everywhere: Snowflake "
        "exposes `sql` as its only probe command, so this withholds its probe entirely, while "
        "BigQuery keeps `projects` and is merely narrowed. Set it on the ingestion executor, "
        "not in the recipe: an agent writes the recipe, so a field there would let it grant "
        "itself the access."
    ),
)
def get_disable_agent_probe_raw_access() -> bool:
    return os.getenv("DATAHUB_PROBE_DISABLE_RAW_ACCESS", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_DISABLED",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
    description=(
        "Refuse every `datahub probe run` command, so the probe never opens a connection to "
        "the source. Wider than `DATAHUB_PROBE_DISABLE_RAW_ACCESS`, which blocks only the "
        "caller-supplied `sql` and `api` passthroughs and leaves the typed listings live. "
        "Scope, stated precisely because a control that promises more than it delivers is "
        "worse than none: `recipe test-connection` is NOT gated -- it opens a connection and "
        "authenticates, but it verifies credentials rather than reading metadata. Commands "
        "that need no connection (`recipe describe`, `scaffold`, `validate`, `probe methods`, "
        "`probe filter`) keep working. Set it where the probe runs, the ingestion executor, "
        "not in the recipe: an agent writes the recipe, so a field there would let it grant "
        "itself the access."
    ),
)
def get_probe_disabled() -> bool:
    return os.getenv("DATAHUB_PROBE_DISABLED", "").lower() in ("true", "1")


@env_var(
    name="DATAHUB_PROBE_VERBOSE_LOGS",
    type=bool,
    group=EnvVarGroup.LOGGING,
    default=False,
    choices=["true", "1"],
    description=(
        "Pass through logs and error text that a probe normally withholds, for debugging a "
        "connector locally. By default a probe scrubs log records the framework did not write "
        "and names untrusted errors by class and code only, because both can quote connection "
        "strings and request URLs. With this on, those logs pass through and each withheld "
        "error text is appended to its label, still scrubbed. For a person debugging, not for "
        "an agent."
    ),
)
def get_probe_verbose_logs() -> bool:
    return os.getenv("DATAHUB_PROBE_VERBOSE_LOGS", "").lower() in ("true", "1")


# ============================================================================
# Data Processing Configuration
# ============================================================================


@env_var(
    name="DATAHUB_DISABLE_EXECUTOR_TASK_SECRET_SCOPE",
    type=bool,
    group=EnvVarGroup.EXECUTOR,
    default=False,
    choices=["true", "1"],
    risky=True,
    description=(
        "Turn off per-task secret scopes in the executor, which are on by default. An "
        "operational rollback, not a tuning knob: with scopes off, every task's secrets go to "
        "one process-global registry as they did before scopes existed, so masking over-masks "
        "across tasks -- a later task's output redacted against an earlier task's secret, with "
        "that task's variable name in the marker -- but never under-masks. Read per task, so a "
        "restart with it set is enough to switch."
    ),
)
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
    choices=["DISABLED", "STORE_ALL", "STORE_FAILED"],
    description=(
        "Whether the SQL aggregator keeps the queries it parses, for debugging lineage. "
        "STORE_ALL keeps every query, STORE_FAILED only those that failed to parse. The "
        "value is looked up in QueryLogSetting at import, so anything else fails the run."
    ),
)
def get_sql_agg_query_log() -> str:
    return os.getenv("DATAHUB_SQL_AGG_QUERY_LOG", "DISABLED")


@env_var(
    name="DATAHUB_SQL_AGG_SKIP_JOINS",
    type=bool,
    group=EnvVarGroup.SQL,
    default=False,
    choices=["true"],
    description="Skip join processing in SQL aggregator (default: False).",
)
def get_sql_agg_skip_joins() -> bool:
    return os.getenv("DATAHUB_SQL_AGG_SKIP_JOINS", "").lower() == "true"


@env_var(
    name="DATAHUB_SQL_AGG_MAX_QUERIES_PER_DOWNSTREAM",
    risky=True,
    type=int,
    group=EnvVarGroup.SQL,
    default=0,
    description=(
        "Optional cap on the queries considered for one downstream table's lineage. 0 "
        "(the default) means no cap: every query is considered, most recent first. "
        "Setting it keeps only the newest N parsed queries per downstream, which can "
        "drop an upstream that only an older query references; use it as a safety "
        "valve for runtime, not as the default."
    ),
)
def get_sql_agg_max_queries_per_downstream() -> int:
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
    description="SQL parse result cache size (number of entries).",
)
def get_sql_parse_cache_size() -> int:
    return int(os.getenv("DATAHUB_SQL_PARSE_CACHE_SIZE", "1000"))


@env_var(
    name="SQL_LINEAGE_TIMEOUT_SECONDS",
    type=int,
    group=EnvVarGroup.SQL,
    default=10,
    description=(
        "Budget for generating column-level lineage for one query, in seconds. "
        "Measured on the wall clock, so a host that stalls under load spends the "
        "budget without the parser doing any work. Raise it where lineage is being "
        "dropped on queries that are merely slow to parse rather than pathological."
    ),
)
def get_sql_lineage_timeout_seconds() -> int:
    return _get_int_env("SQL_LINEAGE_TIMEOUT_SECONDS", 10)


@env_var(
    name="DATAHUB_MAX_TRANSFORM_OPERATION_LENGTH",
    type=int,
    group=EnvVarGroup.SQL,
    risky=True,
    description=(
        "Truncate `transformOperation` in fine-grained lineage to N characters, for "
        "the rare case where column-level lineage aspects grow too large. Set it to "
        "at least 1; a value is kept unchanged when it is at most N + 3 characters, "
        "since truncating would not make it shorter. Unset means no truncation. With "
        "`incremental_lineage`, changing this leaves entries already stored under the "
        "old value in place -- they are only rewritten by a run with "
        "`incremental_lineage: false`."
    ),
)
def get_max_transform_operation_length() -> Optional[int]:
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
    description=(
        "Lowercase dataset names when building URNs. Changing this on an existing "
        "deployment creates duplicate entities."
    ),
)
def get_dataset_urn_to_lower() -> str:
    return os.getenv("DATAHUB_DATASET_URN_TO_LOWER", "false")


# ============================================================================
# Integration-Specific Configuration
# ============================================================================


@env_var(
    name="KAFKA_SCHEMAREGISTRY_URL",
    type=str,
    group=EnvVarGroup.CONNECTORS,
    description=(
        "Schema registry the Kafka connector and Kafka sink use when none is set in "
        "the recipe."
    ),
)
def get_kafka_schema_registry_url() -> Optional[str]:
    return os.getenv("KAFKA_SCHEMAREGISTRY_URL")


@env_var(
    name="DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS",
    risky=True,
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=True,
    choices=["true", "false"],
    description=(
        "Auto-register MCE/MCP schemas with the Kafka Schema Registry (default: "
        'True). Set to "false" when the ingestion user only has read access to the '
        "Schema Registry; the schemas must then be pre-registered."
    ),
)
def get_kafka_auto_register_schemas() -> bool:
    return os.getenv("DATAHUB_KAFKA_AUTO_REGISTER_SCHEMAS", "true").lower() == "true"


@env_var(
    name="SPARK_VERSION",
    type=str,
    group=EnvVarGroup.CONNECTORS,
    description="Spark version (for S3 source).",
)
def get_spark_version() -> Optional[str]:
    return os.getenv("SPARK_VERSION")


@env_var(
    name="DATAHUB_VERTEXAI_DISABLE_PARALLELISM",
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=False,
    choices=["true", "1"],
    description="Disable parallel resource fetching in Vertex AI connector.",
)
def get_vertexai_disable_parallelism() -> bool:
    return os.getenv("DATAHUB_VERTEXAI_DISABLE_PARALLELISM", "").lower() in (
        "1",
        "true",
    )


@env_var(
    name="DATAHUB_BIGQUERY_SCHEMA_PARALLELISM",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=20,
    description="Parallelism level for BigQuery schema extraction.",
)
def get_bigquery_schema_parallelism() -> int:
    return int(os.getenv("DATAHUB_BIGQUERY_SCHEMA_PARALLELISM", "20"))


@env_var(
    name="DATAHUB_SNOWFLAKE_SCHEMA_PARALLELISM",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=20,
    description="Parallelism level for Snowflake schema extraction.",
)
def get_snowflake_schema_parallelism() -> int:
    return int(os.getenv("DATAHUB_SNOWFLAKE_SCHEMA_PARALLELISM", "20"))


@env_var(
    name="DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT",
    type=int,
    group=EnvVarGroup.CONNECTORS,
    default=60,
    description="Timeout for PowerBI M query parsing.",
)
def get_powerbi_m_query_parse_timeout() -> int:
    return int(os.getenv("DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT", "60"))


@env_var(
    name="DATAHUB_TRACE_POWERBI_MQUERY_PARSER",
    type=bool,
    group=EnvVarGroup.CONNECTORS,
    default=False,
    choices=["true"],
    description="Enable PowerBI M query parser tracing.",
)
def get_trace_powerbi_mquery_parser() -> bool:
    return os.getenv("DATAHUB_TRACE_POWERBI_MQUERY_PARSER", "").lower() == "true"


@env_var(
    name="DATAHUB_LOOKML_GIT_TEST_SSH_KEY",
    type=str,
    group=EnvVarGroup.CONNECTORS,
    audience=EnvVarAudience.INTERNAL,
    secret=True,
    description="SSH key for LookML Git tests.",
)
def get_lookml_git_test_ssh_key() -> Optional[str]:
    return os.getenv("DATAHUB_LOOKML_GIT_TEST_SSH_KEY")


# ============================================================================
# AWS/Cloud Configuration
# ============================================================================


@env_var(
    name="AWS_LAMBDA_FUNCTION_NAME",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="Indicates running in AWS Lambda.",
)
def get_aws_lambda_function_name() -> Optional[str]:
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME")


@env_var(
    name="AWS_EXECUTION_ENV",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="AWS execution environment.",
)
def get_aws_execution_env() -> Optional[str]:
    return os.getenv("AWS_EXECUTION_ENV")


@env_var(
    name="AWS_WEB_IDENTITY_TOKEN_FILE",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="OIDC token file path.",
)
def get_aws_web_identity_token_file() -> Optional[str]:
    return os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")


@env_var(
    name="AWS_ROLE_ARN",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="AWS role ARN for OIDC.",
)
def get_aws_role_arn() -> Optional[str]:
    return os.getenv("AWS_ROLE_ARN")


@env_var(
    name="AWS_APP_RUNNER_SERVICE_ID",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="AWS App Runner service ID.",
)
def get_aws_app_runner_service_id() -> Optional[str]:
    return os.getenv("AWS_APP_RUNNER_SERVICE_ID")


@env_var(
    name="ECS_CONTAINER_METADATA_URI_V4",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="ECS metadata endpoint v4.",
)
def get_ecs_container_metadata_uri_v4() -> Optional[str]:
    return os.getenv("ECS_CONTAINER_METADATA_URI_V4")


@env_var(
    name="ECS_CONTAINER_METADATA_URI",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="ECS metadata endpoint v3.",
)
def get_ecs_container_metadata_uri() -> Optional[str]:
    return os.getenv("ECS_CONTAINER_METADATA_URI")


@env_var(
    name="ELASTIC_BEANSTALK_ENVIRONMENT_NAME",
    type=str,
    group=EnvVarGroup.CLOUD,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="Elastic Beanstalk environment.",
)
def get_elastic_beanstalk_environment_name() -> Optional[str]:
    return os.getenv("ELASTIC_BEANSTALK_ENVIRONMENT_NAME")


# ============================================================================
# Docker & Local Development
# ============================================================================


@env_var(
    name="DATAHUB_COMPOSE_PROJECT_NAME",
    type=str,
    group=EnvVarGroup.DOCKER,
    default="datahub",
    description="Docker Compose project name.",
)
def get_compose_project_name() -> str:
    return os.getenv("DATAHUB_COMPOSE_PROJECT_NAME", "datahub")


@env_var(
    name="DOCKER_COMPOSE_BASE",
    type=str,
    group=EnvVarGroup.DOCKER,
    description="Base path for Docker Compose files.",
)
def get_docker_compose_base() -> Optional[str]:
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
    return os.getenv("DATAHUB_VERSION")


@env_var(
    name="DATAHUB_MAPPED_MYSQL_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="MySQL port mapping (set during docker init).",
)
def get_mapped_mysql_port() -> Optional[str]:
    return os.getenv("DATAHUB_MAPPED_MYSQL_PORT")


@env_var(
    name="DATAHUB_MAPPED_KAFKA_BROKER_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="Kafka broker port mapping (set during docker init).",
)
def get_mapped_kafka_broker_port() -> Optional[str]:
    return os.getenv("DATAHUB_MAPPED_KAFKA_BROKER_PORT")


@env_var(
    name="DATAHUB_MAPPED_ELASTIC_PORT",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="Elasticsearch port mapping (set during docker init).",
)
def get_mapped_elastic_port() -> Optional[str]:
    return os.getenv("DATAHUB_MAPPED_ELASTIC_PORT")


@env_var(
    name="METADATA_SERVICE_AUTH_ENABLED",
    type=str,
    group=EnvVarGroup.DOCKER,
    default="false",
    choices=["true", "false"],
    description=(
        "Whether the Metadata Service requires authentication. `datahub docker quickstart` "
        "overwrites it with false on every run, so setting it has no effect there -- it "
        "matters only for a compose stack you drive yourself."
    ),
)
def get_metadata_service_auth_enabled() -> str:
    return os.getenv("METADATA_SERVICE_AUTH_ENABLED", "false")


@env_var(
    name="UI_INGESTION_DEFAULT_CLI_VERSION",
    type=str,
    group=EnvVarGroup.DOCKER,
    audience=EnvVarAudience.AUTO_DETECTED,
    description="CLI version for UI ingestion (set during init).",
)
def get_ui_ingestion_default_cli_version() -> Optional[str]:
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
    description=(
        "Component label reported in the User-Agent header, identifying which DataHub "
        "component made the call."
    ),
)
def get_datahub_component() -> str:
    return os.getenv("DATAHUB_COMPONENT", "datahub")


@env_var(
    name="FORCE_LOCAL_QUICKSTART_MAPPING",
    type=str,
    group=EnvVarGroup.MISC,
    default="",
    audience=EnvVarAudience.INTERNAL,
    description="Force local quickstart mapping file.",
)
def get_force_local_quickstart_mapping() -> str:
    return os.getenv("FORCE_LOCAL_QUICKSTART_MAPPING", "")


@env_var(
    name="DATAHUB_DATAPRODUCT_EXTERNAL_URL",
    type=str,
    group=EnvVarGroup.MISC,
    description="External URL for data products.",
)
def get_dataproduct_external_url() -> Optional[str]:
    return os.getenv("DATAHUB_DATAPRODUCT_EXTERNAL_URL")


@env_var(
    name="OVERRIDE_SQLITE_VERSION_REQ",
    type=str,
    group=EnvVarGroup.MISC,
    default="",
    audience=EnvVarAudience.INTERNAL,
    description="Override SQLite version requirement.",
)
def get_override_sqlite_version_req() -> str:
    return os.getenv("OVERRIDE_SQLITE_VERSION_REQ", "")


@env_var(
    name="UPDATE_ENTITY_REGISTRY",
    type=str,
    group=EnvVarGroup.MISC,
    default="false",
    audience=EnvVarAudience.INTERNAL,
    description="Update entity registry during tests.",
)
def get_update_entity_registry() -> str:
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
    description=(
        "Set by most CI systems. Read to adjust output and to disable telemetry."
    ),
)
def get_ci() -> Optional[str]:
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
