# ABOUTME: Central registry for all environment variables used in metadata-ingestion.
# ABOUTME: All environment variable reads should go through this module for discoverability and maintainability.

import os
from typing import Optional

# ============================================================================
# Core DataHub Configuration
# ============================================================================


def get_gms_url() -> Optional[str]:
    """Complete GMS URL (takes precedence over separate host/port)."""
    return os.getenv("DATAHUB_GMS_URL")


def get_gms_host() -> Optional[str]:
    """GMS host (fallback for URL, deprecated)."""
    return os.getenv("DATAHUB_GMS_HOST")


def get_gms_port() -> Optional[str]:
    """GMS port number."""
    return os.getenv("DATAHUB_GMS_PORT")


def get_gms_protocol() -> str:
    """Protocol for GMS connection (http/https)."""
    return os.getenv("DATAHUB_GMS_PROTOCOL", "http")


def get_gms_token() -> Optional[str]:
    """Authentication token for GMS."""
    return os.getenv("DATAHUB_GMS_TOKEN")


def get_system_client_id() -> Optional[str]:
    """System client ID for OAuth/auth."""
    return os.getenv("DATAHUB_SYSTEM_CLIENT_ID")


def get_system_client_secret() -> Optional[str]:
    """System client secret for OAuth/auth."""
    return os.getenv("DATAHUB_SYSTEM_CLIENT_SECRET")


def get_skip_config() -> bool:
    """Skip loading config file (forces env variables)."""
    return os.getenv("DATAHUB_SKIP_CONFIG", "").lower() == "true"


def get_gms_base_path() -> str:
    """Base path for GMS API endpoints."""
    return os.getenv("DATAHUB_GMS_BASE_PATH", "")


# ============================================================================
# REST Emitter Configuration
# ============================================================================


def get_rest_emitter_default_retry_max_times() -> str:
    """Max retry attempts for failed requests."""
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_RETRY_MAX_TIMES", "4")


def get_rest_emitter_429_retry_multiplier() -> int:
    """Multiplier for 429 retry backoff.

    Number of retries will effectively be this value * get_rest_emitter_default_retry_max_times().
    """
    return int(os.getenv("DATAHUB_REST_EMITTER_429_RETRY_MULTIPLIER", "2")) or 1


def get_rest_emitter_batch_max_payload_bytes() -> int:
    """Maximum payload size in bytes for batch operations."""
    return int(
        os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_BYTES", str(15 * 1024 * 1024))
    )


def get_rest_emitter_batch_max_payload_length() -> int:
    """Maximum number of MCPs per batch."""
    return int(os.getenv("DATAHUB_REST_EMITTER_BATCH_MAX_PAYLOAD_LENGTH", "200"))


def get_emit_mode() -> Optional[str]:
    """Emission mode (SYNC_PRIMARY, SYNC_WAIT, ASYNC, ASYNC_WAIT)."""
    return os.getenv("DATAHUB_EMIT_MODE")


def get_rest_emitter_default_endpoint() -> Optional[str]:
    """REST endpoint type (RESTLI or OPENAPI)."""
    return os.getenv("DATAHUB_REST_EMITTER_DEFAULT_ENDPOINT")


def get_emitter_trace() -> bool:
    """Enable detailed emitter tracing."""
    return os.getenv("DATAHUB_EMITTER_TRACE", "").lower() == "true"


# ============================================================================
# REST Sink Configuration
# ============================================================================


def get_rest_sink_default_max_threads() -> int:
    """Max thread pool size for async operations."""
    return int(os.getenv("DATAHUB_REST_SINK_DEFAULT_MAX_THREADS", "15"))


def get_rest_sink_default_mode() -> Optional[str]:
    """Sink mode (SYNC, ASYNC, ASYNC_BATCH)."""
    return os.getenv("DATAHUB_REST_SINK_DEFAULT_MODE")


# ============================================================================
# Telemetry & Monitoring
# ============================================================================


def get_telemetry_timeout() -> str:
    """Telemetry timeout in seconds."""
    return os.getenv("DATAHUB_TELEMETRY_TIMEOUT", "10")


def get_sentry_dsn() -> Optional[str]:
    """Sentry error tracking DSN."""
    return os.getenv("SENTRY_DSN")


def get_sentry_environment() -> str:
    """Sentry environment (dev/prod)."""
    return os.getenv("SENTRY_ENVIRONMENT", "dev")


# ============================================================================
# Logging & Debug Configuration
# ============================================================================


def get_suppress_logging_manager() -> Optional[str]:
    """Suppress DataHub logging manager initialization."""
    return os.getenv("DATAHUB_SUPPRESS_LOGGING_MANAGER")


def get_no_color() -> bool:
    """Disable colored logging output."""
    return os.getenv("NO_COLOR", "").lower() == "true"


def get_test_mode() -> Optional[str]:
    """Indicates running in test context."""
    return os.getenv("DATAHUB_TEST_MODE")


def get_debug() -> bool:
    """Enable debug mode."""
    return os.getenv("DATAHUB_DEBUG", "").lower() == "true"


def get_disable_secret_masking() -> bool:
    """
    Disable secret masking for debugging purposes.

    WARNING: Only use this in development/debugging scenarios.
    Disabling secret masking will expose sensitive information in logs.
    """
    return os.getenv("DATAHUB_DISABLE_SECRET_MASKING", "").lower() in ("true", "1")


# ============================================================================
# Data Processing Configuration
# ============================================================================


def get_sql_agg_query_log() -> str:
    """SQL aggregator query logging level."""
    return os.getenv("DATAHUB_SQL_AGG_QUERY_LOG", "DISABLED")


def get_sql_agg_skip_joins() -> bool:
    """Skip join processing in SQL aggregator (default: False)."""
    return os.getenv("DATAHUB_SQL_AGG_SKIP_JOINS", "").lower() == "true"


def get_sql_parse_cache_size() -> int:
    """SQL parse result cache size (number of entries)."""
    return int(os.getenv("DATAHUB_SQL_PARSE_CACHE_SIZE", "1000"))


def get_dataset_urn_to_lower() -> str:
    """Convert dataset URNs to lowercase."""
    return os.getenv("DATAHUB_DATASET_URN_TO_LOWER", "false")


# ============================================================================
# Integration-Specific Configuration
# ============================================================================


def get_kafka_schema_registry_url() -> Optional[str]:
    """Kafka schema registry URL."""
    return os.getenv("KAFKA_SCHEMAREGISTRY_URL")


def get_spark_version() -> Optional[str]:
    """Spark version (for S3 source)."""
    return os.getenv("SPARK_VERSION")


def get_bigquery_schema_parallelism() -> int:
    """Parallelism level for BigQuery schema extraction."""
    return int(os.getenv("DATAHUB_BIGQUERY_SCHEMA_PARALLELISM", "20"))


def get_snowflake_schema_parallelism() -> int:
    """Parallelism level for Snowflake schema extraction."""
    return int(os.getenv("DATAHUB_SNOWFLAKE_SCHEMA_PARALLELISM", "20"))


def get_powerbi_m_query_parse_timeout() -> int:
    """Timeout for PowerBI M query parsing."""
    return int(os.getenv("DATAHUB_POWERBI_M_QUERY_PARSE_TIMEOUT", "60"))


def get_trace_powerbi_mquery_parser() -> bool:
    """Enable PowerBI M query parser tracing."""
    return os.getenv("DATAHUB_TRACE_POWERBI_MQUERY_PARSER", "").lower() == "true"


def get_lookml_git_test_ssh_key() -> Optional[str]:
    """SSH key for LookML Git tests."""
    return os.getenv("DATAHUB_LOOKML_GIT_TEST_SSH_KEY")


# ============================================================================
# AWS/Cloud Configuration
# ============================================================================


def get_aws_lambda_function_name() -> Optional[str]:
    """Indicates running in AWS Lambda."""
    return os.getenv("AWS_LAMBDA_FUNCTION_NAME")


def get_aws_execution_env() -> Optional[str]:
    """AWS execution environment."""
    return os.getenv("AWS_EXECUTION_ENV")


def get_aws_web_identity_token_file() -> Optional[str]:
    """OIDC token file path."""
    return os.getenv("AWS_WEB_IDENTITY_TOKEN_FILE")


def get_aws_role_arn() -> Optional[str]:
    """AWS role ARN for OIDC."""
    return os.getenv("AWS_ROLE_ARN")


def get_aws_app_runner_service_id() -> Optional[str]:
    """AWS App Runner service ID."""
    return os.getenv("AWS_APP_RUNNER_SERVICE_ID")


def get_ecs_container_metadata_uri_v4() -> Optional[str]:
    """ECS metadata endpoint v4."""
    return os.getenv("ECS_CONTAINER_METADATA_URI_V4")


def get_ecs_container_metadata_uri() -> Optional[str]:
    """ECS metadata endpoint v3."""
    return os.getenv("ECS_CONTAINER_METADATA_URI")


def get_elastic_beanstalk_environment_name() -> Optional[str]:
    """Elastic Beanstalk environment."""
    return os.getenv("ELASTIC_BEANSTALK_ENVIRONMENT_NAME")


# ============================================================================
# Docker & Local Development
# ============================================================================


def get_compose_project_name() -> str:
    """Docker Compose project name."""
    return os.getenv("DATAHUB_COMPOSE_PROJECT_NAME", "datahub")


def get_docker_compose_base() -> Optional[str]:
    """Base path for Docker Compose files."""
    return os.getenv("DOCKER_COMPOSE_BASE")


def get_datahub_version() -> Optional[str]:
    """DataHub version (set during docker init)."""
    return os.getenv("DATAHUB_VERSION")


def get_mapped_mysql_port() -> Optional[str]:
    """MySQL port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_MYSQL_PORT")


def get_mapped_kafka_broker_port() -> Optional[str]:
    """Kafka broker port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_KAFKA_BROKER_PORT")


def get_mapped_elastic_port() -> Optional[str]:
    """Elasticsearch port mapping (set during docker init)."""
    return os.getenv("DATAHUB_MAPPED_ELASTIC_PORT")


def get_metadata_service_auth_enabled() -> str:
    """Enable/disable auth in Docker."""
    return os.getenv("METADATA_SERVICE_AUTH_ENABLED", "false")


def get_ui_ingestion_default_cli_version() -> Optional[str]:
    """CLI version for UI ingestion (set during init)."""
    return os.getenv("UI_INGESTION_DEFAULT_CLI_VERSION")


# ============================================================================
# Utility & Helper Configuration
# ============================================================================


def get_datahub_component() -> str:
    """Component name for user agent tracking."""
    return os.getenv("DATAHUB_COMPONENT", "datahub")


def get_force_local_quickstart_mapping() -> str:
    """Force local quickstart mapping file."""
    return os.getenv("FORCE_LOCAL_QUICKSTART_MAPPING", "")


def get_dataproduct_external_url() -> Optional[str]:
    """External URL for data products."""
    return os.getenv("DATAHUB_DATAPRODUCT_EXTERNAL_URL")


def get_override_sqlite_version_req() -> str:
    """Override SQLite version requirement."""
    return os.getenv("OVERRIDE_SQLITE_VERSION_REQ", "")


def get_update_entity_registry() -> str:
    """Update entity registry during tests."""
    return os.getenv("UPDATE_ENTITY_REGISTRY", "false")


def get_ci() -> Optional[str]:
    """Indicates running in CI environment."""
    return os.getenv("CI")
