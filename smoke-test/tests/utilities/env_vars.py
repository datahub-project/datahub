# ABOUTME: Central registry for all environment variables used in smoke-test.
# ABOUTME: All environment variable reads should go through this module for discoverability and maintainability.

import os
import re
from typing import Optional

# ============================================================================
# Core DataHub Configuration
# ============================================================================


def get_telemetry_enabled() -> str:
    """Enable/disable telemetry (true/false)."""
    return os.getenv("DATAHUB_TELEMETRY_ENABLED", "false")


def get_suppress_logging_manager() -> Optional[str]:
    """Suppress DataHub logging manager initialization."""
    return os.getenv("DATAHUB_SUPPRESS_LOGGING_MANAGER")


def get_gms_url() -> Optional[str]:
    """GMS URL."""
    return os.getenv("DATAHUB_GMS_URL")


def get_gms_management_url() -> Optional[str]:
    """Override base URL for Spring Actuator / Micrometer (e.g. `http://localhost:4319`).

    If unset, smoke tests infer `http://<gms-host>:4319` when DATAHUB_GMS_URL uses port 8080 (Docker default).
    """
    return os.getenv("DATAHUB_GMS_MANAGEMENT_URL")


def get_mce_management_url() -> Optional[str]:
    """Override base URL for MCE consumer Actuator / Micrometer.

    When unset, smoke tests auto-detect: probe a standalone ``datahub-mce-consumer:4319``
    container if present, otherwise fall back to the GMS management URL (embedded consumers).
    Set explicitly when your deployment uses a non-default host or port.
    """
    return os.getenv("DATAHUB_MCE_MANAGEMENT_URL")


def get_gms_token() -> Optional[str]:
    """GMS Bearer token for authenticated API calls."""
    return os.getenv("DATAHUB_GMS_TOKEN")


def get_base_path() -> str:
    """Base path for DataHub frontend."""
    return os.getenv("DATAHUB_BASE_PATH", "")


def get_gms_base_path() -> str:
    """Base path for GMS API endpoints."""
    return os.getenv("DATAHUB_GMS_BASE_PATH", "")


def get_frontend_url() -> Optional[str]:
    """DataHub frontend URL."""
    return os.getenv("DATAHUB_FRONTEND_URL")


def get_kafka_url() -> Optional[str]:
    """Kafka broker URL."""
    return os.getenv("DATAHUB_KAFKA_URL")


def get_kafka_schema_registry_url() -> Optional[str]:
    """Kafka schema registry URL."""
    return os.getenv("DATAHUB_KAFKA_SCHEMA_REGISTRY_URL")


# ============================================================================
# Admin Credentials
# ============================================================================


def get_admin_username() -> str:
    """Admin username for smoke tests."""
    return os.getenv("ADMIN_USERNAME", "datahub")


def get_admin_password() -> str:
    """Admin password for smoke tests."""
    return os.getenv("ADMIN_PASSWORD", "datahub")


# ============================================================================
# Database Configuration
# ============================================================================


def get_db_type() -> Optional[str]:
    """Database type (mysql/postgres)."""
    return os.getenv("DB_TYPE")


def get_profile_name() -> Optional[str]:
    """Profile name for inferring database type."""
    return os.getenv("PROFILE_NAME")


def get_mysql_url() -> str:
    """MySQL database URL."""
    return os.getenv("DATAHUB_MYSQL_URL", "localhost:3306")


def get_mysql_username() -> str:
    """MySQL username."""
    return os.getenv("DATAHUB_MYSQL_USERNAME", "datahub")


def get_mysql_password() -> str:
    """MySQL password."""
    return os.getenv("DATAHUB_MYSQL_PASSWORD", "datahub")


def get_postgres_url() -> str:
    """PostgreSQL database URL."""
    return os.getenv("DATAHUB_POSTGRES_URL", "localhost:5432")


def get_postgres_username() -> str:
    """PostgreSQL username."""
    return os.getenv("DATAHUB_POSTGRES_USERNAME", "datahub")


def get_postgres_password() -> str:
    """PostgreSQL password."""
    return os.getenv("DATAHUB_POSTGRES_PASSWORD", "datahub")


# ============================================================================
# Testing Configuration
# ============================================================================


# Same gate smoke.sh uses to decide whether to pass -n to pytest. Matched with
# fullmatch, not $: Python's $ also matches just before a trailing newline, so
# "3\n" would pass here while bash's =~ ^[1-9][0-9]*$ rejects it.
_XDIST_WORKERS_PATTERN = re.compile(r"[1-9][0-9]*")


def get_batch_count() -> int:
    """Number of test batches for parallel execution."""
    return int(os.getenv("BATCH_COUNT", "1"))


def get_batch_number() -> int:
    """Current batch number (zero-indexed)."""
    return int(os.getenv("BATCH_NUMBER", "0"))


def get_pytest_xdist_workers() -> int:
    """pytest-xdist worker count used for a batch's parallel phase.

    Deliberately mirrors smoke.sh's gate byte for byte::

        if [[ "${PYTEST_XDIST_WORKERS:-0}" =~ ^[1-9][0-9]*$ ]]; then

    smoke.sh is what decides whether ``-n`` is actually passed, so anything this
    accepts that the shell rejects would have batch weighting assume parallelism
    that never happens. That rules out surrounding whitespace and leading zeros
    (``" 3 "``, ``"03"``), which the shell will not match, and non-ASCII digits
    like ``"²"``, for which ``str.isdigit()`` is True but ``int()`` raises, and
    a trailing newline (``"3\n"``), which Python's ``$`` would otherwise accept.
    Anything unset or not matching means no xdist, i.e. one worker.
    """
    if not _XDIST_WORKERS_PATTERN.fullmatch(os.getenv("PYTEST_XDIST_WORKERS", "")):
        return 1
    return int(os.environ["PYTEST_XDIST_WORKERS"])


def get_test_strategy() -> Optional[str]:
    """Test execution strategy (e.g., 'pytests')."""
    return os.getenv("TEST_STRATEGY")


def get_test_sleep_between() -> int:
    """Sleep duration in seconds between test retries."""
    return int(os.getenv("DATAHUB_TEST_SLEEP_BETWEEN", "20"))


def get_test_sleep_times() -> int:
    """Number of retry attempts for tests."""
    return int(os.getenv("DATAHUB_TEST_SLEEP_TIMES", "3"))


def get_k8s_cluster_enabled() -> bool:
    """Whether Kubernetes cluster is enabled."""
    return os.getenv("K8S_CLUSTER_ENABLED", "false").lower() in ["true", "yes"]


def get_test_datahub_version() -> Optional[str]:
    """DataHub version being tested."""
    return os.getenv("TEST_DATAHUB_VERSION")


# ============================================================================
# Consistency Testing
# ============================================================================


def get_use_static_sleep() -> bool:
    """Use static sleep instead of dynamic wait for consistency."""
    return bool(os.getenv("USE_STATIC_SLEEP", False))


def get_elasticsearch_refresh_interval_seconds() -> int:
    """Elasticsearch refresh interval in seconds.

    This trailing sleep is the last backstop for search-index visibility after
    the consumer offsets have been awaited, so the library default stays
    conservative. CI overrides it to 1 on the pytest step in
    docker-unified.yml, where GMS is started with a 1s bulk-flush period and 1s
    index refresh interval (see run-quickstart.sh).
    """
    return int(os.getenv("ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", "3"))


def get_force_legacy_wait() -> bool:
    """Force wait_for_writes_to_sync() onto the legacy aggregate-lag path.

    Set on CI retry attempts so a batch that failed once re-runs with the more
    conservative wait. Legacy is the wrong *default* -- under full concurrent
    xdist load, aggregate lag may never converge -- but the right choice on a
    retry, which runs a smaller filtered set of tests under much less write
    pressure.
    """
    return os.getenv("DATAHUB_TEST_FORCE_LEGACY_WAIT", "").strip().lower() in (
        "1",
        "true",
        "yes",
    )


def get_lag_auth_timeout_seconds() -> float:
    """How long to retry 401/403 on lag endpoints before failing.

    Auth denials can be transient while policies bootstrap. After this window,
    wait_for_writes_to_sync raises and tells the operator to grant
    VIEW_SYSTEM_STATUS or MANAGE_SYSTEM_OPERATIONS.
    """
    return float(os.getenv("DATAHUB_TEST_LAG_AUTH_TIMEOUT_SECONDS", "20"))


def get_kafka_bootstrap_server() -> str:
    """Kafka bootstrap server for smoke tests."""
    return str(os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092"))


def get_kafka_broker_container() -> Optional[str]:
    """Kafka broker container name."""
    return os.getenv("KAFKA_BROKER_CONTAINER")


def get_datahub_usage_event_topic() -> str:
    """DataHub usage event topic name."""
    return str(os.getenv("DATAHUB_USAGE_EVENT_NAME", "DataHubUsageEvent_v1"))


def get_filtered_tests_file() -> Optional[str]:
    """Path to file containing filtered test paths (one per line)."""
    return os.getenv("FILTERED_TESTS")


def get_smoke_policy_phase() -> Optional[str]:
    """Smoke-test policy phase: ``1`` (non-mutators), ``2`` (mutators), or unset (all)."""
    raw = os.getenv("SMOKE_POLICY_PHASE", "").strip()
    return raw or None


# ============================================================================
# Cleanup Configuration
# ============================================================================


def get_delete_after_test() -> bool:
    """Delete test data after test completion."""
    return os.getenv("DELETE_AFTER_TEST", "false").lower() == "true"


# ============================================================================
# Integration Testing
# ============================================================================


def get_mixpanel_api_secret() -> Optional[str]:
    """Mixpanel API secret for tracking tests."""
    return os.getenv("MIXPANEL_API_SECRET")


def get_mixpanel_project_id() -> str:
    """Mixpanel project ID."""
    return os.getenv("MIXPANEL_PROJECT_ID", "3653440")


def get_elasticsearch_url() -> str:
    """Elasticsearch URL for integration tests."""
    return os.getenv("ELASTICSEARCH_URL", "http://localhost:9200")


def get_elasticsearch_index() -> str:
    """Elasticsearch index name for usage events."""
    return os.getenv("ELASTICSEARCH_INDEX", "datahub_usage_event")


# ============================================================================
# Slack Notifications
# ============================================================================


def get_slack_api_token() -> Optional[str]:
    """Slack API token for test notifications."""
    return os.getenv("SLACK_API_TOKEN")


def get_slack_channel() -> Optional[str]:
    """Slack channel for test notifications."""
    return os.getenv("SLACK_CHANNEL")


def get_slack_thread_ts() -> Optional[str]:
    """Slack thread timestamp for threaded notifications."""
    return os.getenv("SLACK_THREAD_TS")


def get_test_identifier() -> str:
    """Test run identifier for notifications."""
    return os.getenv("TEST_IDENTIFIER", "LOCAL_TEST")
