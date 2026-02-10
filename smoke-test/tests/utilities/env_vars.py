# ABOUTME: Central registry for all environment variables used in smoke-test.
# ABOUTME: All environment variable reads should go through this module for discoverability and maintainability.

import os
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


def get_batch_count() -> int:
    """Number of test batches for parallel execution."""
    return int(os.getenv("BATCH_COUNT", "1"))


def get_batch_number() -> int:
    """Current batch number (zero-indexed)."""
    return int(os.getenv("BATCH_NUMBER", "0"))


def get_test_strategy() -> Optional[str]:
    """Test execution strategy (e.g., 'cypress')."""
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
    """Elasticsearch refresh interval in seconds."""
    return int(os.getenv("ELASTICSEARCH_REFRESH_INTERVAL_SECONDS", "3"))


def get_kafka_bootstrap_server() -> str:
    """Kafka bootstrap server for smoke tests."""
    return str(os.getenv("KAFKA_BOOTSTRAP_SERVER", "broker:29092"))


def get_kafka_broker_container() -> Optional[str]:
    """Kafka broker container name."""
    return os.getenv("KAFKA_BROKER_CONTAINER")


# ============================================================================
# Cypress Testing
# ============================================================================


def get_cypress_record_key() -> Optional[str]:
    """Cypress Cloud recording key."""
    return os.getenv("CYPRESS_RECORD_KEY")


def get_filtered_tests_file() -> Optional[str]:
    """Path to file containing filtered test paths (one per line)."""
    return os.getenv("FILTERED_TESTS")


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
