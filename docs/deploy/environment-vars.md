---
title: "Deployment Environment Variables"
---

# Environment Variables

The following is a summary of a few important environment variables which expose various levers which control how
DataHub works.

---

# DataHub Java Components

This includes GMS, System Update, MAE/MCE Consumers.

## Authentication & Authorization

Reference Links:

- **Authentication Overview**: [Authentication Overview](../authentication/README.md)
- **Authentication Concepts**: [Authentication Concepts](../authentication/concepts.md)
- **Metadata Service Authentication**: [Introducing Metadata Service Authentication](../authentication/introducing-metadata-service-authentication.md)
- **OIDC Configuration**: [Configure OIDC Authentication](../authentication/guides/sso/configure-oidc-react.md)
- **Adding Users**: [Adding Users Guide](../authentication/guides/add-users.md)
- **Plugin Configuration**: [Plugin Documentation](../plugins.md)

### Authentication Configuration

| Environment Variable                                | Default    | Description                                                                 | Components                                                      |
| --------------------------------------------------- | ---------- | --------------------------------------------------------------------------- | --------------------------------------------------------------- |
| `METADATA_SERVICE_AUTH_ENABLED`                     | `true`     | Enable if you want all requests to the Metadata Service to be authenticated | GMS, MAE Consumer, MCE Consumer, PE Consumer, Frontend          |
| `DATAHUB_SYSTEM_CLIENT_SECRET`                      |            | System client secret used by AuthServiceController                          | GMS, MAE Consumer, MCE Consumer, PE Consumer, Actions, Frontend |
| `METADATA_SERVICE_AUTHENTICATOR_EXCEPTIONS_ENABLED` | `false`    | Normally failures are only warnings, enable this to throw them              | GMS                                                             |
| `DATAHUB_TOKEN_SERVICE_SIGNING_KEY`                 |            | Key used to validate incoming tokens and sign new tokens                    | GMS                                                             |
| `DATAHUB_TOKEN_SERVICE_SALT`                        |            | Salt used for token validation and signing                                  | GMS                                                             |
| `DATAHUB_TOKEN_SERVICE_SIGNING_ALGORITHM`           | `HS256`    | Signing algorithm for DataHub tokens                                        | GMS                                                             |
| `SESSION_TOKEN_DURATION_MS`                         | `86400000` | The max duration of a UI session in milliseconds (defaults to 1 day)        | GMS                                                             |
| `GUEST_AUTHENTICATION_USER`                         | `guest`    | Guest user for unauthenticated access                                       | GMS                                                             |
| `GUEST_AUTHENTICATION_ENABLED`                      | `false`    | Enable guest authentication                                                 | GMS                                                             |

### Authorization Configuration

| Environment Variable                                    | Default | Description                                                      | Components |
| ------------------------------------------------------- | ------- | ---------------------------------------------------------------- | ---------- |
| `AUTH_POLICIES_ENABLED`                                 | `true`  | Enable the default DataHub policies-based authorizer             | GMS        |
| `POLICY_CACHE_REFRESH_INTERVAL_SECONDS`                 | `120`   | Cache refresh interval for policies in seconds                   | GMS        |
| `POLICY_CACHE_FETCH_SIZE`                               | `1000`  | Cache policy fetch size                                          | GMS        |
| `REST_API_AUTHORIZATION_ENABLED`                        | `true`  | Enable authorization of reads, writes, and deletes on REST APIs  | GMS        |
| `VIEW_AUTHORIZATION_ENABLED`                            | `false` | Controls whether entity pages can limit access based on policies | GMS        |
| `VIEW_AUTHORIZATION_RECOMMENDATIONS_PEER_GROUP_ENABLED` | `true`  | Enable peer group recommendations for view authorization         | GMS        |

## Ingestion Configuration

Reference Links:

- **CLI Configuration**: [CLI Documentation](../cli.md)
- **DataHub Actions**: [Actions Documentation](../actions/README.md)

| Environment Variable                        | Default | Description                                                                                      | Components        |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------------------------------ | ----------------- |
| `UI_INGESTION_ENABLED`                      | `true`  | Enable UI-based ingestion                                                                        | GMS, MAE Consumer |
| `INGESTION_BATCH_REFRESH_COUNT`             | `100`   | Number of entities to refresh in a single batch when refreshing entities after ingestion         | GMS               |
| `INGESTION_SOURCE_REFRESH_INTERVAL_SECONDS` | `43200` | Interval at which the ingestion source scheduler will check for new or updated ingestion sources | GMS               |

## Telemetry & Analytics

| Environment Variable          | Default | Description                          | Components |
| ----------------------------- | ------- | ------------------------------------ | ---------- |
| `INGESTION_REPORTING_ENABLED` | `false` | Enable ingestion reporting           | GMS        |
| `ENABLE_THIRD_PARTY_LOGGING`  | `false` | Whether mixpanel tracking is enabled | GMS        |

## DataHub Core Configuration

| Environment Variable                   | Default     | Description                                                       | Components |
| -------------------------------------- | ----------- | ----------------------------------------------------------------- | ---------- |
| `DATAHUB_SERVER_TYPE`                  | `prod`      | DataHub server type                                               | GMS        |
| `DATAHUB_GMS_ASYNC_REQUEST_TIMEOUT_MS` | `55000`     | Async request timeout for GMS                                     | GMS        |
| `DATAHUB_GMS_HOST`                     | `localhost` | GMS host                                                          | Frontend   |
| `DATAHUB_GMS_PORT`                     | `8080`      | GMS port                                                          | Frontend   |
| `DATAHUB_GMS_USE_SSL`                  | `false`     | Use SSL for GMS connections                                       | Frontend   |
| `DATAHUB_GMS_URI`                      | `null`      | URI instead of separate host/port/ssl parameters (takes priority) | Frontend   |
| `DATAHUB_GMS_SSL_PROTOCOL`             | `null`      | SSL protocol for GMS                                              | Frontend   |

### Plugin Configuration

| Environment Variable                                 | Default                          | Description                                            | Components |
| ---------------------------------------------------- | -------------------------------- | ------------------------------------------------------ | ---------- |
| `PLUGIN_SECURITY_MODE`                               | `RESTRICTED`                     | Plugin security mode (RESTRICTED or LENIENT)           | GMS        |
| `ENTITY_REGISTRY_PLUGIN_PATH`                        | `/etc/datahub/plugins/models`    | Path for entity registry plugins                       | GMS        |
| `ENTITY_REGISTRY_PLUGIN_LOAD_DELAY_SECONDS`          | `60`                             | Rate at which plugin runnable executes                 | GMS        |
| `IGNORE_FAILURE_WHEN_LOADING_ENTITY_REGISTRY_PLUGIN` | `true`                           | Whether to ignore failure when loading entity registry | GMS        |
| `RETENTION_PLUGIN_PATH`                              | `/etc/datahub/plugins/retention` | Path for retention plugins                             | GMS        |
| `AUTH_PLUGIN_PATH`                                   | `/etc/datahub/plugins/auth`      | Path for auth plugins                                  | GMS        |

### Metrics Configuration

| Environment Variable                                    | Default                           | Description                                    | Components        |
| ------------------------------------------------------- | --------------------------------- | ---------------------------------------------- | ----------------- |
| `DATAHUB_METRICS_HOOK_LATENCY_PERCENTILES`              | `0.5,0.95,0.99,0.999`             | Hook latency percentiles                       | GMS, MAE Consumer |
| `DATAHUB_METRICS_HOOK_LATENCY_SERVICE_LEVEL_OBJECTIVES` | `300,1800,3000,10800,21600,43200` | Hook latency SLOs in seconds                   | GMS, MAE Consumer |
| `DATAHUB_METRICS_HOOK_LATENCY_MAX_EXPECTED_VALUE`       | `86000`                           | Maximum expected hook latency value in seconds | GMS, MAE Consumer |

## Entity Service Configuration

| Environment Variable                       | Default | Description                   | Components        |
| ------------------------------------------ | ------- | ----------------------------- | ----------------- |
| `ENTITY_SERVICE_IMPL`                      | `ebean` | Entity service implementation | GMS, MCE Consumer |
| `ENTITY_SERVICE_ENABLE_RETENTION`          | `true`  | Enable entity retention       | GMS, MCE Consumer |
| `ENTITY_SERVICE_APPLY_RETENTION_BOOTSTRAP` | `false` | Apply retention on bootstrap  | GMS, MCE Consumer |

### Aspect Size Validation

Protects against aspects exceeding Jackson's 16MB deserialization limit. **Debugging flags - enable only when troubleshooting service crashes or memory pressure from oversized aspects.**

| Environment Variable                                              | Default              | Description                                                                                                       | Components |
| ----------------------------------------------------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------- | ---------- |
| `DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_ENABLED`                | `false`              | Enable pre-patch validation - checks existing aspects from DB before patch application                            | GMS        |
| `DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_MAX_SIZE_BYTES`         | 15728640             | Max size in bytes for pre-patch aspects (15MB, safety margin below 16MB Jackson limit)                            | GMS        |
| `DATAHUB_VALIDATION_ASPECT_SIZE_PRE_PATCH_OVERSIZED_REMEDIATION`  | `REPLACE_WITH_PATCH` | Remediation for oversized pre-patch aspects: `REPLACE_WITH_PATCH` (delete old, continue) or `IGNORE` (reject MCP) | GMS        |
| `DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_ENABLED`               | `false`              | Enable post-patch validation - checks aspects after patch application, before DB write                            | GMS        |
| `DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_MAX_SIZE_BYTES`        | 15728640             | Max size in bytes for post-patch aspects (15MB)                                                                   | GMS        |
| `DATAHUB_VALIDATION_ASPECT_SIZE_POST_PATCH_OVERSIZED_REMEDIATION` | `DELETE`             | Remediation for oversized post-patch aspects: `DELETE` (remove from DB) or `IGNORE` (log only)                    | GMS        |

**Validation points:**

- **Pre-patch:** Validates existing aspect from database before applying patches. Zero overhead (uses JSON already fetched from DB).
- **Post-patch:** Validates aspect after patch application, before DB write. Zero overhead (uses JSON already created for DB write).

**Remediation strategies:**

- `REPLACE_WITH_PATCH` (pre-patch only): Deletes oversized existing aspect, continues with write as insert (no merge). If new patch is also oversized, post-patch validation will catch it.
- `DELETE` (post-patch only): Hard deletes oversized aspect from database, logs WARNING, routes MCP to `FailedMetadataChangeProposal` topic
- `IGNORE` (both): Leaves aspect in database (pre-patch) or rejects write (post-patch), logs WARNING, routes MCP to `FailedMetadataChangeProposal` topic

**When to enable:** Use temporarily when investigating GMS crashes, debugging memory pressure, or cleaning up pre-existing oversized data. Prefer fixing the root cause at ingestion time.

See [MCP/MCL Events - Aspect Size Validation](../advanced/mcp-mcl.md#aspect-size-validation) for configuration examples and troubleshooting.

## Graph Service Configuration

| Environment Variable                      | Default         | Description                                                                 | Components        |
| ----------------------------------------- | --------------- | --------------------------------------------------------------------------- | ----------------- |
| `GRAPH_SERVICE_IMPL`                      | `elasticsearch` | Graph service implementation                                                | GMS, MAE Consumer |
| `GRAPH_SERVICE_LIMIT_RESULTS_MAX`         | `10000`         | Maximum allowed result count for queries                                    | GMS               |
| `GRAPH_SERVICE_LIMIT_RESULTS_API_DEFAULT` | `5000`          | Default API result limit                                                    | GMS               |
| `GRAPH_SERVICE_LIMIT_RESULTS_STRICT`      | `false`         | Throw exception if strict is true, otherwise override with default and warn | GMS               |

## Search Service Configuration

| Environment Variable                                  | Default             | Description                                                                 | Components |
| ----------------------------------------------------- | ------------------- | --------------------------------------------------------------------------- | ---------- |
| `SEARCH_SERVICE_BATCH_SIZE`                           | `100`               | Search service batch size                                                   | GMS        |
| `SEARCH_SERVICE_ENABLE_CACHE`                         | `false`             | Enable search service cache                                                 | GMS        |
| `SEARCH_SERVICE_ENABLE_CACHE_EVICTION`                | `false`             | Enable search service cache eviction                                        | GMS        |
| `SEARCH_SERVICE_CACHE_IMPLEMENTATION`                 | `caffeine`          | Search service cache implementation                                         | GMS        |
| `SEARCH_SERVICE_HAZELCAST_SERVICE_NAME`               | `hazelcast-service` | Hazelcast service name for search cache                                     | GMS        |
| `SEARCH_SERVICE_FILTER_CONTAINER_EXPANSION_ENABLED`   | `true`              | Enable container expansion in search filters                                | GMS        |
| `SEARCH_SERVICE_FILTER_CONTAINER_EXPANSION_PAGE_SIZE` | `100`               | Page size for container expansion                                           | GMS        |
| `SEARCH_SERVICE_FILTER_CONTAINER_EXPANSION_LIMIT`     | `100`               | Limit for container expansion                                               | GMS        |
| `SEARCH_SERVICE_FILTER_DOMAIN_EXPANSION_ENABLED`      | `true`              | Enable domain expansion in search filters                                   | GMS        |
| `SEARCH_SERVICE_FILTER_DOMAIN_EXPANSION_PAGE_SIZE`    | `100`               | Page size for domain expansion                                              | GMS        |
| `SEARCH_SERVICE_FILTER_DOMAIN_EXPANSION_LIMIT`        | `100`               | Limit for domain expansion                                                  | GMS        |
| `SEARCH_SERVICE_LIMIT_RESULTS_MAX`                    | `10000`             | Maximum allowed result count for queries                                    | GMS        |
| `SEARCH_SERVICE_LIMIT_RESULTS_API_DEFAULT`            | `5000`              | Default API result limit                                                    | GMS        |
| `SEARCH_SERVICE_LIMIT_RESULTS_STRICT`                 | `false`             | Throw exception if strict is true, otherwise override with default and warn | GMS        |

## Timeseries Aspect Service

| Environment Variable                                  | Default | Description                                                                 | Components |
| ----------------------------------------------------- | ------- | --------------------------------------------------------------------------- | ---------- |
| `TIMESERIES_ASPECT_SERVICE_QUERY_CONCURRENCY`         | `10`    | Parallel threads for timeseries queries                                     | GMS        |
| `TIMESERIES_ASPECT_SERVICE_QUERY_QUEUE_SIZE`          | `500`   | Queue size for timeseries queries                                           | GMS        |
| `TIMESERIES_ASPECT_SERVICE_QUERY_THREAD_KEEP_ALIVE`   | `60`    | Thread keep alive time for timeseries queries                               | GMS        |
| `TIMESERIES_ASPECT_SERVICE_LIMIT_RESULTS_MAX`         | `10000` | Maximum allowed result count for queries                                    | GMS        |
| `TIMESERIES_ASPECT_SERVICE_LIMIT_RESULTS_API_DEFAULT` | `5000`  | Default API result limit                                                    | GMS        |
| `TIMESERIES_ASPECT_SERVICE_LIMIT_RESULTS_STRICT`      | `false` | Throw exception if strict is true, otherwise override with default and warn | GMS        |

## System Metadata Service

| Environment Variable                                | Default | Description                                                                 | Components |
| --------------------------------------------------- | ------- | --------------------------------------------------------------------------- | ---------- |
| `SYSTEM_METADATA_SERVICE_LIMIT_RESULTS_MAX`         | `10000` | Maximum allowed result count for queries                                    | GMS        |
| `SYSTEM_METADATA_SERVICE_LIMIT_RESULTS_API_DEFAULT` | `5000`  | Default API result limit                                                    | GMS        |
| `SYSTEM_METADATA_SERVICE_LIMIT_RESULTS_STRICT`      | `false` | Throw exception if strict is true, otherwise override with default and warn | GMS        |

## Platform Analytics

| Environment Variable                  | Default                                                                                                                                                                                     | Description                                            | Components                  |
| ------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ | --------------------------- |
| `DATAHUB_ANALYTICS_ENABLED`           | `true`                                                                                                                                                                                      | Enable platform analytics                              | GMS, MAE Consumer, Frontend |
| `DATAHUB_ANALYTICS_TRACING_ENABLED`   | `true`                                                                                                                                                                                      | Enable backend usage tracing                           | GMS                         |
| `ANALYTICS_DATAHUB_USAGE_EVENT_TYPES` | `CreateAccessTokenEvent,CreatePolicyEvent,UpdatePolicyEvent,CreateIngestionSourceEvent,UpdateIngestionSourceEvent,RevokeAccessTokenEvent,CreateUserEvent,UpdateUserEvent,DeletePolicyEvent` | Comma separated list of usage event types to listen to | GMS                         |
| `ANALYTICS_GENERIC_ASPECT_TYPES`      | ``                                                                                                                                                                                          | Filter list for generic aspect events                  | GMS                         |
| `ANALYTICS_USER_FILTERS`              | ``                                                                                                                                                                                          | Filter out specific users' events from being published | GMS                         |

## Visual Configuration

### Queries Tab

| Environment Variable                | Default | Description                            | Components |
| ----------------------------------- | ------- | -------------------------------------- | ---------- |
| `REACT_APP_QUERIES_TAB_RESULT_SIZE` | `5`     | Queries tab result size (experimental) | Frontend   |

### Theme Configuration

| Environment Variable        | Default | Description                                       | Components |
| --------------------------- | ------- | ------------------------------------------------- | ---------- |
| `REACT_APP_CUSTOM_THEME_ID` | ``      | Custom theme ID for rendering specific theme file | Frontend   |

### Assets Configuration

| Environment Variable    | Default                             | Description                     | Components |
| ----------------------- | ----------------------------------- | ------------------------------- | ---------- |
| `REACT_APP_LOGO_URL`    | `/assets/platforms/datahublogo.png` | Logo URL for the application    | Frontend   |
| `REACT_APP_FAVICON_URL` | `/assets/icons/favicon.ico`         | Favicon URL for the application | Frontend   |
| `REACT_APP_TITLE`       | ``                                  | Application title               | Frontend   |

### UI Configuration

| Environment Variable                          | Default | Description                                                                        | Components |
| --------------------------------------------- | ------- | ---------------------------------------------------------------------------------- | ---------- |
| `REACT_APP_HIDE_GLOSSARY`                     | `false` | Hide glossary in the UI                                                            | Frontend   |
| `REACT_APP_SHOW_FULL_TITLE_IN_LINEAGE`        | `false` | Show full title in lineage                                                         | Frontend   |
| `DOMAIN_DEFAULT_TAB`                          | ``      | Default tab for domains (set to DOCUMENTATION_TAB to show documentation tab first) | Frontend   |
| `APPLICATION_SHOW_SIDEBAR_SECTION_WHEN_EMPTY` | `false` | Show sidebar section when empty (deprecated)                                       | Frontend   |
| `SEARCH_RESULT_NAME_HIGHLIGHT_ENABLED`        | `true`  | Enable visual highlighting on search result names/descriptions                     | Frontend   |

## Storage Layer Configuration

### EBean Configuration (MySQL/PostgreSQL)

| Environment Variable              | Default                               | Description                                     | Components                       |
| --------------------------------- | ------------------------------------- | ----------------------------------------------- | -------------------------------- |
| `EBEAN_DATASOURCE_USERNAME`       | `datahub`                             | Database username                               | GMS, MCE Consumer, System Update |
| `EBEAN_DATASOURCE_PASSWORD`       | `datahub`                             | Database password                               | GMS, MCE Consumer, System Update |
| `EBEAN_DATASOURCE_URL`            | `jdbc:mysql://localhost:3306/datahub` | JDBC URL                                        | GMS, MCE Consumer, System Update |
| `EBEAN_DATASOURCE_DRIVER`         | `com.mysql.jdbc.Driver`               | JDBC Driver                                     | GMS, MCE Consumer, System Update |
| `EBEAN_MIN_CONNECTIONS`           | `2`                                   | Minimum database connections                    | GMS, MCE Consumer, System Update |
| `EBEAN_MAX_CONNECTIONS`           | `50`                                  | Maximum database connections                    | GMS, MCE Consumer, System Update |
| `EBEAN_MAX_INACTIVE_TIME_IN_SECS` | `120`                                 | Maximum inactive time in seconds                | GMS, MCE Consumer, System Update |
| `EBEAN_MAX_AGE_MINUTES`           | `120`                                 | Maximum age in minutes                          | GMS, MCE Consumer, System Update |
| `EBEAN_LEAK_TIME_MINUTES`         | `15`                                  | Leak time in minutes                            | GMS, MCE Consumer, System Update |
| `EBEAN_WAIT_TIMEOUT_MILLIS`       | `1000`                                | Wait timeout in milliseconds                    | GMS, MCE Consumer, System Update |
| `EBEAN_AUTOCREATE`                | `false`                               | Auto-create DDL                                 | GMS, MCE Consumer, System Update |
| `EBEAN_POSTGRES_USE_AWS_IAM_AUTH` | `false`                               | Use AWS IAM authentication for PostgreSQL       | GMS, MCE Consumer, System Update |
| `EBEAN_USE_IAM_AUTH`              | `false`                               | Enable cross-cloud IAM authentication (AWS/GCP) | GMS, MCE Consumer, System Update |
| `EBEAN_CLOUD_PROVIDER`            | `auto`                                | Cloud provider (auto/aws/gcp/traditional)       | GMS, MCE Consumer, System Update |
| `EBEAN_BATCH_GET_METHOD`          | `IN`                                  | Batch get method (IN or UNION)                  | GMS, MCE Consumer, System Update |
| `EBEAN_URL`                       | _same as EBEAN_DATASOURCE_URL_        | Alternative property for database URL           | System Update                    |
| `EBEAN_MAX_TRANSACTION_RETRY`     | `null`                                | Maximum transaction retries for Ebean           | System Update                    |

#### Cross-Cloud IAM Authentication

DataHub supports cross-cloud IAM authentication for both AWS and GCP cloud providers. This enables secure, passwordless database connections using cloud identity services.

**AWS IAM Authentication:**

- **PostgreSQL**: Uses native `wrapperPlugins: "iam"` configuration
- **MySQL**: Automatically swaps to MariaDB driver with `credentialType=AWS-IAM`
- **Detection**: Based on `AWS_REGION`, `AWS_ACCESS_KEY_ID`, or RDS URLs

**GCP IAM Authentication:**

- **MySQL/PostgreSQL**: Uses Cloud SQL Connector with `enableIamAuth=true`
- **Detection**: Based on `GOOGLE_APPLICATION_CREDENTIALS`, `GCP_PROJECT`, or Cloud SQL URLs

**Configuration Examples:**

```bash
# AWS RDS with IAM authentication
export EBEAN_USE_IAM_AUTH=true
export EBEAN_CLOUD_PROVIDER=aws
export EBEAN_DATASOURCE_URL=jdbc:mysql://rds-instance.amazonaws.com:3306/datahub
export AWS_REGION=us-west-2

# GCP Cloud SQL with IAM authentication
export EBEAN_USE_IAM_AUTH=true
export EBEAN_CLOUD_PROVIDER=gcp
export EBEAN_DATASOURCE_URL=jdbc:mysql://cloudsql-instance:3306/datahub
export INSTANCE_CONNECTION_NAME="project:region:instance"
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

# Auto-detection (recommended)
export EBEAN_USE_IAM_AUTH=true
export EBEAN_CLOUD_PROVIDER=auto
# Cloud provider automatically detected from environment variables
```

**Required Cloud-Specific Environment Variables:**

| Cloud Provider | Required Variables               | Description                                             |
| -------------- | -------------------------------- | ------------------------------------------------------- |
| **AWS**        | `AWS_REGION`                     | AWS region for RDS instances                            |
| **AWS**        | `AWS_ACCESS_KEY_ID`              | AWS access key (or use instance profile)                |
| **AWS**        | `AWS_SECRET_ACCESS_KEY`          | AWS secret key (or use instance profile)                |
| **AWS**        | `AWS_SESSION_TOKEN`              | AWS session token (optional, for temporary credentials) |
| **GCP**        | `INSTANCE_CONNECTION_NAME`       | Cloud SQL instance connection name                      |
| **GCP**        | `GOOGLE_APPLICATION_CREDENTIALS` | Path to service account JSON file                       |
| **GCP**        | `GCP_PROJECT`                    | GCP project ID (optional, for auto-detection)           |

### SQL Setup Configuration

The SQL Setup system provides automated database initialization and user management capabilities. These environment variables control the behavior of the `SqlSetup` upgrade step.

| Environment Variable         | Default       | Description                                                                  | Components    |
| ---------------------------- | ------------- | ---------------------------------------------------------------------------- | ------------- |
| `DATAHUB_SQL_SETUP_ENABLED`  | `false`       | Enable SQL setup functionality (alternative to passing SqlSetup upgrade arg) | System Update |
| `CREATE_TABLES`              | `true`        | Whether to create database tables                                            | System Update |
| `CREATE_DB`                  | `true`        | Whether to create the database (PostgreSQL only)                             | System Update |
| `CREATE_USER`                | `false`       | Whether to create a new database user                                        | System Update |
| `CREATE_USER_USERNAME`       | _none_        | Username for the new database user to create (required if CREATE_USER=true)  | System Update |
| `CREATE_USER_PASSWORD`       | _none_        | Password for the new database user to create (required for traditional auth) | System Update |
| `CDC_MCL_PROCESSING_ENABLED` | `false`       | Whether to create a CDC (Change Data Capture) user                           | System Update |
| `CDC_USER`                   | `datahub_cdc` | Username for the CDC user                                                    | System Update |
| `CDC_PASSWORD`               | `datahub_cdc` | Password for the CDC user                                                    | System Update |

**Note:** When `CREATE_USER=true`, you must explicitly set `CREATE_USER_USERNAME` environment variable. The system will not fall back to Ebean connection credentials for security reasons.

**IAM Authentication:** IAM authentication is automatically detected when `CREATE_USER=true` and `CREATE_USER_PASSWORD` is not set or is empty. The system will create users with IAM authentication for supported cloud databases:

- **AWS RDS MySQL**: Creates user with AWSAuthenticationPlugin
- **AWS RDS PostgreSQL**: Creates user and grants `rds_iam` role
- **GCP Cloud SQL**: IAM authentication is managed through Cloud SQL IAM database users

When using traditional username/password authentication, both `CREATE_USER_USERNAME` and `CREATE_USER_PASSWORD` must be set.

### Cassandra Configuration

| Environment Variable            | Default       | Description           | Components                       |
| ------------------------------- | ------------- | --------------------- | -------------------------------- |
| `CASSANDRA_DATASOURCE_USERNAME` | `cassandra`   | Cassandra username    | GMS, MCE Consumer, System Update |
| `CASSANDRA_DATASOURCE_PASSWORD` | `cassandra`   | Cassandra password    | GMS, MCE Consumer, System Update |
| `CASSANDRA_HOSTS`               | `cassandra`   | Cassandra hosts       | GMS, MCE Consumer, System Update |
| `CASSANDRA_PORT`                | `9042`        | Cassandra port        | GMS, MCE Consumer, System Update |
| `CASSANDRA_DATACENTER`          | `datacenter1` | Cassandra datacenter  | GMS, MCE Consumer, System Update |
| `CASSANDRA_KEYSPACE`            | `datahub`     | Cassandra keyspace    | GMS, MCE Consumer, System Update |
| `CASSANDRA_USE_SSL`             | `false`       | Use SSL for Cassandra | GMS, MCE Consumer, System Update |

### Elasticsearch Configuration

| Environment Variable                       | Default         | Description                                  | Components                                     |
| ------------------------------------------ | --------------- | -------------------------------------------- | ---------------------------------------------- |
| `ELASTICSEARCH_HOST`                       | `localhost`     | Elasticsearch host                           | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_PORT`                       | `9200`          | Elasticsearch port                           | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_THREAD_COUNT`               | `2`             | Elasticsearch thread count                   | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT` | `5000`          | Connection request timeout                   | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_USERNAME`                   | `null`          | Elasticsearch username                       | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_PASSWORD`                   | `null`          | Elasticsearch password                       | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_PATH_PREFIX`                | `null`          | Elasticsearch path prefix                    | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_USE_SSL`                    | `false`         | Use SSL for Elasticsearch                    | GMS, MAE Consumer, MCE Consumer, System Update |
| `OPENSEARCH_USE_AWS_IAM_AUTH`              | `false`         | Use AWS IAM authentication for OpenSearch    | GMS, MAE Consumer, MCE Consumer, System Update |
| `AWS_REGION`                               | `null`          | AWS region                                   | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_IMPLEMENTATION`             | `elasticsearch` | Implementation (elasticsearch or opensearch) | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTIC_ID_HASH_ALGO`                     | `MD5`           | ID hash algorithm                            | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_DATA_NODE_COUNT`            | `1`             | Number of Elasticsearch data nodes           | GMS, MAE Consumer, MCE Consumer, System Update |

#### SSL Context Configuration

| Environment Variable                    | Default | Description                      | Components                                     |
| --------------------------------------- | ------- | -------------------------------- | ---------------------------------------------- |
| `ELASTICSEARCH_SSL_PROTOCOL`            | `null`  | SSL protocol                     | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL`  | `null`  | SSL secure random implementation | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_TRUSTSTORE_FILE`     | `null`  | SSL truststore file              | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_TRUSTSTORE_TYPE`     | `null`  | SSL truststore type              | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD` | `null`  | SSL truststore password          | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_KEYSTORE_FILE`       | `null`  | SSL keystore file                | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_KEYSTORE_TYPE`       | `null`  | SSL keystore type                | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_KEYSTORE_PASSWORD`   | `null`  | SSL keystore password            | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_SSL_KEY_PASSWORD`        | `null`  | SSL key password                 | GMS, MAE Consumer, MCE Consumer, System Update |

#### Bulk Operations Configuration

| Environment Variable           | Default   | Description                   | Components        |
| ------------------------------ | --------- | ----------------------------- | ----------------- |
| `ES_BULK_DELETE_BATCH_SIZE`    | `5000`    | Bulk delete batch size        | GMS, MAE Consumer |
| `ES_BULK_DELETE_SLICES`        | `auto`    | Bulk delete slices            | GMS, MAE Consumer |
| `ES_BULK_DELETE_POLL_INTERVAL` | `30`      | Bulk delete poll interval     | GMS, MAE Consumer |
| `ES_BULK_DELETE_POLL_UNIT`     | `SECONDS` | Bulk delete poll unit         | GMS, MAE Consumer |
| `ES_BULK_DELETE_TIMEOUT`       | `30`      | Bulk delete timeout           | GMS, MAE Consumer |
| `ES_BULK_DELETE_TIMEOUT_UNIT`  | `MINUTES` | Bulk delete timeout unit      | GMS, MAE Consumer |
| `ES_BULK_DELETE_NUM_RETRIES`   | `3`       | Bulk delete number of retries | GMS, MAE Consumer |
| `ES_BULK_ASYNC`                | `true`    | Enable async bulk operations  | GMS, MAE Consumer |
| `ES_BULK_REQUESTS_LIMIT`       | `1000`    | Bulk requests limit           | GMS, MAE Consumer |
| `ES_BULK_FLUSH_PERIOD`         | `1`       | Bulk flush period             | GMS, MAE Consumer |
| `ES_BULK_NUM_RETRIES`          | `3`       | Bulk number of retries        | GMS, MAE Consumer |
| `ES_BULK_RETRY_INTERVAL`       | `1`       | Bulk retry interval           | GMS, MAE Consumer |
| `ES_BULK_REFRESH_POLICY`       | `NONE`    | Bulk refresh policy           | GMS, MAE Consumer |
| `ES_BULK_ENABLE_BATCH_DELETE`  | `false`   | Enable batch delete           | GMS, MAE Consumer |

#### Index Configuration

| Environment Variable                                       | Default | Description                             | Components                                     |
| ---------------------------------------------------------- | ------- | --------------------------------------- | ---------------------------------------------- |
| `INDEX_PREFIX`                                             | ``      | Index prefix                            | GMS, MAE Consumer, MCE Consumer, System Update |
| `ELASTICSEARCH_INDEX_DOC_IDS_SCHEMA_FIELD_HASH_ID_ENABLED` | `false` | Enable hash ID for schema field doc IDs | GMS, MAE Consumer, MCE Consumer, System Update |

#### Build Indices Configuration

| Environment Variable                                       | Default                          | Description                                                 | Components    |
| ---------------------------------------------------------- | -------------------------------- | ----------------------------------------------------------- | ------------- |
| `ELASTICSEARCH_BUILD_INDICES_ALLOW_DOC_COUNT_MISMATCH`     | `false`                          | Allow document count mismatch when clone indices is enabled | System Update |
| `ELASTICSEARCH_BUILD_INDICES_CLONE_INDICES`                | `true`                           | Clone indices                                               | System Update |
| `ELASTICSEARCH_BUILD_INDICES_RETENTION_UNIT`               | `DAYS`                           | Retention unit for indices                                  | System Update |
| `ELASTICSEARCH_BUILD_INDICES_RETENTION_VALUE`              | `60`                             | Retention value for indices                                 | System Update |
| `ELASTICSEARCH_BUILD_INDICES_REINDEX_OPTIMIZATION_ENABLED` | `true`                           | Enable reindex optimization                                 | System Update |
| `ELASTICSEARCH_NUM_SHARDS_PER_INDEX`                       | `${elasticsearch.dataNodeCount}` | Number of shards per index, defaults to dataNodeCount       | System Update |
| `ELASTICSEARCH_NUM_REPLICAS_PER_INDEX`                     | `1`                              | Number of replicas per index                                | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_NUM_RETRIES`                  | `3`                              | Index builder number of retries                             | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_REFRESH_INTERVAL_SECONDS`     | `3`                              | Index builder refresh interval                              | System Update |
| `SEARCH_DOCUMENT_MAX_ARRAY_LENGTH`                         | `1000`                           | Maximum array length in search documents                    | System Update |
| `SEARCH_DOCUMENT_MAX_OBJECT_KEYS`                          | `1000`                           | Maximum object keys in search documents                     | System Update |
| `SEARCH_DOCUMENT_MAX_VALUE_LENGTH`                         | `4096`                           | Maximum value length in search documents                    | System Update |
| `ELASTICSEARCH_MAIN_TOKENIZER`                             | `null`                           | Main tokenizer                                              | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_MAPPINGS_REINDEX`             | `false`                          | Enable mappings reindex                                     | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_SETTINGS_REINDEX`             | `false`                          | Enable settings reindex                                     | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_MAX_REINDEX_HOURS`            | `0`                              | Maximum reindex hours (0 = no timeout)                      | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_SETTINGS_OVERRIDES`           | `null`                           | Index builder settings overrides                            | System Update |
| `ELASTICSEARCH_MIN_SEARCH_FILTER_LENGTH`                   | `3`                              | Minimum search filter length                                | System Update |
| `ELASTICSEARCH_INDEX_BUILDER_ENTITY_SETTINGS_OVERRIDES`    | `null`                           | Entity settings overrides                                   | System Update |

#### Search Configuration

| Environment Variable                                    | Default              | Description                                    | Components |
| ------------------------------------------------------- | -------------------- | ---------------------------------------------- | ---------- |
| `ELASTICSEARCH_QUERY_MAX_TERM_BUCKET_SIZE`              | `60`                 | Maximum term bucket size                       | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_EXCLUSIVE`             | `false`              | Only return exact matches when using quotes    | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_WITH_PREFIX`           | `true`               | Include prefix match in exact match results    | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_FACTOR`                | `16.0`               | Multiply by this number on true exact match    | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_PREFIX_FACTOR`         | `1.1`                | Multiply by this number when prefix match      | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_CASE_FACTOR`           | `0.0`                | Stacked boost multiplier when case mismatch    | GMS        |
| `ELASTICSEARCH_QUERY_EXACT_MATCH_ENABLE_STRUCTURED`     | `true`               | Enable exact match on structured search        | GMS        |
| `ELASTICSEARCH_QUERY_TWO_GRAM_FACTOR`                   | `1.2`                | Boost multiplier when match on 2-gram tokens   | GMS        |
| `ELASTICSEARCH_QUERY_THREE_GRAM_FACTOR`                 | `1.5`                | Boost multiplier when match on 3-gram tokens   | GMS        |
| `ELASTICSEARCH_QUERY_FOUR_GRAM_FACTOR`                  | `1.8`                | Boost multiplier when match on 4-gram tokens   | GMS        |
| `ELASTICSEARCH_QUERY_PARTIAL_URN_FACTOR`                | `0.5`                | Multiplier on Urn token match                  | GMS        |
| `ELASTICSEARCH_QUERY_PARTIAL_FACTOR`                    | `0.4`                | Multiplier on possible non-Urn token match     | GMS        |
| `ELASTICSEARCH_QUERY_CUSTOM_CONFIG_ENABLED`             | `true`               | Enable search query and ranking customization  | GMS        |
| `ELASTICSEARCH_QUERY_CUSTOM_CONFIG_FILE`                | `search_config.yaml` | Location of search customization configuration | GMS        |
| `ELASTICSEARCH_QUERY_SEARCH_FIELD_CONFIG_DEFAULT`       | `legacy`             | Default field configuration for search         | GMS        |
| `ELASTICSEARCH_QUERY_AUTOCOMPLETE_FIELD_CONFIG_DEFAULT` | `legacy`             | Default field configuration for autocomplete   | GMS        |

#### Graph Search Configuration

| Environment Variable                                        | Default                          | Description                                                                                           | Components |
| ----------------------------------------------------------- | -------------------------------- | ----------------------------------------------------------------------------------------------------- | ---------- |
| `ELASTICSEARCH_SEARCH_GRAPH_TIMEOUT_SECONDS`                | `50`                             | Graph DAO timeout seconds                                                                             | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_BATCH_SIZE`                     | `1000`                           | Graph DAO batch size                                                                                  | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_MULTI_PATH_SEARCH`              | `false`                          | Allow path retraversal for all paths                                                                  | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_BOOST_VIA_NODES`                | `true`                           | Boost graph edges with via nodes                                                                      | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_STATUS_ENABLED`                 | `false`                          | Enable soft delete tracking of URNs on edges                                                          | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_LINEAGE_MAX_HOPS`               | `20`                             | Maximum hops to traverse lineage graph                                                                | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_MAX_HOPS`                | `1000`                           | Maximum hops to traverse for impact analysis (impact.maxHops)                                         | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_MAX_RELATIONS`           | `40000`                          | Maximum number of relationships for impact analysis (impact.maxRelations)                             | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_SLICES`                  | `${elasticsearch.dataNodeCount}` | Number of slices for parallel search operations (impact.slices), defaults to dataNodeCount, minimum 2 | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_KEEP_ALIVE`              | `5m`                             | Point-in-Time keepAlive duration for impact analysis queries (impact.keepAlive)                       | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_PARTIAL_RESULTS`         | `false`                          | If true, return partial results when maxRelations is reached; if false (default), throw an error      | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_IMPACT_MAX_THREADS`             | `32`                             | Maximum parallel lineage graph queries                                                                | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_QUERY_OPTIMIZATION`             | `true`                           | Reduce query nesting if possible                                                                      | GMS        |
| `ELASTICSEARCH_SEARCH_GRAPH_POINT_IN_TIME_CREATION_ENABLED` | `true`                           | Enable creation of point in time snapshots for graph queries                                          | GMS        |

### Neo4j Configuration

| Environment Variable                                  | Default            | Description                            | Components                       |
| ----------------------------------------------------- | ------------------ | -------------------------------------- | -------------------------------- |
| `NEO4J_USERNAME`                                      | `neo4j`            | Neo4j username                         | GMS, MAE Consumer, System Update |
| `NEO4J_PASSWORD`                                      | `datahub`          | Neo4j password                         | GMS, MAE Consumer, System Update |
| `NEO4J_URI`                                           | `bolt://localhost` | Neo4j URI                              | GMS, MAE Consumer, System Update |
| `NEO4J_DATABASE`                                      | `graph.db`         | Neo4j database                         | GMS, MAE Consumer, System Update |
| `NEO4J_MAX_CONNECTION_POOL_SIZE`                      | `100`              | Maximum connection pool size           | GMS, MAE Consumer, System Update |
| `NEO4J_MAX_CONNECTION_ACQUISITION_TIMEOUT_IN_SECONDS` | `60`               | Maximum connection acquisition timeout | GMS, MAE Consumer, System Update |
| `NEO4j_MAX_CONNECTION_LIFETIME_IN_SECONDS`            | `3600`             | Maximum connection lifetime            | GMS, MAE Consumer, System Update |
| `NEO4J_MAX_TRANSACTION_RETRY_TIME_IN_SECONDS`         | `30`               | Maximum transaction retry time         | GMS, MAE Consumer, System Update |
| `NEO4J_CONNECTION_LIVENESS_CHECK_TIMEOUT_IN_SECONDS`  | `-1`               | Connection liveness check timeout      | GMS, MAE Consumer, System Update |

## Kafka Configuration

Reference Links:

- **Kafka Configuration**: [Kafka Configuration Guide](../how/kafka-config.md)
- **Confluent Cloud**: [Confluent Cloud Integration](confluent-cloud.md)
- **DataHub Actions**: [Actions Documentation](../actions/README.md)

### Topic Configuration

| Environment Variable       | Default                | Description                    | Components                                         |
| -------------------------- | ---------------------- | ------------------------------ | -------------------------------------------------- |
| `DATAHUB_USAGE_EVENT_NAME` | `DataHubUsageEvent_v1` | DataHub usage event topic name | GMS, MAE Consumer, MCE Consumer, Actions, Frontend |

### Bootstrap Servers

| Environment Variable     | Default                 | Description             | Components                                                      |
| ------------------------ | ----------------------- | ----------------------- | --------------------------------------------------------------- |
| `KAFKA_BOOTSTRAP_SERVER` | `http://localhost:9092` | Kafka bootstrap servers | GMS, MAE Consumer, MCE Consumer, PE Consumer, Actions, Frontend |

### Producer Configuration

| Environment Variable              | Default   | Description                    | Components                       |
| --------------------------------- | --------- | ------------------------------ | -------------------------------- |
| `KAFKA_PRODUCER_RETRY_COUNT`      | `3`       | Producer retry count           | GMS, MCE Consumer, System Update |
| `KAFKA_PRODUCER_DELIVERY_TIMEOUT` | `30000`   | Producer delivery timeout      | GMS, MCE Consumer, System Update |
| `KAFKA_PRODUCER_REQUEST_TIMEOUT`  | `3000`    | Producer request timeout       | GMS, MCE Consumer, System Update |
| `KAFKA_PRODUCER_BACKOFF_TIMEOUT`  | `500`     | Producer backoff timeout       | GMS, MCE Consumer, System Update |
| `KAFKA_PRODUCER_COMPRESSION_TYPE` | `snappy`  | Producer compression algorithm | GMS, MCE Consumer, System Update |
| `KAFKA_PRODUCER_MAX_REQUEST_SIZE` | `5242880` | Maximum bytes sent by producer | GMS, MCE Consumer, System Update |

### Consumer Configuration

| Environment Variable                              | Default                           | Description                                | Components                                                |
| ------------------------------------------------- | --------------------------------- | ------------------------------------------ | --------------------------------------------------------- |
| `KAFKA_LISTENER_CONCURRENCY`                      | `1`                               | Number of Kafka consumer threads           | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_MAX_PARTITION_FETCH_BYTES`        | `5242880`                         | Maximum data per partition                 | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_STOP_ON_DESERIALIZATION_ERROR`    | `true`                            | Stop on deserialization error              | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_HEALTH_CHECK_ENABLED`             | `true`                            | Enable health check for consumers          | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_MCP_AUTO_OFFSET_RESET`            | `earliest`                        | MCP consumer auto offset reset             | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_MCL_AUTO_OFFSET_RESET`            | `earliest`                        | MCL consumer auto offset reset             | GMS, MAE Consumer, MCE Consumer, PE Consumer              |
| `KAFKA_CONSUMER_MCL_FINE_GRAINED_LOGGING_ENABLED` | `false`                           | Enable fine-grained logging for MCL        | GMS, MAE Consumer                                         |
| `KAFKA_CONSUMER_MCL_ASPECTS_TO_DROP`              | ``                                | Aspects to drop for MCL                    | GMS, MAE Consumer                                         |
| `KAFKA_CONSUMER_PE_AUTO_OFFSET_RESET`             | `latest`                          | PE consumer auto offset reset              | GMS, PE Consumer                                          |
| `KAFKA_CONSUMER_PERCENTILES`                      | `0.5,0.95,0.99,0.999`             | Consumer percentiles                       | GMS, MAE Consumer, MCE Consumer, PE Consumer, PE Consumer |
| `KAFKA_CONSUMER_SERVICE_LEVEL_OBJECTIVES`         | `300,1800,3000,10800,21600,43200` | Consumer SLOs in seconds                   | GMS, MAE Consumer, MCE Consumer, PE Consumer, PE Consumer |
| `KAFKA_CONSUMER_MAX_EXPECTED_VALUE`               | `86000`                           | Maximum expected consumer value in seconds | GMS, MAE Consumer, MCE Consumer, PE Consumer, PE Consumer |

### Consumer Pool Configuration

| Environment Variable                                    | Default | Description                                                 | Components |
| ------------------------------------------------------- | ------- | ----------------------------------------------------------- | ---------- |
| `KAFKA_CONSUMER_POOL_INITIAL_SIZE`                      | `1`     | Consumer pool initial size                                  | GMS        |
| `KAFKA_CONSUMER_POOL_MAX_SIZE`                          | `5`     | Consumer pool maximum size                                  | GMS        |
| `KAFKA_CONSUMER_POOL_VALIDATION_TIMEOUT_SECONDS`        | `5`     | Timeout in seconds for validating consumer health           | GMS        |
| `KAFKA_CONSUMER_POOL_VALIDATION_CACHE_INTERVAL_MINUTES` | `5`     | Interval in minutes for caching consumer validation results | GMS        |

### Schema Registry Configuration

| Environment Variable                 | Default                 | Description                                         | Components                                            |
| ------------------------------------ | ----------------------- | --------------------------------------------------- | ----------------------------------------------------- |
| `SCHEMA_REGISTRY_TYPE`               | `KAFKA`                 | Schema registry type (INTERNAL, KAFKA, or AWS_GLUE) | GMS, MAE Consumer, MCE Consumer, PE Consumer          |
| `KAFKA_SCHEMAREGISTRY_URL`           | `http://localhost:8081` | Schema registry URL                                 | GMS, MAE Consumer, MCE Consumer, PE Consumer          |
| `SCHEMA_REGISTRY_URL`                | `http://localhost:8081` | Schema registry URL (Actions)                       | Actions                                               |
| `AWS_GLUE_SCHEMA_REGISTRY_REGION`    | `us-east-1`             | AWS Glue schema registry region                     | GMS, MAE Consumer, MCE Consumer, PE Consumer          |
| `AWS_GLUE_SCHEMA_REGISTRY_NAME`      | `null`                  | AWS Glue schema registry name                       | GMS, MAE Consumer, MCE Consumer, PE Consumer          |
| `KAFKA_PROPERTIES_SECURITY_PROTOCOL` | `PLAINTEXT`             | Kafka security protocol                             | GMS, MAE Consumer, MCE Consumer, PE Consumer, Actions |

## Spring Configuration

### Kafka Security

| Environment Variable             | Default     | Description             | Components                                   |
| -------------------------------- | ----------- | ----------------------- | -------------------------------------------- |
| `spring.kafka.security.protocol` | `PLAINTEXT` | Kafka security protocol | GMS, MAE Consumer, MCE Consumer, PE Consumer |

## Management & Monitoring

### JMX Configuration

| Environment Variable | Default | Description | Components                                   |
| -------------------- | ------- | ----------- | -------------------------------------------- |
| `spring.jmx.enabled` | `true`  | Enable JMX  | GMS, MAE Consumer, MCE Consumer, PE Consumer |

### Endpoints Configuration

| Environment Variable                        | Default                               | Description           | Components |
| ------------------------------------------- | ------------------------------------- | --------------------- | ---------- |
| `management.endpoints.web.exposure.include` | `prometheus,info,healthcheck,metrics` | Exposed web endpoints | GMS        |
| `management.endpoints.jmx.enabled`          | `true`                                | Enable JMX endpoints  | GMS        |

### Metrics Configuration

| Environment Variable                           | Default | Description                      | Components                                   |
| ---------------------------------------------- | ------- | -------------------------------- | -------------------------------------------- |
| `management.metrics.cache.enabled`             | `false` | Enable cache metrics             | GMS, MAE Consumer, MCE Consumer, PE Consumer |
| `management.metrics.export.jmx.enabled`        | `true`  | Enable JMX metrics export        | GMS, MAE Consumer, MCE Consumer, PE Consumer |
| `management.metrics.export.prometheus.enabled` | `true`  | Enable Prometheus metrics export | GMS, MAE Consumer, MCE Consumer, PE Consumer |

### Server Configuration

| Environment Variable   | Default | Description   | Components |
| ---------------------- | ------- | ------------- | ---------- |
| `server.server-header` | `false` | Server header | GMS        |

## Feature Flags

Reference Links:

- **Access Management**: [Access Management Feature](../features/feature-guides/access-roles.md)
- **Structured Properties**: [Structured Properties Overview](../features/feature-guides/properties/overview.md)
- **Lineage Features**: [Data Lineage](../features/feature-guides/lineage.md), [UI Lineage Management](../features/feature-guides/ui-lineage.md)
- **Compliance Forms**: [Compliance Forms Overview](../features/feature-guides/compliance-forms/overview.md)
- **Dataset Usage**: [Dataset Usage & Query History](../features/dataset-usage-and-query-history.md)
- **MCP Server**: [DataHub MCP Server](../features/feature-guides/mcp.md)

| Environment Variable                    | Default | Description                                                        | Components |
| --------------------------------------- | ------- | ------------------------------------------------------------------ | ---------- |
| `SHOW_SIMPLIFIED_HOMEPAGE_BY_DEFAULT`   | `false` | Show simplified homepage with just datasets, charts and dashboards | GMS        |
| `LINEAGE_SEARCH_CACHE_ENABLED`          | `true`  | Enable in-memory cache for searchAcrossLineage query               | GMS        |
| `GRAPH_SERVICE_DIFF_MODE_ENABLED`       | `true`  | Enable diff mode for graph writes                                  | GMS        |
| `POINT_IN_TIME_CREATION_ENABLED`        | `false` | Enable creation of point in time snapshots for scroll API          | GMS        |
| `ALWAYS_EMIT_CHANGE_LOG`                | `false` | Always emit MCL even when no changes detected                      | GMS        |
| `SEARCH_SERVICE_DIFF_MODE_ENABLED`      | `true`  | Enable diff mode for search document writes                        | GMS        |
| `READ_ONLY_MODE_ENABLED`                | `false` | Enable read only mode for instance                                 | GMS        |
| `SHOW_ACCESS_MANAGEMENT`                | `false` | Show AccessManagement tab in UI                                    | GMS        |
| `SHOW_SEARCH_FILTERS_V2`                | `true`  | Show search filters V2 experience                                  | GMS        |
| `SHOW_BROWSE_V2`                        | `true`  | Show browse v2 sidebar experience                                  | GMS        |
| `PLATFORM_BROWSE_V2`                    | `true`  | Enable platform browse experience                                  | GMS        |
| `LINEAGE_GRAPH_V2`                      | `true`  | Enable new lineage visualization                                   | GMS        |
| `PRE_PROCESS_HOOKS_UI_ENABLED`          | `true`  | Circumvent Kafka for UI changes                                    | GMS        |
| `PRE_PROCESS_HOOKS_UI_ENABLED`          | `false` | Reprocess UI sourced events asynchronously                         | GMS        |
| `SHOW_ACRYL_INFO`                       | `false` | Show CTAs around moving to DataHub Cloud                           | GMS        |
| `ER_MODEL_RELATIONSHIP_FEATURE_ENABLED` | `false` | Enable Join Tables Feature                                         | GMS        |
| `NESTED_DOMAINS_ENABLED`                | `true`  | Enable nested Domains feature                                      | GMS        |
| `SCHEMA_FIELD_ENTITY_FETCH_ENABLED`     | `true`  | Enable fetching schema field entities                              | GMS        |
| `BUSINESS_ATTRIBUTE_ENTITY_ENABLED`     | `false` | Enable business attribute entity                                   | GMS        |
| `DATA_CONTRACTS_ENABLED`                | `true`  | Enable Data Contracts feature                                      | GMS        |
| `ALTERNATE_MCP_VALIDATION`              | `false` | Enable alternate MCP validation flow                               | GMS        |
| `THEME_V2_ENABLED`                      | `true`  | Allow theme v2 to be turned on                                     | GMS        |
| `THEME_V2_DEFAULT`                      | `true`  | Set default theme for users                                        | GMS        |
| `THEME_V2_TOGGLEABLE`                   | `true`  | Allow theme v2 to be toggled (Acryl only)                          | GMS        |
| `SCHEMA_FIELD_CLL_ENABLED`              | `false` | Enable schema field-level lineage links                            | GMS        |
| `SCHEMA_FIELD_LINEAGE_IGNORE_STATUS`    | `true`  | Ignore schema field status in lineage                              | GMS        |
| `SHOW_SEPARATE_SIBLINGS`                | `false` | Separate siblings with no combined view                            | GMS        |
| `EDITABLE_DATASET_NAME_ENABLED`         | `false` | Enable editing dataset name in UI                                  | GMS        |
| `SHOW_MANAGE_STRUCTURED_PROPERTIES`     | `true`  | Show manage structured properties button                           | GMS        |
| `HIDE_DBT_SOURCE_IN_LINEAGE`            | `false` | Hide dbt sources in lineage                                        | GMS        |
| `SHOW_NAV_BAR_REDESIGN`                 | `true`  | Show newly designed nav bar                                        | GMS        |
| `SHOW_AUTO_COMPLETE_RESULTS`            | `true`  | Show auto complete results in search bar                           | GMS        |
| `ENTITY_VERSIONING_ENABLED`             | `false` | Enable entity versioning APIs                                      | GMS        |
| `SHOW_HAS_SIBLINGS_FILTER`              | `false` | Show "has siblings" filter in search                               | GMS        |
| `SHOW_SEARCH_BAR_AUTOCOMPLETE_REDESIGN` | `false` | Show redesigned search bar autocomplete                            | GMS        |
| `SHOW_MANAGE_TAGS`                      | `true`  | Allow users to manage tags in UI                                   | GMS        |
| `SHOW_INTRODUCE_PAGE`                   | `true`  | Show introduce page in V2 UI                                       | GMS        |
| `SHOW_INGESTION_PAGE_REDESIGN`          | `false` | Show re-designed Ingestion page                                    | GMS        |
| `SHOW_LINEAGE_EXPAND_MORE`              | `true`  | Show expand more button in lineage graph                           | GMS        |
| `SHOW_HOME_PAGE_REDESIGN`               | `false` | Show re-designed home page                                         | GMS        |
| `LINEAGE_GRAPH_V3`                      | `false` | Enable redesign of lineage v2 graph                                | GMS        |
| `SHOW_PRODUCT_UPDATES`                  | `true`  | Show in-product update popover                                     | GMS        |
| `LOGICAL_MODELS_ENABLED`                | `false` | Enable logical models feature                                      | GMS        |
| `SHOW_HOMEPAGE_USER_ROLE`               | `false` | Display homepage user role underneath name                         | GMS        |
| `VIEWS_ENABLED`                         | `true`  | Enable views feature                                               | GMS        |

## System Updates

Reference Links:

- **Updating DataHub**: [Updating DataHub Guide](../how/updating-datahub.md)

### Bootstrap Configuration

| Environment Variable             | Default                        | Description                                   | Components |
| -------------------------------- | ------------------------------ | --------------------------------------------- | ---------- |
| `BOOTSTRAP_POLICIES_FILE`        | `classpath:boot/policies.json` | Bootstrap policies file                       | GMS        |
| `BOOTSTRAP_SERVLETS_WAITTIMEOUT` | `60`                           | Total waiting time for servlets to initialize | GMS        |

### System Update Configuration

| Environment Variable                              | Default               | Description                          | Components    |
| ------------------------------------------------- | --------------------- | ------------------------------------ | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_INITIAL_BACK_OFF_MILLIS` | `5000`                | Initial back off for system updates  | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_MAX_BACK_OFFS`           | `50`                  | Maximum back offs for system updates | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_BACK_OFF_FACTOR`         | `2`                   | Multiplicative factor for back off   | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_WAIT_FOR_SYSTEM_UPDATE`  | `true`                | Wait for system update to complete   | System Update |
| `SYSTEM_UPDATE_BOOTSTRAP_MCP_CONFIG`              | `bootstrap_mcps.yaml` | Bootstrap MCP configuration          | System Update |

### Data Job Node CLL Configuration

| Environment Variable                                   | Default | Description                             | Components    |
| ------------------------------------------------------ | ------- | --------------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_ENABLED`    | `false` | Enable data job node CLL                | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_BATCH_SIZE` | `1000`  | Data job node CLL batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_DELAY_MS`   | `30000` | Data job node CLL delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DATA_JOB_NODE_CLL_LIMIT`      | `0`     | Data job node CLL limit                 | System Update |

### Domain Description Configuration

| Environment Variable                                    | Default | Description                              | Components    |
| ------------------------------------------------------- | ------- | ---------------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_DOMAIN_DESCRIPTION_ENABLED`    | `true`  | Enable domain description updates        | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DOMAIN_DESCRIPTION_BATCH_SIZE` | `1000`  | Domain description batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DOMAIN_DESCRIPTION_DELAY_MS`   | `30000` | Domain description delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DOMAIN_DESCRIPTION_CLL_LIMIT`  | `0`     | Domain description CLL limit             | System Update |

### Dashboard Info Configuration

| Environment Variable                                | Default | Description                          | Components    |
| --------------------------------------------------- | ------- | ------------------------------------ | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_DASHBOARD_INFO_ENABLED`    | `true`  | Enable dashboard info updates        | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DASHBOARD_INFO_BATCH_SIZE` | `1000`  | Dashboard info batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DASHBOARD_INFO_DELAY_MS`   | `30000` | Dashboard info delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_DASHBOARD_INFO_CLL_LIMIT`  | `0`     | Dashboard info CLL limit             | System Update |

### Browse Paths V2 Configuration

| Environment Variable                                 | Default | Description                       | Components    |
| ---------------------------------------------------- | ------- | --------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_BROWSE_PATHS_V2_ENABLED`    | `true`  | Enable browse paths V2 updates    | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_BROWSE_PATHS_V2_BATCH_SIZE` | `5000`  | Browse paths V2 batch size        | System Update |
| `REPROCESS_DEFAULT_BROWSE_PATHS_V2`                  | `false` | Reprocess default browse paths V2 | System Update |

### Ingestion Indices Configuration

| Environment Variable                                   | Default | Description                             | Components    |
| ------------------------------------------------------ | ------- | --------------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_INGESTION_INDICES_ENABLED`    | `true`  | Enable ingestion indices updates        | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_INGESTION_INDICES_BATCH_SIZE` | `5000`  | Ingestion indices batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_INGESTION_INDICES_DELAY_MS`   | `1000`  | Ingestion indices delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_INGESTION_INDICES_CLL_LIMIT`  | `0`     | Ingestion indices CLL limit             | System Update |

### Policy Fields Configuration

| Environment Variable                               | Default | Description                     | Components    |
| -------------------------------------------------- | ------- | ------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_POLICY_FIELDS_ENABLED`    | `true`  | Enable policy fields updates    | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_POLICY_FIELDS_BATCH_SIZE` | `5000`  | Policy fields batch size        | System Update |
| `REPROCESS_DEFAULT_POLICY_FIELDS`                  | `false` | Reprocess default policy fields | System Update |

### Ownership Types Configuration

| Environment Variable                                 | Default | Description                    | Components    |
| ---------------------------------------------------- | ------- | ------------------------------ | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_OWNERSHIP_TYPES_ENABLED`    | `true`  | Enable ownership types updates | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_OWNERSHIP_TYPES_BATCH_SIZE` | `1000`  | Ownership types batch size     | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_OWNERSHIP_TYPES_REPROCESS`  | `false` | Reprocess ownership types      | System Update |

### Schema Fields Configuration

| Environment Variable                                          | Default | Description                                   | Components    |
| ------------------------------------------------------------- | ------- | --------------------------------------------- | ------------- |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_ENABLED`    | `false` | Enable schema fields from schema metadata     | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_BATCH_SIZE` | `500`   | Schema fields from schema metadata batch size | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_DELAY_MS`   | `1000`  | Schema fields from schema metadata delay      | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA_LIMIT`      | `0`     | Schema fields from schema metadata limit      | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_ENABLED`                 | `false` | Enable schema fields doc IDs                  | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_BATCH_SIZE`              | `500`   | Schema fields doc IDs batch size              | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_DELAY_MS`                | `5000`  | Schema fields doc IDs delay                   | System Update |
| `SYSTEM_UPDATE_SCHEMA_FIELDS_DOC_IDS_LIMIT`                   | `0`     | Schema fields doc IDs limit                   | System Update |

### Process Instance Configuration

| Environment Variable                                        | Default | Description                                 | Components    |
| ----------------------------------------------------------- | ------- | ------------------------------------------- | ------------- |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_ENABLED`     | `true`  | Enable process instance has run events      | System Update |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_BATCH_SIZE`  | `100`   | Process instance has run events batch size  | System Update |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_DELAY_MS`    | `1000`  | Process instance has run events delay       | System Update |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_TOTAL_DAYS`  | `90`    | Process instance has run events total days  | System Update |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_WINDOW_DAYS` | `1`     | Process instance has run events window days | System Update |
| `SYSTEM_UPDATE_PROCESS_INSTANCE_HAS_RUN_EVENTS_REPROCESS`   | `false` | Reprocess process instance has run events   | System Update |

### Edge Status Configuration

| Environment Variable                             | Default | Description                       | Components    |
| ------------------------------------------------ | ------- | --------------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_EDGE_STATUS_ENABLED`    | `false` | Enable edge status updates        | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_EDGE_STATUS_BATCH_SIZE` | `1000`  | Edge status batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_EDGE_STATUS_DELAY_MS`   | `5000`  | Edge status delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_EDGE_STATUS_LIMIT`      | `0`     | Edge status limit                 | System Update |

### Property Definitions Configuration

| Environment Variable                                      | Default | Description                                | Components    |
| --------------------------------------------------------- | ------- | ------------------------------------------ | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_PROPERTY_DEFINITIONS_ENABLED`    | `true`  | Enable property definitions updates        | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_PROPERTY_DEFINITIONS_BATCH_SIZE` | `500`   | Property definitions batch size            | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_PROPERTY_DEFINITIONS_DELAY_MS`   | `1000`  | Property definitions delay in milliseconds | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_PROPERTY_DEFINITIONS_CLL_LIMIT`  | `0`     | Property definitions CLL limit             | System Update |

### Remove Query Edges Configuration

| Environment Variable                                 | Default | Description                | Components    |
| ---------------------------------------------------- | ------- | -------------------------- | ------------- |
| `BOOTSTRAP_SYSTEM_UPDATE_REMOVE_QUERY_EDGES_ENABLED` | `true`  | Enable remove query edges  | System Update |
| `BOOTSTRAP_SYSTEM_UPDATE_REMOVE_QUERY_EDGES_RETRIES` | `20`    | Remove query edges retries | System Update |

## Additional Environment Variables

The following environment variables are used in the codebase but may not be explicitly defined in the application.yaml file:

### Ingestion and Processing

| Environment Variable                | Default | Description                                                | Components |
| ----------------------------------- | ------- | ---------------------------------------------------------- | ---------- |
| `ASYNC_INGEST_DEFAULT`              | `false` | Asynchronously process ingestProposals by writing to Kafka | GMS        |
| `STRICT_URN_VALIDATION_ENABLED`     | `false` | Enable stricter URN validation logic                       | GMS        |
| `DATAHUB_DATASET_URN_TO_LOWER`      | `null`  | Convert dataset URN names to lowercase                     | GMS        |
| `BUSINESS_ATTRIBUTE_ENTITY_ENABLED` | `false` | Enable business attribute entity feature                   | GMS        |

### REST and Servlet Configuration

| Environment Variable     | Default | Description                        | Components        |
| ------------------------ | ------- | ---------------------------------- | ----------------- |
| `RESTLI_SERVLET_THREADS` | `null`  | Number of threads for REST servlet | GMS, MCE Consumer |
| `RESTLI_TIMEOUT_SECONDS` | `60`    | REST timeout in seconds            | GMS, MCE Consumer |

### System and Version Information

| Environment Variable   | Default | Description               | Components    |
| ---------------------- | ------- | ------------------------- | ------------- |
| `DATAHUB_GMS_PROTOCOL` | `http`  | GMS protocol (http/https) | GMS           |
| `DATAHUB_REVISION`     | `0`     | DataHub revision version  | System Update |

### Upgrade and Migration

| Environment Variable                               | Default | Description                                        | Components    |
| -------------------------------------------------- | ------- | -------------------------------------------------- | ------------- |
| `SKIP_REINDEX_EDGE_STATUS`                         | `false` | Skip reindexing edge status                        | System Update |
| `SKIP_REINDEX_DATA_JOB_INPUT_OUTPUT`               | `false` | Skip reindexing data job input/output              | System Update |
| `SKIP_GENERATE_SCHEMA_FIELDS_FROM_SCHEMA_METADATA` | `false` | Skip generating schema fields from schema metadata | System Update |
| `SKIP_MIGRATE_SCHEMA_FIELDS_DOC_ID`                | `false` | Skip migrating schema fields doc IDs               | System Update |
| `SKIP_CREATE_USAGE_EVENT_INDICES_STEP`             | `false` | Skip creating usage event indices/data streams     | System Update |
| `BACKFILL_BROWSE_PATHS_V2`                         | `false` | Enable backfilling browse paths V2                 | System Update |
| `READER_POOL_SIZE`                                 | `null`  | Reader pool size for restore operations            | System Update |
| `WRITER_POOL_SIZE`                                 | `null`  | Writer pool size for restore operations            | System Update |

### OpenTelemetry Configuration

| Environment Variable    | Default | Description                    | Components                                   |
| ----------------------- | ------- | ------------------------------ | -------------------------------------------- |
| `OTEL_METRICS_EXPORTER` | `none`  | OpenTelemetry metrics exporter | GMS, MAE Consumer, MCE Consumer, PE Consumer |
| `OTEL_TRACES_EXPORTER`  | `none`  | OpenTelemetry traces exporter  | GMS, MAE Consumer, MCE Consumer, PE Consumer |
| `OTEL_LOGS_EXPORTER`    | `none`  | OpenTelemetry logs exporter    | GMS, MAE Consumer, MCE Consumer, PE Consumer |
| `OTEL_PROPAGATORS`      | `null`  | OpenTelemetry propagators      | GMS, MAE Consumer, MCE Consumer, PE Consumer |

### Secret Service Configuration

| Environment Variable                  | Default          | Description                            | Components |
| ------------------------------------- | ---------------- | -------------------------------------- | ---------- |
| `SECRET_SERVICE_ENCRYPTION_KEY`       | `ENCRYPTION_KEY` | Secret service encryption key          | GMS        |
| `SECRET_SERVICE_V1_ALGORITHM_ENABLED` | `true`           | Enable v1 algorithm for secret service | GMS        |

### Health Check Configuration

| Environment Variable                  | Default | Description                 | Components |
| ------------------------------------- | ------- | --------------------------- | ---------- |
| `HEALTH_CHECK_CACHE_DURATION_SECONDS` | `5`     | Health check cache duration | GMS        |

### Metadata Tests Configuration

| Environment Variable     | Default | Description           | Components |
| ------------------------ | ------- | --------------------- | ---------- |
| `METADATA_TESTS_ENABLED` | `false` | Enable metadata tests | GMS        |

### Hooks Configuration

| Environment Variable                             | Default       | Description                                          | Components        |
| ------------------------------------------------ | ------------- | ---------------------------------------------------- | ----------------- |
| `ENABLE_SIBLING_HOOK`                            | `true`        | Enable automatic sibling associations                | GMS, MAE Consumer |
| `SIBLINGS_HOOK_CONSUMER_GROUP_SUFFIX`            | ``            | Siblings hook consumer group suffix                  | GMS, MAE Consumer |
| `ENABLE_UPDATE_INDICES_HOOK`                     | `true`        | Enable update indices hook                           | GMS, MAE Consumer |
| `UPDATE_INDICES_CONSUMER_GROUP_SUFFIX`           | ``            | Update indices consumer group suffix                 | GMS, MAE Consumer |
| `ENABLE_INGESTION_SCHEDULER_HOOK`                | `true`        | Enable ingestion scheduling                          | GMS, MAE Consumer |
| `INGESTION_SCHEDULER_HOOK_CONSUMER_GROUP_SUFFIX` | ``            | Ingestion scheduler hook consumer group suffix       | GMS, MAE Consumer |
| `ENABLE_INCIDENTS_HOOK`                          | `true`        | Enable incidents hook                                | GMS, MAE Consumer |
| `MAX_INCIDENT_HISTORY`                           | `100`         | Maximum incident history                             | GMS, MAE Consumer |
| `INCIDENTS_HOOK_CONSUMER_GROUP_SUFFIX`           | ``            | Incidents hook consumer group suffix                 | GMS, MAE Consumer |
| `ENABLE_STRUCTURED_PROPERTIES_HOOK`              | `true`        | Enable structured properties mappings                | GMS, MAE Consumer |
| `ENABLE_STRUCTURED_PROPERTIES_WRITE`             | `true`        | Enable writing structured property values            | GMS, MAE Consumer |
| `ENABLE_STRUCTURED_PROPERTIES_SYSTEM_UPDATE`     | `false`       | Enable structured property mappings in system update | GMS, MAE Consumer |
| `ENABLE_ENTITY_CHANGE_EVENTS_HOOK`               | `true`        | Enable entity change events hook                     | GMS, MAE Consumer |
| `ECE_CONSUMER_GROUP_SUFFIX`                      | ``            | Entity change events consumer group suffix           | GMS, MAE Consumer |
| `ECE_ENTITY_EXCLUSIONS`                          | `schemaField` | Entities to exclude from ECE hook                    | GMS, MAE Consumer |
| `FORMS_HOOK_ENABLED`                             | `true`        | Enable forms hook                                    | GMS, MAE Consumer |
| `FORMS_HOOK_CONSUMER_GROUP_SUFFIX`               | ``            | Forms hook consumer group suffix                     | GMS, MAE Consumer |

### Search and API Configuration

| Environment Variable        | Default                     | Description                    | Components |
| --------------------------- | --------------------------- | ------------------------------ | ---------- |
| `SEARCH_BAR_API_VARIANT`    | `AUTOCOMPLETE_FOR_MULTIPLE` | Search bar API variant         | Frontend   |
| `FIRST_IN_PERSONAL_SIDEBAR` | `YOUR_ASSETS`               | First item in personal sidebar | Frontend   |

### Client Configuration

| Environment Variable                                  | Default | Description                                         | Components                     |
| ----------------------------------------------------- | ------- | --------------------------------------------------- | ------------------------------ |
| `ENTITY_CLIENT_RETRY_INTERVAL`                        | `2`     | Entity client retry interval                        | GMS                            |
| `ENTITY_CLIENT_NUM_RETRIES`                           | `3`     | Entity client number of retries                     | GMS                            |
| `ENTITY_CLIENT_JAVA_GET_BATCH_SIZE`                   | `375`   | Entity client Java get batch size                   | GMS                            |
| `ENTITY_CLIENT_JAVA_INGEST_BATCH_SIZE`                | `375`   | Entity client Java ingest batch size                | GMS                            |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_SIZE`                 | `100`   | Entity client RESTli get batch size                 | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_CONCURRENCY`          | `2`     | Entity client RESTli get batch concurrency          | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_QUEUE_SIZE`           | `500`   | Entity client RESTli get batch queue size           | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_THREAD_KEEP_ALIVE`    | `60`    | Entity client RESTli get batch thread keep alive    | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_INGEST_BATCH_SIZE`              | `50`    | Entity client RESTli ingest batch size              | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_INGEST_BATCH_CONCURRENCY`       | `2`     | Entity client RESTli ingest batch concurrency       | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_INGEST_BATCH_QUEUE_SIZE`        | `500`   | Entity client RESTli ingest batch queue size        | GMS, MAE Consumer, PE Consumer |
| `ENTITY_CLIENT_RESTLI_INGEST_BATCH_THREAD_KEEP_ALIVE` | `60`    | Entity client RESTli ingest batch thread keep alive | GMS, MAE Consumer, PE Consumer |
| `USAGE_CLIENT_RETRY_INTERVAL`                         | `2`     | Usage client retry interval                         | GMS, MAE Consumer, PE Consumer |
| `USAGE_CLIENT_NUM_RETRIES`                            | `0`     | Usage client number of retries                      | GMS, MAE Consumer, PE Consumer |
| `USAGE_CLIENT_TIMEOUT_MS`                             | `3000`  | Usage client timeout in milliseconds                | GMS, MAE Consumer, PE Consumer |

### Cache Configuration

| Environment Variable                                | Default     | Description                                              | Components                     |
| --------------------------------------------------- | ----------- | -------------------------------------------------------- | ------------------------------ |
| `CACHE_TTL_SECONDS`                                 | `600`       | Default cache time to live                               | GMS                            |
| `CACHE_MAX_SIZE`                                    | `10000`     | Maximum number of items to cache                         | GMS                            |
| `CACHE_ENTITY_COUNTS_TTL_SECONDS`                   | `600`       | Homepage entity count time to live                       | GMS                            |
| `CACHE_SEARCH_LINEAGE_TTL_SECONDS`                  | `86400`     | Search lineage cache time to live                        | GMS                            |
| `CACHE_SEARCH_LINEAGE_LIGHTNING_THRESHOLD`          | `300`       | Lineage graphs exceeding this limit will use local cache | GMS                            |
| `CACHE_CLIENT_USAGE_CLIENT_ENABLED`                 | `true`      | Enable usage client cache                                | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_USAGE_CLIENT_STATS_ENABLED`           | `true`      | Enable usage client cache stats                          | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_USAGE_CLIENT_STATS_INTERVAL_SECONDS`  | `120`       | Usage client cache stats interval                        | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_USAGE_CLIENT_TTL_SECONDS`             | `86400`     | Usage client cache TTL                                   | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_USAGE_CLIENT_MAX_BYTES`               | `52428800`  | Usage client cache max bytes (50MB)                      | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_ENTITY_CLIENT_ENABLED`                | `true`      | Enable entity client cache                               | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_ENTITY_CLIENT_STATS_ENABLED`          | `true`      | Enable entity client cache stats                         | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_ENTITY_CLIENT_STATS_INTERVAL_SECONDS` | `120`       | Entity client cache stats interval                       | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_ENTITY_CLIENT_TTL_SECONDS`            | `0`         | Entity client cache TTL (0 = no cache)                   | GMS, MAE Consumer, PE Consumer |
| `CACHE_CLIENT_ENTITY_CLIENT_MAX_BYTES`              | `104857600` | Entity client cache max bytes (100MB)                    | GMS, MAE Consumer, PE Consumer |

### GraphQL Configuration

| Environment Variable                            | Default                                                    | Description                                      | Components |
| ----------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------------ | ---------- |
| `GRAPHQL_CONCURRENCY_SEPARATE_THREAD_POOL`      | `false`                                                    | Enable separate thread pool for GraphQL          | GMS        |
| `GRAPHQL_CONCURRENCY_STACK_SIZE`                | `256000`                                                   | GraphQL thread pool stack size                   | GMS        |
| `GRAPHQL_CONCURRENCY_CORE_POOL_SIZE`            | `-1`                                                       | GraphQL core pool size (default 5 \* cores)      | GMS        |
| `GRAPHQL_CONCURRENCY_MAX_POOL_SIZE`             | `-1`                                                       | GraphQL max pool size (default 100 \* cores)     | GMS        |
| `GRAPHQL_CONCURRENCY_KEEP_ALIVE`                | `60`                                                       | GraphQL thread keep alive time                   | GMS        |
| `GRAPHQL_QUERY_COMPLEXITY_LIMIT`                | `2000`                                                     | GraphQL query complexity limit                   | GMS        |
| `GRAPHQL_QUERY_DEPTH_LIMIT`                     | `50`                                                       | GraphQL query depth limit                        | GMS        |
| `GRAPHQL_QUERY_INTROSPECTION_ENABLED`           | `true`                                                     | Enable GraphQL introspection                     | GMS        |
| `GRAPHQL_METRICS_ENABLED`                       | `true`                                                     | Enable GraphQL metrics collection                | GMS        |
| `GRAPHQL_PERCENTILES`                           | `0.5,0.75,0.95,0.98,0.99,0.999`                            | GraphQL percentiles                              | GMS        |
| `GRAPHQL_METRICS_FIELD_LEVEL_ENABLED`           | `false`                                                    | Enable field-level GraphQL metrics               | GMS        |
| `GRAPHQL_METRICS_FIELD_LEVEL_OPERATIONS`        | `getSearchResultsForMultiple,searchAcrossLineageStructure` | GraphQL field-level operations                   | GMS        |
| `GRAPHQL_METRICS_FIELD_LEVEL_PATH_ENABLED`      | `false`                                                    | Include field path in GraphQL metrics            | GMS        |
| `GRAPHQL_METRICS_FIELD_LEVEL_PATHS`             | ``                                                         | GraphQL field-level paths                        | GMS        |
| `GRAPHQL_METRICS_TRIVIAL_DATA_FETCHERS_ENABLED` | `false`                                                    | Include trivial data fetchers in GraphQL metrics | GMS        |

### Chrome Extension Configuration

| Environment Variable               | Default | Description                     | Components |
| ---------------------------------- | ------- | ------------------------------- | ---------- |
| `CHROME_EXTENSION_ENABLED`         | `true`  | Enable Chrome extension         | Frontend   |
| `CHROME_EXTENSION_LINEAGE_ENABLED` | `true`  | Enable Chrome extension lineage | Frontend   |

### Business Attribute Configuration

| Environment Variable                                      | Default | Description                                                      | Components |
| --------------------------------------------------------- | ------- | ---------------------------------------------------------------- | ---------- |
| `BUSINESS_ATTRIBUTE_RELATED_ENTITIES_COUNT`               | `20000` | Business attribute related entities count                        | GMS        |
| `BUSINESS_ATTRIBUTE_RELATED_ENTITIES_BATCH_SIZE`          | `1000`  | Business attribute related entities batch size                   | GMS        |
| `BUSINESS_ATTRIBUTE_PROPAGATION_CONCURRENCY_THREAD_COUNT` | `-1`    | Business attribute propagation thread count (default 2 \* cores) | GMS        |
| `BUSINESS_ATTRIBUTE_PROPAGATION_CONCURRENCY_KEEP_ALIVE`   | `60`    | Business attribute propagation keep alive time                   | GMS        |

### Metadata Change Proposal Configuration

| Environment Variable                          | Default    | Description                                    | Components        |
| --------------------------------------------- | ---------- | ---------------------------------------------- | ----------------- |
| `MCP_CONSUMER_BATCH_ENABLED`                  | `false`    | Enable MCP consumer batch processing           | GMS, MCE Consumer |
| `MCP_CONSUMER_BATCH_SIZE`                     | `15744000` | MCP consumer batch size                        | GMS, MCE Consumer |
| `MCP_VALIDATION_IGNORE_UNKNOWN`               | `true`     | Ignore unknown fields in MCP validation        | GMS, MCE Consumer |
| `MCP_VALIDATION_PRIVILEGE_CONSTRAINTS`        | `true`     | Enable privilege constraints in MCP validation | GMS, MCE Consumer |
| `MCP_VALIDATION_EXTENSIONS_ENABLED`           | `false`    | Enable extensions in MCP validation            | GMS, MCE Consumer |
| `MCP_SIDE_EFFECTS_SCHEMA_FIELD_ENABLED`       | `false`    | Enable schema field side effects               | GMS, MCE Consumer |
| `MCP_SIDE_EFFECTS_DATA_PRODUCT_UNSET_ENABLED` | `true`     | Enable data product unset side effects         | GMS, MCE Consumer |
| `MCP_THROTTLE_UPDATE_INTERVAL_MS`             | `60000`    | MCP throttle update interval                   | GMS, MCE Consumer |
| `MCP_MCE_CONSUMER_THROTTLE_ENABLED`           | `false`    | Enable MCE consumer throttling                 | GMS, MCE Consumer |
| `MCP_API_REQUESTS_THROTTLE_ENABLED`           | `false`    | Enable API requests throttling                 | GMS, MCE Consumer |
| `MCP_VERSIONED_THROTTLE_ENABLED`              | `false`    | Enable versioned MCL topic throttling          | GMS, MCE Consumer |
| `MCP_VERSIONED_THRESHOLD`                     | `4000`     | Versioned throttle threshold                   | GMS, MCE Consumer |
| `MCP_VERSIONED_MAX_ATTEMPTS`                  | `1000`     | Versioned max attempts                         | GMS, MCE Consumer |
| `MCP_VERSIONED_INITIAL_INTERVAL_MS`           | `100`      | Versioned initial interval                     | GMS, MCE Consumer |
| `MCP_VERSIONED_MULTIPLIER`                    | `10`       | Versioned multiplier                           | GMS, MCE Consumer |
| `MCP_VERSIONED_MAX_INTERVAL_MS`               | `30000`    | Versioned max interval                         | GMS, MCE Consumer |
| `MCP_TIMESERIES_THROTTLE_ENABLED`             | `false`    | Enable timeseries MCL topic throttling         | GMS, MCE Consumer |
| `MCP_TIMESERIES_THRESHOLD`                    | `4000`     | Timeseries throttle threshold                  | GMS, MCE Consumer |
| `MCP_TIMESERIES_MAX_ATTEMPTS`                 | `1000`     | Timeseries max attempts                        | GMS, MCE Consumer |
| `MCP_TIMESERIES_INITIAL_INTERVAL_MS`          | `100`      | Timeseries initial interval                    | GMS, MCE Consumer |
| `MCP_TIMESERIES_MULTIPLIER`                   | `10`       | Timeseries multiplier                          | GMS, MCE Consumer |
| `MCP_TIMESERIES_MAX_INTERVAL_MS`              | `30000`    | Timeseries max interval                        | GMS, MCE Consumer |

### Events API Configuration

| Environment Variable | Default | Description       | Components |
| -------------------- | ------- | ----------------- | ---------- |
| `EVENTS_API_ENABLED` | `true`  | Enable events API | GMS        |

### Iceberg Catalog Configuration

| Environment Variable    | Default             | Description                               | Components |
| ----------------------- | ------------------- | ----------------------------------------- | ---------- |
| `ENABLE_PUBLIC_READ`    | `false`             | Enable public read for Iceberg catalog    | GMS        |
| `PUBLICLY_READABLE_TAG` | `PUBLICLY_READABLE` | Publicly readable tag for Iceberg catalog | GMS        |

## Change Data Capture (CDC) Configuration

Reference Links:

- **MCP/MCL Documentation**: [MCP & MCL Events](../advanced/mcp-mcl.md)
- **CDC Configuration Guide**: [Configure CDC Mode](../how/configure-cdc.md)

DataHub supports CDC mode for MetadataChangeLog generation, which guarantees ordered MCL events matching the order of database writes. CDC mode is optional and disabled by default.

### CDC Processing (Common)

| Environment Variable                | Default                          | Description                                                          | Components                       |
| ----------------------------------- | -------------------------------- | -------------------------------------------------------------------- | -------------------------------- |
| `CDC_MCL_PROCESSING_ENABLED`        | `false`                          | Enable CDC mode for MCL generation                                   | GMS, MCE Consumer, System Update |
| `CDC_CONFIGURE_SOURCE`              | `false`                          | Auto-configure Debezium connector (recommended false for production) | System Update                    |
| `CDC_DB_TYPE`                       | `mysql`                          | Database type for CDC (mysql or postgres)                            | System Update                    |
| `DATAHUB_CDC_CONNECTOR_NAME`        | `datahub-cdc-connector`          | Name of the Debezium connector                                       | System Update                    |
| `CDC_KAFKA_CONNECT_URL`             | `http://kafka-connect:8083`      | Kafka Connect REST API URL                                           | System Update                    |
| `CDC_KAFKA_CONNECT_REQUEST_TIMEOUT` | `10000`                          | Request timeout for Kafka Connect API calls in milliseconds          | System Update                    |
| `CDC_USER`                          | `datahub_cdc`                    | Database username for CDC connector                                  | System Update                    |
| `CDC_PASSWORD`                      | `datahub_cdc`                    | Database password for CDC connector                                  | System Update                    |
| `CDC_TOPIC_NAME`                    | `datahub.metadata_aspect_v2`     | Kafka topic name for CDC events                                      | GMS, MCE Consumer, System Update |
| `CDC_URN_KEY_SPEC`                  | `datahub.metadata_aspect_v2:urn` | Partitioning key specification (table:column format)                 | System Update                    |

### CDC MySQL Configuration

| Environment Variable       | Default                                      | Description                              | Components    |
| -------------------------- | -------------------------------------------- | ---------------------------------------- | ------------- |
| `DEBEZIUM_CONNECTOR_CLASS` | `io.debezium.connector.mysql.MySqlConnector` | Debezium connector class for MySQL       | System Update |
| `DEBEZIUM_PLUGIN_NAME`     | `decoderbufs`                                | Logical decoding plugin for MySQL        | System Update |
| `CDC_SERVER_ID`            | `184001`                                     | Unique server ID for MySQL CDC connector | System Update |

### CDC PostgreSQL Configuration

| Environment Variable       | Default                                              | Description                             | Components    |
| -------------------------- | ---------------------------------------------------- | --------------------------------------- | ------------- |
| `DEBEZIUM_CONNECTOR_CLASS` | `io.debezium.connector.postgresql.PostgresConnector` | Debezium connector class for PostgreSQL | System Update |
| `DEBEZIUM_PLUGIN_NAME`     | `pgoutput`                                           | PostgreSQL logical decoding plugin      | System Update |
| `CDC_INCLUDE_TABLE`        | `public.metadata_aspect_v2`                          | Tables to include in CDC capture        | System Update |
| `CDC_INCLUDE_SCHEMA`       | `public`                                             | Schemas to include in CDC capture       | System Update |

## Component Configuration

| Variable               | Default | Description                                                                               | Components        |
| ---------------------- | ------- | ----------------------------------------------------------------------------------------- | ----------------- |
| `MCP_CONSUMER_ENABLED` | `true`  | When running in standalone mode, disabled on `GMS` and enable on separate `MCE Consumer`. | GMS, MCE Consumer |
| `MCL_CONSUMER_ENABLED` | `true`  | When running in standalone mode, disabled on `GMS` and enable on separate `MAE Consumer`. | GMS, MAE Consumer |
| `PE_CONSUMER_ENABLED`  | `true`  | When running in standalone mode, disabled on `GMS` and enable on separate `MAE Consumer`. | GMS, PE Consumer  |

---

# DataHub Frontend

## Play Framework Configuration

### Secret Key Configuration

| Environment Variable | Default | Description                                       | Components |
| -------------------- | ------- | ------------------------------------------------- | ---------- |
| `DATAHUB_SECRET`     | `null`  | Secret key used to secure cryptographic functions | Frontend   |

### HTTP Parser Configuration

| Environment Variable           | Default | Description                                | Components |
| ------------------------------ | ------- | ------------------------------------------ | ---------- |
| `DATAHUB_PLAY_MEM_BUFFER_SIZE` | `10MB`  | Maximum memory buffer size for HTTP parser | Frontend   |

### Server Configuration

| Environment Variable                   | Default | Description                       | Components |
| -------------------------------------- | ------- | --------------------------------- | ---------- |
| `DATAHUB_AKKA_MAX_HEADER_COUNT`        | `64`    | Maximum number of headers allowed | Frontend   |
| `DATAHUB_AKKA_MAX_HEADER_VALUE_LENGTH` | `32k`   | Maximum header value length       | Frontend   |

### Session Configuration

| Environment Variable    | Default | Description                                     | Components |
| ----------------------- | ------- | ----------------------------------------------- | ---------- |
| `AUTH_COOKIE_SAME_SITE` | `LAX`   | SameSite attribute for authentication cookies   | Frontend   |
| `AUTH_COOKIE_SECURE`    | `false` | Whether authentication cookies should be secure | Frontend   |

## Authentication Configuration

### OIDC Configuration

Reference Links:

- **OIDC Setup Guide**: [Configure OIDC Authentication](../authentication/guides/sso/configure-oidc-react.md)
- **OIDC Prerequisites**: [Initialize OIDC](../authentication/guides/sso/initialize-oidc.md)

#### Required OIDC Configuration

| Environment Variable      | Default | Description                                          | Components |
| ------------------------- | ------- | ---------------------------------------------------- | ---------- |
| `AUTH_OIDC_ENABLED`       | `false` | Enable OIDC authentication                           | Frontend   |
| `AUTH_OIDC_CLIENT_ID`     | `null`  | Unique client ID issued by the identity provider     | Frontend   |
| `AUTH_OIDC_CLIENT_SECRET` | `null`  | Unique client secret issued by the identity provider | Frontend   |
| `AUTH_OIDC_DISCOVERY_URI` | `null`  | The IdP OIDC discovery URL                           | Frontend   |
| `AUTH_OIDC_BASE_URL`      | `null`  | The base URL associated with your DataHub deployment | Frontend   |

#### Optional OIDC Configuration

| Environment Variable                        | Default               | Description                                                              | Components |
| ------------------------------------------- | --------------------- | ------------------------------------------------------------------------ | ---------- |
| `AUTH_OIDC_USER_NAME_CLAIM`                 | `preferred_username`  | The attribute/claim used to derive the DataHub username                  | Frontend   |
| `AUTH_OIDC_USER_NAME_CLAIM_REGEX`           | `(.*)`                | The regex used to parse the DataHub username from the user name claim    | Frontend   |
| `AUTH_OIDC_SCOPE`                           | `oidc email profile`  | String representing the requested scope from the IdP                     | Frontend   |
| `AUTH_OIDC_CLIENT_AUTHENTICATION_METHOD`    | `client_secret_basic` | Authentication method to pass credentials to token endpoint              | Frontend   |
| `AUTH_OIDC_JIT_PROVISIONING_ENABLED`        | `true`                | Whether DataHub users should be provisioned on login if they don't exist | Frontend   |
| `AUTH_OIDC_PRE_PROVISIONING_REQUIRED`       | `false`               | Whether the user should already exist in DataHub on login                | Frontend   |
| `AUTH_OIDC_EXTRACT_GROUPS_ENABLED`          | `true`                | Whether groups should be extracted from a claim in the OIDC profile      | Frontend   |
| `AUTH_OIDC_GROUPS_CLAIM`                    | `groups`              | The OIDC claim to extract groups information from                        | Frontend   |
| `AUTH_OIDC_RESPONSE_TYPE`                   | `null`                | OIDC response type                                                       | Frontend   |
| `AUTH_OIDC_RESPONSE_MODE`                   | `null`                | OIDC response mode                                                       | Frontend   |
| `AUTH_OIDC_USE_NONCE`                       | `null`                | Whether to use nonce in OIDC flow                                        | Frontend   |
| `AUTH_OIDC_CUSTOM_PARAM_RESOURCE`           | `null`                | Custom resource parameter for OIDC                                       | Frontend   |
| `AUTH_OIDC_READ_TIMEOUT`                    | `null`                | OIDC read timeout                                                        | Frontend   |
| `AUTH_OIDC_CONNECT_TIMEOUT`                 | `null`                | OIDC connect timeout                                                     | Frontend   |
| `AUTH_OIDC_EXTRACT_JWT_ACCESS_TOKEN_CLAIMS` | `false`               | Whether to extract claims from JWT access token                          | Frontend   |
| `AUTH_OIDC_PREFERRED_JWS_ALGORITHM`         | `null`                | Which JWS algorithm to use                                               | Frontend   |
| `AUTH_OIDC_ACR_VALUES`                      | `null`                | OIDC ACR values                                                          | Frontend   |
| `AUTH_OIDC_GRANT_TYPE`                      | `null`                | OIDC grant type                                                          | Frontend   |

### Authentication Methods Configuration

| Environment Variable           | Default | Description                                              | Components |
| ------------------------------ | ------- | -------------------------------------------------------- | ---------- |
| `AUTH_JAAS_ENABLED`            | `true`  | Enable JAAS authentication                               | Frontend   |
| `AUTH_NATIVE_ENABLED`          | `true`  | Enable native authentication                             | Frontend   |
| `GUEST_AUTHENTICATION_ENABLED` | `false` | Enable guest authentication                              | Frontend   |
| `GUEST_AUTHENTICATION_USER`    | `guest` | The name of the guest user ID                            | Frontend   |
| `GUEST_AUTHENTICATION_PATH`    | `null`  | The path to bypass login page and get logged in as guest | Frontend   |
| `ENFORCE_VALID_EMAIL`          | `true`  | Enforce the usage of a valid email for user sign up      | Frontend   |

### Authentication Logging

| Environment Variable   | Default | Description                           | Components |
| ---------------------- | ------- | ------------------------------------- | ---------- |
| `AUTH_VERBOSE_LOGGING` | `false` | Enable verbose authentication logging | Frontend   |

### Session Configuration

| Environment Variable     | Default | Description                            | Components |
| ------------------------ | ------- | -------------------------------------- | ---------- |
| `AUTH_SESSION_TTL_HOURS` | `24`    | Login session expiration time in hours | Frontend   |
| `MAX_SESSION_TOKEN_AGE`  | `24h`   | Maximum age of session token           | Frontend   |

## Metadata Service Configuration

### Connection Configuration

| Environment Variable  | Default     | Description                                        | Components |
| --------------------- | ----------- | -------------------------------------------------- | ---------- |
| `DATAHUB_GMS_HOST`    | `localhost` | Metadata service host                              | Frontend   |
| `DATAHUB_GMS_PORT`    | `8080`      | Metadata service port                              | Frontend   |
| `DATAHUB_GMS_USE_SSL` | `false`     | Whether to use SSL for metadata service connection | Frontend   |

### Authentication Configuration

| Environment Variable            | Default                | Description                               | Components |
| ------------------------------- | ---------------------- | ----------------------------------------- | ---------- |
| `METADATA_SERVICE_AUTH_ENABLED` | `false`                | Enable metadata service authentication    | Frontend   |
| `DATAHUB_SYSTEM_CLIENT_SECRET`  | `JohnSnowKnowsNothing` | System client secret for metadata service | Frontend   |

## Entity Client Configuration

| Environment Variable                         | Default | Description                                | Components |
| -------------------------------------------- | ------- | ------------------------------------------ | ---------- |
| `ENTITY_CLIENT_RETRY_INTERVAL`               | `2`     | Entity client retry interval               | Frontend   |
| `ENTITY_CLIENT_NUM_RETRIES`                  | `3`     | Entity client number of retries            | Frontend   |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_SIZE`        | `50`    | Entity client RESTli get batch size        | Frontend   |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_CONCURRENCY` | `2`     | Entity client RESTli get batch concurrency | Frontend   |

---

## Notes

- Environment variables follow the pattern of converting YAML property paths to uppercase with underscores
- Default values are shown in the table above
- For Kafka configuration, refer to the official Spring Kafka documentation for additional properties
- Feature flags control experimental or optional functionality
- System update configurations control various background maintenance tasks
- Cache configurations help optimize performance for different use cases
- GraphQL configurations control query complexity and performance monitoring
- OpenTelemetry variables control observability and tracing behavior
- Play Framework properties are converted to environment variables by:
  - Converting dots (`.`) to underscores (`_`)
  - Converting to uppercase
