---
title: "Cloud Environment Variables"
---

# Cloud Environment Variables

This document lists environment variables specific to Acryl DataHub Cloud / SaaS, complementing the [open-source environment variables](./environment-vars.md). These variables provide additional configuration options available only in the SaaS version.

## Feature Flags and Controls

| Variable                                      | Default | Unit/Type | Components | Description                                                                                                           |
| --------------------------------------------- | ------- | --------- | ---------- | --------------------------------------------------------------------------------------------------------------------- |
| `SHOW_MANAGE_STRUCTURED_PROPERTIES`           | `true`  | boolean   | [`GMS`]    | Controls whether the Structured Properties page is visible and accessible via the UI.                                 |
| `SCHEMA_FIELD_CLL_ENABLED`                    | `true`  | boolean   | [`GMS`]    | Controls whether the Column-level lineage focus view is accessible via the lineage Graph.                             |
| `SCHEMA_FIELD_LINEAGE_IGNORE_STATUS`          | `true`  | boolean   | [`GMS`]    | Controls whether lineage ignores the schema field status aspect, reading the parent's status aspect instead.          |
| `HIDE_DBT_SOURCE_IN_LINEAGE`                  | `false` | boolean   | [`GMS`]    | Hides dbt source entities from lineage graphs when used with specific dbt ingestion settings.                         |
| `LINEAGE_GRAPH_V3`                            | `false` | boolean   | [`GMS`]    | Enables the lineage redesign of version 0.3.12.                                                                       |
| `LINEAGE_DEFAULT_LAST_DAYS_FILTER`            |         | integer   | [`GMS`]    | Default filter for lineage within the last X days. When set, lineage UI defaults to showing data within this window.  |
| `METADATA_SHARE_ENABLED`                      | `false` | boolean   | [`GMS`]    | Enables sharing an entity with another Acryl instance via the share menu on entity profiles.                          |
| `SHOW_NAV_BAR_REDESIGN`                       | `true`  | boolean   | [`GMS`]    | Enables the new navigation bar redesign.                                                                              |
| `USER_TRACKING_ENABLED`                       | `false` | boolean   | [`GMS`]    | Enables Hotjar for user tracking, session recording (with data redacted), and in-app feedback.                        |
| `SEPARATE_SIBLINGS_LINEAGE_BY_DEFAULT`        | `false` | boolean   | [`GMS`]    | Controls whether to separate siblings when fetching lineage data in search results by default.                        |
| `SHOW_TASK_CENTER_REDESIGN`                   | `false` | boolean   | [`GMS`]    | Flag to enable the new proposals redesign in the app, mostly in the task center.                                      |
| `SHOW_DEFAULT_EXTERNAL_LINKS`                 | `true`  | boolean   | [`GMS`]    | Controls whether or not we show the external links for an entity like "View in Snowflake"                             |
| `DATASET_HEALTH_DASHBOARD_V2_ENABLED`         | `false` | boolean   | [`GMS`]    | Controls whether the dataset health dashboard redesign is enabled.                                                    |
| `SHOW_CREATED_AT_FILTER`                      | `false` | boolean   | [`GMS`]    | Controls whether we show a "Created At" filter in the main search exprience.                                          |
| `SERVER_ENV`                                  | `cloud` | boolean   | [`GMS`]    | Controls whether the `/config` endpoint reports `core` or `cloud` for the CLI. `cloud` is the default from helm-fork. |
| `APPLICATION_SHOW_SIDEBAR_SECTION_WHEN_EMPTY` | `false` | boolean   | [`GMS`]    | Controls whether the Application section appears on entity sidebars by default, even when no application is set.      |
| `SHOW_PRODUCT_UPDATES`                        | `false` | boolean   | [`GMS`]    | Controls whether to show or hide the in-product sidebar update banner on new releases.                                |
| `FORMS_NOTIFICATIONS_ENABLED`                 | `false` | boolean   | [`GMS`]    | Controls whether to trigger notifications for forms and show the option on the form editor.                           |
| `SHOW_INGESTION_PAGE_REDESIGN`                | `false` | boolean   | [`GMS`]    | Controls whether to show the new ingestion page and run history tab in the UI.                                        |
| `VIEW_INGESTION_SOURCE_PRIVILEGES_ENABLED`    | `false` | boolean   | [`GMS`]    | If enabled, any user with permissions to view an ingestion source will be able to get to the ingestion page.          |

## Compliance Forms

| Variable                 | Default | Unit/Type | Components | Description                                                                                             |
| ------------------------ | ------- | --------- | ---------- | ------------------------------------------------------------------------------------------------------- |
| `FORM_CREATION_ENABLED`  | `true`  | boolean   | [`GMS`]    | Controls whether users can see the Create Forms flow in the UI, important for Compliance Forms feature. |
| `FORM_ANALYTICS_ENABLED` | `false` | boolean   | [`GMS`]    | Controls whether users can see the "Analytics" sub-tab within the Compliance Forms page.                |

## Slack Integration

| Variable                                           | Default | Unit/Type | Components               | Description                                                                                                                                                                      |
| -------------------------------------------------- | ------- | --------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SLACK_INTEGRATIONS_SERVICE_NOTIFICATIONS_ENABLED` | `true`  | boolean   | [`GMS`]                  | Controls whether some Slack notification types are routed to the integrations service for decoration and additional functionality.                                               |
| `REQUEST_MINIMAL_SLACK_PERMISSIONS`                | `false` | boolean   | [`GMS`]                  | If turned on, request minimal required slack permissions for Slack integration.                                                                                                  |
| `STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED`         | `false` | boolean   | [`Integrations Service`] | Enables updating messages based on state recorded in datahub-gms backend.                                                                                                        |
| `SLACK_AT_MENTION_DEFAULT_ENABLED`                 | `false` | boolean   | [`GMS`]                  | Default value for @DataHub bot mentions when not explicitly configured via UI. Replaces `DATAHUB_SLACK_AT_MENTION_ENABLED` in v0.3.13.                                           |
| `DATAHUB_SLACK_AT_MENTION_ENABLED`                 | `false` | boolean   | [`Integrations Service`] | **Removed in v0.3.13**: Enables the Slackbot, which replies to the @datahub mentions. Replaced by admin-configurable UI toggle with `SLACK_AT_MENTION_DEFAULT_ENABLED` fallback. |

## Classification

AI terms classification is covered below.

| Variable                                       | Default | Unit/Type | Components | Description                                                                   |
| ---------------------------------------------- | ------- | --------- | ---------- | ----------------------------------------------------------------------------- |
| `CLASSIFICATION_CONFIG_ENABLED`                | `false` | boolean   | [`GMS`]    | Controls whether the classification config feature is enabled.                |
| `CLASSIFICATION_AUTOMATIONS_SNOWFLAKE_ENABLED` | `false` | boolean   | [`GMS`]    | Controls whether the classification automations snowflake feature is enabled. |

## AI Features

The AI-powered Slack bot, which replies to the @datahub mentions, is covered in the Slack Integration section.

| Variable                         | Default            | Unit/Type | Components | Description                                                                                                                        |
| -------------------------------- | ------------------ | --------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `AI_FEATURES_ENABLED`            | `true`             | boolean   | [`GMS`]    | Controls whether the AI settings page is visible to admins.                                                                        |
| `AI_TERM_CLASSIFICATION_ENABLED` | `false`            | boolean   | [`GMS`]    | Controls whether the term classification feature is enabled.                                                                       |
| `DOCUMENTATION_AI_ENABLED`       | `false`            | boolean   | [`GMS`]    | Controls the fallback value for whether or not documentation AI is enabled if `globalSettings.documentationAi.enabled` is not set. |
| `DESCRIPTION_GENERATION_MODEL`   | `CLAUDE_3_HAIKU`   | string    | [`GMS`]    | Which Bedrock model to use for description generation.                                                                             |
| `QUERY_DESCRIPTION_MODEL`        | `CLAUDE_35_SONNET` | string    | [`GMS`]    | Which Bedrock model to use for query description generation.                                                                       |
| `TERM_SUGGESTION_MODEL`          | `CLAUDE_3_HAIKU`   | string    | [`GMS`]    | Which Bedrock model to use for term suggestion generation.                                                                         |

## Remote Executor System

| Variable                                                           | Default             | Unit/Type | Components               | Description                                                                                                          |
| ------------------------------------------------------------------ | ------------------- | --------- | ------------------------ | -------------------------------------------------------------------------------------------------------------------- |
| `DISPLAY_EXECUTOR_POOLS`                                           | `false`             | boolean   | [`GMS`]                  | Enables displaying executor pools across ingestion tab and ingestion/automation creation/editing wizards.            |
| `EXECUTOR_POOL_HOOK_ENABLED`                                       | `false`             | boolean   | [`GMS`, `MAE Consumer`]  | Auto provisions SQS queue on pool create, and auto-deletes queue on pool delete.                                     |
| `DATAHUB_EXECUTOR_HOST`                                            | `localhost`         | string    | [`GMS`]                  | Hostname of the DataHub executor service.                                                                            |
| `DATAHUB_EXECUTOR_PORT`                                            | `9004`              | integer   | [`GMS`]                  | Port of the DataHub executor service.                                                                                |
| `DATAHUB_EXECUTOR_USE_SSL`                                         | `false`             | boolean   | [`GMS`]                  | Whether to use SSL when connecting to the DataHub executor.                                                          |
| `DATAHUB_EXECUTOR_RESOLVE_ASSERTION_INGESTION_SOURCE_WITH_ASPECTS` | `datasetProperties` | string    | [`GMS`]                  | CSV listing aspects that the system should reference to determine an ingestion source that can execute an assertion. |
| `RUN_ASSERTIONS_ENABLED`                                           | `false`             | boolean   | [`GMS`]                  | Enables running assertions in Acryl Observe, allowing refresh of native Acryl assertions from the UI.                |
| `FRESHNESS_ASSERTION_EVALUATION_BUFFER_SECONDS`                    | `300`               | seconds   | [`Executor Coordinator`] | The tail buffer to add to freshness assertion boundaries.                                                            |
| `DATAHUB_EXECUTOR_DISCOVERY_INTERVAL`                              | `30`                | seconds   | [`Executor Coordinator`] | Remote executor discovery ping interval.                                                                             |
| `DATAHUB_EXECUTOR_DISCOVERY_EXPIRE_THRESHOLD`                      | `3600`              | seconds   | [`Executor Coordinator`] | Time after which stale discovery status records are flagged as expired.                                              |
| `DATAHUB_EXECUTOR_DISCOVERY_PURGE_THRESHOLD`                       | `86400`             | seconds   | [`Executor Coordinator`] | Time after which stale discovery status records are deleted from the database.                                       |
| `DATAHUB_EXECUTOR_DISCOVERY_PURGE_AFTER`                           | `86400`             | seconds   | [`Executor Coordinator`] | How often sweeper discovery purge task is executed.                                                                  |
| `DATAHUB_EXECUTOR_SWEEPER_RESTART_MAX_ATTEMPTS`                    | `3`                 | integer   | [`Executor Coordinator`] | Maximum number of automated restarts of ABORTED tasks.                                                               |
| `EXECUTOR_TASK_MEMORY_LIMIT`                                       |                     | integer   | [`Executor Coordinator`] | Memory limit for executor tasks.                                                                                     |
| `EXECUTOR_TASK_WEIGHT`                                             |                     | integer   | [`Executor Coordinator`] | Weight assigned to executor tasks for resource allocation.                                                           |

## Sharing Configuration

| Variable                                 | Default | Unit/Type | Components               | Description                                                                           |
| ---------------------------------------- | ------- | --------- | ------------------------ | ------------------------------------------------------------------------------------- |
| `FEAT_SHARE_EXTRA_ASPECTS`               |         | string    | [`Integrations Service`] | Comma-separated list of additional custom aspects to include when sharing.            |
| `FEAT_SHARE_SKIP_CACHE_ON_LINEAGE_QUERY` | `false` | boolean   | [`Integrations Service`] | When true, sharing always uses the latest lineage results at the cost of performance. |
| `FEAT_SHARE_MAX_ENTITIES_PER_SHARE`      | `1000`  | integer   | [`Integrations Service`] | Maximum number of upstream and downstream entities to include in a share.             |

## Acryl Observe and Incidents

| Variable                                 | Default | Unit/Type | Components          | Description                                                                       |
| ---------------------------------------- | ------- | --------- | ------------------- | --------------------------------------------------------------------------------- |
| `ENABLE_INCIDENT_ACTIVITY_EVENTS`        | `true`  | boolean   | [`GMS`]             | Enables generating incident activity events via the incident activity event hook. |
| `BROADCAST_NEW_INCIDENT_UPDATES_ENABLED` | `true`  | boolean   | [`GMS`]             | Enables broadcasting stateful incident update notifications to other systems.     |
| `SCHEMA_ASSERTION_MONITORS_ENABLED`      | `false` | boolean   | [`GMS`]             | Enables schema assertion authoring in the UI.                                     |
| `ONLINE_SMART_ASSERTIONS_ENABLED`        | `false` | boolean   | [`GMS`, `Executor`] | Allow on-demand creation of smart assertions in the UI, and let them run online.  |

## Metadata Change Proposal (MCP) Throttling

| Variable                             | Default | Unit/Type    | Components       | Description                                                                                      |
| ------------------------------------ | ------- | ------------ | ---------------- | ------------------------------------------------------------------------------------------------ |
| `MCP_THROTTLE_UPDATE_INTERVAL_MS`    | `60000` | milliseconds | [`MCE Consumer`] | How often the MAE consumer's kafka lag is sampled for throttling MCE consumer's processing rate. |
| `MCP_VERSIONED_THROTTLE_ENABLED`     | `false` | boolean      | [`MCE Consumer`] | Enable throttling of MCP processing involving versioned aspects.                                 |
| `MCP_TIMESERIES_THROTTLE_ENABLED`    | `false` | boolean      | [`MCE Consumer`] | Enable throttling of MCP processing involving timeseries aspects.                                |
| `MCP_VERSIONED_THRESHOLD`            | `4000`  | integer      | [`MCE Consumer`] | The lag threshold at which throttling should be activated for versioned aspects.                 |
| `MCP_TIMESERIES_THRESHOLD`           | `4000`  | integer      | [`MCE Consumer`] | The lag threshold at which throttling should be activated for timeseries aspects.                |
| `MCP_VERSIONED_MAX_ATTEMPTS`         | `1000`  | integer      | [`MCE Consumer`] | The number of throttling events before the versioned aspect throttle is reset.                   |
| `MCP_TIMESERIES_MAX_ATTEMPTS`        | `1000`  | integer      | [`MCE Consumer`] | The number of throttling events before the timeseries aspect throttle is reset.                  |
| `MCP_VERSIONED_INITIAL_INTERVAL_MS`  | `100`   | milliseconds | [`MCE Consumer`] | The exponential backoff initial interval for versioned aspects.                                  |
| `MCP_TIMESERIES_INITIAL_INTERVAL_MS` | `100`   | milliseconds | [`MCE Consumer`] | The exponential backoff initial interval for timeseries aspects.                                 |
| `MCP_VERSIONED_MULTIPLIER`           | `10`    | integer      | [`MCE Consumer`] | The exponential backoff multiplier for versioned aspects.                                        |
| `MCP_TIMESERIES_MULTIPLIER`          | `10`    | integer      | [`MCE Consumer`] | The exponential backoff multiplier for timeseries aspects.                                       |
| `MCP_VERSIONED_MAX_INTERVAL_MS`      | `30000` | milliseconds | [`MCE Consumer`] | The exponential backoff max interval for versioned aspects.                                      |
| `MCP_TIMESERIES_MAX_INTERVAL_MS`     | `30000` | milliseconds | [`MCE Consumer`] | The exponential backoff max interval for timeseries aspects.                                     |

## Metadata Tests

| Variable                                        | Default | Unit/Type | Components | Description                                                                                                           |
| ----------------------------------------------- | ------- | --------- | ---------- | --------------------------------------------------------------------------------------------------------------------- |
| `METADATA_TESTS_ELASTICSEARCH_EXECUTOR_ENABLED` | `true`  | boolean   | [`GMS`]    | Enables the Elasticsearch executor for metadata tests, allowing test predicates to be executed against Elasticsearch. |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_CONCURRENCY`    | `2`     | integer   | [`GMS`]    | Number of concurrent rest.li calls when the number of urns in a getBatchV2 call exceeds the batch size of 50.         |
| `ENTITY_CLIENT_JAVA_GET_BATCH_SIZE`             | `375`   | integer   | [`GMS`]    | Batch size for Java entity client calls.                                                                              |
| `ENTITY_CLIENT_RESTLI_GET_BATCH_SIZE`           | `100`   | integer   | [`GMS`]    | Batch size for Rest.li entity client calls. Decrease if extremely long URNs cause 414 error codes.                    |

## Automations

| Variable                      | Default | Unit/Type | Components | Description                                                                                                                |
| ----------------------------- | ------- | --------- | ---------- | -------------------------------------------------------------------------------------------------------------------------- |
| `TAG_PROPAGATION_V2_ENABLED`  | `false` | boolean   | [`GMS`]    | Controls whether new tag propagation automations use the generic propagation action, which supports propagating upstream.  |
| `TERM_PROPAGATION_V2_ENABLED` | `false` | boolean   | [`GMS`]    | Controls whether new term propagation automations use the generic propagation action, which supports propagating upstream. |

## Search and Ranking

| Variable                                            | Default | Unit/Type    | Components        | Description                                                     |
| --------------------------------------------------- | ------- | ------------ | ----------------- | --------------------------------------------------------------- |
| `SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_ENABLED`    | `true`  | boolean      | [`System Update`] | Enables search ranking using usage percentiles.                 |
| `SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_BATCH_SIZE` | `500`   | integer      | [`System Update`] | Batch size for usage percentile calculation.                    |
| `SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_DELAY_MS`   | `1000`  | milliseconds | [`System Update`] | Delay between batches for usage percentile calculation.         |
| `SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_LIMIT`      | `0`     | integer      | [`System Update`] | Limit for the number of entities to process (0 means no limit). |

## Kafka Consumer Settings

| Variable                                          | Default | Unit/Type   | Components       | Description                                                                                                                                                           |
| ------------------------------------------------- | ------- | ----------- | ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `KAFKA_CONSUMER_MCL_FINE_GRAINED_LOGGING_ENABLED` | `false` | boolean     | [`MAE Consumer`] | Enables more fine-grained logging and metrics for MCL consumer debugging, including aspect and change type level timings.                                             |
| `KAFKA_CONSUMER_MCL_ASPECTS_TO_DROP`              |         | JSON string | [`MAE Consumer`] | A "break glass" option for skipping processing of specified entities and aspects. Map style configuration taking a JSON string. Supports wildcard for common aspects. |

## Entities and Versions

| Variable                                   | Default | Unit/Type | Components | Description                                                                                                        |
| ------------------------------------------ | ------- | --------- | ---------- | ------------------------------------------------------------------------------------------------------------------ |
| `BOOTSTRAP_SYSTEM_UPDATE_POST_INFO_ENABLE` | `true`  | boolean   | [`GMS`]    | Whether to run a bootstrap step for post entities to allow filtering posts by type.                                |
| `ALTERNATE_MCP_VALIDATION`                 | `false` | boolean   | [`GMS`]    | Enables an alternate MCP validation pathway for MCPs that should be validated only after applying a mutation hook. |
| `GRAPHQL_QUERY_INTROSPECTION_ENABLED`      | `true`  | boolean   | [`GMS`]    | Whether to enable introspection queries in the GraphQL API.                                                        |

## OIDC Implicit

| Variable                           | Default | Unit/Type | Components   | Description                                                               |
| ---------------------------------- | ------- | --------- | ------------ | ------------------------------------------------------------------------- |
| `AUTH_OIDC_IMPLICIT_ENABLED`       | `false` | boolean   | [`Frontend`] | Enables OIDC Implicit Flow                                                |
| `AUTH_OIDC_IMPLICIT_CLIENT_ISSUER` |         | string    | [`Frontend`] | The issuer for the client's IDP. i.e. Google: https://accounts.google.com |
| `AUTH_OIDC_IMPLICIT_JWKS_JSON`     |         | string    | [`Frontend`] | Public keys for the IDP as a json string                                  |
