# Updating Acryl DataHub

<!--

## <version number>

### New env variables

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment Variables
-->

This is over and above updating-datahub.md file

## Next

### Breaking Changes

- [graphql] The `appConfig -> documentationAiEnabled` field has been removed, superseded by `globalSettings -> documentationAi -> enabled`.

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment variables

- [datahub-gms] `AI_FEATURES_ENABLED`: Flag to enable the AI settings page in the UI.
- [datahub-gms] `DOCUMENTATION_AI_ENABLED`: This flag has been repurposed - it now controls the default value for whether or not documentation AI is enabled, but will be superseded by `globalSettings.documentationAi.enabled` if that is set.
- [datahub-gms] `SHOW_PRODUCT_UPDATES`: Flag to show or hide in product sidebar update banner on new releases.

## v0.3.11

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment variables

- [datahub-gms] `DATASET_HEALTH_DASHBOARD_V2_ENABLED`: Flag to show the v2 version of the data health dashboard. `DATASET_HEALTH_DASHBOARD_ENABLED` is for v1 of the data health dashboard.
- [datahub-gms] `SHOW_SEARCH_BAR_AUTOCOMPLETE_REDESIGN`: Flag to show the new search bar experience with updated designs, filters, and the ability to use our search API to show consistent results.
- [datahub-gms] `SEARCH_BAR_API_VARIANT`: Flag to choose which backend API to use when fetching results to render in the search bar dropdown. `SEARCH_ACROSS_ENTITIES` will use our main search API to have consistent results with main search. `AUTOCOMPLETE_FOR_MULTIPLE` will use the same autoComplete API as before.
- [datahub-gms] `SHOW_TASK_CENTER_REDESIGN`: Flag to display the new proposals experience with the redesign in the proposals list page as well as showing some new functionality like the proposals task list on entity profile pages.
- [datahub-gms] `SHOW_CREATED_AT_FILTER`: Flag to display the createdAt filter in the search UI which filters the entities by their source creation time. Not every entity has this source creation time, so the search results might be misleading.

## v0.3.10.1

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment variables

- [frontend] `AUTH_OIDC_IMPLICIT_ENABLED`, `AUTH_OIDC_IMPLICIT_CLIENT_ISSUER`, `AUTH_OIDC_IMPLICIT_JWKS_JSON`: Controls OIDC implicit authentication flow. The client issue and jwks keys must be proved by the customer. This is the URL for the IDP, i.e. `https://accounts.google.com` and their JWKS keys (public keys for the IDP).

## v0.3.10

### Breaking Changes

- There was a regression introduced in the integrations service GraphQL client code that made generating queries either sometimes or always fail. This breaks our slack app. This was prompty fixed and we moved to 0.3.10.1 as a result.

### Potential Downtime

### Deprecations

- Deprecated `assertionInferenceDetails` aspect. However, the record is still used within the monitor entity.

### Other Notable Changes

- New nav bar is switched on by default in the V2 UI ([datahub-gms] `SHOW_NAV_BAR_REDESIGN`)

### Environment variables

- [datahub-gms] `ONLINE_SMART_ASSERTIONS_ENABLED`: Flag to allow on-demand creation of smart assertions in the UI, and let them run online. (TODO @John extend to indicate that this variable is also in use in executor)
- [datahub-gms] `DISPLAY_EXECUTOR_POOLS`: Flag to enable displaying executor pools across the ingestion tab and the ingestion/automation creation/editing wizards.
- [datahub-upgrade] `BOOTSTRAP_SYSTEM_UPDATE_EXECUTOR_POOLS_ENABLED`: Flag to enable bootstrapping remote executor pools when instance is setup.
- [datahub-mae-consumer | datahub-gms] `EXECUTOR_POOL_HOOK_ENABLED`: Auto provisions SQS queue on pool create, and auto-deletes queue on pool delete.
- [datahub-gms] `SHOW_DEFAULT_EXTERNAL_LINKS`: Flag to disable showing the external links we produce by default like "View in snowflake." This was requested by Rokos when they start populating their custom external links they may want to choose to hide the default ones and we can do that with this flag. This is default true.
- [datahub-gms] `SHOW_TASK_CENTER_REDESIGN`: Flag to enable the new proposals redesign in the app, mostly in the task center. This should be off for most folks for 0.3.10 but we may turn on depending what gets into the release.
- [integrations-service] `DATAHUB_SLACK_AT_MENTION_ENABLED` (default: false) - Whether to enable the Slackbot, which replies to the @datahub mentions.
- [integrations-service] `DATAHUB_INTEGRATIONS_SEND_MIXPANEL_EVENTS` (default: false): Controls whether telemetry events are sent directly to Mixpanel. When set to "true", "1", or "yes" (case-insensitive), events will be sent to both Mixpanel and the DataHub API. When not set or set to any other value (default), events will only be sent to the DataHub API. By default, the integrations service depends on GMS to write to Mixpanel.

## v0.3.9

### Breaking Changes

### Potential Downtime

### Deprecations

- #5287: The unused observability Anomaly entity has been removed from the system, replaced by MonitorAnomalyEvent. This should not affect existing deployments, as the entity was not consumed anywhere.

### Other Notable Changes

We initially told Figma to produce `DataProcessInstanceInput.inputs` and `DataProcessInstanceOutput.outputs`
for their AI/ML use cases. However, if they wish to see these data process instances and their edges
in lineage, they should instead produce `DataProcessInstanceInput.inputEdges` and `DataProcessInstanceOutput.outputEdges`.

We have a non-blocking upgrade job to migrate the data from the old fields to the new fields,
that can be specified in datahub-apps, e.g.:

```yaml
datahubSystemUpdate:
  upgrades:
    migrateProcessInstanceEdges:
      enabled: true
      reprocess: false
      inputPlatforms: "workbench,mlflow"
      outputPlatforms: "workbench,mlflow"
      parentPlatforms: "workbench,mlflow"
```

### Environment Variables

- [datahub-gms] `DATAHUB_EXECUTOR_RESOLVE_ASSERTION_INGESTION_SOURCE_WITH_ASPECTS`: CSV listing aspects that the system should reference to determine an ingestion source that can execute an assertion. Default: datasetProperties
- [datahub-gms] `SHOW_STATS_TAB_REDESIGN`: Displays the updated stats tab experience for Datasets and Columns. This includes our new designs for charts and a few new charts themselves. Default: true
- [datahub-upgrade] `BOOTSTRAP_SYSTEM_UPDATE_PURGE_LEGACY_EXECUTORS_ENABLED`: Enables purging of legacy executor entities. On by default to clear malformed data.

## v0.3.8

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment Variables

- [datahub-gms] `SHOW_NAV_BAR_REDESIGN`: Whether to enable the new Nav Bar redesign. Some customers have explicitly requested this already, but should aim to evangelize and drive up adoption as we will continue to improve this experience as the primary going forward.
- [datahub-executor-coordinator] `FRESHNESS_ASSERTION_EVALUATION_BUFFER_SECONDS`: The tail buffer to add to freshness assertion boundaries. This defaults to 5 minutes, but can be extended if the customer has a lot of queued queries coming into their data warehouse.
- [datahub-executor-coordinator] `DATAHUB_EXECUTOR_DISCOVERY_INTERVAL`: Remote executor discovery ping interval in seconds. Default: 30
- [datahub-executor-coordinator] `DATAHUB_EXECUTOR_DISCOVERY_EXPIRE_THRESHOLD`: Time in seconds after which stale discovery status records are flagged as expired. Default: 3600 (1 hour)
- [datahub-executor-coordinator] `DATAHUB_EXECUTOR_DISCOVERY_PURGE_THRESHOLD` Time in seconds after which stale discovery status records are deleted from the database. Default: 24 \* 3600 (1 month)
- [datahub-executor-coordinator] `DATAHUB_EXECUTOR_DISCOVERY_PURGE_AFTER`: How often sweeper discovery purge task is executed in seconds. Default: 24 \* 3600 (1 day)
- [datahub-executor-coordinator] `DATAHUB_EXECUTOR_SWEEPER_RESTART_MAX_ATTEMPTS`: Maximum number of automated restarts of ABORTED tasks. Default: 3
- [datahub-executor-coordinator] `EXECUTOR_TASK_MEMORY_LIMIT`: See [Remote Executor Troubleshooting](https://www.notion.so/acryldata/Remote-Executor-Troubleshooting-120fc6a6427780e48a6bc900f6e2a358?pvs=4#168fc6a642778060a2a1d135ee61e559)
- [datahub-executor-coordinator] `EXECUTOR_TASK_WEIGHT`: See [Remote Executor Troubleshooting](https://www.notion.so/acryldata/Remote-Executor-Troubleshooting-120fc6a6427780e48a6bc900f6e2a358?pvs=4#154fc6a64277800a855df67d6e11705f)
- [datahub-mae-consumer] `KAFKA_CONSUMER_MCL_FINE_GRAINED_LOGGING_ENABLED`: Configuration for the MCL consumer enabling more fine-grained logging and metrics for debugging purposes. Gives aspect and change type level timings for consumer processing.
- [datahub-mae-consumer] `KAFKA_CONSUMER_MCL_ASPECTS_TO_DROP`: A "break glass" option for skipping processing of specified entities and aspects. Map style configuration taking a Json string. Support wildcard for common aspects.
- [datahub-gms,datahub-mce-consumer,datahub-mae-consumer] `ENTITY_VERSIONING_ENABLED`: Enables entity versioning related resolvers, validators, sideeffects, etc. to support versioned entities. Initial feature implementation which will likely see limited rollout to specific customers.
- [datahub-gms] `BOOTSTRAP_SYSTEM_UPDATE_POST_INFO_ENABLE`: Whether to run a bootstrap step for post entities to allow filtering posts by type. Default true

## v0.3.7

### Breaking Changes

- None

### Potential Downtime

- None

### Deprecations

- None

### Other Notable Changes

- Introduced Usage Based Search Ranking By Default
- Introduced Compliance Forms Feature
- Introduced Structured Properties Feature
- Introduced Column Level Lineage View
- Introduced AI Glossary Term Classification Automation
- Introduced BigQuery Metadata Sync Automation
- Introduced Support for Running Remote Automations (Remote Executor)

### Environment Variables

datahub-gms

- `FORM_CREATION_ENABLED` (app default: true, helm default should be: true): Controls whether users can see the Create Forms flow in the UI. This is important to be enabled for Compliance Forms feature, but if customers do not want it they can disable.
- `FORM_ANALYTICS_ENABLED` (app default: false, helm default should be: false): Controls whether users can see the "Analytics" sub-tab within the Compliance Forms page. This is disabled until we have form analytics ingestion source + UI ready for V1 (likely 1-2 releases from now). Caveat: This can be enabled for anyone who requests it explicitly.
- `SHOW_MANAGE_STRUCTURED_PROPERTIES` (app default: true, helm default should be: true): Controls whether the Structured Properties page is visible and accessible viat he UI. This allows users to create and update structured properties. It should be enabled by default unless folks request otherwise.
- `SCHEMA_FIELD_CLL_ENABLED` (app default: false, helm default should be: true): Controls whether the Column-level lineage focus view is accessible via the lineage Graph. This should actually be TRUE for all customers by default, unless explicitly opted out of.
- `SCHEMA_FIELD_LINEAGE_IGNORE_STATUS` (app default: true, helm default should be: true): Controls whether lineage ignores the schema field status aspect, reading the parent's status aspect instead.
- `SLACK_INTEGRATIONS_SERVICE_NOTIFICATIONS_ENABLED` (app default: false, helm default should be: true): Controls whether some Slack notification types are routed to integrations service, which is responsible for decorating and adding functionality (like callbcak buttons and hooks). In practice, this uplifts the incident notifications specifically. It is already enabled for WB Games but should be enabled for everyone being changed to TRUE by default. As of v0.3.7 we should enable this for everyone, since it's been running in prod successfully for WB games for many months now. Luckily we already have these flags in helm and set for WB:

      notifications:
        slack:
          # Enable the new sink v2 for EVERYONE
          sinkV2: true
          # and stateful message handling for EVERYONE
          statefulMessagesEnabled: true

## v0.3.6

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment Variables

- `SCHEMA_FIELD_CLL_ENABLED` (default: false) - Turns on the schema-field level lineage feature, by adding
  links to the schema field drawer header and to lineage visualization columns. These links bring the user to
  the schema field entity page Lineage tab, which shows a column-level lineage graph. From there, users can
  explore lineage like normal, with links to other columns and links back to dataset-level lineage. Without
  the flag enabled, schema-field lineage is still available but there are no direct links to it, so it's
  hidden from users.
- `SCHEMA_FIELD_LINEAGE_IGNORE_STATUS` (default: false) - Should be enabled if `SCHEMA_FIELD_CLL_ENABLED`
  is enabled without the schema field side effect being enabled (via `MCP_SIDE_EFFECTS_SCHEMA_FIELD_ENABLED`).
  This will tell the schema-field level lineage visualization to ignore the status of the schema field
  when determining if it should be included in the lineage graph, because most schema fields will not
  exist in the schema field index / database without the side effect. The side effect has yet to be load
  tested on a production environment, so this enables turning on schema-field level lineage now,
  while that testing is being completed.

## v0.3.5

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

- Metadata test results should now not appear in entity pages for tests that have been soft deleted.
- Incident related entity change events now include the type, title, description, stage & message of the incident
- A new privilege has been introduced ("Manage User Subscriptions"), granted to admins, will allow DataHub operators to create & subscriptions for other users using GraphQL. In order to do so, add `userUrn` property to the input objects in the graphQL calls.
- Timeline API version calculations may change due to bug fixes, the following scenarios have been modified:
  1. Loop counter was incorrectly incremented twice in the case where a native data type was changed.
  2. Rename events did not correctly check for TECHNICAL_SCHEMA ChangeCategory.
  3. Rename logic did not handle empty descriptions properly.
  4. Handles primary key changes under a single change event

### Environment Variables

- `HIDE_DBT_SOURCE_IN_LINEAGE` (default: false) - Only should be enabled if the user is running dbt
  ingestion with `skip_sources_in_lineage: true` (requires either `entities_enabled.sources: NO` or
  `prefer_sql_parser_lineage: true`). This will hide the dbt source entities from the lineage graph,
  even though the graph index stores an edge between a dbt source and its sibling, and represent the
  warehouse sibling as a "combined" entity. If the user is on the dbt entity page's lineage tab, the
  visualization will show the warehouse sibling as the home node.

## v0.3.4

### Breaking Changes

### Potential Downtime

- Metadata Test entities will not be displayed in the Metadata Test UI until it has executed at least once with the new version of DataHub.

### Deprecations

### Other Notable Changes

- Behavior of Custom Properties Metadata Tests has changed, querying the customProperties field directly returns the keys
  in the map and each key is referencable as a subquery part. This enables more advanced use cases without the need of using
  Regex matching. So instead of querying `datasetProperties.customProperties` with a Regex operator to filter on a specific property
  being present you can just query `datasetProperties.customProperties.myProp`

### Environment Variables

`ENTITY_CLIENT_RESTLI_GET_BATCH_CONCURRENCY` (default: 2) - Number of concurrent rest.li calls when the number of urns in a getBatchV2 call exceeds the batch
size of 50. This is typical in metadata tests.

`SEPARATE_SIBLINGS_LINEAGE_BY_DEFAULT` (default: false) - Whether we should
separate siblings when fetching lineage data in search results by default.
Typically this is false, but for Apple, we need this to be set to true.

`ALTERNATE_MCP_VALIDATION` (default: false) - Enables an alternate MCP validation pathway for when we have MCPs we want to validate only after applying a mutation hook

#### Sharing

`FEAT_SHARE_EXTRA_ASPECTS` (default: empty) - Set this env variable for integrations service to
additionally share custom aspects that might have been provisioned.

`FEAT_SHARE_SKIP_CACHE_ON_LINEAGE_QUERY` (default: false) - Set this env
variable to true (case insensitive) in case you want sharing to always use the
latest lineage results at the cost of performance.

`FEAT_SHARE_MAX_ENTITIES_PER_SHARE` (default: 1000) - By default the sharing
system will share a maximum of 1000 upstreams and 1000 downstreams. Change this
env variable to share higher numbers.

`GRAPHQL_QUERY_INTROSPECTION_ENABLED` (default: true) - Whether to enable introspection queries in the GraphQL API. By default, this was already true.

#### Acryl Observe

`ENABLE_INCIDENT_ACTIVITY_EVENTS` (default: true) - Whether to enable generating incident activity events via the incident activity event hook.

`BROADCAST_NEW_INCIDENT_UPDATES_ENABLED` (default: true) - Whether to broadcast stateful incident update notifications. This is useful for propagating
stateful incident changes to other systems.

`RUN_ASSERTIONS_ENABLED` (default: false) - Whether to enable running assertions in Acryl Observe.
This is a new feature that allows you to refresh native Acryl assertions from the UI. Currently ONLY supported for NON-REMOTE-EXECUTOR assertions.
Some customers have explicitly requested this (Mozilla).

`STATEFUL_SLACK_INCIDENT_MESSAGES_ENABLED` (datahub-integrations-service, default: false) - Whether to enable updating messages based on
state recorded in datahub-gms backend. This is today powering stateful incident messages, where the original incident notification is updated
based on the state of the incident in the backend when an incident has an update. If `BROADCAST_NEW_INCIDENT_UPDATES_ENABLED` is true, this should be true.

#### DataHub Executor Coordinator

`DATAHUB_EXECUTOR_HOST` (default: "localhost") - Hostname of the DataHub executor. Used for connecting to the executor service. Migrated from
DATAHUB_MONITORS_HOST. Should be part of updated Helm Chart.

`DATAHUB_EXECUTOR_PORT` (default: 9004) - Port of the DataHub executor. Used for connecting to the executor service. Migrated from
DATAHUB_MONITORS_PORT. Should be part of updated Helm Chart.

`DATAHUB_EXECUTOR_USE_SSL` (default: false) - Whether to use SSL when connecting to the DataHub executor.
Migrated from DATAHUB_MONITORS_USE_SSL. Should be part of updated Helm Chart.

#### Metadata Change Proposal Throttle

`MCP_THROTTLE_UPDATE_INTERVAL_MS` (default: 60000) - How often the MAE consumer's kafka lag is sampled for throttleing MCE consumer's processing rate.

Enable flags:

`MCP_VERSIONED_THROTTLE_ENABLED` (default: false) - Enable throttling of MCP processing involving versioned aspects.

`MCP_TIMESERIES_THROTTLE_ENABLED` (default: false) - Enable throttling of MCP processing involving timeseries aspects.

Threshold flags:

`MCP_VERSIONED_THRESHOLD` (default: 4000) - The lag threshold at which throttling should be activated for versioned aspects.

`MCP_TIMESERIES_THRESHOLD` (default: 4000) - The lag threshold at which throttling should be activated for timeseries aspects.

Max throttling attempts:

`MCP_VERSIONED_MAX_ATTEMPTS` (default: 1000) - The number of throttling events before the versioned aspect throttle is reset.

`MCP_TIMESERIES_MAX_ATTEMPTS` (default: 1000) - The number of throttling events before the timeseries aspect throttle is reset.

Exponential backoff configuration:

`MCP_VERSIONED_INITIAL_INTERVAL_MS` (default: 100) - The exponential backoff initial interval for versioned aspects.

`MCP_TIMESERIES_INITIAL_INTERVAL_MS` (default: 100) - The exponential backoff initial interval for versioned aspects.

`MCP_VERSIONED_MULTIPLIER` (default: 10) - The exponential backoff multiplier for versioned aspects.

`MCP_TIMESERIES_MULTIPLIER` (default: 10) - The exponential backoff multiplier for timeseries aspects.

`MCP_VERSIONED_MAX_INTERVAL_MS` (default: 30000) - The exponential backoff max interval for versioned aspects.

`MCP_TIMESERIES_MAX_INTERVAL_MS` (default: 30000) - The exponential backoff max interval for timeseries aspects.

#### Metadata Tests

Big change alert!

`METADATA_TESTS_ELASTICSEARCH_EXECUTOR_ENABLED` (default: true) - Whether to enable the Elasticsearch executor for metadata tests.
This is a new feature that allows metadata test predicates to be executed against Elasticsearch when the data is fully available there!

## v0.3.2

### Breaking Changes

- RestoreIndices arguements now match OSS arguments.
  - `BATCH_SIZE` => `batchSize`
  - `ASPECT_NAME` => `aspectName`
  - `URN_LIKE` => `urnLike`
  - `URN` => `urn`
  - `URN_BASED_PAGINATION` => `urnBasedPagination`

### Potential Downtime

### Deprecations

### Other Notable Changes

### Environment Variables

- Introduced a new environment variable that allows you to configure a default lookback window when viewing lineage in the UI. When `LINEAGE_DEFAULT_LAST_DAYS_FILTER` is set (an integer), we will default filter for lineage within the last `x` days. For example, if you set this value to `7`, we will default show you lineage within the last 7 days. Not setting this variable results in default showing lineage all time as per usual. The user can always change their filter window, this variable just changes the default when they first go to lineage.
- Introduced a new environment variable that allows you to share an entity with another Acryl instance. With `METADATA_SHARE_ENABLED` is set to `true`, you will see an option under the share menu on an entity profile to send an entity to another instance.
- Introduce a new environment variable to enable schema assertion authoring in the UI: `SCHEMA_ASSERTION_MONITORS_ENABLED`. It is currently defaulted to OFF since this is the first release with the feature. Will first roll out to folks who explicitly request.

System Update: Search Percentile Index Job

Can be disabled if search ranking using usage percentiles is not desired. Other settings are configured to avoid causing latency with other ingestion sources.

- SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_ENABLED (default: true)
- SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_BATCH_SIZE (default: 500)
- SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_DELAY_MS (default: 1000)
- SYSTEM_UPDATE_USAGE_STORAGE_PERCENTILE_LIMIT (default: 0)

Entity Client: Limit Batch Sizes

Most important setting is the Rest.li client which prevents Metadata Tests from executing actions which exceed URI length limits. If extremely long
URNs cause 414 error codes from Rest.li batch calls, decrease the Rest.li batch size.

- ENTITY_CLIENT_JAVA_GET_BATCH_SIZE (default: 375)
- ENTITY_CLIENT_RESTLI_GET_BATCH_SIZE (default: 100)

User Tracking: Hotjar

We use a tool called Hotjar to track users, record user sessions (with data redacted), and ask for product feedback in-app (ie. "How helpful did you find our search results?"). This is either on or off based on the following variable set in GMS:

- USER_TRACKING_ENABLED (default: false)
