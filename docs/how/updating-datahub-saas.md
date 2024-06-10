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


### Potential Downtime

- Metadata Test entities will not be displayed in the Metadata Test UI until it has executed at least once with the new version of DataHub.

### Deprecations

### Other Notable Changes

### Environment Variables

`ENTITY_CLIENT_RESTLI_GET_BATCH_CONCURRENCY` (default: 2) - Number of concurrent rest.li calls when the number of urns in a getBatchV2 call exceeds the batch
size of 50. This is typical in metadata tests.

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
