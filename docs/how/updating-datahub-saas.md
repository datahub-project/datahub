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

### Deprecations

### Other Notable Changes

### Environment Variables

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

User Tracking: Hotjar

We use a tool called Hotjar to track users, record user sessions (with data redacted), and ask for product feedback in-app (ie. "How helpful did you find our search results?"). This is either on or off based on the following variable set in GMS:

- USER_TRACKING_ENABLED (default: false)
