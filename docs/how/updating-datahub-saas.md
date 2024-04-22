# Updating Acryl DataHub

<!--

## <version number>

### New env variables

### Breaking Changes

### Potential Downtime

### Deprecations

### Other Notable Changes

-->

This is over and above updating-datahub.md file

## Next

### Breaking Changes


### Potential Downtime

### Deprecations

### Other Notable Changes

- Introduced a new environment variable that allows you to configure a default lookback window when viewing lineage in the UI. When `LINEAGE_DEFAULT_LAST_DAYS_FILTER` is set (an integer), we will default filter for lineage within the last `x` days. For example, if you set this value to `7`, we will default show you lineage within the last 7 days. Not setting this variable results in default showing lineage all time as per usual. The user can always change their filter window, this variable just changes the default when they first go to lineage.
- Introduced a new environment variable that allows you to share an entity with another Acryl instance. With `METADATA_SHARE_ENABLED` is set to `true`, you will see an option under the share menu on an entity profile to send an entity to another instance.
