# Updating DataHub

This file documents any backwards-incompatible changes in DataHub and assists people when migrating to a new version.

## Next

### Breaking Changes

### Potential Downtime

### Deprecations

### Other notable Changes
- #4961 Dropped profiling is not reported by default as that caused a lot of spurious logging in some cases. Set `profiling.report_dropped_profiles` to `True` if you want older behaviour.

## `v0.8.35`

### Breaking Changes

### Potential Downtime

### Deprecations
  - #4875 Lookml view file contents will no longer be populated in custom_properties, instead view definitions will be always available in the View Definitions tab.

### Other notable Changes

## `v0.8.34`

### Breaking Changes
- #4644 Remove `database` option from `snowflake` source which was deprecated since `v0.8.5`
- #4595 Rename confusing config `report_upstream_lineage` to `upstream_lineage_in_report` in `snowflake` connector which was added in `0.8.32`

### Potential Downtime

### Deprecations
- #4644 `host_port` option of `snowflake` and `snowflake-usage` sources deprecated as the name was confusing. Use `account_id` option instead.

### Other notable Changes
- #4760 `check_role_grants` option was added in `snowflake` to disable checking roles in `snowflake` as some people were reporting long run times when checking roles.
