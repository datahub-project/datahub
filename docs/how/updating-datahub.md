# Updating DataHub

This file documents any backwards-incompatible changes in DataHub and assists people when migrating to a new version.

## Next

### Breaking Changes
- #4644 Remove `database` option from `snowflake` source which was deprecated since `v0.8.5`
- #4595 Rename confusing config `report_upstream_lineage` to `upstream_lineage_in_report` in `snowflake` connector which was added in `0.8.32`

### Potential Downtime

### Deprecations
- #4644 `host_port` option of `snowflake` and `snowflake-usage` sources deprecated as the name was confusing. Use `account_id` option instead.

### Other notable Changes
