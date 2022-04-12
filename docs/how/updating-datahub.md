# Updating DataHub

This file documents any backwards-incompatible changes in DataHub and assists people when migrating to a new version.

## Next

### Breaking Changes
- Remove `database` option from `snowflake` source which was deprecated since a few versions

### Potential Downtime

### Deprecations
- `host_port` option of `snowflake` and `snowflake-usage` sources deprecated as the name was confusing. Use `account_id` option instead.

### Other notable Changes
