# Next

:::info

<!-- This contains detailed release notes, but there is also an [announcement blog post](https://datahub.com/blog/next/) that covers the highlights. -->

:::

#### Release Availability Date

TBD

#### Recommended Versions

- **CLI/SDK**: TBD
- **Remote Executor**: TBD
- **On-Prem Versions**:
  - **Helm**: TBD
  - **API Gateway**: TBD
  - **Actions**: TBD

## Release Changelog

### Next

New Features:

- TODO

Fixes:

- **MongoDB**: System collections (`system.*`) are now excluded from ingestion by default.
  Previously, if MongoDB profiling was enabled, the connector would attempt to sample
  `system.profile`, which requires `dbAdmin` access and caused ingestion to fail with
  an authorization error. `system.views` was also silently ingested, producing noisy
  schema metadata from view definitions rather than actual data. A new
  `excludeSystemCollections` config option (default: `true`) controls this behavior.
  The `read` role is now sufficient for all standard ingestion — `readWrite` access is
  not required and was never needed.

## Known Issues

- TODO
