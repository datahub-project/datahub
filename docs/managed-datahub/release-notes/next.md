---
description: "Preview of upcoming DataHub Cloud features and changes planned for the next scheduled release."
---

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

## Release Changelog

### Next

Breaking Changes:

- **(UI / Lineage)** The `LINEAGE_GRAPH_V2` and `LINEAGE_GRAPH_V3` feature flags (GMS environment variables) have been removed, along with the older lineage graph visualizations they gated. DataHub now always renders the latest lineage graph experience. **Action:** if you set `LINEAGE_GRAPH_V2` or `LINEAGE_GRAPH_V3` in your GMS environment or Helm values, remove them — they are no longer recognized. There is no longer a way to switch back to the previous lineage visualizations.

New Features:

- TODO

Fixes:

- TODO

## Known Issues

- TODO
