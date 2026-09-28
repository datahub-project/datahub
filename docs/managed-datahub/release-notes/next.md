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

- **(Remote Executor) Dynamic ingestion virtual environments now hardlink from the `uv` cache by default**, sharply reducing ephemeral-storage growth. The DataHub-provided Remote Executor Helm chart, Terraform module, and CloudFormation template now set `UV_LINK_MODE=hardlink`, so venvs built for non-[bundled](/docs/docker/bundled-ingestion-venvs.md) runs link their package files from the shared cache instead of copying them — many concurrent or successive runs that share dependencies no longer each consume a full copy. **Action:** none for standard deployments. If you place `/tmp` and the `uv` cache on **separate** volumes, review [Remote Executor best practices → Ingestion virtual environments and the uv cache](../remote-executor/best-practices.md#ingestion-virtual-environments-and-the-uv-cache) to preserve the savings. If you run with a **read-only root filesystem**, point `UV_CACHE_DIR` at a writable volume — otherwise dynamic venv builds fail (see [Read-only root filesystem](../remote-executor/best-practices.md#read-only-root-filesystem)). Set `UV_LINK_MODE=copy` to opt out.

Fixes:

- **(GMS rate limiting)** Mounted `RATE_LIMITS_CONFIG_FILE` rule lists now apply: policy is loaded once by `RateLimitEffectiveConfig` (Binder + classpath default, file replaces it, `RATE_LIMITS_CONFIG_JSON` overlays) and shared by Hazelcast bootstrap and the engine. Previously `@PropertySource` sat below packaged `application.yaml` and Boot kept the bundled empty `endpoint.rules` list. **Action:** none if the file URI already has a `file:` prefix. Confirm `GET /openapi/v1/rate-limits/config` shows the mounted rules after upgrade.

## Known Issues

- TODO
