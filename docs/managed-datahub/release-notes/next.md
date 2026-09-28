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
- **(Remote Executor) Reusable ingestion virtual environments are now cached between runs.** A dynamic venv whose contents are fully determined by its name is kept in a node-local cache at `/tmp/datahub/ingest/_venv_cache/` and reused, instead of being rebuilt and deleted on every run — which mainly benefits short tasks such as **test connection**, where the rebuild dominated the wall clock. The cache is bounded by entry count and age (`DATAHUB_VENV_CACHE_MAX_ENTRIES`, default `10`; `DATAHUB_VENV_CACHE_MAX_AGE_HOURS`, default `24`), and a venv in use by a running task is never evicted. Entries that can resolve differently over time — `version: latest`, or any unpinned `extra_pip_requirements` entry — are rebuilt once they pass `DATAHUB_VENV_CACHE_LATEST_TTL_HOURS` (default `24`), so a long-lived pod still picks up newly published connector fixes. An entry whose version and every requirement are exactly pinned never expires. Venvs built from a dev-build wheel URL are not cached at all: their install bypasses `uv`'s package cache, so unlike other entries they hold no hardlinks and cost their full size on disk. **Action:** venv disk is no longer released continuously as runs finish, so re-check ephemeral-storage sizing — budget for the retained cache as well as the `uv` cache and your retained logs, and lower `DATAHUB_VENV_CACHE_MAX_ENTRIES` if the volume is tight (see [Remote Executor best practices → Storage](../remote-executor/best-practices.md#storage)). Set `DATAHUB_VENV_CACHE_ENABLED=false` to restore a freshly built venv per run; note that this stops new entries but does not delete existing ones, so remove `/tmp/datahub/ingest/_venv_cache/` by hand to reclaim that space.

Fixes:

- **(GMS rate limiting)** Mounted `RATE_LIMITS_CONFIG_FILE` rule lists now apply: policy is loaded once by `RateLimitEffectiveConfig` (Binder + classpath default, file replaces it, `RATE_LIMITS_CONFIG_JSON` overlays) and shared by Hazelcast bootstrap and the engine. Previously `@PropertySource` sat below packaged `application.yaml` and Boot kept the bundled empty `endpoint.rules` list. **Action:** none if the file URI already has a `file:` prefix. Confirm `GET /openapi/v1/rate-limits/config` shows the mounted rules after upgrade.

## Known Issues

- TODO
