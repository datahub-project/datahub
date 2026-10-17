---
title: Ingestion CLI version matrix
description: Override the ingestion CLI version per connector, and optionally per deployment cohort, without redeploying GMS.
---

# Ingestion CLI version matrix

By default every ingestion execution installs `defaultCliVersion` (`UI_INGESTION_DEFAULT_CLI_VERSION`, pinned to the server's own CLI version). The **CLI version matrix** lets you override that per connector — pin `snowflake` to `1.5.0.6rc1` while everything else stays on the default — by publishing a small JSON document that GMS polls, with no GMS redeploy required to change versions later.

## Enabling it

Set `ingestion.cliVersionMatrix.uri` (env var `INGESTION_VERSION_MATRIX_URL`) to the matrix document's location. The URI's **scheme** selects the backend:

| Scheme              | Backend              | Auth                                                                                                              |
| ------------------- | -------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `s3://bucket/key`   | AWS S3               | Ambient AWS credentials (IAM role / IRSA)                                                                         |
| `gs://bucket/key`   | Google Cloud Storage | Application Default Credentials (Workload Identity on GKE)                                                        |
| `file:///path`      | Local filesystem     | None — mount the file (e.g. a ConfigMap)                                                                          |
| `https://host/path` | Plain HTTP(S)        | Optional `authToken` (env var `INGESTION_VERSION_MATRIX_AUTH_TOKEN`), sent verbatim as the `Authorization` header |

Leave `uri` empty (the default) to disable the matrix entirely — every connector uses `defaultCliVersion`. That is also the safe outcome of anything going wrong: an unreadable object, a bucket the service account can't reach, an unsupported scheme, or a non-positive `refreshSeconds` all degrade to the same no-op rather than blocking GMS startup.

`ingestion.cliVersionMatrix.refreshSeconds` (env var `INGESTION_VERSION_MATRIX_REFRESH_SECONDS`, default `600`) controls how often GMS re-fetches the document. A refresh failure keeps serving the last-known-good matrix.

## Matrix document format

```json
{
  "0.8.28": {
    "snowflake": {
      "_default": "1.5.0.6rc1",
      "cohorts": [
        {
          "version": "1.5.0.13.post1",
          "deployments": ["deployment-a", "deployment-b"]
        }
      ]
    }
  }
}
```

- Top level: server version (must match the running server's own version — a matrix entry for a version other than the one GMS is running on never matches).
- Per connector under that server version:
  - `_default` — the version to install when no cohort matches.
  - `cohorts` — an ordered list of `{version, deployments}`. Evaluated in array order; the first cohort whose `deployments` list contains the current deployment's id wins.
- Version strings accept standard PyPI releases (`1.5.0.5`), pre/post-release suffixes (`1.5.0.6rc1`, `1.5.0.13.post1`), PEP 440 epochs (`1!0.0.0.dev0`), and the executor's non-PyPI sentinels (`bundled`, `no-acryl-datahub`). Malformed entries are skipped with a WARN log naming the offending server/connector/cohort — the rest of the document still loads.

Cohort matching requires `ingestion.deploymentId` (env var `DATAHUB_EXECUTOR_CUSTOMER_ID`) to be set to this deployment's id. Single-tenant installs that leave it unset simply never match a cohort and fall through to `_default` — cohorts are aimed at multi-tenant fleets rolling a version out to a subset of deployments first.

## Resolution order

For a given connector execution:

1. A per-source CLI version override on the ingestion source itself, if set.
2. The first matching cohort in the matrix (deployment id in `cohorts[].deployments`).
3. The connector's `_default` in the matrix.
4. `defaultCliVersion` (`UI_INGESTION_DEFAULT_CLI_VERSION`).

## Troubleshooting

Refresh failures are logged with a bracketed, greppable token so `grep '\[permission\]'` (etc.) over GMS logs finds exactly the refreshes needing attention:

| Token          | Cause                                                                    | Fix                                                                                                                           |
| -------------- | ------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------- |
| `[permission]` | Credentials, IAM policy, bucket policy, or auth token rejected the read  | Grant the service account GMS runs as (Workload Identity / IRSA) read access to the object, or set `authToken` for `https://` |
| `[not-found]`  | The URI names an object, bucket, or path that doesn't exist              | Check `ingestion.cliVersionMatrix.uri` points at an existing object                                                           |
| `[payload]`    | The document was read but isn't valid JSON, or violates the schema above | Fix the matrix document                                                                                                       |
| `[transport]`  | Network, timeout, or otherwise unclassified                              | Usually transient; check connectivity to the matrix location                                                                  |

## Related documentation

- [Bundled ingestion virtual environments](/docs/docker/bundled-ingestion-venvs.md)
- [Ingestion executor security and hardening](/docs/docker/ingestion-executor-security.md)
- [Environment Variables](/docs/deploy/environment-vars.md)
