### Overview

The `thoughtspot` module ingests ThoughtSpot Liveboards, Answers, Worksheets, and Tables into DataHub via the REST API v2.0. It also extracts ownership, tags, per-entity view counts, and table- and column-level lineage from charts back to worksheets and out to your warehouse.

### Prerequisites

To get started you need:

1. A reachable ThoughtSpot instance (`https://<your-cluster>.thoughtspot.cloud`).
2. A ThoughtSpot user (or service account) with the permissions described below.
3. Trusted-auth credentials (recommended) or username + password.

#### Permissions

The ingestion user needs:

- **`Has data download permissions`** (`DATADOWNLOADING`) — required to read metadata.
- **`Has administration privileges`** (`ADMINISTRATION`) — recommended; covers Org enumeration and other admin-gated APIs.
- **Read access** on each Liveboard, Answer, Worksheet, and Table you want to ingest. In the ThoughtSpot UI, open the object → **Share** → grant the user (or a group it belongs to) at least **Can view** access. Or set the ingestion user as the owner.

This is enough for a working catalog: worksheet schemas, column-level lineage from worksheets to tables, cross-platform lineage to your warehouse, dashboard-to-dataset lineage, ownership, and tags all emit from `metadata/search`, which is gated only by ordinary read access on the object itself.

:::tip For full chart-level lineage, also share dependencies
The chart-tile layer under a Liveboard and the per-Answer source-table lineage are enriched from ThoughtSpot's `metadata/tml/export` endpoint, which walks the full dependency graph of each Worksheet (connection → joined tables → referenced answers) and returns `FORBIDDEN` if any one is invisible to the principal. Granting `ADMINISTRATION` does **not** override these per-object ACLs.

If TML access is missing for a given object, the connector still emits the entity itself and falls back to dashboard-level lineage — only the chart tiles and per-Answer edges are skipped, and a structured warning names every affected object so you can tell exactly what's missing. See [Lineage Coverage](#lineage-coverage) and the [TML troubleshooting](#charts-disappear-with-a-liveboard-visualization-fetch-failed-warning) section.
:::

#### Authentication

Trusted authentication is recommended for production. Generate the secret key under **Develop > Customizations > Security Settings > Enable Trusted Authentication**:

```yaml
source:
  type: thoughtspot
  config:
    connection:
      base_url: "https://your-cluster.thoughtspot.cloud"
      auth:
        type: trusted
        username: "${THOUGHTSPOT_USERNAME}"
        secret_key: "${THOUGHTSPOT_SECRET_KEY}"
```

Password authentication is also supported — set `type: password` and provide `password:` instead of `secret_key:`. Both methods mint a short-lived bearer token per run via `auth_token_full`, so the credential on disk stays stable across token expiry. Pre-generated bearer tokens are not accepted (see [Limitations](#bearer-token-auth-is-not-supported)).
