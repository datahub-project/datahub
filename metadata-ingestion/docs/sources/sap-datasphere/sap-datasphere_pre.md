### Overview

The connector emits assets on **two platforms**, depending on whether an asset is
managed (lives in Datasphere) or federated (lives in an external system that
Datasphere accesses via a Remote Table).

#### Managed assets → `sap-datasphere` platform

Views, Analytical Models, and Local Tables — the objects you create _inside_
Datasphere — emit on the `sap-datasphere` platform. Their URN shape:

```text
urn:li:dataset:(urn:li:dataPlatform:sap-datasphere, <space>.<asset>, ENV)
```

(prefixed with `<platform_instance>.` if you set the connector's top-level
`platform_instance`).

Datasphere-specific subtypes (`Local Table`, `View`, `Analytic Model`),
CDS-annotation tags, and Space hierarchy are all emitted on top of these URNs.

#### Federated Remote Tables → storage platform

A Datasphere Remote Table that federates from e.g. Snowflake emits on the
_Snowflake_ platform — its URN matches what DataHub's native Snowflake
connector emits for the same physical table. Lineage joins automatically:
a downstream Datasphere View's `UpstreamLineage` points at the same
`snowflake:` URN that the Snowflake connector ingests, with no Siblings
configuration needed.

Federated routing is driven by `connection_to_platform_map` (per connection
name) and `platform_type_defaults` (per typeId fallback). See the recipe for
examples.

#### Built-in typeId routing

The following SAP Datasphere connection typeIds ship with built-in platform
defaults (verified against a live tenant). You can override any of them in
your recipe under `platform_type_defaults`.

| Datasphere typeId    | DataHub platform | Common usage                                                          |
| -------------------- | ---------------- | --------------------------------------------------------------------- |
| `HANA`               | `hana`           | HANA on-prem / external HANA Cloud federated as a Remote Table source |
| `MSSQL`              | `mssql`          | SQL Server federation                                                 |
| `S3`                 | `s3`             | S3 buckets federated as remote tables                                 |
| `GCS`                | `gcs`            | Google Cloud Storage federation                                       |
| `ABAP`               | `sap-abap`       | SAP ABAP system extraction                                            |
| `SAPS4HANACLOUD`     | `sap-s4hana`     | SAP S/4HANA Cloud federation                                          |
| `SAPBWMODELTRANSFER` | `sap-bw`         | SAP BW analytical model transfer                                      |

Other typeIds (Snowflake, BigQuery, Kafka, Salesforce, ...) default to
`enabled: false` with a warning — opt in by adding them to
`platform_type_defaults` in your recipe. The connector reports each
unmapped-typeId asset once via `report.assets_skipped_unknown_typeid`.

#### Local Tables (base tables)

By default the connector emits only Datasphere assets exposed for OData
consumption (views and analytical models). Base tables — which lineage edges
typically point at — are not in that surface. Set `include_local_tables: true`
to ALSO discover them via the supported
`/dwaas-core/api/v1/spaces/X/localtables` endpoint (the same endpoint the
official `datasphere` CLI uses; SAP-blessed, no policy caveat).

Local Tables emit on the `sap-datasphere` platform (same as Views and
Analytical Models) so phantom-lineage edges from consuming views resolve to
real entities. Their column schema is read from the per-table CSN
(`/dwaas-core/api/v1/spaces/X/localtables/Y`) when available, enabling
column-level lineage edges between a View and its base table; if the CSN is
unavailable the table is still emitted as a schema-less stub. Each Local Table
has subtype `Local Table` and parents directly to its Space container — the
connector uses a 2-tier Space → object model, with no separate folder layer.

### Prerequisites

#### Connection

Set `base_url` to your SAP Datasphere tenant URL (e.g.
`https://yourtenant.eu10.hcs.cloud.sap`). The previous field name `tenant_url`
remains accepted as a deprecated alias; new recipes should use `base_url`.

#### Authentication

The connector supports three authentication methods (in priority order):

1. **Raw bearer token** (`token`) — For local development. Obtain from your browser's DevTools after logging into Datasphere.
2. **OAuth refresh token** (`refresh_token`) — Authorization code flow. Compatible with credentials created for Atlan. Requires `client_id` too.
3. **XSUAA client credentials** (`client_id` + `client_secret`) — Recommended for production. Requires a Technical User OAuth Client created under **System → Administration → App Integration** by a DW Administrator.

#### Creating an OAuth Client (Client Credentials)

1. Log into your SAP Datasphere tenant as DW Administrator.
2. Navigate to **System → Administration → App Integration**.
3. Click **Add a New OAuth Client** → Purpose: **API Access**.
4. Note the generated `client_id`, `client_secret`, and the OAuth Token URL (this is your `xsuaa_url`).

#### Space membership

> **The ingestion principal must be a member of every Datasphere space you want to ingest.**

Both the consumption catalog and the dwaas-core APIs only return spaces that the
ingestion OAuth **principal** (the user behind a refresh token, or the technical
user behind a client-credentials OAuth client) **is a member of**. A space the
principal is not a member of returns **HTTP 403** and is silently skipped — the
connector logs a `report` warning _"Not a member of SAP Datasphere space"_ and
moves on.

Add the principal as a member of each target space:

1. Open **Space Management**.
2. Select the space.
3. Under **Members**, add the ingestion user / OAuth client.
