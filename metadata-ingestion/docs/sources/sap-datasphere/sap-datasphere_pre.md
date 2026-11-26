### Overview

The `sap-datasphere` module ingests metadata from SAP Datasphere into DataHub.
It is intended for production ingestion workflows and module-specific
capabilities are documented below.

Managed Views, Analytic Models, and Local Tables emit on the `sap-datasphere`
platform. Federated Remote Tables emit on their storage platform (for example
Snowflake) so lineage joins the native warehouse connector without Siblings
configuration — see **Capabilities → Platform routing**.

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
