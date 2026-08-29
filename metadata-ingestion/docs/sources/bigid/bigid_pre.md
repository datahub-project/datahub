### Overview

The `bigid` module ingests classification and governance metadata from BigID into DataHub. It reads BigID's data catalog, business glossary, classifications, and IDSoR correlation results, then enriches matching DataHub datasets with GlossaryTerms, Tags, risk scores, and profiles.

By default this connector runs in **pure enrichment mode** (`create_datasets: false`): it never emits structural aspects and only augments datasets that already exist in DataHub. Enable `create_datasets` to also emit `DatasetProperties` and `SchemaMetadata` for sources that BigID knows about but DataHub does not.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to your BigID instance over HTTPS.
2. **A BigID service account** (a System User) with a long-lived **user token** and read access to the catalog, classification, business glossary, and (if used) correlation/IDSoR APIs. See [Authentication](#authentication) and [Required permissions](#required-permissions) below.
3. **Datasets already present in DataHub** for the sources BigID scans, unless you enable `create_datasets`.
4. **A compatible DataHub version**: **1.8.0+** (DataHub Core) or **2.1.0+** (DataHub Cloud). See [DataHub version compatibility](#datahub-version-compatibility) below.

#### DataHub version compatibility

Field-level enrichment (column classifications and field glossary terms) applies tags and terms to schema fields via PATCH. Adding a field-level tag or term to a field that has no existing `editableSchemaMetadata` entry requires a server-side fix present in **DataHub Core 1.8.0+** and **DataHub Cloud 2.1.0+**. On earlier versions those field-level PATCHes are rejected with `HTTP 422 - fieldPath is required`; dataset-level enrichment is unaffected.

#### Authentication

The connector authenticates to the BigID REST API with a **bearer token**. It does **not** perform an interactive login, so single sign-on (SSO/SAML/OIDC) is never invoked at ingestion time — the token is what grants access. There are two ways to supply that token, in order of preference:

| Config                         | Token kind                                | Lifetime                      | Auto-refresh                                                                                     | Use for                                                                                       |
| ------------------------------ | ----------------------------------------- | ----------------------------- | ------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------- |
| `user_token` **(recommended)** | Long-lived user token generated in the UI | Up to 999 days                | Yes — exchanged for a short-lived session token at startup and re-fetched automatically on a 401 | Scheduled / production ingestion                                                              |
| `access_token`                 | Short-lived session token, used directly  | Minutes–hours (BigID default) | No — a run that outlives the token fails                                                         | One-off/manual runs, or SSO-only tenants where you cannot create a service-account user token |

Provide exactly one. If you set **both**, `user_token` takes precedence (it can auto-refresh) and the standalone `access_token` is ignored. Paste the **raw token** for either — do **not** add a `Bearer` prefix (the connector sends it exactly as given).

`user_token` is strongly preferred: it is exchanged for a session token at startup (via `GET /api/v1/refresh-access-token`) and transparently refreshed if that session token expires mid-run, so scheduled ingestion keeps working without manual rotation. `access_token` skips the exchange but is not refreshed, so it is only suitable for short, manual runs.

##### SSO / SAML environments

Because the connector uses token auth rather than an interactive login, an SSO-only tenant does **not** block ingestion — but note:

- **Preferred:** create a local **System User** service account (independent of your SSO directory) and generate a `user_token` for it. This is the most robust option and is unaffected by SSO. Username/password session login (`POST /api/v1/sessions`) is **not** supported by the connector, and would not work for SSO/federated users anyway.
- **If local service users are disallowed:** an SSO user can sign in to BigID and obtain a short-lived session token, then pass it as `access_token` for a manual run. This will expire, so it is not suitable for scheduled ingestion.

##### Generating a user token

Generate the long-lived `user_token` from the BigID UI:

1. Go to **Administration → Access Management** and select (or create) a user from the **System Users List**. A dedicated, read-only service-account user is recommended over a personal login.
2. Open the user's profile in the right-hand detail panel and, in the **Tokens** section, click **Generate**.
3. Set an expiration (BigID allows up to **999 days**) and click **Generate** again.
4. **Copy the token value immediately** — BigID does not display it again after the dialog closes.
5. Click **Save** on the user profile. **This step is required**: an unsaved token stays inactive and the API rejects it with `{"message":"Refresh token not valid"}` (HTTP 401). Tokens cannot be edited after creation — to rotate, generate a new one and Save again.

##### Required permissions

Assign the service user a role with **read** access to the resources the connector reads. It issues only `GET` requests to these endpoints:

| Endpoint                                             | Purpose                                                    | Required when                               |
| ---------------------------------------------------- | ---------------------------------------------------------- | ------------------------------------------- |
| `GET /api/v1/refresh-access-token`                   | Exchange the user token for a session token                | Always (when using `user_token`)            |
| `GET /api/v1/ds-connections`                         | Data source → DataHub platform resolution; connection test | Always                                      |
| `GET /api/v1/data-catalog/`                          | Catalog objects (datasets)                                 | Always                                      |
| `GET /api/v1/data-catalog/columns`                   | Column-level schema, classifications, and profiles         | Structured sources                          |
| `GET /api/v1/all-classifications`                    | Classifier → Business Glossary linkage                     | Always                                      |
| `GET /api/v1/business_glossary_items`                | Business Glossary terms                                    | Always                                      |
| `GET /api/v1/data-catalog/results-tuning/attributes` | IDSoR (correlation) attribute → glossary mapping           | Only when `sync_idsor` is enabled (default) |

A read-only role granting the **Data Catalog**, **Classification**, **Business Glossary**, and **Correlation** permission groups covers all of the above. If IDSoR sync is not needed, you can omit the Correlation permission and set `sync_idsor: false`.

#### Connection-to-Platform Resolution

BigID connection `type` values are mapped to DataHub platform names automatically (for example `rdb-postgresql` → `postgres`, `snowflake` → `snowflake`). Two levers let you override this:

- `datasource_platform_mapping` — per-connection overrides of platform, `env`, `platform_instance`, and `convert_urns_to_lowercase`. Required when a connection's type has no built-in mapping, or when a dataset's URN must match a specific platform instance created by a native connector. Set `convert_urns_to_lowercase` on a connection when the native connector's URN casing differs from BigID's default (Snowflake, BigQuery and Redshift are lowercased by default) — for example a Snowflake source ingested with `convert_urns_to_lowercase: false`.
- `connection_pattern` — regex allow/deny patterns matched against the BigID connection name. Use this to scope ingestion to a subset of connections in large deployments that expose hundreds of data sources.

#### Confidence Filtering

Classification findings carry a BigID confidence rank. Ranks map to `HIGH = 0.75`, `MEDIUM = 0.50`, `LOW = 0.25`. Set `minimum_confidence_threshold` (0.0–1.0) to drop low-confidence findings.

#### Domain Handling

BigID `domain`/`sub_domain` values are mapped into DataHub according to `domain_mode`:

- `none` (default) — domain values are stored in `customProperties` only; no domain entities are created.
- `auto_namespaced` — one `urn:li:domain` entity is auto-created per BigID domain/sub-domain, keyed deterministically by name (the human-readable label is carried on `domainProperties.name`).
- `config_map` — BigID domain values are mapped to pre-existing DataHub domain URNs via `domain_mapping`.

In `auto_namespaced` mode the generated domain GUID is **scoped by `env` and `platform_instance`**, mirroring how datasets and data products are separated. The same domain name under different `env` or `platform_instance` values resolves to **distinct** `urn:li:domain` entities. To share a domain across two BigID ingestions, give them the same `env` and `platform_instance`; to keep them separate, vary either. `config_map` mode is unaffected (URNs come from your `domain_mapping`).
