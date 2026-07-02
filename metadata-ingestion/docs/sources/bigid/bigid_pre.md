### Overview

The `bigid` module ingests classification and governance metadata from BigID into DataHub. It reads BigID's data catalog, business glossary, classifications, and IDSoR correlation results, then enriches matching DataHub datasets with GlossaryTerms, Tags, risk scores, and profiles.

By default this connector runs in **pure enrichment mode** (`create_datasets: false`): it never emits structural aspects and only augments datasets that already exist in DataHub. Enable `create_datasets` to also emit `DatasetProperties` and `SchemaMetadata` for sources that BigID knows about but DataHub does not.

### Prerequisites

Before running ingestion, ensure you have:

1. **Network connectivity** to your BigID instance over HTTPS.
2. **A BigID user token** with read access to the catalog, classifications, business glossary, and (if used) IDSoR/results-tuning APIs. Set it as `user_token` (see below to generate one).
3. **Datasets already present in DataHub** for the sources BigID scans, unless you enable `create_datasets`.

#### Generating a User Token

The connector authenticates with a long-lived BigID **user token**, which it exchanges for a short-lived session token at startup (via `GET /api/v1/refresh-access-token`). Generate the user token from the BigID UI:

1. Go to **Administration → Access Management** and select (or create) a user from the **System Users List**. A dedicated, read-only service-account user is recommended over a personal login.
2. Open the user's profile in the right-hand detail panel and, in the **Tokens** section, click **Generate**.
3. Set an expiration (BigID allows up to **999 days**) and click **Generate** again.
4. **Copy the token value immediately** — BigID does not display it again after the dialog closes.
5. Click **Save** on the user profile. **This step is required**: an unsaved token stays inactive and the API rejects it with `{"message":"Refresh token not valid"}` (HTTP 401). Tokens cannot be edited after creation — to rotate, generate a new one and Save again.

Provide the copied value as `user_token`. Paste the **raw token** — do not add a `Bearer` prefix (the connector sends it exactly as given). The short-lived `access_token` field is for testing only, when you already hold a session token and want to skip the exchange step.

#### Connection-to-Platform Resolution

BigID connection `type` values are mapped to DataHub platform names automatically (for example `rdb-postgresql` → `postgres`, `snowflake` → `snowflake`). Two levers let you override this:

- `datasource_platform_mapping` — per-connection overrides of platform, `env`, and `platform_instance`. Required when a connection's type has no built-in mapping, or when a dataset's URN must match a specific platform instance created by a native connector.
- `connection_pattern` — regex allow/deny patterns matched against the BigID connection name. Use this to scope ingestion to a subset of connections in large deployments that expose hundreds of data sources.

#### Confidence Filtering

Classification findings carry a BigID confidence rank. Ranks map to `HIGH = 0.75`, `MEDIUM = 0.50`, `LOW = 0.25`. Set `minimum_confidence_threshold` (0.0–1.0) to drop low-confidence findings.

#### Domain Handling

BigID `domain`/`sub_domain` values are mapped into DataHub according to `domain_mode`:

- `none` (default) — domain values are stored in `customProperties` only; no domain entities are created.
- `auto_namespaced` — one `urn:li:domain` entity is auto-created per BigID domain/sub-domain, keyed deterministically by name (the human-readable label is carried on `domainProperties.name`).
- `config_map` — BigID domain values are mapped to pre-existing DataHub domain URNs via `domain_mapping`.

In `auto_namespaced` mode the generated domain URN is a deterministic GUID that is **scoped by `env` and `platform_instance`**. This means the same BigID domain name (e.g. `Customer`) ingested under different `env` values (`PROD` vs `DEV`) or different `platform_instance` values resolves to **distinct** `urn:li:domain` entities rather than silently merging into one. This mirrors how datasets and data products are separated across environments and instances — if you run two BigID ingestions that should share a domain, give them the same `env` and `platform_instance`; if they should stay separate, vary either value. `config_map` mode is unaffected (URNs come straight from your `domain_mapping`).
