---
title: Migrate Snowflake Ingestion from Password to Key-Pair Auth
description: "Step-by-step guide to switch a DataHub Snowflake ingestion recipe from username+password (DEFAULT_AUTHENTICATOR) to key-pair (KEY_PAIR_AUTHENTICATOR) authentication ahead of Snowflake's Strong Authentication deprecation."
---

# Migrate Snowflake Ingestion from Password to Key-Pair Auth

Snowflake is deprecating username + password authentication (`DEFAULT_AUTHENTICATOR`) as part of its [Strong Authentication](https://docs.snowflake.com/en/user-guide/security-mfa) rollout. Password auth will stop working on an **account-specific enforcement date** during Phase 3 (August–October 2026). Check your account's exact date in the [Snowflake Strong Authentication Hub](https://docs.snowflake.com/en/release-notes/bcr-poly/strong-authentication).

This guide walks you through switching an existing DataHub Snowflake ingestion recipe from password auth to **key-pair authentication** — the recommended target for scheduled, headless ingestion.

:::tip No re-backfill needed
Changing the authentication mode does **not** reset your ingestion cursor, lineage, or any metadata already in DataHub. The Snowflake source URN stays the same, so the next run simply continues from where the last one left off. You do **not** need to re-backfill.
:::

## Why key-pair auth

| Auth mode                                                            | Headless / scheduled?                   | MFA / IdP dependency                                | Recommended?                                                                   |
| -------------------------------------------------------------------- | --------------------------------------- | --------------------------------------------------- | ------------------------------------------------------------------------------ |
| **Key-pair** (`KEY_PAIR_AUTHENTICATOR`)                              | ✅ Yes                                  | None                                                | ✅ **Recommended** for service accounts and scheduled ingestion                |
| **Workload identity federation** (`WORKLOAD_IDENTITY_AUTHENTICATOR`) | ✅ Yes                                  | None, uses the cloud workload's own identity        | ✅ Recommended when ingestion runs on AWS, Azure or GCP, with no key to rotate |
| OAuth (`OAUTH_AUTHENTICATOR`)                                        | ✅ Yes (with token refresh)             | Requires an IdP (e.g. Okta) and token-refresh setup | Viable when you already run an IdP-backed OAuth flow                           |
| External browser SSO (`EXTERNAL_BROWSER_AUTHENTICATOR`)              | ❌ No — requires an interactive browser | Requires SSO                                        | Manual testing only; **not** for scheduled ingestion                           |
| Username + password (`DEFAULT_AUTHENTICATOR`)                        | ✅ Yes                                  | None                                                | ❌ **Deprecated** by Snowflake — migrate off this                              |

Key-pair auth needs no MFA, no IdP, and no interactive browser, which makes it the best fit for ingestion that runs on a schedule.

If your ingestion already runs on AWS, Azure or GCP, workload identity federation removes the key as well; see the [Snowflake connector guide](https://docs.datahub.com/docs/generated/ingestion/sources/snowflake#authentication) for the setup.

## What you will need

- `ACCOUNTADMIN` (or a role with `MANAGE GRANTS` and user-management rights) in Snowflake.
- The existing DataHub Snowflake recipe you want to migrate.
- Access to create or update a [secret](../../secret-resolution.md) in DataHub (UI secret, environment variable, or a file secret on a Remote Executor).

## Step 1 — Snowflake side: create a key-pair and assign it

### 1a. Generate the key-pair

Generate an unencrypted private key (simplest for headless ingestion) and its public key:

```bash
# Unencrypted 2048-bit RSA private key (PEM)
openssl genrsa 2048 | openssl pkcs8 -topk8 -nocrypt -out snowflake_key.p8

# Public key to assign to the Snowflake user
openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
```

If your security policy requires an encrypted private key, add a passphrase and keep it — you will need it as `private_key_password` in the recipe:

```bash
# Encrypted private key (you will be prompted for a passphrase)
openssl genrsa 2048 | openssl pkcs8 -topk8 -out snowflake_key_encrypted.p8
openssl rsa -in snowflake_key_encrypted.p8 -pubout -out snowflake_key.pub
```

### 1b. Assign the public key to the DataHub Snowflake user

Use the user that your DataHub recipe already connects with (the `username` in your recipe). Strip the `-----BEGIN/END-----` lines and newlines from the public key when pasting it into `RSA_PUBLIC_KEY`:

```sql
USE ROLE ACCOUNTADMIN;

-- Assign the public key to the existing DataHub user
ALTER USER datahub_user SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0B...';
```

:::note Keep the old password for one run
Do not drop the password or change the user type yet. Keep the old password as a rollback for one ingestion run — you will retire it in [Step 4](#step-4--validate-before-retiring-the-old-credentials) after the new key-pair connection is validated. Switching the user to `TYPE = SERVICE` now would also disable password auth, which breaks that rollback.
:::

### 1c. Confirm role and warehouse grants

The role and warehouse grants the recipe relies on do not change when you switch auth mode, but confirm they are still in place. See the [Snowflake prerequisites](https://docs.datahub.com/docs/generated/ingestion/sources/snowflake/#prerequisites) for the full grant set. At minimum:

```sql
GRANT ROLE datahub_role TO USER datahub_user;
GRANT OPERATE, USAGE ON WAREHOUSE "<your-warehouse>" TO ROLE datahub_role;
GRANT USAGE ON DATABASE "<your-database>" TO ROLE datahub_role;
```

## Step 2 — DataHub side: update the recipe

Replace `password` with the private key. You can provide the key **inline** (`private_key`) or **from a file** (`private_key_path`).

### Recipe diff (before → after)

**Before** (password auth):

```yaml
source:
  type: snowflake
  config:
    account_id: "abc48144"
    warehouse: "COMPUTE_WH"
    role: "datahub_role"
    username: "${SNOWFLAKE_USER}"
    password: "${SNOWFLAKE_PASS}"
```

**After** (key-pair auth, key inline as a secret):

```yaml
source:
  type: snowflake
  config:
    account_id: "abc48144"
    warehouse: "COMPUTE_WH"
    role: "datahub_role"
    username: "${SNOWFLAKE_USER}"
    authentication_type: KEY_PAIR_AUTHENTICATOR
    private_key: "${SNOWFLAKE_PRIVATE_KEY}"
    # Required only if the private key is passphrase-protected:
    # private_key_password: "${SNOWFLAKE_PRIVATE_KEY_PASSWORD}"
```

:::caution Keep the private key in PEM format
`private_key` must be the full PEM string, including the `-----BEGIN PRIVATE KEY-----` / `-----END PRIVATE KEY-----` markers, with `\n` line breaks at the beginning, end, and roughly every 64 characters. When you store it as a DataHub secret, paste the literal PEM content (newlines included) into the secret value.
:::

### `private_key` vs `private_key_path`

| Option             | When to use                                                 | Notes                                                                                           |
| ------------------ | ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| `private_key`      | Key stored as a DataHub UI secret or environment variable   | Recommended for DataHub Cloud; rotate via the secret backend without touching the recipe        |
| `private_key_path` | Key lives as a file on the ingestion host / Remote Executor | Useful when policy forbids storing keys in a secret store; mount the file and point to its path |

Example using a file path (e.g. on a Remote Executor where the key is mounted):

```yaml
source:
  type: snowflake
  config:
    account_id: "abc48144"
    username: "${SNOWFLAKE_USER}"
    authentication_type: KEY_PAIR_AUTHENTICATOR
    private_key_path: "/mnt/secrets/snowflake_key.p8"
    # Required only if the private key is passphrase-protected:
    # private_key_password: "${SNOWFLAKE_PRIVATE_KEY_PASSWORD}"
    role: "datahub_role"
    warehouse: "COMPUTE_WH"
```

### Wiring the private key as a secret

Reference the private key with the same `${SECRET_NAME}` syntax as any other secret (see [Secret Resolution](../../secret-resolution.md)):

- **DataHub UI secret:** create a secret named `SNOWFLAKE_PRIVATE_KEY` whose value is the full PEM content, then reference it as `private_key: ${SNOWFLAKE_PRIVATE_KEY}`.
- **Environment variable:** set `SNOWFLAKE_PRIVATE_KEY` in the ingestion environment.
- **File secret on a Remote Executor:** mount the key file under `/mnt/secrets/` and reference it. For a Kubernetes Remote Executor, mount the key from a Kubernetes Secret (see [Deploy on Kubernetes](../../managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md#deploy-on-kubernetes) in the Remote Executor guide):

```yaml
extraVolumes:
  - name: snowflake-secret
    secret:
      secretName: my-snowflake-secret
extraVolumeMounts:
  - mountPath: /mnt/secrets/SNOWFLAKE_PRIVATE_KEY
    name: snowflake-secret
    subPath: snowflake-private-key
    readOnly: true
  - mountPath: /mnt/secrets/SNOWFLAKE_PRIVATE_KEY_PASSWORD
    name: snowflake-secret
    subPath: snowflake-private-key-password
    readOnly: true
```

Then reference both in the recipe:

```yaml
private_key: "${SNOWFLAKE_PRIVATE_KEY}"
private_key_password: "${SNOWFLAKE_PRIVATE_KEY_PASSWORD}" # only if passphrase-protected
```

## Step 3 — Swap without losing ingestion state

Changing only the authentication fields leaves every other part of the recipe (coordinates, patterns, profiling, stateful ingestion state) untouched. Because the Snowflake source URN is derived from `account_id` / `platform_instance` / database / schema — **not** from the auth mode — the source identity is unchanged.

Concretely:

- The ingestion cursor (stateful ingestion checkpoints) is preserved.
- Lineage, usage, and profiling history already in DataHub are preserved.
- No re-backfill is required.

You can apply the recipe change in place — edit the existing source in the DataHub UI or update the YAML recipe and re-run.

## Step 4 — Validate before retiring the old credentials

1. **Test the connection** before running a full ingestion. You can do this from either the CLI or the UI:

   - **CLI** — using the same recipe file you just updated:

     ```bash
     datahub ingest -c <path-to-your-recipe>.yml --test-source-connection
     ```

   - **UI** — in the DataHub source builder, open the Snowflake source you just edited and click **Test Connection** (in the connection-details step). This runs the same check against the saved recipe without leaving the page.

   A successful connection test confirms the key-pair is wired correctly.

2. **Run one ingestion** and confirm the run reports `SUCCESS` in the DataHub UI.

3. **Keep the old password for one run as a rollback.** If the new run fails, revert the recipe to the password config, re-run, and re-check the Snowflake-side key assignment.

4. **Only after a successful run**, retire the old credentials in Snowflake. Switch the user to a service account and drop the password together — once the user is `TYPE = SERVICE` it can no longer authenticate with a password, so do this last:

   ```sql
   ALTER USER datahub_user SET TYPE = SERVICE;
   ALTER USER datahub_user UNSET PASSWORD;
   ```

## What NOT to use

- **`EXTERNAL_BROWSER_AUTHENTICATOR`** opens an interactive browser prompt on each connection. It is useful for a one-off manual test but **not** suitable for scheduled, headless ingestion — it will block forever waiting for a human.
- **Microsoft OAuth** is supported by the connector config but is not yet documented in the connector guide. If you require it, contact DataHub support before adopting it for production ingestion.

## Migrating from the DataHub UI

The DataHub Snowflake ingestion form supports key-pair auth directly. In the source builder, set **Authentication Type** to **Private Key**, then provide the private key via the **Private Key** field (as a secret reference) and, if your key is passphrase-protected, the **Private Key Password** field. The form fields map to the same `authentication_type`, `private_key`, and `private_key_password` recipe keys used in [Step 2](#step-2--datahub-side-update-the-recipe), so the YAML diff above applies equally to the YAML editor in the source configuration flow.

If you need help confirming your account's enforcement date or want a guided migration, contact DataHub support.
