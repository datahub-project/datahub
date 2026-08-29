# Secret Resolution in Recipes

DataHub recipes support `${SECRET_NAME}` syntax to inject secrets at runtime.

## Variable Syntax

Secrets are referenced using bash-style variable substitution:

```yaml
source:
  type: snowflake
  config:
    username: ${SNOWFLAKE_USER}
    password: ${SNOWFLAKE_PASSWORD}
```

### Naming Rules

Variable names **must** follow these rules:

- Start with a letter (`A-Z`, `a-z`) or underscore (`_`)
- Contain only letters, numbers (`0-9`), and underscores
- No whitespace or special characters with the exception of underscores

:::caution Hyphens Have Special Meaning
The `-` character is interpreted as a bash default-value operator:

- `${DB-PASSWORD}` is parsed as "variable `DB`, with default value `PASSWORD`"
- If `DB` is unset, this resolves to the literal string `PASSWORD`

This is a common gotcha when using file-based secrets.
:::

### Bash-Style Defaults

Bash-like parameter expansion is supported:

| Syntax              | Meaning                                       |
| ------------------- | --------------------------------------------- |
| `${VAR:-default}`   | Use `default` if `VAR` is unset or empty      |
| `${VAR-default}`    | Use `default` if `VAR` is unset               |
| `${VAR:+alternate}` | Use `alternate` if `VAR` is set and non-empty |
| `${VAR:?error}`     | Error if `VAR` is unset or empty              |

## Secret Backends

| Backend         | Source                             |
| --------------- | ---------------------------------- |
| **DataHub**     | Secrets created in DataHub UI      |
| **File**        | Files in `/mnt/secrets/` directory |
| **Environment** | Environment variables              |

All backends are checked and values are merged. If the same secret exists in multiple backends, **DataHub** takes precedence over **File**, which takes precedence over **Environment**.

For example, if `DB_PASSWORD` is set as an environment variable and also created in the DataHub UI, the value from DataHub is used.

:::caution Remote Executor — DataHub UI Secrets
On DataHub Cloud with a [Remote Executor](managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md), the **DataHub** backend is convenient but weaker for strict security: credentials are stored in DataHub and returned in plaintext to the executor over the API (TLS) at job time. Prefer **File**, **Environment**, or cloud secret manager backends. See [Secret security considerations](managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md#secret-security-considerations).
:::

:::note Secure by default — blocking human secret reads
GMS defaults to `SECRET_SERVICE_CALLER_GUARD_MODE=ENFORCE`, which prevents browser sessions and user PATs from fetching plaintext secret values via `getSecretValues`. This does **not** stop a trusted ingestion worker — **datahub-actions** in OSS or an **embedded executor** on DataHub Cloud — from resolving UI secrets at job time. Use local backends when credentials must stay in your environment.
:::

:::note DataHub Cloud
On DataHub Cloud, the File and Environment backends are only applicable when using a [Remote Executor](managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md).
:::

### Remote Executor: recommended backends

| Backend                      | When to use                                                          |
| ---------------------------- | -------------------------------------------------------------------- |
| **File** (`/mnt/secrets/`)   | Kubernetes-mounted secrets; runtime updates without executor restart |
| **Environment**              | Simple env vars on the executor container                            |
| **AWS / GCP Secret Manager** | Runtime lookup from your cloud account (executor-side integration)   |
| **DataHub UI**               | Convenience only; not recommended for security-sensitive deployments |

---

## Remote Executor: File Secret Store

The file backend reads secrets from files in a directory (default: `/mnt/secrets`). Each filename is the secret name, and the file contents are the secret value.

```
/mnt/secrets/
├── SNOWFLAKE_PASSWORD    # contains: my-secret-pw
├── API_KEY               # contains: abc123
└── DB_CONNECTION_STRING  # contains: postgres://...
```

Reference in recipes:

```yaml
password: ${SNOWFLAKE_PASSWORD} # reads /mnt/secrets/SNOWFLAKE_PASSWORD
```

**Configuration:**

| Environment Variable                   | Default         | Description                       |
| -------------------------------------- | --------------- | --------------------------------- |
| `DATAHUB_EXECUTOR_FILE_SECRET_BASEDIR` | `/mnt/secrets`  | Directory containing secret files |
| `DATAHUB_EXECUTOR_FILE_SECRET_MAXLEN`  | `1048576` (1MB) | Maximum secret file size          |

For customers using DataHub Cloud with Remote Executor, see [Configuring Secret Mounting](managed-datahub/operator-guide/setting-up-remote-ingestion-executor.md#configure-secret-mounting-optional) for Kubernetes integration examples.
