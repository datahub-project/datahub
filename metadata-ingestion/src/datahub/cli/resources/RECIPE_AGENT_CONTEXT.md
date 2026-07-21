# DataHub Recipe CLI - Agent Context

Best practices for AI agents building and validating DataHub ingestion recipes using the `datahub recipe` command group.

## Purpose

Build or fix an ingestion recipe without ever handling a resolved secret value.

## Credential Boundary — What You Can and Cannot See

This interface is built so a resolved secret value never reaches you. The guarantee holds
only if you follow these rules — the boundary is a shared responsibility.

**What the CLI guarantees:** Secrets are resolved from `${ENV_VAR}` references **inside the
CLI process**, never in your context. All `datahub recipe` output is redacted — on success
and on error, on stdout and stderr, for top-level and nested secret fields. So you may run
`probe` and `test-connection` freely and will only ever see redacted results.

**What you MUST NOT do (these break the boundary):**
- **Never inline a secret value** into a recipe. Always use `${ENV_VAR}` references. If you
  see a plaintext secret in a recipe, you have already been exposed to it — do not copy it;
  recommend switching that field to `${ENV_VAR}` and rerun `validate`.
- **Never set, read, or print a secret value yourself.** Do not `export SECRET=<value>`, do
  not `echo $SECRET`, do not run `env`, and do not read files that contain literal secret
  values (e.g. an `.env` file holding real credentials). The operator sets the environment
  variables out of band before your session; the CLI inherits them without your involvement.
- **Never obtain credentials outside `datahub recipe`.** Redaction lives in this command
  group only. Running any other command that prints a resolved credential (a raw driver call,
  an ingestion run with verbose logging, etc.) can leak it into your context.

**Inline secrets:** a recipe with a plaintext secret is still probeable, and `validate` will
warn about it — but the boundary is already broken for that recipe because reading the file
exposed the value. Treat the warning as a prompt to externalize the secret to `${ENV_VAR}`.

## Workflow: Required Order

Follow these steps in sequence:

1. **Describe** — Inspect config fields and capabilities
   ```bash
   datahub recipe describe <source_type>
   ```
   Lists all config fields with their types (secret, pattern, plain, nested) and connector capabilities. No connection required.

2. **Scaffold** — Generate a starter recipe
   ```bash
   datahub recipe scaffold <source_type> > recipe.yml
   ```
   Creates a template recipe with all required fields set to `${ENV_VAR}` references for secrets.

3. **Edit** — Modify the recipe locally
   Keep secrets as `${ENV_VAR}` references. Do not inline secret values into the YAML file.

4. **Validate** — Check syntax and configuration
   ```bash
   datahub recipe validate <recipe.yml>
   ```
   Validates the recipe against the connector's real config schema. Warnings highlight any inline-secret fields; recommend externalizing them.

5. **Probe** — Enumerate live metadata (SQL-family sources only)
   ```bash
   datahub recipe probe schemas --recipe <recipe.yml>
   datahub recipe probe tables --recipe <recipe.yml> --schema <schema_name>
   datahub recipe probe columns --recipe <recipe.yml> --schema <schema_name> --table <table_name>
   ```
   Returns schema/table/column names and counts (names only, no row data). Each node reports:
   - `pattern_field` — which `*_pattern` config filter governs it (edit to refine discovery).
   - `included` — whether it would actually be ingested given the recipe's filters **and** the source's built-in exclusions (reused from the connector's own ingestion logic, not re-implemented).
   - `excluded_by` — why a node is dropped: a `*_pattern` field (your filter), `"default_schema"` (system catalog the source always skips, e.g. `information_schema`), or `"system_object"`; `null` when included.

   Every node is shown (nothing is hidden), so you can confirm both that your allow/deny filters behave and that system objects are auto-dropped.

   **Three-level sources (Snowflake, BigQuery):** these have an extra top
   container, so the top-level filter is first-class and the second-level filter
   matches a fully-qualified `TOP.CHILD` form. Probe the extra level with
   `probe databases` and pass `--database` (the top container) down the chain:
   ```bash
   datahub recipe probe databases --recipe <recipe.yml>
   datahub recipe probe schemas --recipe <recipe.yml> --database <top>
   datahub recipe probe tables --recipe <recipe.yml> --database <top> --schema <second>
   datahub recipe probe columns --recipe <recipe.yml> --database <top> --schema <second> --table <table>
   ```
   The `--database`/`--schema` flags are generic container slots; each source
   reports its own kinds and `pattern_field` in the output:
   - **Snowflake**: `--database`=database (`database_pattern`),
     `--schema`=schema (`schema_pattern`); with `match_fully_qualified_names: true`
     `schema_pattern` matches the `DATABASE.SCHEMA` fqn.
   - **BigQuery**: `--database`=project (`project_id_pattern`),
     `--schema`=dataset (`dataset_pattern`, matched against `PROJECT.DATASET`).

   A missing required level flag is reported as a config error (exit 2), and
   `probe databases` on a 2-level SQL source (Postgres, MySQL, Redshift, …) is
   rejected with the source's actual levels.

6. **Test Connection** — Verify connectivity
   ```bash
   datahub recipe test-connection --recipe <recipe.yml>
   ```
   Confirms the connection succeeds; reports success or connection error.

## Boundary Rules

**Secret Handling:**
- You provide a recipe path containing `${ENV_VAR}` references. The CLI resolves secrets inside its own process.
- Secret values never appear in output (all redacted).
- Inline-secret recipes (credentials in YAML) are probeable, but `validate` warns; recommend moving to environment variables.

**Exit Codes:**
- `0` — Success
- `2` — Configuration error (schema, missing required field, validation failure)
- `3` — Connection error (authentication, timeout, network)
- `1` — Internal error (CLI bug, unexpected state)

**Live Probe Support:**
- Live probes (schemas/tables/columns) work for SQL-family sources: any connector with `get_sql_alchemy_url()` in its config (Postgres, MySQL, Redshift, Snowflake, etc.).
- Snowflake (database → schema → table) and BigQuery (project → dataset → table) get dedicated database-aware probing — Snowflake via `SHOW`, BigQuery via the BigQuery client (`get_bigquery_client()`).
- **Non-SQL sources probe too**, by reusing their own client (not raw HTTP): **Kafka** lists topics (filtered by `topic_patterns`); **ThoughtSpot** lists Worksheets → Columns via its REST client (filtered by `worksheet_pattern`). The probe interface is source-agnostic — any connector can opt in by implementing `probe_hierarchy()` + `list_probe_children()`.
- Other source types return `supported: false` → fall back to `test-connection` for verification.

**Probing non-SQL hierarchies** — the `databases/schemas/tables/columns` commands are SQL-shaped. For any other hierarchy (Kafka topics, ThoughtSpot worksheets) use the generic lister, which follows whatever levels the source declares:

```bash
datahub recipe probe list --recipe recipe.yml                       # top level (e.g. Kafka topics, TS worksheets)
datahub recipe probe list --recipe recipe.yml --parent <name>       # descend one level (e.g. a worksheet's columns)
```

## Probing a Source With No Connector

You do not need a purpose-built connector to interrogate a source — useful when authoring a recipe (or a connector) for a system DataHub doesn't ship support for.

**Any SQL-reachable database** — use the generic `sqlalchemy` source with a `connect_uri`; schema/table/column probing works with no bespoke connector:

```yaml
source:
  type: sqlalchemy
  config:
    platform: <platform>
    connect_uri: "<sqlalchemy-url, secrets as ${ENV_VAR}>"
```

**A REST/HTTP API** — describe it with a top-level `probe:` block (no `source:`) and run `probe api`:

```bash
datahub recipe probe api --recipe api_probe.yml
```

```yaml
probe:
  kind: rest
  base_url: https://api.example.com
  headers: { Authorization: "Bearer ${API_TOKEN}" } # secrets as ${ENV_VAR}
  endpoints: [/v1/orders, /v1/customers]
  budget: 10          # max endpoints to GET (default 10)
  verify_ssl: true
```

`probe api` is **read-only** (only issues `GET`), **budget-bounded**, and returns **shapes only** — per endpoint: HTTP status, content type, and the inferred JSON schema as `{path, json_type, nullable}` entries. It never returns response **values**, so live data (and any PII in it) never crosses the boundary. Auth secrets resolve in-process from `${ENV_VAR}` and are redacted from all output. Non-JSON or failed endpoints report an `error` and are skipped, not fatal.

## Common Recipes

```bash
# Inspect a connector's config fields
datahub recipe describe snowflake

# Generate a starter recipe
datahub recipe scaffold snowflake > my_recipe.yml

# Edit your recipe (set database, warehouse, user; keep password as ${SNOWFLAKE_PASSWORD})
# Then validate it
datahub recipe validate my_recipe.yml

# Probe schemas
datahub recipe probe schemas --recipe my_recipe.yml

# Probe tables in a specific schema
datahub recipe probe tables --recipe my_recipe.yml --schema my_schema

# Test the connection
datahub recipe test-connection --recipe my_recipe.yml
```

## Error Handling

Errors are reported with type and message:
- **Config errors**: Fix the recipe YAML and re-validate.
- **Connection errors**: Verify credentials, network, and connection parameters; check `test-connection` output.
- **Probe unsupported**: The source type doesn't support live probing; use `describe` + `test-connection`.
