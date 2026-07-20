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
   Returns schema/table/column names and counts (names only, no row data). Each result includes a `pattern_field` indicating which `*_pattern` config filter to edit to refine discovery.

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
- Live probes (schemas/tables/columns) work for SQL-family sources: any connector with `get_sql_alchemy_url()` in its config (Snowflake, Postgres, MySQL, BigQuery, Redshift, etc.).
- Other source types return `supported: false` → fall back to `test-connection` for verification.

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
