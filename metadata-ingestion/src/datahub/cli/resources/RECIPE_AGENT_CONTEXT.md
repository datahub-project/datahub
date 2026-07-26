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

5. **Probe** — Discover the shape, then enumerate live metadata

   First, discover what levels this source declares — connection-free, works for
   every probe-capable source (SQL databases, Kafka, Snowflake, BigQuery, …), not
   just SQL:
   ```bash
   datahub recipe probe shape --recipe <recipe.yml>
   ```
   Most sources are **linear** — one level nests inside the next, e.g. a 3-level
   SQL source:
   ```json
   {
     "source_type": "snowflake",
     "supported": true,
     "linear": true,
     "hierarchy": ["Database", "Schema", "Table", "Column"],
     "shape": {
       "kind": "Database",
       "children": [
         {
           "kind": "Schema",
           "children": [{ "kind": "Table", "children": [{ "kind": "Column", "children": [] }] }]
         }
       ]
     }
   }
   ```
   A **branching** source (its levels form a tree, not a chain — e.g. a BI
   workspace holding both reports and dashboards) reports `"linear": false` and
   `"hierarchy": null`; read `shape` instead, whose `children` list the sibling
   levels directly under a node:
   ```json
   {
     "source_type": "bi-thing",
     "supported": true,
     "linear": false,
     "hierarchy": null,
     "shape": {
       "kind": "Workspace",
       "children": [
         { "kind": "Report", "children": [] },
         { "kind": "Dashboard", "children": [] }
       ]
     }
   }
   ```
   `"supported": false` (with `shape`/`hierarchy` both `null`) means this source
   has no live-probe support at all; fall back to `test-connection`.

   Then list the top level, and descend one `--parent` per level, in the order
   `shape` reported:
   ```bash
   datahub recipe probe list --recipe <recipe.yml>
   datahub recipe probe list --recipe <recipe.yml> --parent <schema_name>
   datahub recipe probe list --recipe <recipe.yml> --parent <schema_name> --parent <table_name>
   ```
   Returns names and counts only, never row data. Each node reports:
   - `pattern_field` — which `*_pattern` config filter governs it (edit to refine discovery).
   - `included` — whether it would actually be ingested given the recipe's filters **and** the source's built-in exclusions (reused from the connector's own ingestion logic, not re-implemented).
   - `excluded_by` — why a node is dropped: a `*_pattern` field (your filter), `"default_schema"` (system catalog the source always skips, e.g. `information_schema`), or `"system_object"`; `null` when included. One value, `"unnamed"`, means something different from the rest: the source returned a node with no usable name, so the probe couldn't filter or address it at all — this is **not** a prediction that ingestion will skip it, just that the probe can't tell you either way.

   Every node is shown (nothing is hidden), so you can confirm both that your allow/deny filters behave and that system objects are auto-dropped.

   The top-level response also carries a `warnings` list, alongside `nodes`/`truncated`. A non-empty `warnings` means part of the listing couldn't be read cleanly (e.g. one sub-resource returned a permission error) and was shown as empty rather than failing the whole call — treat that differently from a `nodes` list that's empty with no warnings, which means "confirmed empty."

   **Branching sources and ambiguous siblings:** when a `shape` node has more than
   one child kind, a bare name is ambiguous — qualify it as `Kind:name`. For the
   branching example above, to list what's inside a report named `my_report`
   (rather than a dashboard of the same name):
   ```bash
   datahub recipe probe list --recipe <recipe.yml> --parent 'Report:my_report'
   ```

6. **Probe Methods** — call a metadata getter

   `probe shape`/`probe list` tell you what containers exist. `probe methods`/
   `probe run` are the other half: point getters that return one specific
   piece of structural metadata (columns, DDL, constraints, topic config,
   report SQL, ...) for a container you already found by walking the
   hierarchy, or already know the name of.

   First, discover what this source offers — connection-free, like `probe shape`:
   ```bash
   datahub recipe probe methods --recipe <recipe.yml>
   ```
   Each entry lists a `command`, its `params` (name, type, required, default),
   and a `description`. **The description is the getter's own docstring** —
   the full help text a human or agent reads to decide which method to call
   and how — so it is not duplicated here: call `probe methods` against your
   actual source rather than assuming a getter's parameters or behavior from
   this file, since docstrings can change independently of this doc.

   Then call one:
   ```bash
   datahub recipe probe run <command> --recipe <recipe.yml> [--param value ...]
   ```
   One example per family:
   ```bash
   # SQL-family (Postgres, MySQL, Redshift, Snowflake, BigQuery, Unity Catalog):
   # columns of a table found via `probe list`
   datahub recipe probe run columns --recipe <recipe.yml> --schema my_schema --table my_table

   # Kafka: broker-side config for one topic
   datahub recipe probe run topic_config --recipe <recipe.yml> --topic my_topic

   # Mode: the queries inside one report (name comes from `probe list`; no --parent needed)
   datahub recipe probe run report_queries --recipe <recipe.yml> --report "My Report"
   ```

   **Exit codes** — branch on these, not on message text:
   - `0` — the getter ran and produced a result. Still check `warnings` (below) before treating an empty/partial result as "confirmed empty."
   - `1` — internal error (a CLI bug); report it rather than retrying with different arguments.
   - `2` — your input was wrong: an unknown command, a missing/mistyped `--param`, or a name that couldn't be resolved — misspelled, out of scope, or **ambiguous** (the same name resolves to more than one object, e.g. two Mode reports of that name in different spaces). Fix the input and retry.
   - `3` — the getter's own connection/backend call failed (auth, timeout, a 5xx from the source, or a name search that couldn't complete because a sub-request failed) — not your input's fault; check credentials/connectivity, not spelling.

   **`warnings`:** both `probe list` and `probe run` output carry a `warnings`
   list. A non-empty `warnings` alongside an empty or partial `nodes`/`result`
   means *a sub-fetch could not be read cleanly* — a 403 on one endpoint, an
   object deleted between listing and fetch — **not** that the thing you
   asked about doesn't exist. Only an empty result with an empty `warnings`
   list means "confirmed empty"; don't conflate the two.

   A getter that needs to resolve a name (e.g. Mode's `report` parameter)
   **raises** rather than returning `[]` for a name it cannot resolve — exit
   `2` for not-found/ambiguous, exit `3` if the search itself couldn't
   complete. So `[]` from a getter always means the object was found and is
   genuinely empty, never "I couldn't find it."

   Like every probe command, this returns **metadata only** — names, types,
   DDL, constraints, counts, SQL text — never table rows, cell values, or
   message payloads.

7. **Test Connection** — Verify connectivity
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
- `probe shape` + `probe list` are source-agnostic — they follow whatever levels a connector declares, not a fixed SQL-shaped set of flags. Live probes work for SQL-family sources (any connector with `get_sql_alchemy_url()` in its config: Postgres, MySQL, Redshift, Snowflake, BigQuery, Unity Catalog, etc.) and for non-SQL sources that reuse their own client (not raw HTTP): **Kafka** lists Topics, filtered by `topic_patterns`. **Mode** lists Spaces, which branch into Reports (holding Queries) and Datasets, filtered by `space_pattern`/`report_pattern` — a branching source like Mode reports `"linear": false` from `probe shape`, and descending into a Space where a Report and a Dataset share a name needs a qualified `--parent 'Report:name'` (see the branching example above). Any connector can opt in by implementing `probe_hierarchy()` + `list_probe_children()`.
- Other source types return `supported: false` on `probe shape` → fall back to `test-connection` for verification.

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

# Discover the levels this source declares
datahub recipe probe shape --recipe my_recipe.yml

# Probe the top level (e.g. Snowflake's databases)
datahub recipe probe list --recipe my_recipe.yml

# Descend one --parent per level, in the order `shape` reported
datahub recipe probe list --recipe my_recipe.yml --parent my_database --parent my_schema

# Discover the metadata getters this source offers
datahub recipe probe methods --recipe my_recipe.yml

# Call one against a table found via probe list
datahub recipe probe run columns --recipe my_recipe.yml --schema my_schema --table my_table

# Test the connection
datahub recipe test-connection --recipe my_recipe.yml
```

## Error Handling

Errors are reported with type and message:
- **Config errors**: Fix the recipe YAML and re-validate.
- **Connection errors**: Verify credentials, network, and connection parameters; check `test-connection` output.
- **Probe unsupported**: The source type doesn't support live probing; use `describe` + `test-connection`.
