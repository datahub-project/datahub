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

**Redaction can over-mask.** Any string equal to a secret is blanked, so if a password
happens to match a real identifier (a database also named `datahub`), that identifier reports
as `***` everywhere. If a name comes back masked, that is why; it is not a probe failure.

## Workflow

1. **Describe** — config fields and capabilities, no connection

   ```bash
   datahub recipe describe <source_type>
   ```

2. **Scaffold** — a starter recipe with secrets as `${ENV_VAR}` references

   ```bash
   datahub recipe scaffold <source_type> > recipe.yml
   ```

3. **Edit** — modify the recipe locally, keeping secrets as `${ENV_VAR}`.

4. **Validate** — schema check, plus warnings for inline secrets

   ```bash
   datahub recipe validate recipe.yml
   ```

5. **Explore** — see what is actually in the source (below).

6. **Check filters** — see what your patterns would keep (below).

7. **Test connection**

   ```bash
   datahub recipe test-connection --recipe recipe.yml
   ```

## Exploring a source

Two ways in, depending on the connector. Always start with `probe methods`, which is
connection-free and tells you what this source offers:

```bash
datahub recipe probe methods --recipe recipe.yml
```

Each entry carries a `command`, its `params`, and a `description` taken from the method's own
docstring. Parameters imply the nesting: a `tables(schema)` command sits under whatever
lists schemas, and `columns(schema, table)` under that. Call one with:

```bash
datahub recipe probe run columns --recipe recipe.yml --schema public --table orders
```

**SQL sources expose a `sql` command** for catalog queries, usually the faster route. It is
an ordinary command in the list above — nothing special to remember:

```bash
datahub recipe probe run sql --recipe recipe.yml --limit 50 \
  --query "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
```

Only **single SELECT statements over catalog schemas** are permitted — the framework checks
the query before the connector sees it —
`information_schema`, plus `pg_catalog` on Postgres-likes. Anything else is refused before
the database sees it, with exit code 2 and a message naming the reference that failed:

```json
{
  "error": "'public.orders' is outside the catalog metadata this probe may read; permitted schemas: information_schema"
}
```

Refusals you should expect, and not try to work around: user tables, unqualified table names,
multiple statements, non-SELECT statements, and vendor-specific functions (`pg_read_file`,
`dblink`, `load_file`). BigQuery addresses catalog views as
`<dataset>.INFORMATION_SCHEMA.TABLES`, which is understood.

Results come back as `columns` plus positional `rows`, with `truncated` telling you whether
more exist beyond `--limit`. `--limit` is clamped to 1000 however large a value you pass, so
narrow the query rather than raising the limit when a listing comes back truncated.

**Some sources expose an `api` command**, a read passthrough to their own API, for questions
no typed command answers:

```bash
datahub recipe probe run api --recipe recipe.yml --path /spaces/sp1/reports
```

Only GET, and only paths the connector lists; anything else exits 2. Prefer `probe run` where a
getter exists — it returns the names patterns are matched against, whereas a raw record leaves
you guessing which field that is.

Both passthroughs can be switched off for a deployment
(`DATAHUB_PROBE_DISABLE_RAW_ACCESS`). If you see that named in a refusal, the typed commands are
still available — use those and do not look for another route to raw access.

## Which levels a source has, and in what order

`describe` tells you which config fields gate a hierarchy level. Read `filters` on each pattern
field — a level-bearing pattern reports the kind it filters, and one that filters something
outside the hierarchy reports `null`:

```bash
datahub recipe describe postgres
```

```
database_pattern   filters: "Database"
schema_pattern     filters: "Schema"
table_pattern      filters: "Table"
view_pattern       filters: "View"
procedure_pattern  filters: null      <- a real filter, not a level
profile_pattern    filters: null
user_email_pattern filters: null
```

This matters more than it looks: Snowflake has fifteen pattern fields and only four are levels.
Guessing from the name would have you editing `procedure_pattern` to fix a missing table.

**Containment order is not machine-readable** — `Database` and `Schema` as names do not say which
contains which. Use this table, and note that a source's levels are a subset of it:

| family                       | order                                       |
| ---------------------------- | ------------------------------------------- |
| Postgres, MSSQL, Snowflake   | Database → Schema → Table/View → Column     |
| BigQuery                     | Project → Dataset → Table/View → Column     |
| MySQL and other two-tier SQL | Database → Table/View → Column              |
| Redshift, most other SQL     | Schema → Table/View → Column                |
| Kafka                        | Topic                                       |
| Mode                         | Space → Report → Query, and Space → Dataset |

**You rarely need the order to diagnose something.** Each `probe filter --kind X` answer is
correct on its own, so to find out why an object was skipped you can check its own kind first and
then walk outwards — table, then schema, then database — stopping at the first `included: false`.
Order matters only if you want to enumerate a source top-down.

## Checking what your filters would do

This is the step most worth not skipping, because **you cannot work it out yourself from the
names alone.**

```bash
datahub recipe probe filter --recipe recipe.yml \
  --kind Table --parent public --names orders,users,audit_log_v2
```

`AllowDenyPattern` matches with a **start-anchored** regex against the identifier _ingestion_
uses — which is usually **not** the bare name. MySQL matches `schema.table`, Postgres
`db.schema.table`, Druid the bare name. So a pattern like `^orders.*` looks precise and
matches **nothing**, because the string being tested is `public.orders`.

Measured against a real MySQL instance, four of six plausible patterns gave the wrong answer
when reasoned about from bare names. One `allow: ^INNODB.*` suggested 31 tables would be kept;
ingestion kept **0**. Patterns starting with `.*` happen to agree; anchored ones do not.

Every result reports the `target` it was matched against — read it, because that is what
explains a surprising verdict:

```json
{
  "pattern_field": "table_pattern",
  "results": [
    {
      "name": "audit_log_v2",
      "target": "public.audit_log_v2",
      "included": false,
      "excluded_by": "table_pattern"
    }
  ]
}
```

`pattern_field` names the field that actually decided, which is not always the one named after
the kind — MySQL copies `table_pattern` into `view_pattern`, so a view is decided by the
latter. Edit the field the output names.

**Test a pattern before committing to it** with `--try-allow` / `--try-deny`, which judge
against a hypothetical instead of the recipe's own:

```bash
datahub recipe probe filter --recipe recipe.yml --kind Table --parent public \
  --names orders,users --try-allow '^public\.ord.*'
```

`probe filter` needs no connection: it judges names you already have.

## Boundary Rules

- Probe output is **metadata only** — names, types, DDL, constraints, counts. Never table
  rows or message payloads. If you need row data, you are outside this tool's purpose.
- The scope check on the `sql` command narrows what a query can reach; it is **not** a security
  boundary. Recommend the recipe authenticate as a read-only role scoped to catalog metadata.
- An empty result **with** warnings means part of it could not be read — never "this is
  empty". Read `warnings` before concluding a source is empty.

## Error Handling

Branch on exit codes, not on message text:

| code | meaning                                                         |
| ---- | --------------------------------------------------------------- |
| 0    | success                                                         |
| 1    | internal error                                                  |
| 2    | your input was wrong — bad recipe, bad parameter, refused query |
| 3    | the source could not be reached                                 |

## Common Recipes

```bash
# Inspect a connector's config fields
datahub recipe describe snowflake

# Generate a starter recipe, then edit it (keep secrets as ${ENV_VAR})
datahub recipe scaffold snowflake > recipe.yml
datahub recipe validate recipe.yml

# See what this source offers
datahub recipe probe methods --recipe recipe.yml

# Explore: catalog query (SQL sources) or a typed command
datahub recipe probe run sql --recipe recipe.yml \
  --query "SELECT schema_name FROM information_schema.schemata"
datahub recipe probe run columns --recipe recipe.yml --schema public --table orders

# Would my filters keep these?
datahub recipe probe filter --recipe recipe.yml --kind Table --parent public --names orders,users

# Verify connectivity
datahub recipe test-connection --recipe recipe.yml
```
