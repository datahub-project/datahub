# datahub recipe

The `recipe` commands help you author and verify an ingestion recipe before you run it. They
answer two kinds of question: _what does this source type accept?_ (offline, no connection)
and _what does this source actually contain, given my recipe?_ (live, needs working
credentials).

They are designed to be driven by an AI coding assistant as well as by hand — every command
prints JSON, and every failure uses a distinct exit code so a caller can tell "your input was
wrong" from "I could not reach the source".

## Offline commands

These need only a source type or a recipe file — no connection, no credentials.

```shell
# What configuration fields does this source accept, and what does the connector
# declare it can do? Reports each field's type, whether it is required or a secret,
# and for AllowDenyPattern fields which hierarchy level it filters.
datahub recipe describe snowflake

# Emit a starter recipe for a source type
datahub recipe scaffold snowflake

# Validate a recipe's configuration without connecting
datahub recipe validate my_recipe.yml
```

## Live commands

These connect using the recipe's own credentials. Secrets are resolved in-process and
redacted from all output.

```shell
# Verify the credentials work
datahub recipe test-connection --recipe my_recipe.yml
```

**Recipe variables** resolve exactly as `datahub ingest` resolves them: `${X}`, a `$X` that starts
the value, `${X:-default}` and `${X:=default}`; `$$` is kept as written. A dotted name such as
`${gms.server}` is not a path into `~/.datahubenv`: it reads as `${gms}` and becomes an empty
string, and `recipe validate` warns about it. Use `${DATAHUB_GMS_URL}` and `${DATAHUB_GMS_TOKEN}`.
A reference that takes part of a value (`${X:0:8}`, `${#X}`) is refused with exit code 2, since
only whole values can be masked in the output.

**Exploring what a source contains.** A source that can be probed lists `Probe` among its
capabilities in `datahub recipe describe <source_type>`, and in its connector docs. Start with
`probe methods`, which is connection-free and lists what this connector offers — each command's
parameters and what it returns:

```shell
datahub recipe probe methods --recipe my_recipe.yml

# Call one; a command's parameters imply the nesting
datahub recipe probe run columns --recipe my_recipe.yml --schema public --table orders
datahub recipe probe run topics --recipe my_recipe.yml --limit 50
```

Schema, table and view names are matched exactly as `containers`, `tables` and `views` list them;
a table argument also matches views and materialized views. Any other name, such as a foreign
table or one that differs only in case, is refused with exit code 2 before the database sees it.
When a listed name differs only in case, the error names it.

SQL sources expose a `sql` command for catalog queries, usually faster. It is an ordinary
command in the `probe methods` list — there is no separate subcommand to learn:

```shell
datahub recipe probe run sql --recipe my_recipe.yml --limit 50 \
  --query "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
```

Only a single `SELECT` over catalog metadata is permitted: `information_schema`, and beyond it
only the catalog relations the source names one by one. On Postgres that admits `pg_class` but not
`pg_stats`, `pg_statistic` or `pg_shadow`, which hold sampled column values or password hashes;
the refusal says what the source permits. Anything else — a user table, a second statement, a
vendor-specific function, a comment or an optimizer hint — is refused with exit code 2 before the
database sees it. A query the database rejects as the caller's mistake (SQLSTATE class 42, such
as a misspelled column) also exits 2. The check narrows what a query can reach; the credential
decides what each relation shows, so point the recipe at a read-only role.

**Checking what your filters would do.** `probe filter` judges names you already have, with no
connection:

```shell
datahub recipe probe filter --recipe my_recipe.yml \
  --kind Table --parent public --name orders --name users --name audit_log_v2

# Try a different pattern without editing the recipe
datahub recipe probe filter --recipe my_recipe.yml --kind Table --parent public \
  --name orders --name users --try-allow '^public\.ord.*'
```

Each result reports the `target` the pattern was matched against — which is usually the
qualified identifier, not the bare name. That matters: `AllowDenyPattern` is start-anchored, so
`^orders.*` matches nothing when ingestion evaluates `public.orders`. The output also names the
`pattern_field` that actually decided, which is not always the one named after the kind (MySQL
copies `table_pattern` into `view_pattern`).

The `--parent` containers are judged too: a table under a schema or database the recipe
excludes is reported excluded by that container's pattern, because ingestion never reaches it.
A `--name` or `--parent` that comes back masked (`***`, because it equals a secret) is refused
with exit code 2 rather than judged.

To judge a whole listing, write it with `probe run ... --report-to run.json` and pass
`--from-run run.json` instead of `--name`; its kind and parent are used unless you pass `--kind`
or `--parent`. The file must be a regular file of at most 50 MB.

`filtering` in the output says how the kind is filtered: `by_pattern` (`pattern_field` names the
field, as a dotted path when it sits in a nested block), `by_rule` (rules that are not a pattern,
such as `path_specs`, decide; `--try-allow`/`--try-deny` do not apply), `unfiltered`, or
`unresolved` (usually a kind this source does not have).

Probe output is **metadata only** — names, types, constraints, DDL, counts. No table rows,
no column values, no message payloads.

## Exit codes

| Code | Meaning                                                                                                   |
| ---- | --------------------------------------------------------------------------------------------------------- |
| 0    | success                                                                                                   |
| 1    | a defect in DataHub or the connector, or a missing Python package (named in the error)                    |
| 2    | invalid input — a bad recipe or URL, an unknown command, a name the source does not list, a refused query |
| 3    | could not connect to the source, the source returned an error, or a read was recorded as failed           |

Errors are a JSON line on stderr, `{"error": "..."}`. An error raised by a driver, an SDK or the
connector is shown by label only, its class and at most one code, because its text is where
connection strings and query literals leak from: `'tables' failed (ProgrammingError; SQLSTATE
42P01)`. To see the full text while debugging locally, rerun with `DATAHUB_PROBE_VERBOSE_LOGS=1`;
this also turns log scrubbing off, so do not use it where the output is shared or read by an agent.
stderr also carries the connector's log lines at INFO and above, scrubbed of the recipe's secrets
and of credential shapes, with tracebacks dropped.

When a source cannot be opened and the driver's exception chain holds a standard network error,
the label is followed by a reason and a fixed hint, for example `opening source 'mysql' failed
(OperationalError; errno 2003): ConnectionRefused - nothing is listening at the recipe's host and
port; ...`. The reason is read from the exception's type, never its text, and the exit code is
still 3:

| Reason              | Meaning and fix                                                                               |
| ------------------- | --------------------------------------------------------------------------------------------- |
| `HostNotResolved`   | The host name did not resolve. Check its spelling, and the DNS or VPN in use.                 |
| `ConnectionRefused` | Nothing listens at the host and port. Check the port, and that the server is running.         |
| `Timeout`           | The host did not answer. A firewall, allowlist or private network is likely dropping traffic. |
| `HostUnreachable`   | There is no network route to the host from this machine.                                      |
| `TlsVerifyFailed`   | The server's TLS certificate failed verification. Check the recipe's CA or TLS settings.      |

Drivers built on libpq (Postgres and its relatives) report no reason, since libpq is C and leaves no
Python network error to read.

A non-empty `warnings` list alongside an empty or partial result means _part of the source
could not be read_ — not that the source is empty. Treat the two differently.
