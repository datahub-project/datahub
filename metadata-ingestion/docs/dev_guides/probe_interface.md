# Adding probe support to a source

`datahub recipe probe` lets a person or an AI coding assistant ask a live source _"what is in you,
and what would my recipe pick up?"_ before running an ingestion. This guide covers what a connector
author writes to support it, and the rules that keep it safe.

## Purpose and shape

A probe answers two questions, kept apart on purpose:

|                                | command        | connects? |
| ------------------------------ | -------------- | --------- |
| **fetch**: what is in here     | `probe run`    | yes       |
| **judge**: what gets picked up | `probe filter` | **no**    |

`probe run <command>` calls one of the connector's probe methods. `probe filter` judges names the
caller already has against the recipe's filters, the way ingestion would, so a caller can try many
patterns (`--try-allow`, `--try-deny`) against one listing without touching the source again.
`probe filter --from-run r.json` judges a listing that `probe run --report-to r.json` wrote, and hands
the connector each record's scalar fields as `ctx.attributes` (for a source that filters on an id).
Each record must be a name string or a mapping with a string `name` key; any other record makes the
listing unjudgeable (exit 2), so a listing method returning records names each one `name`.
`probe methods` lists the commands, connection-free: a method's parameters are its CLI flags and its
docstring is its help text. `recipe describe`, `recipe scaffold` and `recipe validate` need no
connection either.

Every command prints JSON, and the exit code says who has to act: **2** the caller's input is wrong,
**3** the source could not be reached or read, **1** a defect in DataHub or the connector.

## The minimal provider

**A SQLAlchemy-family source gets a provider without writing one.** A config inheriting
`SQLCommonConfig` gets `SqlAlchemyMetadataProbe` and its ten commands, verdict hooks judging tables
and views on the connector's own `get_identifier`, and an engine set up for the wire protocol its
URL names; see [The SQL family](#the-sql-family). It declares a hook only where its ingestion
differs from those defaults: a `get_identifier` the probe cannot call without ingestion's state
(`probe_filter_target`), a catalog beyond `information_schema` (`probe_catalog_scope`), engine setup
ingestion does that `create_engine()` does not (`probe_engine_settings`), system schemas it drops
whatever the pattern says (`default_schemas`), and the rest of the
[SQL-family hooks](#sql-family-hooks). Check with `datahub recipe probe methods --recipe r.yml`, and
check the verdicts against ingestion with a parity test.

Anything else needs one config hook and one provider class:

```python
# mysource/config.py
from typing import Annotated

from pydantic import Field

# Not datahub.api.entities.forms.forms.Filters, which is another class.
from datahub.configuration.common import AllowDenyPattern, ConfigModel, Filters
from datahub.ingestion.source.common.subtypes import DatasetSubTypes


class MySourceConfig(ConfigModel):
    host: str
    table_pattern: Annotated[AllowDenyPattern, Filters(DatasetSubTypes.TABLE)] = Field(
        default=AllowDenyPattern.allow_all(), description="Tables to ingest."
    )

    @classmethod
    def probe_provider_class(cls) -> type:
        # Imported lazily, so ingestion never loads the probe module.
        from datahub.ingestion.source.mysource.mysource_probe import MyMetadataProbe

        return MyMetadataProbe


# mysource/mysource_probe.py
from typing import List

from datahub.ingestion.agent.probe_methods import probe_method
from datahub.ingestion.agent.provider_helpers import ProbeProviderBase, take
from datahub.ingestion.source.common.subtypes import DatasetSubTypes
from datahub.ingestion.source.mysource.client import MyClient
from datahub.ingestion.source.mysource.config import MySourceConfig


class MyMetadataProbe(ProbeProviderBase):
    def __init__(self, config: MySourceConfig) -> None:
        self._config = config

    @classmethod
    def for_config(cls, config: MySourceConfig) -> "MyMetadataProbe":
        return cls(config)

    def _client(self) -> MyClient:
        return self._open_once(
            "client", lambda: MyClient(self._config.host), close=MyClient.close
        )

    @probe_method(kind=DatasetSubTypes.TABLE, row_limit_param="limit")
    def tables(self, limit: int = 200) -> List[str]:
        """Tables this source exposes, including ones table_pattern would
        exclude: a denied table is reported, not hidden. Metadata only."""
        return take(self._client().iter_tables(), limit)
```

That is a complete probe: `probe methods` lists `tables`, `probe run tables` returns names with
`kind: Table` and `truncated`, and `probe filter --kind Table` judges them with `table_pattern`.
`MyClient` stands for the connector's own client. Put the provider in its own `<connector>_probe.py`
module and delegate to the connector's fetchers. Declare commands on the provider, never on the
`Source`: a declaration error raises at import, which would break ingestion too.

`probe_provider_class()` is the config's only statement about its provider: `probe methods` describes
that class and `probe run` builds it with `for_config`, so the two cannot disagree. Build clients in a
command (`_open_once`), not in `for_config`, so a bad credential is that command's failure. The SQL
family is the exception: `SqlAlchemyMetadataProbe.__init__` builds the Inspector, which connects,
so there a bad credential reads `opening source '<type>' failed (<label>)`, still exit 3. Never
construct the `Source`: its `__init__` opens connections and emits telemetry. A provider that needs
connector methods builds an uninitialised instance with `__new__` and primes only what they touch.

`@probe_method` options:

| Option              | Effect                                                                                              |
| ------------------- | --------------------------------------------------------------------------------------------------- |
| `name`              | the command name; defaults to the method name                                                       |
| `kind`              | the DataHub subtype of the returned names, so `probe filter` picks the right field; omit for `sql`  |
| `row_limit_param`   | the parameter bounding the result: clamped to `1..MAX_PROBE_ITEMS`, and truncation is reported      |
| `parent_params`     | the parameters naming the container; echoed as `parent_path`, so the caller needs no `--parent`     |
| `scoped_sql_param`  | the parameter carrying SQL, scope-checked first (see [Gated commands](#gated-commands-sql-and-api)) |
| `scoped_path_param` | the parameter carrying an API path, allowlist-checked first                                         |
| `shapes_own_result` | the method returns its own envelope and truncation flag (`sql`)                                     |

Parameters are `str`, `int` or `bool` (or `Optional` of those), and the docstring is required. A
declaration naming a parameter that does not exist raises at decoration time.

**Kind strings.** `kind=` is the subtype ingestion gives those entities: a `DatasetSubTypes` constant
for datasets, a `DatasetContainerSubTypes` or `BIContainerSubTypes` constant for containers
(`datahub.ingestion.source.common.subtypes`), or the literal ingestion uses where none exists
(Mode's `"Space"`). `Filters(kind)` on the pattern field must be the same string, since it is
compared exactly; only the caller's `--kind` may differ in case.

**Limits.** A method declaring `row_limit_param` receives one more than the caller's limit, so
truncation shows. Return up to what it receives (`take(items, limit)` does); the framework drops
the extra item and reports `truncated`.

The framework reads these provider attributes by name. `ProbeProviderBase` declares each with a
default that reads as absent, and `test_probe_contract.py` refuses a near-miss name (`probe_reports`):

| Attribute                       | Declare it when                                                                       | How                                                                                       |
| ------------------------------- | ------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| `warnings`                      | a listing degrades rather than fails                                                  | inherited: add with `self._warn(message)`                                                 |
| `failures`                      | a read can fail without raising; any entry makes the result incomplete (exit 3)       | `self.failures = []` in `__init__`, never a class-level list (the base's default is `()`) |
| `probe_report`                  | you reuse ingestion fetchers: their `SourceReport` warnings and failures are read off | a `@property` returning the report; the base's is a read-only property                    |
| `silenced_loggers`              | reused code logs source values that carry no credential shape (see the rules)         | a class attribute: a tuple of logger names                                                |
| `probe_error_code(exc)`         | your vendor's errors carry a code the generic readers miss (see the rules)            | a `@staticmethod`                                                                         |
| `sql_dialect`, `catalog_scope`  | a method declares `scoped_sql_param`                                                  | a class attribute, or set on the instance in `for_config`                                 |
| `api_allowlist`, `api_base_url` | a method declares `scoped_path_param`                                                 | a class attribute, or set on the instance in `for_config`                                 |

An attribute that raises when read is reported as the provider's defect (exit 1), by class and name.

## The rules

1. **Metadata only.** Names, types, constraints, DDL, counts. Never rows, cell values or payloads.
2. **Raise `ProbeArgumentError` to show a message.** Exception text is shown by type only:
   `ProbeArgumentError` and `ProbeSoftError` (exit 2), `ProbeConnectionError` and `ProbeReadFailed`
   (exit 3), `ProbeInternalError` (exit 1), and the gates' refusals (exit 2). Anything else, your own
   plain `ValueError` included, is reported as a label, its class and at most one short code
   (`'tables' failed (ProgrammingError; SQLSTATE 42P01)`), on the exit code that
   [Exit codes by phase](#exit-codes-by-phase) gives.
3. **Never interpolate an exception you did not raise.**
   `ProbeConnectionError(f"login failed: {exc}")` would carry a driver's text out under a trusted
   type. Name the operation and the class.
   As a backstop, a foreign exception's verbatim `str` or `repr` in your message is replaced by its
   label; text rebuilt from parts of it (`e.doc`) is not caught. A lookup error naming a value the
   caller passed this call is the caller's own text, so
   `except KeyError: raise ProbeArgumentError(f"no thing named '{name}'")` keeps the name. That
   exemption covers failures `run_probe_method` handles: a `soft_listing` warning withholds a quoted
   lookup error even when it names the caller's argument.
4. **Codes, not text.** The framework reads an HTTP status (`.response.status_code`, `.status_code`,
   `.status`), a SQLSTATE (`.sqlstate`) and an errno (`.errno`) along the `raise ... from` chain. For a
   vendor shape, declare a staticmethod `probe_error_code(exc) -> Optional[str]` on the provider. It is
   asked first, also while the provider is being built, and its answer is shown only if it is a name
   with at most one dot plus an optional short token holding a digit (`AccessDenied`,
   `SQLSTATE 42P01`).
5. **Degrade with `soft_listing`, and say so.** A 403 or 404 on one sub-listing becomes a warning and
   your fallback; auth failures and 5xx still fail the command. An empty result must never stand in
   for "could not look": record unreadable reads in `failures` (exit 3).
6. **Withhold personal records.** Owner names, emails and personal workspaces that ingestion would
   not emit are left out with a count (`PersonalWithholding`), or masked (`mask_identity_columns`).
7. **Logs are scrubbed, so leave them alone.** While a probe or `test-connection` runs, every log
   record not from the framework's own loggers is scrubbed of secrets and credential shapes, its
   traceback dropped, including `warnings.warn` and loggers created mid-probe (mechanics and limits:
   `agent/log_guard.py`). Scrubbing works by shape, so list loggers that print shape-free source
   values (a connector config, a response body) in `silenced_loggers`, a tuple or list of logger
   names: they, and children inheriting their level, are dropped while `probe run` runs
   (`test-connection` builds no provider, so there they are only scrubbed). A framework logger
   (`datahub.ingestion.agent`, `datahub.cli.recipe_cli` and `datahub.masking`:
   `log_guard.FRAMEWORK_LOGGERS`), a logger under one, an ancestor of one, or root is refused as the
   provider's defect (exit 1). Every other logger is scrubbed, other `datahub.cli` modules and
   telemetry included. Never call `setLevel` yourself, and never log responses, URLs or exception
   text. The guard is process-wide and on by default in `run_probe_method`. An embedder that masks
   its own logs and needs its other threads' tracebacks passes `guard_logs=False`.
8. **`DATAHUB_PROBE_VERBOSE_LOGS=1` is for local debugging only.** It turns the log guard off and puts
   each withheld exception's text after its label, scrubbed of credential shapes (the CLI masks the
   recipe's secrets on top), for `probe run` and for a crashed `test-connection` alike:
   `'tables' failed (HTTPError; HTTP 403): 403 Client Error: Forbidden for url: https://***@host/api`.
   A crashed `test-connection` is labelled, on the exit code `probe run` gives the same exception
   (a `SystemExit` included, whatever its status); a failed one, which returns a report, prints the
   source's own `failure_reason` text, scrubbed of the recipe's secrets and credential shapes, with
   or without the switch.
9. **Exit codes are a contract.** 2 for the caller's input, 3 for the source, 1 for a defect. Test
   every code your provider can produce.

### Exit codes by phase

What a provider raises becomes this exit code and message (`classify_foreign` in
`agent/error_policy.py`, and the open, call and close path in `agent/probe_methods.py`). `<label>` is
the class and at most one short code.

| Raised                                                                                                                                    | Opening or closing the provider               | During a command                                                 | After the provider recorded failures                        |
| ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------- |
| a trusted type above, or a gate's refusal                                                                                                 | its own code and message                      | its own code and message                                         | 3, its message, then `; the connector recorded: <failures>` |
| a Python defect: `TypeError`, `KeyError`, `AttributeError`, `AssertionError`, `IndexError`, `NameError`                                   | 3, `opening source '<type>' failed (<label>)` | 1, `'<command>' failed (<label>)`                                | 3, `<label>; the connector recorded: <failures>`            |
| a failure reading what the source sent: `OSError`, `UnicodeError`, `binascii.Error`, `json.JSONDecodeError`, a pydantic `ValidationError` | 3, as above                                   | 3, as above                                                      | 3, as above                                                 |
| the rest of the `ValueError` family, and `re.error`                                                                                       | 3, as above                                   | 2, as above                                                      | 3, as above                                                 |
| `NotImplementedError`                                                                                                                     | 3, as above                                   | 2, `source '<type>' does not support the '<command>' command...` | 2, as during a command                                      |
| an `ImportError` naming a module (a missing driver)                                                                                       | 1, as above, naming the module                | 3, as above                                                      | 3, as above                                                 |
| anything else: a driver's, an SDK's or an HTTP error, `SystemExit`                                                                        | 3, as above                                   | 3, as above                                                      | 3, as above                                                 |

A gated `sql` command failing with SQLSTATE class 42 exits 2: the caller wrote that query. A
SQLAlchemy URL naming no installed dialect, or no URL at all, is refused while opening (exit 2).

Closing reads `closing source '<type>' failed (<label>)`, and only when the command succeeded: a
command's own failure is never replaced by its close's. A command that returns with failures
recorded prints its result, then exits 3 with `'<command>' could not be completed: <failures>`. A
hook from the [hook reference](#hook-reference) or a provider attribute raising anything untrusted
is the connector's defect (exit 1), named by class and label.

## Secrets

The CLI collects every secret a recipe holds before a command prints anything, and masks them on the
way out; a connector's part is to type each credential field `SecretStr`. Secrets are found
(`_resolved_recipe` in `cli/recipe_cli.py`; `_secrets_in_recipe`, best-effort, for `validate`) as:

- every value a variable reference resolves to, a host included (`agent/secrets.resolve_config_collecting`,
  which resolves `${REF}`, a leading `$REF` and `${REF:-default}` exactly as `datahub ingest` does; an
  inline default is recipe text, not a secret);
- every `SecretStr` field's value at any depth, as written and as the source validates the config,
  which adds a renamed field's value or a file a validator read (`introspect.secret_field_values`);
- strings under credential-looking keys in free-form dicts such as Kafka's `consumer_config`
  (`redact.collect_nested_secret_values`);
- every value in a stdin envelope's `__secrets__`, registered before the YAML is parsed.

Four barriers keep them out of the output:

- **Payload redaction.** Collected values become `***`, and free text is scrubbed of credential
  shapes (`redact.redact`, `redact.scrub_text`); `probe filter` and `probe run` warn when that
  changed the result. A secret equal to an identifier masks that identifier too, so `probe filter`
  refuses (exit 2) a `--parent` or `--name` holding `***` rather than judge it.
- **The masking registry.** `SecretRegistry`, whose stdout wrapper, logging filter and excepthook
  the `recipe` group installs, masks stdout and `--report-to` as a structure.
- **Error labels.** A foreign exception is named by its label, never its text, and each error line
  is scrubbed like the payload.
- **The log guard.** Other code's log records are scrubbed while a probe or `test-connection` runs
  (rule 7).

## Helpers

These hold what providers would otherwise copy. None is a hook: the framework never looks them up.
`ProbeProviderBase`, `resolve_name`, `take`, `PersonalWithholding`, `soft_listing` and `echoed` are
in `datahub.ingestion.agent.provider_helpers`; `mask_identity_columns` in
`datahub.ingestion.agent.redact`; `pattern_verdict`, `Verdict`, `parent_required`, `ancestors_in`
and the `ProbeArgumentError` family in `datahub.ingestion.agent.verdicts`; `Filters`,
`FiltersByRule`, `Enables` and `Qualifier` in `datahub.configuration.common`.

| Helper                                                   | Use it for                                                                                                                   |
| -------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `ProbeProviderBase`                                      | `warnings`/`_warn`, `_open_once(key, opener, close=...)`, `_on_exit(close)`, and an `__exit__` closing all, last first       |
| `resolve_name(arg, records, *, key, kind, ...)`          | a caller's name (or id, with `id_key`) to the listed record; exact, case-only misses hinted, ambiguity refused (exit 2)      |
| `take(items, limit, *, keep=None)`                       | any listing with a limit: stops paging at the limit and closes the source                                                    |
| `PersonalWithholding(is_personal=..., would_ingest=...)` | leaving out personal records: pass `.keep` to `take`, warn with `.count_text(stopped_early=...)`; `is_personal` fails closed |
| `soft_listing(self._warn, 403, 404, context=...)`        | one sub-listing that may degrade: `return fetch()` inside the block, the fallback after it                                   |
| `echoed(value)`                                          | a caller's argument or a listed name in a refusal: clipped and repr-quoted                                                   |
| `mask_identity_columns(columns, rows)`                   | masking identity columns (`user_name`, `email`, grantees) in a result you admit                                              |
| `pattern_verdict(config, field, target)`                 | the standard allow/deny check, for a `probe_verdict_override` that defers to a pattern                                       |
| `Verdict.include()` / `Verdict.exclude(field)`           | a verdict, naming the field or rule that excluded it                                                                         |
| `parent_required(ctx)`                                   | a `probe_match_target` that needs the container: warns and returns `True` when there is no `--parent`                        |
| `ancestors_in(chain, kind, leaves)`                      | implementing `probe_ancestor_kinds` from a container chain                                                                   |

```python
@probe_method(kind="Report", parent_params=("workspace",), row_limit_param="limit")
def reports(self, workspace: str, limit: int = 100) -> List[str]:
    """Reports in one workspace, by name. Metadata only."""
    ws = resolve_name(
        workspace,
        self._client().list_workspaces(),
        key=lambda w: w["name"],
        kind="workspace",
        list_command="probe run workspaces",
    ).record
    with soft_listing(self._warn, 403, 404, context=f"reports in {echoed(ws['name'])}"):
        return take((r["name"] for r in self._client().reports(ws["id"])), limit)
    return []
```

Pass `resolve_name` the records after withholding: its hint and ambiguity list print listed names.
Resolve outside your own error translator, so a refusal stays exit 2. Do not give `warnings` a
class-level list default in a subclass; `ProbeProviderBase` refuses one, since every instance would
share it.

## Gated commands: `sql` and `api`

A method taking SQL or an API path declares which parameter carries it, and the framework checks it
before the method runs, so a connector cannot forget a check it does not perform:

| Declaration         | What the framework does first                                                       |
| ------------------- | ----------------------------------------------------------------------------------- |
| `scoped_sql_param`  | `sql_gate`: one SELECT over the provider's `catalog_scope`, nothing else            |
| `scoped_path_param` | `api_gate`: GET only, a path on the connector's own host, listed in `api_allowlist` |
| `row_limit_param`   | clamps the value into `1..MAX_PROBE_ITEMS`                                          |

A provider missing `sql_dialect` or `api_allowlist` is refused as its defect (exit 1). A method that
takes a `query`, `path` or `limit` and declares nothing runs unchecked, so `test_probe_contract.py`
scans every registered connector for such parameters. The rules each gate enforces are in the module
docstrings of `agent/sql_gate.py` and `agent/api_gate.py`.

Both passthroughs below are `ProbeProviderBase` subclasses, so a provider inherits one of them
instead of `ProbeProviderBase` and keeps `_warn`, `_open_once` and the closing `__exit__`.

- **Not SQLAlchemy-backed but speaks SQL:** inherit `SqlCatalogPassthrough`, set `sql_dialect`, and
  implement `execute_catalog_query(query, limit) -> CatalogRows`. `limit` already includes the one
  extra row that detects truncation: return what was asked for, and never fetch everything to slice.
  `rows_from_mappings` shapes a dict-per-row driver.
- **Has a REST API:** inherit `RestApiPassthrough`, set `api_base_url` and `api_allowlist`, and set
  `api_session` (the connector's own, with its rate limiter and auth) or override `api_fetch_json`.
- **Writing the allowlist** is your judgement, and the gate enforces it as written. An endpoint in a
  metadata API can still return user data (a notebook cell's SQL), so leave it out. A `{placeholder}`
  matches any one segment, literal siblings included. Query parameters are listed per endpoint
  (`"GET /projects?include&limit"`), names only; an entry listing none refuses any.
- **The gates bound requests; the credential bounds data.** A query escaping `sql_gate` or a path
  escaping `api_gate` is a security bug. Give the probe a least-privilege, read-only credential.

`DATAHUB_PROBE_DISABLE_RAW_ACCESS=true` refuses every gated command (typed listings keep working), and
`DATAHUB_PROBE_DISABLED=true` refuses every `probe run` (`recipe test-connection` is not covered).
Both are environment variables, set where the probe runs, because the agent writes the recipe.

## Hook reference

Every hook the framework reads off a config, by name: `CONFIG_HOOKS` in `agent/probe_methods.py`. All
are optional except `probe_provider_class`. Copy the signature exactly: instance hooks are called
with keyword arguments. `test_probe_contract.py` checks the names in this table against
`CONFIG_HOOKS`, refuses a `probe_*` method the framework does not read, and checks the keyword
arguments. A hook is held to a provider call's rule: a trusted exception keeps its type and message,
and anything else it raises is the connector's defect (exit 1), reported by class, hook and label.

| Hook                       | Signature                                                       | Declare it when                                                                     |
| -------------------------- | --------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| `probe_provider_class`     | classmethod `() -> type`                                        | always: it names the provider class                                                 |
| `probe_validation_context` | classmethod `(source_type: str) -> Optional[Dict[str, object]]` | registered names share a config class but validate with different pydantic contexts |
| `probe_kind_overrides`     | classmethod `() -> Mapping[str, str]`                           | the config, not the provider, decides a command's kind (command → kind)             |
| `probe_match_target`       | `(self, ctx: ClassifyContext) -> Optional[str]`                 | a pattern matches something other than the bare name (step 2)                       |
| `probe_verdict_override`   | `(self, ctx: VerdictContext) -> Optional[Verdict]`              | no single pattern states ingestion's decision (step 4)                              |
| `probe_unfiltered_kinds`   | classmethod `() -> Set[str]`                                    | nothing filters a kind, on purpose                                                  |
| `probe_ancestor_kinds`     | `(self, kind: str) -> Optional[Sequence[str]]`                  | containers sit above a kind (step 6)                                                |

Four field annotations complete it: `Filters(kind)` on the pattern field that filters a kind,
`FiltersByRule(kind)` on a field whose rules decide a kind instead, `Enables(kind)` on the bool field
that switches a kind off, and `Qualifier()` on a field naming the container a qualified name starts
with. A new hook goes into `CONFIG_HOOKS` and this table in the same change (a SQL-family hook:
`SQL_FAMILY_HOOKS` in `source/sql/sql_config.py` and [its table](#sql-family-hooks)).

The probe checks a config class's markers the first time it reads one, and refuses a misdeclared
class as the connector's defect (exit 1), every problem listed. `Filters` must mark an
`AllowDenyPattern`, nested or not; `Enables` a top-level bool; `FiltersByRule` and `Qualifier` a
top-level field. A kind is marked on one field per marker, the container on one `Qualifier` field,
and a `FiltersByRule` kind is neither `Filters`-declared nor unfiltered and is judged by
`probe_verdict_override`. The rules live in `agent/declarations.py` (`marker_problems`), which the
contract test runs over every registered config.

## Making verdicts match ingestion

`probe filter` resolves a verdict in this order. Each step's default is right for most connectors,
so implement a hook only where the default answers differently from ingestion.

| Step | What it does                                                                     | Change it with                                                   |
| ---- | -------------------------------------------------------------------------------- | ---------------------------------------------------------------- |
| 1    | find the field that filters the kind                                             | `Filters(kind)`, `FiltersByRule(kind)`; `probe_unfiltered_kinds` |
| 2    | build the string the pattern is matched against                                  | `probe_match_target`; `None` keeps the bare name                 |
| 3    | a bool switch that turns the whole kind off                                      | `Enables(kind)`                                                  |
| 4    | the connector's own verdict, told step 3's                                       | `probe_verdict_override`                                         |
| 5    | match the pattern against the target, if steps 3 and 4 gave no verdict           | none                                                             |
| 6    | judge the immediate `--parent` the same way; inside an excluded container is out | `probe_ancestor_kinds`                                           |

**Step 1.** The field is chosen in this order: a top-level `FiltersByRule(kind)` field
(`filtering: "by_rule"`); else a kind in `probe_unfiltered_kinds` (`"unfiltered"`); else the
`Filters(kind)` field (`"by_pattern"`); else the name convention below. A `by_rule` kind is judged
on its bare name, so step 2 is skipped, and `--try-allow`/`--try-deny` are ignored with a warning.

Declare `Filters(kind)` on every pattern field, nested ones included
(`filter_config.entries.pattern` is reported under its dotted name, and `--try-*` reruns only its
own block's validators). The `<kind>_pattern` name convention is a fallback for out-of-tree
connectors, and the contract test refuses it in-tree. A kind nothing filters on purpose goes in
`probe_unfiltered_kinds`, so `probe filter` reports `filtering: "unfiltered"` rather than
`"unresolved"`, which is what a dropped annotation looks like. A subclass redeclaring a marked
field repeats the marker, since pydantic replaces the field's metadata; the contract test refuses a
dropped one.

A kind decided by rules that are not an `AllowDenyPattern` carries `FiltersByRule(kind)` on that
top-level field, whatever its type: a `path_specs` list, or a bool under which the kind follows from
what else is ingested. `probe filter` reports `filtering: "by_rule"`, and `probe_verdict_override`
must return a `Verdict` for every name of it, with `excluded_by` naming the sub-rule
(`path_specs[0].exclude`).

```python
path_specs: Annotated[List[PathSpec], FiltersByRule(DatasetSubTypes.TABLE)] = Field(
    description="Which paths are datasets, and how they are laid out."
)
```

**Step 2.** The target decides the verdict: where ingestion matches `schema.table`, judging the
bare name gives `^orders$` an answer ingestion never gives. Return the string ingestion matches,
built by ingestion's own code; never re-derive it. A target that needs the container calls
`parent_required(ctx)` first.

**Step 3.** Mark a bool field whose `False` stops ingestion emitting a kind at all with
`Enables(kind)`, one per kind it switches off. `True` must mean the kind is emitted, so a field whose
`True` means skip must not carry it; an `Optional[bool]` left unset reads as enabled. A flag deciding
what is emitted about an object (lineage, profiling) is not a switch.

```python
include_views: Annotated[bool, Enables(DatasetSubTypes.VIEW)] = Field(
    default=True, description="Whether views should be ingested."
)
```

**Step 4.** The escape hatch, for decisions no single pattern states: a view that must pass
`table_pattern` too, a pinned database, an id the pattern matches. `ctx.structural` is step 3's
verdict: return it, overrule it, or return `None` to let steps 3 and 5 decide. Re-check a pattern
with `pattern_verdict(self, field, ctx.target)`; under `--try-allow` the config the override is called
on carries the trial pattern. A returned `Verdict` is final for this level, and its `matched_target`,
when set, is reported as the target. A fact read from `ctx.attributes` may be absent (bare `--name`s,
or a parent judged by name): degrade with `ctx.warn`. An included verdict names no `excluded_by` and
an excluded one must name it.

**Step 6.** `probe_ancestor_kinds(kind=...)` returns the container kinds above `kind`, outermost
first; `()` for a top-level kind, which nothing contains; or `None` when it cannot say. Given a
`--parent`, `None` (or no hook at all) is a warning that the parents were not judged.
`ancestors_in(chain, kind, leaves)` builds the answer from the container chain, with `leaves` the
kinds that sit under the whole chain (`()` if there are none).

Patterns match from the start, are not anchored at the end, and ignore case unless the recipe sets
`ignoreCase: false`; `deny` is the same:

| Entry      | Target             | Matches                     |
| ---------- | ------------------ | --------------------------- |
| `prod`     | `production`       | yes: a prefix is enough     |
| `^prod$`   | `production`       | no                          |
| `orders`   | `analytics.orders` | no: the match starts at `a` |
| `.*orders` | `analytics.orders` | yes                         |

Never re-implement matching with `re.search`, `re.fullmatch` or `in`. To ask whether a pattern
filters anything, use `pattern.is_allow_all()` and read `False` as "unknown"; do not compare with
`AllowDenyPattern.allow_all()`, whose equality includes a regex cache.

### One predicate for ingestion and the probe

An override that restates ingestion's rules drifts from them. Put the connector's selection
decisions in a pure module, `<connector>_selection.py`, with no I/O and no client, which the source
and the override both call. Each function takes the recipe and facts about one object and returns a
`Verdict`:

```python
def workspace_verdict(config: WorkspaceFilterConfig, facts: WorkspaceFacts) -> Verdict:
    if not config.workspace_name_pattern.allowed(facts.name):
        return Verdict.exclude("workspace_name_pattern")
    return Verdict.include()
```

Type the config as a `Protocol` with read-only properties (the config module imports the selection
module, not the reverse). Read patterns off the config, so `--try-allow` reaches them. A fact the
probe may lack is `Optional`, and `None` skips that rule; ingestion always passes it.

Prove the two agree with `assert_probe_parity` (`tests/test_helpers/probe_parity.py`). It runs
ingestion (`pipeline_ingestion`) and the probe on one recipe under your mocks, judges each listing
as `probe filter --from-run` would, and asserts both directions per kind. Give one `ParityListing`
per kind: `FanOut` for a child kind listed under every parent, kept or not, and `identity` where a
listing's name is not the emitted id. A listing that warned is refused as partial; name a warning
the source gives on every normal run in `accept_warnings`. Parametrize over recipes that exercise
each rule, and pin the reasons with `report.excluded_by(label)`.

## The SQL family

A connector whose config inherits `SQLCommonConfig`, and names no provider of its own, gets
`SqlAlchemyMetadataProbe` and ten commands:
the listings `containers`, `tables` and `views`; `columns`, `foreign_keys`, `indexes`,
`primary_key`, `table_comment` and `view_definition`; and `sql`. The listings come from the
SQLAlchemy Inspector, which is what ingestion enumerates through. `tables` and `views` are separate
because `table_pattern` and `view_pattern` are, and both carry their schema as `parent_path`.
`containers` lists schemas on a three-tier source and databases on a two-tier one, as
`probe_container_kind` declares.

**Every caller-supplied identifier reaches reflection only as a catalog-listed string.** Several
dialects format schema and table names into their reflection SQL, which the `sql` gate never sees.
The inherited commands resolve each name with `resolve_listed_name`
(`source/sql/sql_identifier_resolver.py`): an unlisted name is refused with exit 2, a case-only miss
with the listed spelling as a hint, and a failing listing is exit 3. A subclass overriding one of
these commands resolves the same way and joins `_RESOLVES_ITS_OWN` in
`test_sql_identifier_hostile_input.py` after review. A new identifier-taking command is not
auto-checked.

`SQLCommonConfig` also declares the verdict hooks: `include_tables`/`include_views` carry
`Enables`, and its `probe_verdict_override`, inherited from `SQLFilterConfig` so that a config
filtering like the family without being a `SQLCommonConfig` (`snowflake-queries`, say) gets it
too, is `sql_structural_verdict(self, ctx)`, which excludes a database in `default_databases()` or
a schema in `default_schemas()` and, with `match_fully_qualified_names`, judges a schema as
`<container>.<schema>`. A subclass marks a switch
of its own on its own field, and one with rules of its own returns `sql_structural_verdict(self, ctx)`
for the names its rules leave alone.

### SQL-family hooks

Read only by `source/sql/`, so only a `SQLCommonConfig` subclass declares them;
`test_probe_contract.py` checks the `probe_*` rows against `SQL_FAMILY_HOOKS`. Any other `probe_*`
attribute on a config, or on a provider beyond its attributes and commands, is refused as the
connector's defect (exit 1) the first time the probe reads it, so a removed or misspelled hook
fails loudly instead of doing nothing. The two `default_*`
classmethods are read by name too, but have a base on `SQLCommonConfig` (or none at all), so a
misspelling is not caught: test them against ingestion.

| Hook                        | Signature                                                                                                        | Declare it when                                                          |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------ |
| `probe_container_kind`      | classmethod `() -> str`                                                                                          | `containers` returns databases rather than schemas (two-tier)            |
| `probe_catalog_scope`       | classmethod `() -> CatalogScope`                                                                                 | your catalog is more than `information_schema`                           |
| `probe_filter_target`       | `(self, schema: str, entity: str, warn: Callable[[str], None], database: Optional[str] = None) -> Optional[str]` | the `get_identifier` shim cannot build your table identifier             |
| `probe_engine_settings`     | `(self, budget: QueryBudget) -> ProbeEngineSettings`                                                             | ingestion sets its engine up in a way a bare `create_engine()` misses    |
| `probe_sqlglot_dialect`     | classmethod `() -> Optional[str]`                                                                                | sqlglot spells your dialect differently from SQLAlchemy                  |
| `probe_normalize_container` | `(self, name: str) -> str`                                                                                       | the Inspector spells a container differently from what ingestion matches |
| `probe_sql_alchemy_url`     | `(self) -> str`                                                                                                  | the probe must dial another URL than `get_sql_alchemy_url()`             |
| `default_schemas`           | classmethod `() -> FrozenSet[str]`                                                                               | ingestion drops system schemas whatever `schema_pattern` says            |
| `default_databases`         | classmethod `() -> FrozenSet[str]`                                                                               | ingestion drops system databases whatever `database_pattern` says        |

**Catalog scope.** `probe sql` reads only what `probe_catalog_scope()` names; the default is
`information_schema`. The family withholds `processlist` and `innodb_trx` from every scope, yours
included, since the MySQL protocol keeps other sessions' SQL text there and a config's URL can name
a MySQL-protocol server whatever its type. Name relations, not whole schemas: a vendor catalog
schema is rarely all metadata (query logs carry WHERE-clause literals), and a schema-level allow
with exclusions admits the next such view by default. List an unqualified relation only where the
dialect exposes its catalog unqualified. sqlglot must know your dialect (declare
`probe_sqlglot_dialect` where its name differs from SQLAlchemy's), or `sql` refuses everything. A
connector with its own provider sets `catalog_scope` on that class instead, where a bare
`CatalogScope()` admits `information_schema` whole. Set `split_dotted_identifiers=True` only where
sqlglot leaves a path's dots inside one identifier slot and an identifier cannot contain a dot
(BigQuery's scope does).

**Qualification.** `SQLCommonConfig.probe_match_target` matches tables and views on the connector's
own `get_identifier`, called on an uninitialised `Source` (see `source/sql/sql_probe.py`), and
containers on their bare name. That `Source` is found by name (`_source_class_for`): `FooConfig`'s
is `FooSource` in the config's own module when that is a `SQLAlchemySource`, else
`SQLAlchemySource` itself. Where tables match `container.schema.entity`, declare it: mark the
config field that pins the container with `Qualifier()` (a list field qualifies only when it pins
one value; `Qualifier(authoritative=True)` makes it beat `--parent`), or, where only the caller knows
the container, return `qualified_table_target(database, schema, entity, warn)` from
`probe_filter_target`, importing it from `source/sql/sql_probe_verdicts.py` inside the hook: a
config module imports no probe framework at load, since ingestion would pay for it. Override `probe_filter_target` too when your `Source` is not a
`SQLAlchemySource` or its `get_identifier` reads state ingestion sets while walking. It is called by
keyword, `database` included, and `warn` reports a less precise fallback. Which provider a connector
brings is never read as a declaration.

**Engine settings.** The probe connects with the recipe's own URL and `options`.
`probe_engine_settings(budget)` returns a `ProbeEngineSettings`: `connect_args` merged over the
recipe's, `prepare(engine)` run on the built engine before the Inspector exists, and
`timeout_applies`, which is `True` only when every probe statement is bounded (where it is not,
`sql` says so in its warnings). The default gives
every config the settings of the wire protocol its URL names (libpq, the MySQL protocol, Redshift's
driver; `probe_settings_for_url` in `source/sql/protocol_probe_settings.py`), so a config pointed at
another protocol's dialect gets that one's. Add your own engine setup on top:
`return super().probe_engine_settings(budget).followed_by(step)` runs `step(engine)` after the
protocol's. A new protocol's settings go in that module, named for the URL's dialect, which
`url_dialect_and_driver(url)` (`source/sql/sqlalchemy_uri.py`) splits out: declare only arguments
its drivers are known to accept. A ceiling set by a statement in `prepare` may claim
`timeout_applies` only if a refused statement fails the connection (Redshift's does; MySQL's is
best effort and claims nothing).

`ProbeEngineSettings` (also importable from `source/sql/sql_config.py`) and
`probe_label_connect_arg(config, kwarg)` (the client label, unless the recipe names its connection
itself) are in `source/sql/protocol_probe_settings.py`.
`QueryBudget` is in `agent/sql_passthrough.py`: `timeout_seconds` (30 by default) and
`max_bytes_billed` (none by default), where `None` means no ceiling.

## Testing and docs checklist

- [ ] **Hooks and methods.** `probe_provider_class()` on the config, `for_config` on the provider,
      optional hooks copied from the [hook reference](#hook-reference). `kind=` and
      `row_limit_param=` on each listing, `Filters(...)` on each pattern field (`FiltersByRule(...)`
      on a rule field), `Enables(...)` on each switch.
- [ ] **The contract scan passes.** `pytest tests/unit/agent/test_probe_contract.py` covers a new
      connector the moment it registers. If you add a rule there, add a deliberately bad provider
      that proves it fires.
- [ ] **Exit codes.** A test per code the provider can produce, through the CLI where the message
      matters: `tests/unit/cli/test_recipe_probe_cli.py` drives the `recipe` group with
      `CliRunner` and a patched `_resolve_for_probe`; `tests/unit/agent/test_error_policy.py` calls
      `run_probe_method` in-process.
- [ ] **Errors and logs.** Caller mistakes raise `ProbeArgumentError`; no foreign `{exc}` in a
      message or warning; no scrubbers or `setLevel` calls of your own; `silenced_loggers` where
      reused code logs shape-free values.
- [ ] **Degrade path.** A 403 or 404 on one sub-listing gives an empty result and a warning; auth
      failures and 5xx raise.
- [ ] **Personal data.** Personal records withheld with a count, identity columns masked.
- [ ] **Verdicts match ingestion.** A parity test through `assert_probe_parity`, and for SQL
      sources `tests/unit/agent/test_sql_filter_target.py`. Where the connector has selection rules
      of its own (a `probe_verdict_override` beyond the defaults), they live in
      `<connector>_selection.py`, called by the source and the override alike.
- [ ] **Switches and rules have parity cases.** Each `Enables` field gets a case with it off
      (`tests/unit/agent/test_sql_enables_parity.py`), each `FiltersByRule` field one whose rule
      excludes something; a connector without a parity test adds these cases when it adds one.
- [ ] **Gated commands.** Anything touching `sql_gate` has attack cases (a user table in a CTE,
      subquery, `UNION` branch or join; two statements; a vendor function in the projection) and
      false-positive cases (a trailing semicolon, a recursive CTE).
- [ ] **A real instance.** Where the connector has a docker-backed integration suite, add a probe
      test next to it (see `tests/integration/agent/test_probe_methods_sqlalchemy.py`).
- [ ] **Existing suites pass unedited.** A probe adds to a connector; it does not change it.
- [ ] **Docs headings.** Probe docs go in `docs/sources/<platform>/<plugin>_post.md` under
      `### Capabilities` as `#### Probe support`: a new H3 breaks the docs build.
      `pytest tests/unit/test_source_doc_headings.py` checks it.

Configs are pydantic models, so a test cannot set a hook on an instance (`ValueError`, and
`object.__setattr__` silently tests an unvalidated object). Patch the hook on the class with
`monkeypatch.setattr(MySourceConfig, "probe_unfiltered_kinds", classmethod(...))`, or on a small
subclass when the class is shared. Build a new config with `model_validate` to change a field:
`model_copy(update=...)` skips the validators that normalize patterns.
