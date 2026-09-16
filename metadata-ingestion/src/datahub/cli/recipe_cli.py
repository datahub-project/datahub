import importlib.resources
import json
import re
import sys
from contextlib import contextmanager
from typing import Dict, Iterator, List, NoReturn, Optional, Set, Tuple, Type

import click
import yaml

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.probe_methods import (
    BARE_FLAG,
    list_probe_methods,
    run_probe_method,
)
from datahub.ingestion.agent.recipe import scaffold, validate_recipe
from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
    collect_plain_config_values,
    collect_secret_values,
    redact,
)
from datahub.ingestion.agent.secrets import (
    MappingResolver,
    SecretResolver,
    default_resolvers,
    resolve_config_collecting,
)

EXIT_OK = 0
EXIT_INTERNAL = 1
EXIT_USER = 2
EXIT_CONNECTION = 3


class _AgentAwareGroup(click.Group):
    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)
        if not sys.stdout.isatty():
            try:
                agent_text = (
                    importlib.resources.files("datahub.cli.resources")
                    .joinpath("RECIPE_AGENT_CONTEXT.md")
                    .read_text(encoding="utf-8")
                )
            except (FileNotFoundError, ModuleNotFoundError):
                # The agent-context resource file is optional; --help must never
                # crash just because it hasn't been added yet.
                return
            formatter.write("\n")
            formatter.write(agent_text)


def _emit(payload: object) -> None:
    click.echo(json.dumps(payload, indent=2, default=str))


def _json_default(o: object) -> object:
    # Never let a SecretStr's raw value into serialized output: its __dict__
    # exposes _secret_value, so mask before falling back to attribute dumping.
    from pydantic import SecretBytes, SecretStr  # local: pydantic types only here

    if isinstance(o, (SecretStr, SecretBytes)):
        return "***"
    return getattr(o, "__dict__", str(o))


def _write_report(report_to: Optional[str], payload: object) -> None:
    # Redacted payload only -- this file is written for a caller that captures
    # a structured report instead of parsing stdout, and it must carry no more
    # than stdout does.
    if report_to:
        try:
            with open(report_to, "w") as f:
                json.dump(payload, f)
        except OSError as exc:
            # An unwritable path or missing parent directory is the caller's
            # argument being wrong, so it must read as EXIT_USER like any other
            # bad argument -- not escape as a traceback, and not fall through to
            # the connection-error handler, which would send an agent looking
            # at the source instead of at its own --report-to.
            raise ValueError(f"cannot write report to '{report_to}': {exc}") from exc


# Exceptions that mean "your input was wrong" (EXIT_USER), in one place.
#
# There were seven copies of this ladder and they had drifted apart: `validate`
# omitted KeyError, and five of the seven had no catch-all at all, so anything
# unexpected escaped as an unredacted traceback. verdicts.py's own comment
# predicted it -- "the CLI has four such ladders, and adding a clause to three
# of four is how this landed on the wrong code to begin with" -- and there were
# seven, not four. Classifying once is the fix; adding an eighth clause is not.
#
# The last two are the ones that were escaping. Neither is a ValueError:
#   ConfigurationError is MetaError, raised by source_registry.get() when a
#     plugin extra is not installed -- the most likely first-contact failure,
#     and its message already carries the `pip install 'acryl-datahub[x]'` hint.
#   re.error comes from an AllowDenyPattern compiling lazily inside .allowed(),
#     so a malformed --try-allow crashed the very command meant to diagnose it.
_USER_ERRORS: Tuple[Type[BaseException], ...] = (
    ValueError,  # SqlScopeError, ApiScopeError, ProbeSoftError all subclass it
    TypeError,
    AssertionError,
    KeyError,
    ConfigurationError,
    re.error,
)


def _redacted_text(exc: BaseException, secret_values: Set[str]) -> str:
    # SECURITY: exception text is where credentials leak in practice -- a driver
    # echoing a connection string, a pydantic ValidationError echoing its
    # input_value. Redact before it reaches stderr.
    redacted = redact(str(exc), secret_values)
    assert isinstance(redacted, str)
    return redacted


@contextmanager
def _exit_codes(
    secret_values: Optional[Set[str]] = None, fallback: int = EXIT_INTERNAL
) -> Iterator[None]:
    """Map any exception to this CLI's exit-code contract, redacting on the way.

    `fallback` is what an unclassifiable exception becomes: EXIT_CONNECTION for
    a command that reaches the source, EXIT_INTERNAL for one that cannot (a
    connection-free command reporting "I could not reach the source" would send
    an agent to retry something it never attempted).
    """
    # `or set()` would be wrong here, and subtly: an empty set is falsey, so it
    # would hand back a *new* set and drop the caller's reference -- and the
    # caller's set is empty at entry precisely because the secrets get collected
    # inside the block. The redaction would then silently do nothing.
    secrets = set() if secret_values is None else secret_values
    try:
        yield
    except _USER_ERRORS as exc:
        _fail(_redacted_text(exc, _with_stdin_secrets(secrets)), EXIT_USER)
    except Exception as exc:
        _fail(_redacted_text(exc, _with_stdin_secrets(secrets)), fallback)


def _with_stdin_secrets(secrets: Set[str]) -> Set[str]:
    """`secrets` plus anything piped in, for the error path.

    The caller's set is empty until _load_recipe returns, so a failure *during*
    loading -- malformed YAML inside the envelope, whose parser error quotes the
    offending line -- would be redacted against nothing. Read at raise time
    rather than at entry, so a command that never reaches its own collection
    step still masks what it was handed.

    Minus the disclosed values, because unioning the envelope back in undid the
    exemption exactly where it is most visible. A password equal to the
    database name is dropped from the redaction set so `probe filter` can print
    `target` -- and then `could not connect to analytics` came back with the
    database blanked anyway, which both corrupts the message and announces the
    collision the exemption exists to hide. Empty until the envelope is parsed,
    so a failure before that still masks everything.
    """
    return (secrets | {v for v in _stdin_secrets.values() if v}) - (
        _disclosed_stdin_values - secrets
    )


def _fail(message: str, code: int) -> NoReturn:
    click.echo(json.dumps({"error": message}), err=True)
    sys.exit(code)


# Secrets handed in on stdin alongside the recipe. Module-level because the
# resolve step happens well after loading, and threading an extra argument
# through every probe subcommand to carry it would be noise for a value that
# is set at most once per process.
_stdin_secrets: Dict[str, str] = {}

# Envelope values the recipe also states in the clear under a non-sensitive
# key. Kept beside _stdin_secrets and for the same reason: the exemption is
# decided while loading, and every later redaction has to agree with it.
# Without this the error path re-added them and the exemption only half
# applied -- see _with_stdin_secrets.
_disclosed_stdin_values: Set[str] = set()


def _stdin_aware_resolvers() -> List[SecretResolver]:
    """The environment chain, preceded by anything that arrived on stdin.

    Every command that takes `-` shares this, not just the probe ones: `validate`
    resolving with a different chain than `probe run` meant the two disagreed
    about the same envelope, and an agent validates before it probes.

    A value the caller piped in wins over a same-named ambient variable: they
    passed it that way precisely to avoid the environment. With no envelope this
    is `default_resolvers()` unchanged, so the file path behaves as before.
    """
    if _stdin_secrets:
        return [MappingResolver(_stdin_secrets), *default_resolvers()]
    return default_resolvers()


def _envelope_disclosed_values(recipe_yaml: object) -> Set[str]:
    """Values the envelope's recipe states in the clear, which cannot be masked.

    Best-effort and never raises: the envelope's secrets are registered BEFORE
    the recipe is parsed, so a parse error quoting the offending line is
    already covered, and that ordering must not change. An unparseable recipe
    simply discloses nothing.

    Inline secret literals are excluded, as everywhere else -- a recipe with
    `password: p` and `database: p` discloses the credential itself, and the
    child's output travels further than the recipe does.
    """
    if not isinstance(recipe_yaml, str):
        return set()
    try:
        loaded = yaml.safe_load(recipe_yaml)
        config = loaded["source"]["config"]
    except Exception:
        return set()
    if not isinstance(config, dict):
        return set()
    return collect_plain_config_values(
        config, _SENSITIVE_KEY_HINTS
    ) - collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)


def _recipe_from_stdin() -> Dict[str, object]:
    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("no recipe received on stdin")
    try:
        envelope = json.loads(raw)
    except ValueError:
        envelope = None
    if isinstance(envelope, dict) and "__recipe_yaml__" in envelope:
        # Checked before anything reads it. A non-string reached yaml.safe_load
        # and came back as `'dict' object has no attribute 'read'` at exit 1 --
        # an internal-error code for a malformed input, which tells an agent to
        # retry when it should be rebuilding the envelope. Exit codes are the
        # agent's control flow, so the wrong one misroutes it.
        if not isinstance(envelope["__recipe_yaml__"], str):
            raise ValueError(
                "__recipe_yaml__ must be a string holding the recipe YAML; got "
                f"{type(envelope['__recipe_yaml__']).__name__}"
            )
        secrets = envelope.get("__secrets__") or {}
        if isinstance(secrets, dict):
            # Strings only, deliberately. str(v) would turn a JSON null into
            # the literal "None" -- so a secret the caller failed to resolve
            # became a password of "None" and the probe reported whatever the
            # server said about it, instead of "${REF} could not be resolved".
            # The registry will not even mask that value ("none" is on its
            # unmaskable-literals list). Dropping the entry lets resolution
            # fail by name, which is the honest answer. load_config_file does
            # not coerce either.
            #
            # An empty string is kept, though: it is a value the caller chose,
            # and dropping it left nothing for MappingResolver, so EnvVarResolver
            # went on to read the ambient variable of the same name -- the
            # fall-through the envelope exists to prevent. The registry drops it
            # on its own (it is below MIN_SECRET_LENGTH), and _with_stdin_secrets
            # keeps it out of the redaction set, where "" would match everything.
            _stdin_secrets.update(
                {str(k): v for k, v in secrets.items() if isinstance(v, str)}
            )
            # Feed the masking backstop the `recipe` group installs: its
            # excepthook, logging handlers and stdout wrapper all read the
            # registry, and per-command redact() does not populate it.
            #
            # ConfigModel registers its own SecretStr fields (common.py), so
            # the registry is not empty once a connector config is built -- but
            # that is late and partial. It covers nothing before validation
            # succeeds (a YAML parse error, an unresolvable ref, an unknown
            # source type), nothing for a command that never builds a config
            # (describe, validate), and no envelope value that is not a typed
            # SecretStr on that connector. Registering here closes that window;
            # it happens before the YAML is parsed so a parse failure is
            # already covered. Same thing load_config_file does for
            # `ingest -c -`.
            from datahub.masking.secret_registry import SecretRegistry

            # Minus what the recipe states in the clear, for the same reason
            # _resolve_for_probe subtracts it from the redaction set -- except
            # the stakes here are the whole stdout stream, not one payload. A
            # password equal to the database name otherwise masks it inside
            # every unrelated word the child prints, turning
            # `datahub.ingestion.source.sql` into
            # `***REDACTED:PW***.ingestion.source.sql`, and those lines become
            # the task's operator-visible logs.
            disclosed = _envelope_disclosed_values(envelope["__recipe_yaml__"])
            # Recorded for the error path, which builds its own redaction set
            # and would otherwise union these straight back in.
            _disclosed_stdin_values.update(disclosed)
            SecretRegistry.get_instance().register_secrets_batch(
                {k: v for k, v in _stdin_secrets.items() if v not in disclosed}
            )
        raw = envelope["__recipe_yaml__"]
    try:
        loaded = yaml.safe_load(raw) or {}
    except yaml.YAMLError as exc:
        raise ValueError(f"cannot parse recipe from stdin: {exc}") from exc
    if not isinstance(loaded, dict):
        raise ValueError("recipe must be a YAML mapping")
    return loaded


def _load_recipe(path: str) -> Dict[str, object]:
    """The recipe at `path`, or from stdin when `path` is `-`.

    `-` accepts the same JSON envelope `datahub ingest -c -` does:
    `{"__recipe_yaml__": ..., "__secrets__": {...}}`. The executor uses it so
    resolved credentials reach the probe without being written to the
    environment -- where they would be readable from /proc/<pid>/environ and
    `ps e`, and inherited by every process the CLI spawns. A plain recipe on
    stdin works too. The secrets travel out through `_stdin_secrets` rather
    than being substituted here, so the normal resolve-and-collect path still
    sees the `${refs}` and can record what to mask.
    """
    if path == "-":
        return _recipe_from_stdin()
    try:
        with open(path) as f:
            loaded = yaml.safe_load(f) or {}
    except OSError as exc:
        # Surface a bad --recipe path as a user error (EXIT_USER) rather than an
        # uncaught traceback or a mislabeled connection error.
        raise ValueError(f"cannot read recipe file '{path}': {exc}") from exc
    except yaml.YAMLError as exc:
        # Same reasoning, different failure: a recipe that does not parse is the
        # caller's file being wrong. YAMLError is not a ValueError, so without
        # this it reached the connection-error handler.
        raise ValueError(f"cannot parse recipe file '{path}': {exc}") from exc
    if not isinstance(loaded, dict):
        raise ValueError("recipe must be a YAML mapping")
    return loaded


def _resolve_for_probe(
    recipe: Dict[str, object],
) -> Tuple[str, Dict[str, object], Set[str]]:
    raw_source = recipe.get("source")
    source: Dict[str, object] = raw_source if isinstance(raw_source, dict) else {}
    source_type = str(source.get("type"))
    raw_config = source.get("config")
    config: Dict[str, object] = raw_config if isinstance(raw_config, dict) else {}
    resolved = resolve_config_collecting(config, _stdin_aware_resolvers())
    spec = describe_source(source_type)
    secret_fields = {f.name for f in spec.fields if f.kind == FieldKind.SECRET}
    # Union of every ${ref}-sourced value (nested-safe) and top-level inline
    # secret fields (which may be literals with no ${ref} to record).
    secret_values = resolved.secret_values | collect_secret_values(
        resolved.config, secret_fields
    )
    # Defense-in-depth: catch secrets living in free-form dict config fields
    # (e.g. Kafka's consumer_config) that aren't typed SecretStr and so aren't
    # covered by either collection above.
    secret_values |= collect_nested_secret_values(resolved.config, _SENSITIVE_KEY_HINTS)
    # Anything piped in is a secret by declaration, so mask it whether or not
    # the recipe happened to reference it (it may have arrived already
    # substituted). Mirrors what load_config_file does for `ingest -c -`.
    secret_values |= {v for v in _stdin_secrets.values() if v}
    # Finally, drop what masking cannot protect. A secret equal to a value the
    # recipe states in the clear -- a password the same as the database name --
    # is not made safer by blanking it: the recipe already says the identifier,
    # `target` has to print it, and the mask itself is what reveals the
    # collision to a reader who can see the identifier but not the ${ref}.
    #
    # Exempt only what the raw recipe does NOT also carry as an inline secret
    # literal. A recipe with `password: p` and `database: p` discloses the
    # credential itself, and the report travels further than the recipe does --
    # to GMS, the logs and an LLM -- so that one keeps its mask. Everything is
    # read off the RAW config: resolved values put the ${ref}-sourced secret
    # under `password` too, which would exempt every colliding secret and
    # defeat the distinction.
    raw_inline_secrets = collect_secret_values(
        config, secret_fields
    ) | collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)
    secret_values -= (
        collect_plain_config_values(config, _SENSITIVE_KEY_HINTS) - raw_inline_secrets
    )
    return source_type, resolved.config, secret_values


def _secrets_in_recipe(recipe: Dict[str, object]) -> Set[str]:
    """Every secret this recipe resolves to, best-effort, never raising.

    Independent of describe_source on purpose. _resolve_for_probe resolves
    secrets and *then* validates the source type, so a recipe with an unknown
    type but a resolvable ${SECRET} reached the error handler with an empty
    secret set and emitted unredacted. Each fallback below keeps whatever was
    already collected rather than returning nothing.
    """
    values: Set[str] = set()
    # Anything piped in is a secret by declaration, and goes in before the
    # early returns below so no failure can cost us it. A value may arrive
    # already substituted into the recipe under a key no hint recognises, in
    # which case nothing else here would collect it. Mirrors _resolve_for_probe.
    values |= {v for v in _stdin_secrets.values() if v}
    raw_source = recipe.get("source")
    source: Dict[str, object] = raw_source if isinstance(raw_source, dict) else {}
    raw_config = source.get("config")
    config: Dict[str, object] = raw_config if isinstance(raw_config, dict) else {}
    # Floor: inline literals recognisable by key name, straight off the raw
    # recipe, so a later failure cannot cost us these.
    values |= collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)
    try:
        resolved = resolve_config_collecting(config, _stdin_aware_resolvers())
    except Exception:
        # An unresolvable ${ref} is the command's own finding to report, and it
        # produced no value, so there is nothing further to mask.
        return values
    values |= resolved.secret_values
    values |= collect_nested_secret_values(resolved.config, _SENSITIVE_KEY_HINTS)
    try:
        spec = describe_source(str(source.get("type")))
    except Exception:
        # Unknown or uninstalled source type: keep the refs already resolved.
        return values
    values |= collect_secret_values(
        resolved.config, {f.name for f in spec.fields if f.kind == FieldKind.SECRET}
    )
    return values


@click.group(cls=_AgentAwareGroup, name="recipe")
def recipe() -> None:
    """Agent-facing probe/introspection interface for ingestion recipes."""
    # SECURITY backstop. Every command redacts its own output and error text
    # against the secrets it resolved, but that only covers what reaches _fail.
    # This installs the masking sys.excepthook, so a traceback that escapes
    # anyway is masked too -- `datahub ingest` has had it since it was written
    # (ingest_cli.py) and the recipe path did not, which left the commands that
    # had no catch-all printing raw connection strings.
    from datahub.masking.bootstrap import initialize_secret_masking

    initialize_secret_masking()


def _ping_probe(command: str, source_type: str, **dims: object) -> None:
    """Record which probe command ran against which connector.

    The auto-applied with_telemetry wrapper (see cli_utils.enable_auto_decorators)
    already reports the function, its duration and its exit code -- and thanks to
    _fail() raising SystemExit, the exit-2-vs-3 split is measurable from it. What
    it cannot report is *which* command or *which* connector, because it only
    captures kwargs a decorator names, and source_type is not a CLI flag at all:
    it is read out of the recipe.

    Those are the two questions worth asking about an agent-facing CLI nobody has
    used yet. Ten commands ship per SQL connector; if traffic is all `tables` and
    `columns` the rest are maintenance burden, and if it is all `sql` the typed
    getters are not earning their place. Neither is answerable after the fact.

    A separate ping rather than with_telemetry(capture_kwargs=...): that decorator
    is already applied automatically, and adding a second one used to double every
    function-call event for the command. Nothing here is customer data -- a
    connector name, a command name, a filter kind.
    """
    from datahub.telemetry import telemetry

    props: Dict[str, object] = {"command": command, "source_type": source_type}
    props.update({k: v for k, v in dims.items() if v is not None})
    telemetry.telemetry_instance.ping("recipe-probe", props)


# Reported when redaction actually changed the payload, because the agent cannot
# tell "***" from a name otherwise.
_MASKED_NOTICE = (
    "something in this result was redacted and now reads '***'. Read it as "
    "'redacted', never as a name. When a secret happens to equal an identifier -- "
    "a password the same as a database, schema or table name -- that identifier is "
    "masked everywhere it occurs, `target` included, so a verdict can name a "
    "pattern while the thing it matched shows as '***'. The recipe has the real "
    "name; this output deliberately does not."
)


def _redacted_payload(payload: object, secret_values: Set[str]) -> object:
    """Redact, and say so when redaction changed what the caller is reading.

    Over-masking is the safe failure and stays (see agent.redact.redact), but
    silent over-masking is not: `probe filter`'s whole purpose is to report the
    `target` a pattern was matched against, and a target reading '***' with no
    explanation is exactly the confidently-unreadable answer this interface
    exists to avoid. The notice says nothing about *which* secret collided, and
    nothing that is not already visible in the output -- naming the field would
    tell a caller who cannot see a ${ENV_VAR} secret that it equals an
    identifier they can see.
    """
    redacted = redact(payload, secret_values)
    if redacted == payload:
        return redacted
    warnings = redacted.get("warnings") if isinstance(redacted, dict) else None
    if isinstance(warnings, list):
        warnings.append(_MASKED_NOTICE)
    return redacted


@recipe.command()
@click.argument("source_type")
def describe(source_type: str) -> None:
    with _exit_codes():
        _ping_probe("describe", source_type)
        _emit(describe_source(source_type).to_dict())


@recipe.command(name="scaffold")
@click.argument("source_type")
def recipe_scaffold(source_type: str) -> None:
    with _exit_codes():
        _emit(scaffold(source_type))


@recipe.command(name="validate")
@click.argument("path")
def recipe_validate(path: str) -> None:
    # SECURITY: this was the one command with no redaction. validate_recipe
    # reports raw str(exc) from model_validate, and pydantic v2 embeds
    # input_value= in most messages -- so the command whose job is warning you
    # about a plaintext secret could echo that secret back in the same breath.
    secret_values: Set[str] = set()
    with _exit_codes(secret_values):
        recipe_doc = _load_recipe(path)
        secret_values.update(_secrets_in_recipe(recipe_doc))
        # Same resolvers the probe path uses, so `validate -` does not report a
        # ${REF} unresolvable when the caller piped its value in and `probe run`
        # on the identical envelope would accept it.
        _emit(
            redact(validate_recipe(recipe_doc, _stdin_aware_resolvers()), secret_values)
        )


@recipe.command(name="test-connection")
@click.option("--recipe", "recipe_path", required=True)
def test_connection(recipe_path: str) -> None:
    # Bound before the try so it is always defined, even if resolution itself
    # fails before any secret can be collected.
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_CONNECTION):
        source_type, resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        # Lazy import: keeps TestableSource / source_registry out of this
        # module's import-time surface until test-connection is actually invoked.
        from datahub.ingestion.api.source import TestableSource
        from datahub.ingestion.source.source_registry import source_registry

        source_cls = source_registry.get(source_type)
        if not issubclass(source_cls, TestableSource):
            _fail(f"source '{source_type}' does not support test-connection", EXIT_USER)
        report = source_cls.test_connection(resolved)
        # SECURITY: normalize to pure JSON types before redacting, so a raw
        # exception/driver object nested in the report cannot smuggle a secret
        # past the redactor (which only inspects str/dict/list values).
        safe_report = json.loads(json.dumps(report, default=_json_default))
        _emit(redact(safe_report, secret_values))
        # The report was emitted but never consulted, so a FAILED connection
        # test exited 0 -- in a CLI whose whole contract is that the caller
        # reads the exit code to tell "your input was wrong" from "I could not
        # reach the source", the one command named after reaching the source
        # did not use it. An agent read a bad credential as a success.
        capable = getattr(getattr(report, "basic_connectivity", None), "capable", None)
        if capable is None and isinstance(safe_report, dict):
            # test_connection returns a TestConnectionReport, but a source may
            # hand back a plain dict; read either shape rather than trusting one.
            basic = safe_report.get("basic_connectivity")
            if isinstance(basic, dict):
                capable = basic.get("capable")
        internal = getattr(report, "internal_failure", None)
        if internal is None and isinstance(safe_report, dict):
            internal = safe_report.get("internal_failure")
        # A connector can fail before it ever gets to basic_connectivity, in
        # which case capable stays None and only internal_failure is set --
        # which exited 0, the very thing this branch exists to stop.
        if capable is False or internal is True:
            # Names the field the reason is actually in. When internal_failure
            # fires, basic_connectivity is absent -- so pointing there sent an
            # agent looking for a key the report does not carry.
            where = (
                "internal_failure_reason"
                if capable is not False
                else "basic_connectivity.failure_reason"
            )
            _fail(
                f"connection test failed for source '{source_type}'; "
                f"see {where} in the emitted report",
                EXIT_CONNECTION,
            )


@recipe.group(name="probe")
def probe_group() -> None:
    """Live source probes (need a resolved secret)."""


def _parse_extra_params(tokens: Tuple[str, ...]) -> Dict[str, object]:
    # Hand-rolled rather than a second click.Command: tokens here are dynamic,
    # connector-specific probe-method parameters (e.g. --schema/--table) that
    # aren't known until list_probe_methods() resolves the source type.
    out: Dict[str, object] = {}
    toks = list(tokens)
    i = 0
    while i < len(toks):
        tok = toks[i]
        if not tok.startswith("--"):
            raise ValueError(f"unexpected argument '{tok}'; use --name value")
        key = tok[2:]
        if "=" in key:
            name, value = key.split("=", 1)
            out[name.replace("-", "_")] = value
            i += 1
        elif i + 1 < len(toks) and not toks[i + 1].startswith("--"):
            out[key.replace("-", "_")] = toks[i + 1]
            i += 2
        else:
            # A sentinel, not "true": the parser does not know the parameter's
            # declared type, and `--schema --table orders` would otherwise pass
            # schema="true" to a str parameter. That reaches the driver as a
            # real schema name -- `SHOW CREATE TABLE \`true\`.\`orders\`` on
            # MySQL, and on dialects whose listing filters by name rather than
            # erroring, an empty result at exit 0 indistinguishable from an
            # empty schema. _coerce holds the spec and can refuse it properly.
            out[key.replace("-", "_")] = BARE_FLAG
            i += 1
    return out


@probe_group.command(name="methods")
@click.option("--recipe", "recipe_path", required=True)
@click.option(
    "--report-to",
    "report_to",
    default=None,
    help="Also write the JSON payload to this file.",
)
def probe_methods_cmd(recipe_path: str, report_to: Optional[str]) -> None:
    """List what this source can be asked about, and how to ask it.

    Connection-free: reports each command, its params, and its description --
    the help the agent reads to decide which method to call.
    """
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_INTERNAL):
        source_type, resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        _ping_probe("methods", source_type)
        specs = list_probe_methods(source_type, resolved)
        payload = {
            "source_type": source_type,
            "methods": [s.to_dict() for s in specs],
        }
        _write_report(report_to, payload)
        _emit(payload)


@probe_group.command(name="filter")
@click.option("--recipe", "recipe_path", required=True)
@click.option(
    "--kind",
    required=True,
    help="The subtype being judged, e.g. Table, View, Schema, Topic. Selects "
    "which *_pattern field applies.",
)
@click.option(
    "--parent",
    "parents",
    multiple=True,
    help="Container names above these objects, outermost first. Part of the "
    "identifier most connectors filter on, so omitting it changes the answer.",
)
@click.option(
    "--name",
    "names",
    multiple=True,
    required=True,
    help="An object name to judge, exactly as the source reports it. Repeat for "
    "each name. Not comma-separated: a Mode collection or a quoted SQL "
    "identifier may legitimately contain a comma, and splitting on it would "
    "judge names that do not exist.",
)
@click.option(
    "--try-allow",
    "try_allow",
    multiple=True,
    help="Judge against this allow pattern instead of the recipe's, to test a "
    "change before making it.",
)
@click.option("--try-deny", "try_deny", multiple=True, help="As --try-allow, for deny.")
@click.option(
    "--report-to",
    "report_to",
    default=None,
    help="Write the redacted result as JSON to this file, in addition to stdout. "
    "For a caller that captures a structured report rather than parsing stdout.",
)
def probe_filter_cmd(
    recipe_path: str,
    kind: str,
    parents: Tuple[str, ...],
    names: Tuple[str, ...],
    try_allow: Tuple[str, ...],
    try_deny: Tuple[str, ...],
    report_to: Optional[str],
) -> None:
    """Would the recipe's filters keep these objects, and what decided?

    Needs no connection: it judges names you already have. Each result reports
    the `target` the pattern was matched against, which is usually the
    qualified identifier rather than the bare name -- that is what explains a
    pattern matching nothing.
    """
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_INTERNAL):
        source_type, resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        _ping_probe("filter", source_type, kind=kind)
        result = check_filters(
            source_type=source_type,
            config_dict=resolved,
            kind=kind,
            parent_path=list(parents),
            names=list(names),
            try_allow=list(try_allow),
            try_deny=list(try_deny),
        )
        payload = _redacted_payload(result.to_dict(), secret_values)
        _write_report(report_to, payload)
        _emit(payload)


@probe_group.command(
    name="run",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.argument("command")
@click.option("--recipe", "recipe_path", required=True)
@click.argument("params", nargs=-1, type=click.UNPROCESSED)
@click.option(
    "--report-to",
    "report_to",
    default=None,
    help="Write the redacted result as JSON to this file, in addition to stdout. "
    "For a caller that captures a structured report rather than parsing stdout.",
)
def probe_run_cmd(
    command: str,
    recipe_path: str,
    params: Tuple[str, ...],
    report_to: Optional[str],
) -> None:
    """Run one of the source's listing commands against the live source.

    The connecting half of the probe: `probe methods` names the available
    commands and the params each takes, and this calls one of them. Params go
    as `--<name> <value>`, repeatable.
    """
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_CONNECTION):
        source_type, resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        # Before the call, so a command that fails to reach the source is still
        # counted -- "which methods do agents reach for" must include the ones
        # that did not work.
        _ping_probe("run", source_type, probe_command=command)
        call_kwargs: Dict[str, object] = dict(_parse_extra_params(params))
        result = run_probe_method(source_type, resolved, command, call_kwargs)
        # SECURITY: normalize to pure JSON types before redacting, so a raw
        # exception/driver object nested in the result cannot smuggle a secret
        # past the redactor (which only inspects str/dict/list values).
        safe = json.loads(json.dumps(result.to_dict(), default=_json_default))
        payload = _redacted_payload(safe, secret_values)
        _write_report(report_to, payload)
        _emit(payload)
        # A failure means the result is not a complete answer, so the command
        # must not read as success. Emitting first keeps the partial result and
        # the reason available to the caller; only the exit code changes.
        if result.failures:
            # SECURITY: redacted like everything else that leaves this command.
            # The payload above was masked and then this line joined the raw
            # failure strings onto stderr -- and those come from driver and
            # report text, which is exactly the channel the rest of this file
            # treats as leaky. A secret masked on stdout still reached the
            # agent on the error line.
            joined = "; ".join(str(f) for f in result.failures)
            _fail(
                _redacted_text(
                    ValueError(f"'{command}' could not be completed: {joined}"),
                    secret_values,
                ),
                EXIT_CONNECTION,
            )
