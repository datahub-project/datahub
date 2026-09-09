import importlib.resources
import json
import re
import sys
from contextlib import contextmanager
from typing import Dict, Iterator, NoReturn, Optional, Set, Tuple, Type

import click
import yaml

from datahub.configuration.common import ConfigurationError
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.recipe import scaffold, validate_recipe
from datahub.ingestion.agent.redact import (
    _SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
    collect_secret_values,
    redact,
)
from datahub.ingestion.agent.secrets import (
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
        _fail(_redacted_text(exc, secrets), EXIT_USER)
    except Exception as exc:
        _fail(_redacted_text(exc, secrets), fallback)


def _fail(message: str, code: int) -> NoReturn:
    click.echo(json.dumps({"error": message}), err=True)
    sys.exit(code)


def _load_recipe(path: str) -> Dict[str, object]:
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
    resolved = resolve_config_collecting(config, default_resolvers())
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
    raw_source = recipe.get("source")
    source: Dict[str, object] = raw_source if isinstance(raw_source, dict) else {}
    raw_config = source.get("config")
    config: Dict[str, object] = raw_config if isinstance(raw_config, dict) else {}
    # Floor: inline literals recognisable by key name, straight off the raw
    # recipe, so a later failure cannot cost us these.
    values |= collect_nested_secret_values(config, _SENSITIVE_KEY_HINTS)
    try:
        resolved = resolve_config_collecting(config, default_resolvers())
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
        _emit(redact(validate_recipe(recipe_doc), secret_values))


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


def _parse_extra_params(tokens: Tuple[str, ...]) -> Dict[str, str]:
    # Hand-rolled rather than a second click.Command: tokens here are dynamic,
    # connector-specific probe-method parameters (e.g. --schema/--table) that
    # aren't known until list_probe_methods() resolves the source type.
    out: Dict[str, str] = {}
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
            out[key.replace("-", "_")] = "true"  # bare flag => boolean true
            i += 1
    return out


@probe_group.command(name="methods")
@click.option("--recipe", "recipe_path", required=True)
def probe_methods_cmd(recipe_path: str) -> None:
    # Connection-free: lists each command, its params, and its docstring (the
    # help the agent reads to decide which method to call).
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_INTERNAL):
        source_type, _resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        _ping_probe("methods", source_type)
        specs = list_probe_methods(source_type)
        _emit({"source_type": source_type, "methods": [s.to_dict() for s in specs]})


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
