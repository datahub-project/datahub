import importlib.resources
import json
import os
import re
import stat
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Callable,
    Dict,
    Iterator,
    List,
    Mapping,
    NoReturn,
    Optional,
    Set,
    Tuple,
    Type,
)

import click
import yaml
from pydantic import SecretBytes, SecretStr, ValidationError

from datahub.configuration.common import ConfigurationError
from datahub.configuration.config_loader import (
    MalformedRecipeEnvelope,
    parse_recipe_envelope,
)
from datahub.ingestion.agent.config_validation import describe_validation_error
from datahub.ingestion.agent.error_policy import (
    DEFECT_TYPES,
    PASS_THROUGH,
    classify_foreign,
    is_trusted,
    police_trusted,
)
from datahub.ingestion.agent.filter_check import check_filters
from datahub.ingestion.agent.filter_input import filter_request, listing_from_run
from datahub.ingestion.agent.introspect import describe_source, secret_field_values
from datahub.ingestion.agent.log_guard import quiet_reused_logs
from datahub.ingestion.agent.probe_methods import (
    BARE_FLAG,
    ProbeMethodResult,
    config_class_for,
    list_probe_methods,
    require_config_class,
    run_probe_method,
    source_class_for,
)
from datahub.ingestion.agent.recipe import scaffold, validate_recipe
from datahub.ingestion.agent.redact import (
    SENSITIVE_KEY_HINTS,
    collect_nested_secret_values,
    redact,
    scrub_strings,
    scrub_text,
)
from datahub.ingestion.agent.secrets import (
    MappingResolver,
    SecretResolver,
    default_resolvers,
    resolve_config_collecting,
)
from datahub.ingestion.agent.verdicts import (
    ProbeArgumentError,
    ProbeConnectionError,
    ProbeInternalError,
)
from datahub.ingestion.api.source import TestableSource
from datahub.masking.bootstrap import initialize_secret_masking
from datahub.masking.masking_filter import SecretMaskingFilter
from datahub.masking.secret_registry import SecretRegistry
from datahub.telemetry import telemetry

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


def _masked(payload: object) -> object:
    """`payload` as pure JSON types with every string masked against the
    registry. Normalized first so no object reaches the masker unconverted."""
    plain = json.loads(json.dumps(payload, default=_json_default))
    return SecretMaskingFilter().mask_structure(plain)


def _emit(payload: object) -> None:
    # The stdout wrapper also masks, but only the serialized text; see _masked.
    click.echo(json.dumps(_masked(payload), indent=2, default=str))


def _json_default(o: object) -> object:
    """What to emit for a value json cannot serialize.

    SecretStr first and explicitly: its __dict__ exposes _secret_value, so
    any attribute-based fallback would print the secret.

    Everything else becomes str(o), NOT o.__dict__. Dumping __dict__ walked
    arbitrary object graphs -- a driver error or an exception object carries
    whatever its library put on it, including connection strings, and
    json.dumps would recurse through the whole structure. Redaction runs
    afterwards, but it only knows the values it collected, so an
    unregistered credential nested three attributes deep went out in the
    clear. str(o) is bounded and is what the caller can act on anyway.
    """
    if isinstance(o, (SecretStr, SecretBytes)):
        return "***"
    return str(o)


def report_to_text(payload: object) -> str:
    """The text `--report-to` writes for `payload`: masked against the
    registry as a structure, then serialized. See _write_report for why."""
    return json.dumps(_masked(payload), default=_json_default)


def _write_report(report_to: Optional[str], payload: object) -> None:
    # Redacted payload only -- this file is written for a caller that captures
    # a structured report instead of parsing stdout, and it must carry no more
    # than stdout does.
    if report_to:
        try:
            # Serialized BEFORE the file is opened: opening truncates, so a
            # value that cannot be serialized would otherwise leave a
            # half-written report that reads as valid output.
            text = report_to_text(payload)

            # SECURITY: masked against the registry, the same as stdout.
            #
            # `_emit` goes through the stdout wrapper bootstrap installs, so
            # it masks every value the registry knows. A file write does not
            # touch that wrapper, and the two then disagree: with an
            # envelope secret registered but not collected into this
            # command's `secret_values`, stdout masked it and the file
            # carried it in the clear.
            #
            # Per-command redaction is not a substitute and does not cover
            # this: `probe methods` passes its payload with no
            # _redacted_payload at all, and the other two redact only what
            # they collected.
            #
            # Masked as a structure, before serializing: JSON escaping changes
            # how a secret renders, so masking the serialized text can miss it.
            with open(report_to, "w") as f:
                f.write(text)
        except OSError as exc:
            # An unwritable path or missing parent directory is the caller's
            # argument being wrong, so it must read as EXIT_USER like any other
            # bad argument -- not escape as a traceback, and not fall through to
            # the connection-error handler, which would send an agent looking
            # at the source instead of at its own --report-to.
            raise ValueError(f"cannot write report to '{report_to}': {exc}") from exc


# Exceptions that mean "your input was wrong" (EXIT_USER), in one place: every
# command classifies through _exit_codes, so a clause added here applies to
# all of them. Only what the CLI's own checks raise: code a connector supplies
# (a provider, a config hook, test_connection) is classified by
# agent.error_policy before it gets here, and a Python defect is EXIT_INTERNAL.
#
# The last two are not ValueErrors:
#   ConfigurationError is MetaError, which a config validator may raise about
#     the recipe it was given; pydantic wraps only ValueError.
#   re.error comes from an AllowDenyPattern compiling lazily inside .allowed(),
#     so a malformed --try-allow is the caller's input, not a crash.
_USER_ERRORS: Tuple[Type[BaseException], ...] = (
    ValueError,  # SqlScopeError, ApiScopeError, ProbeSoftError all subclass it
    ConfigurationError,
    re.error,
)


def _redacted_text(exc: BaseException, secret_values: Set[str]) -> str:
    # SECURITY: exception text is where credentials leak in practice -- a driver
    # echoing a connection string, a pydantic ValidationError echoing its
    # input_value. Mask registered values, then credential shapes.
    return scrub_text(str(exc), secret_values)


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

    def fail(exc: BaseException, code: int) -> NoReturn:
        _fail(_redacted_text(exc, _with_stdin_secrets(secrets)), code)

    try:
        yield
    except ProbeConnectionError as exc:
        # Before _USER_ERRORS, and not in it: it wraps exception types that
        # list would otherwise read as bad input (Snowflake's ConfigurationError).
        fail(exc, EXIT_CONNECTION)
    except ProbeInternalError as exc:
        fail(exc, EXIT_INTERNAL)
    except _USER_ERRORS as exc:
        fail(exc, EXIT_USER)
    except DEFECT_TYPES as exc:
        fail(exc, EXIT_INTERNAL)
    except Exception as exc:
        fail(exc, fallback)


def _with_stdin_secrets(secrets: Set[str]) -> Set[str]:
    """`secrets` plus anything piped in, for the error path.

    The caller's set is empty until _load_recipe returns, so a failure *during*
    loading -- malformed YAML inside the envelope, whose parser error quotes the
    offending line -- would be redacted against nothing. Read at raise time
    rather than at entry, so a command that never reaches its own collection
    step still masks what it was handed.
    """
    return secrets | {v for v in _stdin_secrets.values() if v}


def _fail(message: str, code: int) -> NoReturn:
    click.echo(json.dumps({"error": message}), err=True)
    sys.exit(code)


# Secrets handed in on stdin alongside the recipe. Module-level because the
# resolve step happens well after loading, and threading an extra argument
# through every probe subcommand to carry it would be noise.
#
# Reset per invocation by the `recipe` group callback, NOT merely updated:
# anything that dispatches the group more than once per process would
# otherwise hand a later recipe resolving ${REF} the EARLIER caller's
# credential, registered for masking as though it had been handed it.
_stdin_secrets: Dict[str, str] = {}


def _stdin_aware_resolvers(
    stdin_secrets: Optional[Mapping[str, str]] = None,
) -> List[SecretResolver]:
    """The environment chain, preceded by anything that arrived on stdin.

    Every command that takes `-` shares this, not just the probe ones: `validate`
    resolving with a different chain than `probe run` meant the two disagreed
    about the same envelope, and an agent validates before it probes.

    A value the caller piped in wins over a same-named ambient variable: they
    passed it that way precisely to avoid the environment. With no envelope this
    is `default_resolvers()` unchanged, so the file path behaves as before.
    `stdin_secrets` stands in for what arrived; by default this invocation's.
    """
    piped = _stdin_secrets if stdin_secrets is None else stdin_secrets
    if piped:
        return [MappingResolver(dict(piped)), *default_resolvers()]
    return default_resolvers()


def _recipe_from_stdin() -> Dict[str, object]:
    raw = sys.stdin.read()
    if not raw.strip():
        raise ValueError("no recipe received on stdin")

    # One parser for this format, shared with load_config_file, so a
    # malformed envelope fails the same way here and on `ingest -c -`. The
    # strings-only secret filter and the reasoning for keeping an empty
    # string live there, beside the check.
    try:
        envelope = parse_recipe_envelope(raw)
    except MalformedRecipeEnvelope as exc:
        # Register before reporting: the envelope was readable enough to
        # yield its secrets, and an unmasked failure is the worst place to
        # lose them.
        if exc.secrets:
            _stdin_secrets.update(exc.secrets)
            SecretRegistry.get_instance().register_secrets_batch(exc.secrets)
        raise ValueError(str(exc)) from exc
    except ConfigurationError as exc:
        # A user error on this CLI's contract: ConfigurationError is in
        # _USER_ERRORS, but raising ValueError keeps the message shape the
        # probe commands already produce.
        raise ValueError(str(exc)) from exc

    if envelope is not None:
        if envelope.secrets:
            _stdin_secrets.update(envelope.secrets)

            # Feed the masking backstop the `recipe` group installs: its
            # excepthook, logging handlers and stdout wrapper all read the
            # registry, and per-command redact() does not populate it.
            #
            # ConfigModel registers its own SecretStr fields (common.py), so
            # the registry is not empty once a connector config is built --
            # but that is late and partial. It covers nothing before
            # validation succeeds (a YAML parse error, an unresolvable ref,
            # an unknown source type), nothing for a command that never
            # builds a config (describe, validate), and no envelope value
            # that is not a typed SecretStr on that connector. Registering
            # here closes that window; it happens before the YAML is parsed
            # so a parse failure is already covered. Same thing
            # load_config_file does for `ingest -c -`.
            SecretRegistry.get_instance().register_secrets_batch(_stdin_secrets)
        raw = envelope.recipe_yaml

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


def _register_for_masking(secret_values: Set[str]) -> None:
    """Hand the recipe's secrets to the masking registry, as `ingest` does with
    what it resolves. The stdout wrapper, logging filter and excepthook the
    `recipe` group installs mask only what the registry holds, and with
    DATAHUB_PROBE_VERBOSE_LOGS the log guard is off, so the registry is all
    that masks reused code's log lines. Named by position: a name is logged
    when a value cannot be masked, and a value must never be."""
    SecretRegistry.get_instance().register_secrets_batch(
        {f"recipe_secret_{i}": v for i, v in enumerate(sorted(secret_values))}
    )


def _resolve_for_probe(
    recipe: Dict[str, object],
    stdin_secrets: Optional[Mapping[str, str]] = None,
) -> Tuple[str, Dict[str, object], Set[str]]:
    """resolve_probe_recipe for this invocation, its secrets registered for
    masking before any command can print."""
    source_type, config, secret_values = _resolved_recipe(recipe, stdin_secrets)
    _register_for_masking(secret_values)
    return source_type, config, secret_values


def _resolved_recipe(
    recipe: Dict[str, object],
    stdin_secrets: Optional[Mapping[str, str]] = None,
) -> Tuple[str, Dict[str, object], Set[str]]:
    piped = _stdin_secrets if stdin_secrets is None else stdin_secrets
    raw_source = recipe.get("source")
    source: Dict[str, object] = raw_source if isinstance(raw_source, dict) else {}
    source_type = str(source.get("type"))
    raw_config = source.get("config")
    config: Dict[str, object] = raw_config if isinstance(raw_config, dict) else {}
    resolved = resolve_config_collecting(config, _stdin_aware_resolvers(piped))
    # Union of every ${ref}-sourced value and every SecretStr field's value at
    # any depth of the config (which may be literals with no ${ref} to record).
    secret_values = resolved.secret_values | secret_field_values(
        source_type, resolved.config
    )
    # Defense-in-depth: catch secrets living in free-form dict config fields
    # (e.g. Kafka's consumer_config) that aren't typed SecretStr and so aren't
    # covered by either collection above. secret_field_values has resolved the
    # class already, so this does not raise.
    secret_values |= collect_nested_secret_values(
        resolved.config,
        SENSITIVE_KEY_HINTS,
        config_cls=require_config_class(source_type),
    )
    # Anything piped in is a secret by declaration, so mask it whether or not
    # the recipe happened to reference it (it may have arrived already
    # substituted). Mirrors what load_config_file does for `ingest -c -`.
    secret_values |= {v for v in piped.values() if v}
    return source_type, resolved.config, secret_values


def resolve_probe_recipe(
    recipe: Dict[str, object],
    stdin_secrets: Optional[Mapping[str, str]] = None,
) -> Tuple[str, Dict[str, object], Set[str]]:
    """The source type, resolved config and secret values the probe commands
    work from, for a recipe document already loaded. For a caller outside the
    CLI that must reproduce exactly what `probe run` and `probe filter` see.

    `stdin_secrets` are an envelope's secrets, resolved as though piped in.
    Only these: what an earlier CLI invocation in this process read from
    stdin is left in _stdin_secrets, and nothing here clears it. Registers
    nothing for masking."""
    return _resolved_recipe(recipe, stdin_secrets or {})


def _secrets_in_recipe(recipe: Dict[str, object]) -> Set[str]:
    """Every secret this recipe resolves to, best-effort, never raising.

    Independent of the source type resolving, on purpose. _resolve_for_probe
    resolves secrets and *then* validates the source type, so a recipe with an
    unknown type but a resolvable ${SECRET} reached the error handler with an
    empty secret set and emitted unredacted. Each fallback below keeps whatever
    was already collected rather than returning nothing.
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
    config_cls = _config_class_if_any(str(source.get("type")))
    # Floor: inline literals recognisable by key name, straight off the raw
    # recipe, so a later failure cannot cost us these.
    values |= collect_nested_secret_values(
        config, SENSITIVE_KEY_HINTS, config_cls=config_cls
    )
    try:
        resolved = resolve_config_collecting(config, _stdin_aware_resolvers())
    except Exception:
        # An unresolvable ${ref} is the command's own finding to report, and it
        # produced no value, so there is nothing further to mask.
        return values
    values |= resolved.secret_values
    values |= collect_nested_secret_values(
        resolved.config, SENSITIVE_KEY_HINTS, config_cls=config_cls
    )
    try:
        values |= secret_field_values(str(source.get("type")), resolved.config)
    except Exception:
        # Unknown or uninstalled source type: keep the refs already resolved.
        pass
    return values


def _config_class_if_any(source_type: str) -> Optional[type]:
    """The source's config class, or None when the type does not resolve or
    declares none: the nested walk then reads every mapping as free-form, the
    safe side. Never raises, for _secrets_in_recipe."""
    try:
        return config_class_for(source_type)
    except Exception:
        return None


@click.group(cls=_AgentAwareGroup, name="recipe")
def recipe() -> None:
    """Agent-facing probe/introspection interface for ingestion recipes."""
    # SECURITY backstop. Every command redacts its own output and error text
    # against the secrets it resolved, but that only covers what reaches _fail.
    # This installs the masking sys.excepthook, so a traceback that escapes
    # anyway is masked too -- `datahub ingest` has had it since it was written
    # (ingest_cli.py) and the recipe path did not, which left the commands that
    # had no catch-all printing raw connection strings.
    initialize_secret_masking()

    # SECURITY: start each invocation with no envelope. These are module
    # globals (see _stdin_secrets) and were only ever updated, so a second
    # dispatch in the same interpreter inherited the first caller's secrets
    # and could resolve its own ${REF}s from them.
    _stdin_secrets.clear()


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
    props: Dict[str, object] = {"command": command, "source_type": source_type}
    props.update({k: v for k, v in dims.items() if v is not None})
    telemetry.telemetry_instance.ping("recipe-probe", props)


# Reported when redaction actually changed the payload, because the agent cannot
# tell "***" from a name otherwise.
_MASKED_NOTICE = (
    "something in this result was redacted and now reads '***'. Read it as "
    "'redacted', never as a name. Credential-shaped text in warnings and "
    "failures is masked, and when a secret happens to equal an identifier -- "
    "a password the same as a database, schema or table name -- that "
    "identifier is masked everywhere it occurs, `target` included, so a "
    "verdict can name a pattern while the thing it matched shows as '***'. "
    "The recipe has the real name; this output deliberately does not."
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

    The free-text `warnings` / `failures` lists are also scrubbed for
    credential shapes. Nothing here mutates `payload`.
    """
    redacted = redact(payload, secret_values)
    if not isinstance(redacted, dict):
        return redacted
    redacted = dict(redacted)
    for key in ("warnings", "failures"):
        items = redacted.get(key)
        if isinstance(items, list):
            redacted[key] = [
                scrub_text(item, secret_values) if isinstance(item, str) else item
                for item in items
            ]
    if redacted == payload:
        return redacted
    warnings = redacted.get("warnings")
    redacted["warnings"] = (
        [*warnings, _MASKED_NOTICE] if isinstance(warnings, list) else [_MASKED_NOTICE]
    )
    return redacted


def probe_run_envelope(result: ProbeMethodResult, secret_values: Set[str]) -> object:
    """The payload `probe run` emits and writes to --report-to, before the
    registry masking both of those apply on output (see report_to_text).

    A ProbeRunEnvelope, redacted, so `object` like everything the redactor
    returns; a reader takes it back through filter_input.run_envelope_view."""
    # SECURITY: normalize to pure JSON types before redacting, so a raw
    # exception/driver object nested in the result cannot smuggle a secret
    # past the redactor (which only inspects str/dict/list values).
    safe = json.loads(json.dumps(result.to_dict(), default=_json_default))
    return _redacted_payload(safe, secret_values)


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
        _register_for_masking(secret_values)
        # Same resolvers the probe path uses, so `validate -` does not report a
        # ${REF} unresolvable when the caller piped its value in and `probe run`
        # on the identical envelope would accept it.
        report = validate_recipe(recipe_doc, _stdin_aware_resolvers())
        # The errors quote validators, which may echo what they rejected under
        # a key no hint marks secret, so they are scrubbed for credential
        # shapes too. The warnings are this module's own text, whose
        # `password: ${PASSWORD}` advice a shape scrub would mask.
        report["errors"] = [scrub_text(e, secret_values) for e in report["errors"]]
        _emit(redact(report, secret_values))


def _test_connection_crash(exc: BaseException, source_type: str) -> Exception:
    """What a source's test_connection raising is reported as: by label, never
    by its text (the source's own connect code wrote it), on the exit code
    `probe run` gives a provider call's (agent.error_policy.classify_foreign).
    A SystemExit is the source giving up, whatever its status.

    Except a ValidationError: test_connection is handed the recipe's config
    unvalidated, so one it raises is the recipe failing the source's model
    (exit 2), where a provider call's is a response failing its own (3)."""
    context = f"source '{source_type}' test_connection"
    if isinstance(exc, ValidationError):
        # Field paths and messages, never inputs; scrubbed on the way out like
        # every other error line.
        return ProbeArgumentError(f"{context} failed: {describe_validation_error(exc)}")
    return classify_foreign(exc, context)


@dataclass(frozen=True)
class _TestedConnection:
    """What a source's test_connection returned, and its JSON-shaped form."""

    report: object
    # as_obj(): TestConnectionReport is a Report, not a dict, and json.dumps
    # would hand the whole object to _json_default, which stringifies it, so
    # basic_connectivity would not be a key of the payload.
    rendered: object


def _run_test_connection(
    test: Callable[[Dict[str, object]], object],
    resolved: Dict[str, object],
    source_type: str,
) -> _TestedConnection:
    try:
        report = test(resolved)
        # The source's own report code, so policed like test_connection.
        as_obj = getattr(report, "as_obj", None)
        return _TestedConnection(
            report=report, rendered=as_obj() if callable(as_obj) else report
        )
    except PASS_THROUGH:
        raise
    except BaseException as exc:
        if not is_trusted(exc):
            raise _test_connection_crash(exc, source_type) from None
        replacement = police_trusted(exc)
        if replacement is not None:
            raise replacement from None
        raise


@recipe.command(name="test-connection")
@click.option("--recipe", "recipe_path", required=True)
def test_connection(recipe_path: str) -> None:
    # Bound before the try so it is always defined, even if resolution itself
    # fails before any secret can be collected.
    secret_values: Set[str] = set()
    with _exit_codes(secret_values, fallback=EXIT_CONNECTION):
        source_type, resolved, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        # SECURITY: test_connection runs the source's own connect code, which
        # logs connection strings and request URLs like any reused fetcher.
        # The guard also covers looking the source up, which imports its
        # module, as `probe run`'s does.
        with quiet_reused_logs(secret_values):
            source_cls = source_class_for(source_type)
            if not issubclass(source_cls, TestableSource):
                _fail(
                    f"source '{source_type}' does not support test-connection",
                    EXIT_USER,
                )
            tested = _run_test_connection(
                source_cls.test_connection, resolved, source_type
            )
        report = tested.report
        # SECURITY: normalize to pure JSON types before redacting, so a raw
        # exception/driver object nested in the report cannot smuggle a secret
        # past the redactor (which only inspects str/dict/list values).
        safe_report = json.loads(json.dumps(tested.rendered, default=_json_default))
        _emit(scrub_strings(redact(safe_report, secret_values), secret_values))
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
        source_type, _, found = _resolve_for_probe(_load_recipe(recipe_path))
        secret_values.update(found)
        _ping_probe("methods", source_type)
        specs = list_probe_methods(source_type)
        payload = {
            "source_type": source_type,
            "methods": [s.to_dict() for s in specs],
        }
        _write_report(report_to, payload)
        _emit(payload)


# A `probe run` result is capped at MAX_PROBE_ITEMS entries, so a real one is
# far below this; anything larger is not one.
MAX_RUN_FILE_BYTES = 50 * 1024 * 1024


def _open_without_waiting(path: str, flags: int) -> int:
    """os.open, non-blocking: opening a named pipe for reading otherwise
    waits for a writer, before the regular-file check can refuse it. A
    regular file reads the same either way. No such flag on Windows."""
    return os.open(path, flags | getattr(os, "O_NONBLOCK", 0))


def _read_run_file(path: str) -> object:
    """The parsed JSON of a `probe run --report-to` file.

    click checks the file is readable, but only at parse time; a permission
    or I/O error at the read itself is the caller's argument being wrong, as
    _write_report treats it, so it exits 2 rather than as an internal error.
    """
    try:
        with open(path, "rb", opener=_open_without_waiting) as handle:
            # Judged on the open file's own stat, before reading a byte: a
            # device or pipe (/dev/zero) reports no size and never ends.
            info = os.fstat(handle.fileno())
            if not stat.S_ISREG(info.st_mode):
                raise ValueError(
                    f"--from-run file '{path}' is not a regular file; pass the "
                    f"file `probe run --report-to` wrote"
                )
            if info.st_size > MAX_RUN_FILE_BYTES:
                raise ValueError(
                    f"--from-run file '{path}' is {info.st_size} bytes, over "
                    f"the {MAX_RUN_FILE_BYTES}-byte limit for a `probe run` result"
                )
            # Bounded too, in case the file grows after the stat.
            data = handle.read(MAX_RUN_FILE_BYTES + 1)
    except OSError as exc:
        raise ValueError(f"cannot read --from-run file '{path}': {exc}") from exc
    if len(data) > MAX_RUN_FILE_BYTES:
        raise ValueError(
            f"--from-run file '{path}' is over the {MAX_RUN_FILE_BYTES}-byte "
            f"limit for a `probe run` result"
        )
    return json.loads(data.decode("utf-8"))


@probe_group.command(name="filter")
@click.option("--recipe", "recipe_path", required=True)
@click.option(
    "--kind",
    default=None,
    help="The subtype being judged, e.g. Table, View, Schema, Topic. Selects "
    "which *_pattern field applies. Taken from --from-run when omitted.",
)
@click.option(
    "--parent",
    "parents",
    multiple=True,
    help="Container names above these objects, outermost first. Part of the "
    "identifier most connectors filter on, so omitting it changes the answer. "
    "Their own patterns are judged too: an object inside an excluded container "
    "is reported excluded.",
)
@click.option(
    "--name",
    "names",
    multiple=True,
    help="An object name to judge, exactly as the source reports it. Repeat for "
    "each name. Not comma-separated: a Mode collection or a quoted SQL "
    "identifier may legitimately contain a comma, and splitting on it would "
    "judge names that do not exist.",
)
@click.option(
    "--from-run",
    "from_run",
    default=None,
    type=click.Path(exists=True, dir_okay=False),
    help="Judge the listing a `probe run --report-to` wrote: its names, the "
    "facts listed with each (an id, a type) that some sources filter on, and "
    "its kind and parent unless --kind/--parent are given. Instead of --name.",
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
    kind: Optional[str],
    parents: Tuple[str, ...],
    names: Tuple[str, ...],
    from_run: Optional[str],
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
        if from_run is not None and names:
            # Before the file is read, so a bad file cannot hide the conflict.
            raise ValueError(
                "--name and --from-run both name the objects to judge; pass one"
            )
        request = filter_request(
            source_type=source_type,
            kind=kind,
            parents=parents,
            names=names,
            listing=(
                None if from_run is None else listing_from_run(_read_run_file(from_run))
            ),
        )
        _ping_probe("filter", source_type, kind=request.kind)
        result = check_filters(
            source_type=source_type,
            config_dict=resolved,
            kind=request.kind,
            parent_path=request.parent_path,
            names=request.names,
            try_allow=list(try_allow),
            try_deny=list(try_deny),
            attributes=request.attributes,
        )
        # Before redaction, so these pass through it like every other warning.
        result.warnings.extend(request.warnings)
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
        # SECURITY: reused ingestion code logs connection strings and request
        # URLs at DEBUG, which `datahub --debug` prints. Guarded here as well as
        # inside run_probe_method because only this caller holds the recipe's
        # secret values, which have no credential shape for scrub_text to find.
        with quiet_reused_logs(secret_values):
            result = run_probe_method(source_type, resolved, command, call_kwargs)
        payload = probe_run_envelope(result, secret_values)
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
