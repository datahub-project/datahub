import importlib.resources
import json
import sys
from typing import Dict, List, NoReturn, Optional, Set, Tuple

import click
import yaml

from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.probe import (
    ProbeBranchesError,
    probe,
    probe_hierarchy,
    probe_shape,
)
from datahub.ingestion.agent.probe_methods import list_probe_methods, run_probe_method
from datahub.ingestion.agent.recipe import explain, scaffold, validate_recipe
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


@click.group(cls=_AgentAwareGroup, name="recipe")
def recipe() -> None:
    """Agent-facing probe/introspection interface for ingestion recipes."""


@recipe.command()
@click.argument("source_type")
def describe(source_type: str) -> None:
    try:
        _emit(describe_source(source_type).to_dict())
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        _fail(str(exc), EXIT_USER)


@recipe.command()
@click.argument("source_type")
def capabilities(source_type: str) -> None:
    try:
        spec = describe_source(source_type)
        _emit({"source_type": source_type, "capabilities": spec.capabilities})
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        _fail(str(exc), EXIT_USER)


@recipe.command(name="scaffold")
@click.argument("source_type")
def recipe_scaffold(source_type: str) -> None:
    try:
        _emit(scaffold(source_type))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        _fail(str(exc), EXIT_USER)


@recipe.command(name="validate")
@click.argument("path")
def recipe_validate(path: str) -> None:
    try:
        _emit(validate_recipe(_load_recipe(path)))
    except (ValueError, TypeError, AssertionError) as exc:
        _fail(str(exc), EXIT_USER)


@recipe.command(name="explain")
@click.argument("path")
def recipe_explain(path: str) -> None:
    try:
        _emit(explain(_load_recipe(path)))
    except (ValueError, TypeError, AssertionError) as exc:
        _fail(str(exc), EXIT_USER)


@recipe.command(name="test-connection")
@click.option("--recipe", "recipe_path", required=True)
def test_connection(recipe_path: str) -> None:
    # Bound before the try so it is always defined, even if resolution itself
    # fails before any secret can be collected.
    secret_values: Set[str] = set()
    try:
        source_type, resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
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
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: the exception text may embed a resolved secret (e.g. a
        # Pydantic ValidationError's input_value or a connection string with
        # an embedded password), so redact before it reaches stderr.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)
    except Exception as exc:
        # SECURITY: same rationale as above -- DBAPI/SQLAlchemy connection
        # errors routinely embed the connection string, password included.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_CONNECTION)


@recipe.group(name="probe")
def probe_group() -> None:
    """Live source probes (need a resolved secret)."""


@probe_group.command(name="shape")
@click.option("--recipe", "recipe_path", required=True)
def probe_shape_cmd(recipe_path: str) -> None:
    # Connection-free: describes the levels this source declares, so a caller
    # knows what to pass to `probe list --parent`. A branching source has no
    # single chain, so `hierarchy` is null and `shape` carries the tree.
    try:
        source_type, _resolved, _secrets = _resolve_for_probe(_load_recipe(recipe_path))
        shape = probe_shape(source_type)
        try:
            hierarchy: Optional[List[str]] = [
                str(k) for k in (probe_hierarchy(source_type) or [])
            ] or None
        except ProbeBranchesError:
            # A tree has no single chain; `shape` carries it instead. Catch the
            # specific error so an unrelated ValueError still surfaces.
            hierarchy = None
        _emit(
            {
                "source_type": source_type,
                "supported": shape is not None,
                "linear": hierarchy is not None,
                "hierarchy": hierarchy,
                "shape": shape.to_dict() if shape else None,
            }
        )
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        _fail(str(exc), EXIT_USER)


@probe_group.command(name="list")
@click.option("--recipe", "recipe_path", required=True)
@click.option(
    "--parent",
    "parents",
    multiple=True,
    help="Parent container name, repeated in hierarchy order to descend a level "
    "(e.g. --parent my_schema --parent my_table). Omit to list the top level.",
)
@click.option("--limit", default=200, type=int)
@click.option(
    "--report-to",
    "report_to",
    default=None,
    help="Write the redacted probe result as JSON to this file (in addition to "
    "stdout). Used by the executor's probe task to capture a structured report.",
)
def probe_list(
    recipe_path: str, parents: Tuple[str, ...], limit: int, report_to: Optional[str]
) -> None:
    # Source-agnostic lister: descends whatever hierarchy the source declares via
    # probe_hierarchy(), so non-SQL shapes (Kafka topics, ThoughtSpot worksheets)
    # work without the SQL-shaped --database/--schema/--table flags.
    secret_values: Set[str] = set()
    try:
        source_type, resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
        result = probe(source_type, resolved, list(parents), limit)
        payload = redact(result.to_dict(), secret_values)
        if report_to:
            with open(report_to, "w") as f:
                json.dump(payload, f)
        _emit(payload)
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)
    except Exception as exc:
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_CONNECTION)


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
    try:
        source_type, _resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
        specs = list_probe_methods(source_type)
        _emit({"source_type": source_type, "methods": [s.to_dict() for s in specs]})
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: see test_connection -- exception text may embed a resolved
        # secret (validation input_value / connection string password).
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)


@probe_group.command(
    name="run",
    context_settings={"ignore_unknown_options": True, "allow_extra_args": True},
)
@click.argument("command")
@click.option("--recipe", "recipe_path", required=True)
@click.argument("params", nargs=-1, type=click.UNPROCESSED)
def probe_run_cmd(command: str, recipe_path: str, params: Tuple[str, ...]) -> None:
    secret_values: Set[str] = set()
    try:
        source_type, resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
        call_kwargs: Dict[str, object] = dict(_parse_extra_params(params))
        result = run_probe_method(source_type, resolved, command, call_kwargs)
        # SECURITY: normalize to pure JSON types before redacting, so a raw
        # exception/driver object nested in the result cannot smuggle a secret
        # past the redactor (which only inspects str/dict/list values).
        safe = json.loads(json.dumps(result.to_dict(), default=_json_default))
        _emit(redact(safe, secret_values))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: exception text may embed a resolved secret (e.g. a
        # connection-string password) surfaced by a failed provider call.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)
    except Exception as exc:
        # SECURITY: same rationale as above -- underlying driver errors
        # routinely embed the connection string, password included.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_CONNECTION)
