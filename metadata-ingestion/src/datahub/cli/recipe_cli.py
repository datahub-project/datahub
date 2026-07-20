import importlib.resources
import json
import sys
from typing import Dict, List, NoReturn, Set, Tuple

import click
import yaml

from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind
from datahub.ingestion.agent.probe import probe
from datahub.ingestion.agent.recipe import explain, scaffold, validate_recipe
from datahub.ingestion.agent.redact import collect_secret_values, redact
from datahub.ingestion.agent.secrets import default_resolvers, resolve_config

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


def _fail(message: str, code: int) -> NoReturn:
    click.echo(json.dumps({"error": message}), err=True)
    sys.exit(code)


def _load_recipe(path: str) -> Dict[str, object]:
    with open(path) as f:
        loaded = yaml.safe_load(f) or {}
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
    resolved = resolve_config(config, default_resolvers())
    spec = describe_source(source_type)
    secret_fields = {f.name for f in spec.fields if f.kind == FieldKind.SECRET}
    secret_values = collect_secret_values(resolved, secret_fields)
    return source_type, resolved, secret_values


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
        safe_report = json.loads(json.dumps(report, default=lambda o: o.__dict__))
        _emit(redact(safe_report, secret_values))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: the exception text may embed a resolved secret (e.g. a
        # Pydantic ValidationError's input_value or a connection string with
        # an embedded password), so redact before it reaches stderr.
        _fail(redact(str(exc), secret_values), EXIT_USER)
    except Exception as exc:
        # SECURITY: same rationale as above -- DBAPI/SQLAlchemy connection
        # errors routinely embed the connection string, password included.
        _fail(redact(str(exc), secret_values), EXIT_CONNECTION)


def _probe(level_path: List[str], recipe_path: str, limit: int) -> None:
    # Bound before the try so it is always defined, even if resolution itself
    # fails before any secret can be collected.
    secret_values: Set[str] = set()
    try:
        source_type, resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
        result = probe(source_type, resolved, level_path, limit)
        _emit(redact(result.to_dict(), secret_values))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: see test_connection -- exception text may embed a resolved
        # secret (validation input_value / connection string password).
        _fail(redact(str(exc), secret_values), EXIT_USER)
    except Exception as exc:
        # SECURITY: same rationale as above.
        _fail(redact(str(exc), secret_values), EXIT_CONNECTION)


@recipe.group(name="probe")
def probe_group() -> None:
    """Live source probes (need a resolved secret)."""


@probe_group.command(name="schemas")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--limit", default=200, type=int)
def probe_schemas(recipe_path: str, limit: int) -> None:
    _probe([], recipe_path, limit)


@probe_group.command(name="tables")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--schema", "schema", required=True)
@click.option("--limit", default=500, type=int)
def probe_tables(recipe_path: str, schema: str, limit: int) -> None:
    _probe([schema], recipe_path, limit)


@probe_group.command(name="columns")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--schema", "schema", required=True)
@click.option("--table", "table", required=True)
@click.option("--limit", default=1000, type=int)
def probe_columns(recipe_path: str, schema: str, table: str, limit: int) -> None:
    _probe([schema, table], recipe_path, limit)
