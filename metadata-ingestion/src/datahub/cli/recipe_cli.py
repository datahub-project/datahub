import importlib.resources
import json
import sys
from typing import Dict, List, NoReturn, Optional, Set, Tuple

import click
import yaml

from datahub.ingestion.agent.api_probe import probe_api
from datahub.ingestion.agent.introspect import describe_source
from datahub.ingestion.agent.models import FieldKind, ProbeLeafKind, ProbeNodeKind
from datahub.ingestion.agent.probe import probe, probe_hierarchy
from datahub.ingestion.agent.recipe import explain, scaffold, validate_recipe
from datahub.ingestion.agent.redact import collect_secret_values, redact
from datahub.ingestion.agent.secrets import (
    default_resolvers,
    resolve_config_collecting,
)
from datahub.ingestion.source.common.subtypes import (
    DatasetContainerSubTypes,
    DatasetSubTypes,
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


# Each container kind maps to the CLI flag that supplies its value, so the parent
# path for a probe can be assembled generically from whatever flags the target
# level requires. --database is the top container (SQL database / BigQuery
# project); --schema is the second (SQL schema / BigQuery dataset).
_KIND_FLAG: Dict[str, str] = {
    "Database": "database",
    "Project": "database",
    "Schema": "schema",
    "Dataset": "schema",
    "Table": "table",
}


def _probe_kind(
    recipe_path: str,
    target_kinds: Tuple[ProbeNodeKind, ...],
    flags: Dict[str, object],
    limit: int,
) -> None:
    # target_kinds lists the equivalent node kinds a command accepts across source
    # families (e.g. Database or BigQuery Project for `probe databases`); the first
    # one present in this source's hierarchy is the level to list.
    # Bound before the try so it is always defined, even if resolution itself
    # fails before any secret can be collected.
    secret_values: Set[str] = set()
    try:
        source_type, resolved, secret_values = _resolve_for_probe(
            _load_recipe(recipe_path)
        )
        hierarchy = probe_hierarchy(source_type)
        if hierarchy is None:
            # Unsupported source: let probe() emit its standard fallback payload.
            result = probe(source_type, resolved, [], limit)
            _emit(redact(result.to_dict(), secret_values))
            return
        target_kind = next((k for k in target_kinds if k in hierarchy), None)
        if target_kind is None:
            wanted = " / ".join(str(k) for k in target_kinds)
            _fail(
                f"source '{source_type}' has no {wanted} level; "
                f"levels are: {', '.join(str(k) for k in hierarchy)}",
                EXIT_USER,
            )
        # The ancestors of the target level are exactly the container levels above
        # it; require the matching flag for each and assemble the parent path.
        parent_path: List[str] = []
        for ancestor in hierarchy[: hierarchy.index(target_kind)]:
            flag_name = _KIND_FLAG[str(ancestor)]
            value = flags.get(flag_name)
            if not value:
                _fail(
                    f"--{flag_name} is required to list {target_kind} "
                    f"for source '{source_type}'",
                    EXIT_USER,
                )
            parent_path.append(str(value))
        result = probe(source_type, resolved, parent_path, limit)
        _emit(redact(result.to_dict(), secret_values))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: see test_connection -- exception text may embed a resolved
        # secret (validation input_value / connection string password).
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)
    except Exception as exc:
        # SECURITY: same rationale as above.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_CONNECTION)


@recipe.group(name="probe")
def probe_group() -> None:
    """Live source probes (need a resolved secret)."""


# Top-level container is a Database (SQL) or a Project (BigQuery); the second is a
# Schema (SQL) or a Dataset (BigQuery). Each command accepts whichever the source
# actually has.
_TOP_CONTAINER_KINDS = (
    DatasetContainerSubTypes.DATABASE,
    DatasetContainerSubTypes.BIGQUERY_PROJECT,
)
_SECOND_CONTAINER_KINDS = (
    DatasetContainerSubTypes.SCHEMA,
    DatasetContainerSubTypes.BIGQUERY_DATASET,
)


@probe_group.command(name="databases")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--limit", default=200, type=int)
def probe_databases(recipe_path: str, limit: int) -> None:
    _probe_kind(recipe_path, _TOP_CONTAINER_KINDS, {}, limit)


@probe_group.command(name="schemas")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--database", "database", default=None)
@click.option("--limit", default=200, type=int)
def probe_schemas(recipe_path: str, database: Optional[str], limit: int) -> None:
    _probe_kind(
        recipe_path,
        _SECOND_CONTAINER_KINDS,
        {"database": database},
        limit,
    )


@probe_group.command(name="tables")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--database", "database", default=None)
@click.option("--schema", "schema", required=True)
@click.option("--limit", default=500, type=int)
def probe_tables(
    recipe_path: str, database: Optional[str], schema: str, limit: int
) -> None:
    _probe_kind(
        recipe_path,
        (DatasetSubTypes.TABLE,),
        {"database": database, "schema": schema},
        limit,
    )


@probe_group.command(name="columns")
@click.option("--recipe", "recipe_path", required=True)
@click.option("--database", "database", default=None)
@click.option("--schema", "schema", required=True)
@click.option("--table", "table", required=True)
@click.option("--limit", default=1000, type=int)
def probe_columns(
    recipe_path: str,
    database: Optional[str],
    schema: str,
    table: str,
    limit: int,
) -> None:
    _probe_kind(
        recipe_path,
        (ProbeLeafKind.COLUMN,),
        {"database": database, "schema": schema, "table": table},
        limit,
    )


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


@probe_group.command(name="api")
@click.option("--recipe", "recipe_path", required=True)
def probe_api_cmd(recipe_path: str) -> None:
    # Connectorless REST probe: interrogate a source that has no purpose-built
    # connector, described by a top-level `probe:` block instead of `source:`.
    # Secrets in headers (${ENV}) resolve in-process and are redacted from output;
    # only response shapes (never values) are returned.
    secret_values: Set[str] = set()
    try:
        raw = _load_recipe(recipe_path).get("probe")
        block: Dict[str, object] = raw if isinstance(raw, dict) else {}
        if str(block.get("kind")) != "rest":
            _fail("probe.kind must be 'rest' for probe api", EXIT_USER)
        resolved = resolve_config_collecting(block, default_resolvers())
        secret_values = resolved.secret_values
        cfg = resolved.config
        base_url = cfg.get("base_url")
        raw_endpoints = cfg.get("endpoints")
        if not isinstance(base_url, str) or not base_url:
            _fail("probe.base_url is required", EXIT_USER)
        if not isinstance(raw_endpoints, list) or not raw_endpoints:
            _fail("probe.endpoints must be a non-empty list", EXIT_USER)
        raw_headers = cfg.get("headers")
        headers = (
            {str(k): str(v) for k, v in raw_headers.items()}
            if isinstance(raw_headers, dict)
            else None
        )
        budget = cfg.get("budget")
        verify_ssl = cfg.get("verify_ssl")
        result = probe_api(
            base_url=base_url,
            endpoints=[str(e) for e in raw_endpoints],
            headers=headers,
            budget=int(budget) if isinstance(budget, int) else 10,
            verify_ssl=verify_ssl if isinstance(verify_ssl, bool) else True,
        )
        _emit(redact(result.to_dict(), secret_values))
    except (ValueError, TypeError, AssertionError, KeyError) as exc:
        # SECURITY: exception text may embed a resolved auth secret from a header.
        redacted = redact(str(exc), secret_values)
        assert isinstance(redacted, str)
        _fail(redacted, EXIT_USER)
