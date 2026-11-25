"""Source tools of the opt-in ``ingestion`` group: list, create and update the
ingestion sources DataHub runs on its executor (``datahub ingest deploy`` /
``list-source-runs``).

Each runs with the caller's own privileges: managing ingestion sources needs
DataHub's Manage Metadata Ingestion privilege.
"""

import copy
import json
from typing import Optional, cast

import yaml
from fastmcp.exceptions import ToolError
from pydantic import JsonValue

from .. import graphql_helpers
from ..arg_validation import OptionalJsonObject, OptionalStrList
from ..read_backend import get_read_backend
from ..version_requirements import destructive, idempotent, read_only
from .ingestion_recipes import (
    checked_recipe,
    literal_secret_paths,
    recipe_source_type,
    require_secret_references,
)

JsonObject = dict[str, JsonValue]

MAX_LISTED_SOURCES = 100
DEFAULT_EXECUTOR_ID = "default"
# The executor applies this recipe key to every run of a source, so a saved
# recipe must never carry it: each scheduled run would become a dry run.
RUN_OPTIONS_RECIPE_KEY = "__run_options__"
_MASKED = "********"
_FIND_SOURCES = "Call list_ingestion_sources to find its URN."


def masked_secrets(value: JsonValue) -> JsonValue:
    """``value`` with each literal credential (one that is not a ${SECRET}
    reference) masked. A source saved from the UI may hold one."""
    paths = set(literal_secret_paths(value))
    if not paths:
        return value

    def mask(node: JsonValue, path: str) -> JsonValue:
        if path in paths:
            return _MASKED
        if isinstance(node, dict):
            return {
                k: mask(v, f"{path}.{k}" if path else str(k)) for k, v in node.items()
            }
        if isinstance(node, list):
            return [mask(v, f"{path}[{i}]") for i, v in enumerate(node)]
        return node

    return mask(value, "")


def existing_source(source_urn: str) -> JsonObject:
    """The source at ``source_urn``, or a ToolError naming how to find one."""
    source_urn = source_urn.strip()
    if not source_urn.startswith("urn:li:dataHubIngestionSource:"):
        raise ToolError(
            f"{source_urn!r} is not an ingestion source URN "
            f"(urn:li:dataHubIngestionSource:...). {_FIND_SOURCES}"
        )
    source = get_read_backend().ingestion_source(source_urn)
    if not source:
        raise ToolError(f"No ingestion source exists at {source_urn}. {_FIND_SOURCES}")
    return source


def stored_recipe(source: JsonObject) -> JsonObject:
    """A source's stored recipe, parsed (GMS stores it as JSON)."""
    config = source.get("config")
    text = config.get("recipe") if isinstance(config, dict) else None
    try:
        parsed = yaml.safe_load(text) if isinstance(text, str) else None
    except yaml.YAMLError as e:
        raise ToolError(
            f"The stored recipe of {source.get('urn')} is invalid: {e}"
        ) from e
    if not isinstance(parsed, dict):
        raise ToolError(f"{source.get('urn')} has no stored recipe.")
    return cast(JsonObject, parsed)


@read_only
def list_ingestion_sources(
    query: Optional[str] = None, count: int = 50, source_urn: Optional[str] = None
) -> dict:
    """List ingestion sources with their type, schedule and latest run status (`datahub ingest list-source-runs`).

    Pass `source_urn` to get that one source in full instead: its recipe,
    executor, schedule and CLI version pin. Credentials stored as literal
    values are masked. A source created a few seconds ago may not appear in
    the list yet; read it by `source_urn`.
    """
    if source_urn is not None:
        source = dict(existing_source(source_urn))
        config = source.get("config")
        if isinstance(config, dict):
            source["config"] = {
                **config,
                "recipe": masked_secrets(stored_recipe(source)),
            }
        return source

    count = min(max(1, count), MAX_LISTED_SOURCES)
    sources = get_read_backend().ingestion_sources(
        {"start": 0, "count": count, "query": (query or "").strip() or "*"}
    )
    rows = []
    for source in sources:
        executions = source.get("executions")
        runs = (
            executions.get("executionRequests")
            if isinstance(executions, dict)
            else None
        )
        latest = runs[0] if isinstance(runs, list) and runs else None
        result = latest.get("result") if isinstance(latest, dict) else None
        rows.append(
            {
                "urn": source.get("urn"),
                "name": source.get("name"),
                "type": source.get("type"),
                "schedule": source.get("schedule"),
                "latest_run": {
                    "execution_urn": latest.get("urn"),
                    **(result if isinstance(result, dict) else {}),
                }
                if isinstance(latest, dict)
                else None,
            }
        )
    return {"sources": rows, "count": len(rows)}


def create_ingestion_source(
    name: str,
    recipe: str,
    schedule: Optional[str] = None,
    timezone: str = "UTC",
    executor_id: str = DEFAULT_EXECUTOR_ID,
) -> dict:
    """Create an ingestion source from a recipe (`datahub ingest deploy`).

    `recipe` is YAML or JSON with a `source` block; credentials must be
    ${SECRET_NAME} references to DataHub secrets. Check it with validate_recipe
    first. `schedule` is a cron expression such as "0 */6 * * *" in `timezone`;
    omit it for a source that only runs on demand. `executor_id` picks the
    executor ("default" is the one DataHub runs). The source is not run: call
    run_ingestion to run it.
    """
    if not name.strip():
        raise ToolError("`name` must not be empty.")
    parsed = checked_recipe(recipe)
    dropped = parsed.pop(RUN_OPTIONS_RECIPE_KEY, None) is not None
    source_type = recipe_source_type(parsed)
    source_input: dict = {
        "name": name.strip(),
        "type": source_type,
        "config": {
            "recipe": json.dumps(parsed),
            "executorId": executor_id.strip() or DEFAULT_EXECUTOR_ID,
        },
    }
    if schedule and schedule.strip():
        source_input["schedule"] = {"interval": schedule.strip(), "timezone": timezone}
    result = graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("ingestion.gql"),
        operation_name="createIngestionSource",
        variables={"input": source_input},
        unknown_outcome_hint=(
            f'Look for it with list_ingestion_sources(query="{name.strip()}") '
            "before creating it again."
        ),
    )
    urn = result.get("createIngestionSource")
    if not urn:
        raise ToolError("createIngestionSource returned no URN.")
    out: dict = {"urn": urn, "name": name.strip(), "type": source_type}
    if dropped:
        out["dropped_keys"] = [RUN_OPTIONS_RECIPE_KEY]
        out["message"] = (
            f"Dropped `{RUN_OPTIONS_RECIPE_KEY}` from the saved recipe: every "
            "scheduled run of the source would have applied it."
        )
    return out


def _set_dotted(target: dict, path: str, value: JsonValue) -> None:
    parts = path.split(".")
    node = target
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[parts[-1]] = value


def _del_dotted(target: dict, path: str) -> bool:
    """Delete a dotted path; whether there was anything to delete."""
    parts = path.split(".")
    node = target
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            return False
        node = child
    if parts[-1] not in node:
        return False
    del node[parts[-1]]
    return True


@destructive
@idempotent
def update_ingestion_source(
    source_urn: str,
    config_updates: OptionalJsonObject = None,
    remove_fields: OptionalStrList = None,
    name: Optional[str] = None,
    schedule: Optional[str] = None,
    executor_id: Optional[str] = None,
) -> dict:
    """Change named fields of an ingestion source's recipe, keeping every other field.

    `config_updates` maps dotted paths under `source.config` to new values, for
    example {"profiling.enabled": true}. `remove_fields` deletes paths. Do not
    send the whole recipe back: the stored connection details are kept as they
    are. The executor, CLI version pin, extra args and schedule are kept unless
    you change them; a new `schedule` keeps the stored timezone. Credentials
    must be ${SECRET_NAME} references.
    """
    if not (config_updates or remove_fields or name or schedule or executor_id):
        raise ToolError(
            "Nothing to change: pass config_updates, remove_fields, "
            "name, schedule or executor_id."
        )
    require_secret_references(config_updates or {}, "config_updates")
    source = existing_source(source_urn)
    urn = str(source.get("urn") or source_urn.strip())
    recipe = copy.deepcopy(stored_recipe(source))
    recipe_source = recipe.setdefault("source", {})
    if not isinstance(recipe_source, dict):
        raise ToolError(f"The stored recipe of {urn} has no `source` mapping.")
    source_config = recipe_source.setdefault("config", {})
    if not isinstance(source_config, dict):
        raise ToolError(f"The stored recipe of {urn} has no `source.config` mapping.")

    applied: JsonObject = {}
    for path, value in (config_updates or {}).items():
        _set_dotted(source_config, path, value)
        applied[path] = value
    removed: list[str] = []
    not_found: list[str] = []
    for path in remove_fields or []:
        (removed if _del_dotted(source_config, path) else not_found).append(path)

    # updateIngestionSource replaces the whole info aspect: anything not sent
    # again (CLI version, debug mode, extra args, provenance) would be lost.
    stored = source.get("config")
    stored_config = stored if isinstance(stored, dict) else {}
    config_input: dict = {
        "recipe": json.dumps(recipe),
        "executorId": (executor_id or "").strip()
        or stored_config.get("executorId")
        or DEFAULT_EXECUTOR_ID,
    }
    if stored_config.get("version"):
        config_input["version"] = stored_config["version"]
    if stored_config.get("debugMode") is not None:
        config_input["debugMode"] = stored_config["debugMode"]
    extra_args = stored_config.get("extraArgs")
    if isinstance(extra_args, list) and extra_args:
        config_input["extraArgs"] = [
            {"key": e["key"], "value": e.get("value")}
            for e in extra_args
            if isinstance(e, dict) and e.get("key") is not None
        ]
    source_input: dict = {
        "name": (name or "").strip() or source.get("name") or "",
        "type": source.get("type") or "",
        "config": config_input,
    }
    provenance = source.get("source")
    if isinstance(provenance, dict) and provenance.get("type"):
        source_input["source"] = {"type": provenance["type"]}
    stored_schedule = source.get("schedule")
    schedule_obj = stored_schedule if isinstance(stored_schedule, dict) else {}
    interval = (schedule or "").strip() or schedule_obj.get("interval")
    if interval:
        source_input["schedule"] = {
            "interval": interval,
            "timezone": schedule_obj.get("timezone") or "UTC",
        }

    graphql_helpers.execute_write(
        graphql_helpers.get_datahub_client()._graph,
        query=graphql_helpers.load_gql_document("ingestion.gql"),
        operation_name="updateIngestionSource",
        variables={"urn": urn, "input": source_input},
        unknown_outcome_hint=(
            f"Read it with list_ingestion_sources(source_urn={urn!r}) to see "
            "whether the change applied; sending the same change again is safe."
        ),
    )
    return {
        "urn": urn,
        "applied": applied,
        "removed": removed,
        "not_found": not_found,
    }
