import json
from typing import Callable, Dict, List, Optional, Sequence, Tuple

import click

from datahub.cli.edit_history import EditHistory, HistoryEntry
from datahub.cli.edit_operations import (
    SYSTEM_OWNERSHIP_TYPES,
    EditCliError,
    EditOp,
    EditPlan,
    apply_op,
    owner_urn,
    plan_deprecation,
    plan_description,
    plan_domain,
    plan_owner,
    plan_tag,
    plan_term,
    require_exists,
)
from datahub.emitter.mce_builder import make_domain_urn, make_tag_urn, make_term_urn
from datahub.ingestion.graph.client import DataHubGraph, get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.upgrade import upgrade

Planner = Callable[[DataHubGraph, str], Optional[EditPlan]]


@click.group()
def edit() -> None:
    """Edit tags, terms, owners, domain, docs and deprecation on data assets.

    \b
    Every edit reads the entity first and is skipped when it would change
    nothing. Edits that change something are recorded in a local history
    (~/.datahub/edit_history.json, per server) so `datahub edit undo` can
    revert them. Output is JSON.

    \b
    Examples:
      datahub edit add-tag --urn "urn:li:dataset:(...)" --tag pii
      datahub edit add-owner --urn "urn:li:dataset:(...)" --owner group:data-eng
      datahub edit set-description --urn "urn:li:chart:(...)" --text "Daily revenue"
      datahub edit undo
    """


_URN_OPTION = click.option(
    "--urn",
    "urns",
    required=True,
    multiple=True,
    help="Entity URN. Repeat to apply the same edit to several entities.",
)
_DRY_RUN_OPTION = click.option(
    "--dry-run",
    is_flag=True,
    help="Show what would change without changing anything.",
)


def _echo(data: Dict) -> None:
    click.echo(json.dumps(data, indent=2))


def _apply_all(
    graph: DataHubGraph, plans: Sequence[EditPlan]
) -> Tuple[List[EditPlan], Optional[Exception]]:
    applied: List[EditPlan] = []
    for plan in plans:
        try:
            apply_op(graph, plan.forward)
        except Exception as e:
            return applied, e
        applied.append(plan)
    return applied, None


def _run_edit(
    command: str,
    urns: Sequence[str],
    planner: Planner,
    dry_run: bool,
    reference: Optional[Tuple[str, str]] = None,
) -> None:
    """Plan, apply and record one edit command.

    ``reference`` is the (urn, how-to-create hint) of the tag, term, owner or
    domain being attached, checked once before any entity is planned.
    """
    history = EditHistory.load()
    with get_default_graph(ClientMode.CLI) as graph:
        if reference is not None:
            require_exists(graph, *reference)
        # Plan every entity before touching any, so a typo in the third URN
        # does not leave the first two edited.
        planned = [(urn, planner(graph, urn)) for urn in dict.fromkeys(urns)]
        plans = [plan for _, plan in planned if plan is not None]
        unchanged = [urn for urn, plan in planned if plan is None]
        if dry_run:
            _echo(
                {
                    "dry_run": True,
                    "would_apply": [p.forward.to_dict() for p in plans],
                    "unchanged": unchanged,
                }
            )
            return

        applied, error = _apply_all(graph, plans)
        if applied:
            history.record(
                graph.config.server,
                HistoryEntry(
                    command=command,
                    forward=[p.forward for p in applied],
                    inverse=[p.inverse for p in reversed(applied)],
                ),
            )
            history.save()
        if error is not None and not applied:
            raise EditCliError(
                f"Edit failed: {error}", error_type="edit_failed"
            ) from error
        if error is not None:
            raise EditCliError(
                f"Applied {len(applied)} of {len(plans)} edits, then failed: {error}",
                error_type="partial_failure",
                suggestion="Run `datahub edit undo` to revert the edits that "
                "were applied, or rerun the command; it skips entities already "
                "in the requested state.",
            ) from error
        _echo(
            {
                "changed": [p.forward.to_dict() for p in applied],
                "unchanged": unchanged,
            }
        )


def _replay(ops: Sequence[EditOp], graph: DataHubGraph, action: str) -> None:
    for i, op in enumerate(ops):
        try:
            apply_op(graph, op)
        except Exception as e:
            raise EditCliError(
                f"{action} stopped after {i} of {len(ops)} operations: {e}",
                error_type=f"{action}_failed",
                suggestion=f"Fix the cause and run `datahub edit {action}` again; "
                "operations that already ran are safe to repeat.",
            ) from e


@edit.command("add-tag")
@_URN_OPTION
@click.option("--tag", required=True, help="Tag name or urn:li:tag URN.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def add_tag(urns: Tuple[str, ...], tag: str, dry_run: bool) -> None:
    """Add a tag to entities."""
    _run_edit(
        "add-tag",
        urns,
        lambda g, u: plan_tag(g, u, tag, add=True),
        dry_run,
        reference=(make_tag_urn(tag), "Create it with `datahub tag create`."),
    )


@edit.command("remove-tag")
@_URN_OPTION
@click.option("--tag", required=True, help="Tag name or urn:li:tag URN.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def remove_tag(urns: Tuple[str, ...], tag: str, dry_run: bool) -> None:
    """Remove a tag from entities."""
    _run_edit("remove-tag", urns, lambda g, u: plan_tag(g, u, tag, add=False), dry_run)


@edit.command("add-term")
@_URN_OPTION
@click.option("--term", required=True, help="Glossary term id or URN.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def add_term(urns: Tuple[str, ...], term: str, dry_run: bool) -> None:
    """Add a glossary term to entities."""
    _run_edit(
        "add-term",
        urns,
        lambda g, u: plan_term(g, u, term, add=True),
        dry_run,
        reference=(
            make_term_urn(term),
            "Create it with `datahub glossary create-term`.",
        ),
    )


@edit.command("remove-term")
@_URN_OPTION
@click.option("--term", required=True, help="Glossary term id or URN.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def remove_term(urns: Tuple[str, ...], term: str, dry_run: bool) -> None:
    """Remove a glossary term from entities."""
    _run_edit(
        "remove-term", urns, lambda g, u: plan_term(g, u, term, add=False), dry_run
    )


_OWNER_OPTION = click.option(
    "--owner",
    required=True,
    help="User name (alice), group:<name> for a group, or a corpuser/corpGroup URN.",
)
_OWNERSHIP_TYPE_OPTION = click.option(
    "--ownership-type",
    default="technical",
    show_default=True,
    help=f"One of {', '.join(sorted(SYSTEM_OWNERSHIP_TYPES))}, "
    "or an urn:li:ownershipType URN.",
)


@edit.command("add-owner")
@_URN_OPTION
@_OWNER_OPTION
@_OWNERSHIP_TYPE_OPTION
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def add_owner(
    urns: Tuple[str, ...], owner: str, ownership_type: str, dry_run: bool
) -> None:
    """Add an owner to entities."""
    _run_edit(
        "add-owner",
        urns,
        lambda g, u: plan_owner(g, u, owner, ownership_type, add=True),
        dry_run,
        reference=(
            owner_urn(owner),
            "Create the user or group first (`datahub user add`, `datahub group upsert`).",
        ),
    )


@edit.command("remove-owner")
@_URN_OPTION
@_OWNER_OPTION
@_OWNERSHIP_TYPE_OPTION
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def remove_owner(
    urns: Tuple[str, ...], owner: str, ownership_type: str, dry_run: bool
) -> None:
    """Remove an owner (of the given ownership type) from entities."""
    _run_edit(
        "remove-owner",
        urns,
        lambda g, u: plan_owner(g, u, owner, ownership_type, add=False),
        dry_run,
    )


@edit.command("set-domain")
@_URN_OPTION
@click.option("--domain", required=True, help="Domain id or urn:li:domain URN.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def set_domain(urns: Tuple[str, ...], domain: str, dry_run: bool) -> None:
    """Put entities in a domain."""
    _run_edit(
        "set-domain",
        urns,
        lambda g, u: plan_domain(g, u, domain),
        dry_run,
        reference=(make_domain_urn(domain), "Create it with `datahub domain create`."),
    )


@edit.command("unset-domain")
@_URN_OPTION
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def unset_domain(urns: Tuple[str, ...], dry_run: bool) -> None:
    """Remove entities from their domain."""
    _run_edit("unset-domain", urns, lambda g, u: plan_domain(g, u, None), dry_run)


@edit.command("set-description")
@_URN_OPTION
@click.option("--text", required=True, help="New description (markdown).")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def set_description(urns: Tuple[str, ...], text: str, dry_run: bool) -> None:
    """Set the description shown in the UI (the editable one)."""
    _run_edit(
        "set-description", urns, lambda g, u: plan_description(g, u, text), dry_run
    )


@edit.command("deprecate")
@_URN_OPTION
@click.option("--note", default="", help="Why, and what to use instead.")
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def deprecate(urns: Tuple[str, ...], note: str, dry_run: bool) -> None:
    """Mark entities deprecated."""
    _run_edit(
        "deprecate",
        urns,
        lambda g, u: plan_deprecation(g, u, deprecated=True, note=note),
        dry_run,
    )


@edit.command("undeprecate")
@_URN_OPTION
@_DRY_RUN_OPTION
@upgrade.check_upgrade
def undeprecate(urns: Tuple[str, ...], dry_run: bool) -> None:
    """Clear deprecation on entities."""
    _run_edit(
        "undeprecate",
        urns,
        lambda g, u: plan_deprecation(g, u, deprecated=False),
        dry_run,
    )


@edit.command("history")
@upgrade.check_upgrade
def history() -> None:
    """List edits on the current server that `undo` can revert, oldest first."""
    edits = EditHistory.load()
    with get_default_graph(ClientMode.CLI) as graph:
        server = graph.config.server
    _echo(
        {
            "server": server,
            "history_file": str(edits.path),
            "edits": [
                {
                    "command": e.command,
                    "at": e.at,
                    "operations": [op.to_dict() for op in e.forward],
                }
                for e in edits.entries(server)
            ],
        }
    )


@edit.command("undo")
@upgrade.check_upgrade
def undo() -> None:
    """Revert the last `datahub edit` command run against the current server."""
    edits = EditHistory.load()
    with get_default_graph(ClientMode.CLI) as graph:
        server = graph.config.server
        entry = edits.peek_undo(server)
        _replay(entry.inverse, graph, "undo")
    edits.mark_undone(server)
    edits.save()
    _echo(
        {
            "undone": entry.command,
            "applied": [op.to_dict() for op in entry.inverse],
        }
    )


@edit.command("redo")
@upgrade.check_upgrade
def redo() -> None:
    """Re-apply the last edit that `undo` reverted."""
    edits = EditHistory.load()
    with get_default_graph(ClientMode.CLI) as graph:
        server = graph.config.server
        entry = edits.peek_redo(server)
        _replay(entry.forward, graph, "redo")
    edits.mark_redone(server)
    edits.save()
    _echo(
        {
            "redone": entry.command,
            "applied": [op.to_dict() for op in entry.forward],
        }
    )
