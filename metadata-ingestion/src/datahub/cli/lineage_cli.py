"""CLI command for lineage exploration."""

import importlib.resources
import json
import sys
from typing import Optional

import click

from datahub.ingestion.graph.client import get_default_graph
from datahub.ingestion.graph.config import ClientMode
from datahub.metadata.urns import ChartUrn, DashboardUrn, DatasetUrn, Urn
from datahub.sdk.lineage_client import LineageResult
from datahub.sdk.main_client import DataHubClient
from datahub.upgrade import upgrade


class _AgentAwareGroup(click.Group):
    """Group that appends agent context to --help when stdout is not a TTY."""

    def format_help(self, ctx: click.Context, formatter: click.HelpFormatter) -> None:
        super().format_help(ctx, formatter)
        if not sys.stdout.isatty():
            agent_text = (
                importlib.resources.files("datahub.cli.resources")
                .joinpath("LINEAGE_AGENT_CONTEXT.md")
                .read_text(encoding="utf-8")
            )
            formatter.write("\n")
            formatter.write(agent_text)


def _name_from_urn(urn_str: str) -> str:
    """Extract a readable name from a URN using proper URN utilities."""
    try:
        urn = Urn.from_string(urn_str)
        if isinstance(urn, DatasetUrn):
            return urn.name
        if isinstance(urn, ChartUrn):
            return urn.chart_id
        if isinstance(urn, DashboardUrn):
            return urn.dashboard_id
        return urn.entity_ids[-1] if urn.entity_ids else urn_str
    except Exception:
        return urn_str


def _format_table(results: list[LineageResult], direction: str) -> str:
    """Format lineage results as a human-readable table."""
    if not results:
        return f"No {direction} lineage found."

    lines = [
        f"{'Hop':<5} {'Type':<15} {'Platform':<15} {'Name':<40} URN",
        f"{'---':<5} {'---':<15} {'---':<15} {'---':<40} ---",
    ]
    for r in sorted(results, key=lambda x: (x.hops, x.urn)):
        name = r.name if r.name else _name_from_urn(r.urn)
        platform = r.platform or "?"
        lines.append(f"{r.hops:<5} {r.type:<15} {platform:<15} {name:<40} {r.urn}")
    return "\n".join(lines)


def _format_json(
    results: list[LineageResult], direction: str, hops: int, count: int
) -> str:
    """Format lineage results as JSON with metadata."""
    max_hop_seen = max((r.hops for r in results), default=0)
    metadata: dict = {
        "direction": direction,
        "hops_requested": hops,
        "max_hops_found": max_hop_seen,
        "count": len(results),
        "capped": len(results) >= count,
    }
    if len(results) >= count:
        metadata["hint"] = f"Results capped at {count}. Increase --count to see more."
    elif hops < 3 and len(results) > 0:
        metadata["hint"] = f"Showing {hops} hop(s). Increase --hops to see more."

    return json.dumps(
        {
            "metadata": metadata,
            "results": [
                {
                    "urn": r.urn,
                    "type": r.type,
                    "hops": r.hops,
                    "direction": r.direction,
                    "platform": r.platform or _platform_from_urn(r.urn),
                    "name": r.name if r.name else _name_from_urn(r.urn),
                }
                for r in results
            ],
        },
        indent=2,
    )


def _platform_from_urn(urn_str: str) -> Optional[str]:
    """Extract platform from a URN."""
    try:
        urn = Urn.from_string(urn_str)
        if isinstance(urn, (ChartUrn, DashboardUrn)):
            return urn.dashboard_tool
    except Exception:
        pass
    return None


@click.group(cls=_AgentAwareGroup, invoke_without_command=True)
@click.option("--urn", required=False, help="Entity URN to trace lineage for.")
@click.option(
    "--agent-context", is_flag=True, hidden=True, help="Print agent context and exit."
)
@click.option(
    "--direction",
    type=click.Choice(["upstream", "downstream"]),
    default="upstream",
    help="Direction of lineage traversal.",
)
@click.option(
    "--hops",
    type=int,
    default=3,
    help="Maximum number of hops (1, 2, or 3+ for unlimited). Default: 3 (full graph).",
)
@click.option(
    "--column",
    default=None,
    help="Column name for column-level lineage (datasets only).",
)
@click.option(
    "--count",
    type=int,
    default=100,
    help="Maximum number of results. Default: 100.",
)
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format.",
)
@click.pass_context
@upgrade.check_upgrade
def lineage(
    ctx: click.Context,
    urn: Optional[str],
    agent_context: bool,
    direction: str,
    hops: int,
    column: Optional[str],
    count: int,
    output_format: str,
) -> None:
    """Explore lineage for any DataHub entity.

    Examples:

      datahub lineage --urn "urn:li:dataset:(...)" --direction upstream

      datahub lineage --urn "urn:li:dataset:(...)" --direction downstream --hops 3

      datahub lineage --urn "urn:li:dataset:(...)" --column customer_id --direction upstream

      datahub lineage --urn "urn:li:chart:(...)" --direction upstream
    """
    if ctx.invoked_subcommand is not None:
        return

    if agent_context:
        text = (
            importlib.resources.files("datahub.cli.resources")
            .joinpath("LINEAGE_AGENT_CONTEXT.md")
            .read_text(encoding="utf-8")
        )
        click.echo(text)
        return

    if not urn:
        click.echo(ctx.get_help())
        ctx.exit(0)
        return

    # Validate column usage
    if column and "dataset:" not in urn:
        click.echo(
            "Error: --column is only supported for dataset URNs.",
            err=True,
        )
        ctx.exit(2)
        return

    if hops > 3:
        click.echo(
            f"Warning: --hops {hops} will be treated as unlimited (3+). "
            "This may return a large number of results.",
            err=True,
        )
    if count > 500:
        click.echo(
            f"Warning: --count {count} is very large and may be slow. "
            "Consider using a smaller value.",
            err=True,
        )

    with get_default_graph(ClientMode.CLI) as graph:
        client = DataHubClient(graph=graph)
        results = client.lineage.get_lineage(
            source_urn=urn,
            source_column=column,
            direction=direction,
            max_hops=hops,
            count=count,
        )

    # Summary
    max_hop_seen = max((r.hops for r in results), default=0)
    summary = f"{direction.title()} lineage: {len(results)} entities, up to {max_hop_seen} hops"
    if len(results) >= count:
        summary += f" (capped at --count {count}, increase to see more)"
    elif hops < 3 and len(results) > 0:
        summary += f" (showing {hops} hop{'s' if hops > 1 else ''}, increase --hops to see more)"

    if output_format == "json":
        click.echo(_format_json(results, direction, hops, count))
    else:
        click.echo(summary)
        click.echo()
        click.echo(_format_table(results, direction))


@lineage.command()
@click.option("--from", "from_urn", required=True, help="Source entity URN.")
@click.option("--to", "to_urn", required=True, help="Target entity URN.")
@click.option("--from-column", default=None, help="Source column (datasets only).")
@click.option("--to-column", default=None, help="Target column (datasets only).")
@click.option(
    "--format",
    "output_format",
    type=click.Choice(["table", "json"]),
    default="table",
    help="Output format.",
)
@upgrade.check_upgrade
def path(
    from_urn: str,
    to_urn: str,
    from_column: Optional[str],
    to_column: Optional[str],
    output_format: str,
) -> None:
    """Find lineage paths between two entities.

    Examples:

      datahub lineage path --from "urn:li:dataset:(...)" --to "urn:li:dashboard:(...)"

      datahub lineage path --from "urn:li:dataset:(...)" --from-column customer_id \\
                           --to "urn:li:dataset:(...)" --to-column cust_id
    """
    # Validate column usage
    for label, urn, col in [
        ("--from", from_urn, from_column),
        ("--to", to_urn, to_column),
    ]:
        if col and "dataset:" not in urn:
            click.echo(
                f"Error: {label}-column is only supported for dataset URNs.",
                err=True,
            )
            sys.exit(2)

    with get_default_graph(ClientMode.CLI) as graph:
        client = DataHubClient(graph=graph)
        # TODO: implement get_paths_between in LineageClient
        # For now, use get_lineage in the from→to direction and check if target appears
        results = client.lineage.get_lineage(
            source_urn=from_urn,
            source_column=from_column,
            direction="downstream",
            max_hops=3,
            count=200,
        )

        # Filter to only results that match the target
        matching = [r for r in results if r.urn == to_urn]
        if not matching:
            # Try upstream direction
            results = client.lineage.get_lineage(
                source_urn=to_urn,
                source_column=to_column,
                direction="downstream",
                max_hops=3,
                count=200,
            )
            matching = [r for r in results if r.urn == from_urn]

    if output_format == "json":
        click.echo(_format_json(matching or results, "path", 3, 200))
    else:
        if matching:
            click.echo(f"Path found ({matching[0].hops} hops):")
            click.echo(_format_table(matching, "path"))
        else:
            click.echo("No path found between the two entities within 3 hops.")
