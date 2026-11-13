import importlib.resources
import sys

import click


def _append_agent_context(resource_name: str, formatter: click.HelpFormatter) -> None:
    if not sys.stdout.isatty():
        text = (
            importlib.resources.files("datahub.cli.resources")
            .joinpath(resource_name)
            .read_text(encoding="utf-8")
        )
        formatter.write("\n" + text)


def make_agent_aware_command(resource_name: str) -> type[click.Command]:
    class AgentAwareCommand(click.Command):
        def format_help(
            self, ctx: click.Context, formatter: click.HelpFormatter
        ) -> None:
            super().format_help(ctx, formatter)
            _append_agent_context(resource_name, formatter)

    return AgentAwareCommand


def make_agent_aware_group(resource_name: str) -> type[click.Group]:
    class AgentAwareGroup(click.Group):
        def format_help(
            self, ctx: click.Context, formatter: click.HelpFormatter
        ) -> None:
            super().format_help(ctx, formatter)
            _append_agent_context(resource_name, formatter)

    return AgentAwareGroup
