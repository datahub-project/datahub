"""Render the declared environment variables into the public reference page.

Output is a ``.md.snippet`` rather than a ``.md``: generateDocsDir.ts discovers pages by
extension, and a table fragment has no H1 for it to title. The snippet is pulled into the
hand-authored page with an ``{{ inline }}`` directive, the same arrangement
``sql_profiling_support_table.md.snippet`` uses.

Written to ``docs/generated/python-sdk/`` and NOT ``docs/generated/ingestion/``, because
docGen starts with ``rm -rf`` on the latter and would delete this on its way past.
"""

import importlib
import pathlib
import re
import sys
from typing import Dict, List

import click

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from env_var_scan import default_source_root, scan_tree

from datahub.configuration.env_var_registry import (
    EnvVarAudience,
    EnvVarGroup,
    EnvVarSpec,
    get_conflicts,
    get_registry,
)

# Only matches outside existing backticks; the negative lookbehind keeps already-formatted
# names from being wrapped twice.
_BARE_NAME = re.compile(r"(?<![`\w])[A-Z][A-Z0-9_]{3,}(?![`\w])")

_GROUP_BLURBS: Dict[EnvVarGroup, str] = {
    EnvVarGroup.CONNECTION: (
        "Where DataHub is and how to authenticate. These take precedence over `~/.datahubenv`, "
        "and a CLI flag takes precedence over them."
    ),
    EnvVarGroup.OAUTH: (
        "Set `DATAHUB_AUTH_TYPE` to obtain tokens from an identity provider instead of using a "
        "static `DATAHUB_GMS_TOKEN`. See "
        "[External OAuth providers](../docs/authentication/external-oauth-providers.md) for the "
        "full flow and precedence rules."
    ),
    EnvVarGroup.SINK: (
        "Defaults for a recipe that declares no `sink`. A recipe that declares one is unaffected."
    ),
    EnvVarGroup.REST_SINK: "Defaults for the `datahub-rest` sink; any recipe field wins over these.",
    EnvVarGroup.EMITTER: (
        "Transport tuning shared by the REST sink and the Python SDK's emitter and graph clients."
    ),
    EnvVarGroup.REPORTING: (
        "How many entries the ingestion report keeps. Each has an equivalent recipe flag, which "
        "takes precedence."
    ),
    EnvVarGroup.LOGGING: "Verbosity, colour, and the switches that gate what a probe may do.",
    EnvVarGroup.SQL: "Budgets and caches for SQL parsing and column-level lineage.",
    EnvVarGroup.CONNECTORS: "Settings that only affect one connector.",
    EnvVarGroup.RECORDING: "Capturing and replaying an ingestion run for debugging.",
    EnvVarGroup.EXECUTOR: (
        "Read by the ingestion executor rather than the CLI. Note that a recipe's "
        "`extra_env_vars` cannot override connection or `DATAHUB_AUTH_*` variables."
    ),
    EnvVarGroup.TELEMETRY: "Anonymous usage telemetry and error reporting.",
    EnvVarGroup.CLOUD: (
        "Read to recognise the surrounding runtime. The platform sets these; setting them by "
        "hand only misreports where ingestion ran."
    ),
    EnvVarGroup.DOCKER: "Used by `datahub docker quickstart` and local development.",
    EnvVarGroup.MISC: "Everything else.",
}


def format_default(spec: EnvVarSpec) -> str:
    if spec.default is None:
        return "_(unset)_"
    if spec.default == "":
        return "_(empty)_"
    if isinstance(spec.default, bool):
        return f"`{str(spec.default).lower()}`"
    return f"`{spec.default}`"


def backtick_known_names(text: str, known: "frozenset[str]") -> str:
    """Wrap bare variable names in code formatting.

    Descriptions name sibling variables in prose; typing the backticks by hand in every
    docstring is the kind of thing that gets done inconsistently.
    """

    def repl(match: "re.Match[str]") -> str:
        name = match.group(0)
        return f"`{name}`" if name in known else name

    return _BARE_NAME.sub(repl, text)


def format_description(spec: EnvVarSpec, known: "frozenset[str]") -> str:
    parts: List[str] = []
    if spec.risky:
        parts.append("⚠️ **Changing this may have side effects.**")
    if spec.deprecated:
        replacement = f" Use `{spec.replaced_by}` instead." if spec.replaced_by else ""
        version = f" since {spec.deprecated_in}" if spec.deprecated_in else ""
        parts.append(f"**Deprecated{version}.**{replacement}")
    if spec.audience is EnvVarAudience.AUTO_DETECTED:
        parts.append("**Detected, not set by you.**")
    if spec.secret:
        parts.append("**Secret.**")

    description = backtick_known_names(spec.description.strip(), known)
    # Docstrings write an em dash as " -- "; a CLI flag such as --debug has no surrounding
    # spaces, so this does not touch them.
    description = description.replace(" -- ", " — ")
    parts.append(description.replace("|", "\\|"))

    if spec.choices:
        parts.append("Accepts: " + ", ".join(f"`{c}`" for c in spec.choices) + ".")
    if spec.recipe_field:
        parts.append(f"Recipe field: `{spec.recipe_field}` (takes precedence).")
    if spec.since:
        parts.append(f"Added in {spec.since}.")
    return " ".join(parts)


def render(specs: List[EnvVarSpec]) -> str:
    by_group: Dict[EnvVarGroup, List[EnvVarSpec]] = {}
    for spec in specs:
        by_group.setdefault(spec.group, []).append(spec)

    # Basic first, then advanced, then experimental, so a reader meets the variables they are
    # most likely to need before the tuning knobs. Sorted here rather than relying on the
    # caller, so the ordering holds however the specs arrive.
    for group_specs in by_group.values():
        group_specs.sort(key=lambda spec: (spec.tier.order, spec.name))

    known = frozenset(spec.name for spec in specs)

    out: List[str] = []
    for group in sorted(by_group, key=lambda g: g.order):
        out.append(f"### {group.value}\n")
        blurb = _GROUP_BLURBS.get(group)
        if blurb:
            out.append(f"{blurb}\n")
        out.append("<div className='config-table'>\n")
        out.append("| Variable | Type | Default | Description |")
        out.append("| --- | --- | --- | --- |")
        for spec in by_group[group]:
            # Every row states its tier. Leaving the commonest one implicit meant a reader had
            # to know that no tag meant advanced.
            out.append(
                f"| `{spec.name}`<br />_{spec.tier.display_name}_ | {spec.display_type} "
                f"| {format_default(spec)} | {format_description(spec, known)} |"
            )
        out.append("\n</div>\n")
    return "\n".join(out) + "\n"


def load_declaring_modules(verbose: bool) -> List[str]:
    """Import every module that declares a variable, so the registry is complete.

    The module list comes from the static scan rather than a hand-kept list: declarations live
    beside the reads they describe, so any curated list would go stale the first time someone
    declared a variable in a new file.
    """
    scan = scan_tree(default_source_root())
    failed: List[str] = []
    for module in sorted(scan.declaring_modules):
        try:
            importlib.import_module(module)
        except Exception as exc:
            failed.append(f"{module}: {exc}")
            if verbose:
                click.echo(f"  ! could not import {module}: {exc}", err=True)

    conflicts = get_conflicts()
    if conflicts:
        raise click.ClickException(
            "The same variable is declared twice with different metadata, so the page would "
            "show whichever module imported first:\n"
            + "\n".join(f"  {name}" for name in sorted(conflicts))
        )

    missing = sorted(
        set(scan.declared) - {s.name for s in get_registry(include_undocumented=True)}
    )
    if missing:
        raise click.ClickException(
            "These variables are declared in the source but missing from the registry, so the "
            "reference page would silently omit them:\n"
            + "\n".join(f"  {name}  ({scan.declared[name]})" for name in missing)
            + (
                "\n\nModules that failed to import:\n"
                + "\n".join(f"  {f}" for f in failed)
                if failed
                else ""
            )
        )
    return failed


@click.command()
@click.option("--out-file", type=str, required=True)
@click.option("--verbose", is_flag=True, default=False)
def generate(out_file: str, verbose: bool) -> None:
    failed = load_declaring_modules(verbose)
    specs = get_registry()
    if not specs:
        raise click.ClickException(
            "No environment variables were declared; nothing to render."
        )

    out_path = pathlib.Path(out_file)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(render(specs))

    click.echo(f"Wrote {len(specs)} environment variables to {out_path}")
    if failed:
        click.echo(
            f"({len(failed)} declaring module(s) could not be imported)", err=True
        )


if __name__ == "__main__":
    generate()
