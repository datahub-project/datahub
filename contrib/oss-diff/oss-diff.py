#!/usr/bin/env -S uv run
# /// script
# # We use pathlib.Path.full_match(), which was added in Python 3.13.
# requires-python = ">=3.13"
# dependencies = ["loguru", "PyYAML", "typer", "dash", "pandas"]
# [tool.uv]
# exclude-newer = "2025-04-07T00:00:00Z"
# ///

import collections
import copy
import json
import math
import os
import subprocess
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import dash
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import typer
import yaml
from dash import dcc, html
from loguru import logger

_script_dir = Path(__file__).parent
_rules_file = _script_dir / "oss-diff-rules.yml"
_exceptions_file = _script_dir / "oss-diff-exceptions.json"

_repo_root = _script_dir.parent.parent
assert (_repo_root / ".git").exists(), f"Expected {_repo_root} to be a git repo"

app = typer.Typer()

_default_oss_branch = os.getenv("OSS_BRANCH", "master")
_default_saas_branch = os.getenv("SAAS_BRANCH", "")  # empty = HEAD
_default_change_specifier = f"{_default_oss_branch}...{_default_saas_branch}"


class ChangeType(Enum):
    ADDED = auto()
    DELETED = auto()
    RENAMED = auto()
    MODIFIED = auto()


@dataclass
class FileChange:
    type: ChangeType
    filepath: str
    additions: int
    deletions: int

    @property
    def total_changes(self) -> int:
        return self.additions + self.deletions


_inf: Literal["inf"] = "inf"


@dataclass
class ExceptionLimit:
    filepath: str
    additions: Optional[int | Literal["inf"]] = None
    deletions: Optional[int | Literal["inf"]] = None


def load_exceptions(exceptions_file: Path) -> Dict[str, ExceptionLimit]:
    """Load exceptions from JSON file."""
    if not exceptions_file.exists():
        return {}

    with open(exceptions_file, "r") as f:
        exceptions_dict = json.load(f)

    exceptions = {}
    for filepath, limits_dict in exceptions_dict.items():
        # Convert JSON keys to our internal format
        limits = ExceptionLimit(
            filepath=filepath,
            additions=limits_dict.get("additions"),
            deletions=limits_dict.get("deletions"),
        )
        exceptions[filepath] = limits

    return exceptions


def save_exceptions(
    exceptions_file: Path, exceptions: Dict[str, ExceptionLimit]
) -> None:
    """Save exceptions to JSON file."""
    exceptions_dict = {
        filepath: {"additions": limit.additions, "deletions": limit.deletions}
        for filepath, limit in exceptions.items()
    }
    with open(exceptions_file, "w") as f:
        json.dump(exceptions_dict, f, indent=2, sort_keys=True)


@dataclass
class DiffLimit:
    max_additions: Optional[int] = None
    max_deletions: Optional[int] = None
    max_total: Optional[int] = None


@dataclass
class Rule:
    pattern: List[str]
    change_type: Optional[ChangeType]
    limits: DiffLimit


def load_rules(rules_file: Path) -> List[Rule]:
    """Load rules from YAML file."""
    if not rules_file.exists():
        return []

    with open(rules_file, "r") as f:
        config = yaml.safe_load(f)

    rules = []
    for rule_dict in config.get("rules", []):
        # Convert YAML keys to our internal format
        # TODO: Move to pydantic.
        limits_dict = {}
        for key, value in rule_dict.items():
            if key in ("pattern", "change_type"):
                continue
            if key == "max_removals":
                limits_dict["max_deletions"] = value
            else:
                limits_dict[key] = value

        limits = DiffLimit(**limits_dict)

        change_type = None
        if "change_type" in rule_dict:
            change_type = ChangeType[rule_dict["change_type"]]

        pattern = rule_dict["pattern"]
        if isinstance(pattern, str):
            pattern = [pattern]
        rules.append(Rule(pattern=pattern, change_type=change_type, limits=limits))

    return rules


class DiffValidator:
    def __init__(
        self,
        change_specifier: str,
        rules: List[Rule],
        exceptions: Dict[str, ExceptionLimit],
        allow_new_exceptions: bool = False,
        allow_exception_removal: bool = False,
        allow_exception_loosening: bool = False,
        allow_exception_tightening: bool = False,
    ):
        self._change_specifier = change_specifier
        self._rules = rules

        self._original_exceptions = exceptions
        self._used_exceptions = set[str]()
        self._exceptions = copy.deepcopy(exceptions)

        self._allow_new_exceptions = allow_new_exceptions
        self._allow_exception_removal = allow_exception_removal
        self._allow_exception_loosening = allow_exception_loosening
        self._allow_exception_tightening = allow_exception_tightening

    @property
    def _allow_exception_changes(self) -> bool:
        return (
            self._allow_new_exceptions
            or self._allow_exception_removal
            or self._allow_exception_loosening
            or self._allow_exception_tightening
        )

    @classmethod
    def _parse_numstat_line(cls, line: str) -> Tuple[int, int, str]:
        # 1	1	{datahub-graphql-core/src/main/java/com/linkedin/datahub/graphql/types/common/mappers/util => entity-registry/src/main/java/com/linkedin/metadata/utils}/RunInfo.java
        # 1	1	src/main/java/com/linkedin/datahub/graphql/types/common/mappers/util/RunInfo.java
        parts = line.split(maxsplit=2)

        if parts[0] == parts[1] == "-":
            adds = dels = 0
        else:
            adds = int(parts[0])
            dels = int(parts[1])

        filepath = parts[2]
        if "=>" in filepath:
            filepath = cls._parse_git_rename_syntax(filepath)

        return adds, dels, filepath

    @classmethod
    def _parse_git_rename_syntax(cls, filepath: str) -> str:
        # Handle git's rename/move format: prefix/{path1 => path2}/suffix
        logger.debug(f"Renamed file: {filepath}")
        parts = filepath.split("=>", maxsplit=1)
        assert len(parts) == 2
        prefix = parts[0].rsplit("{", 1)[0]
        suffix = parts[1].split("}", 1)[1] if "}" in parts[1] else ""
        new_path = parts[1].strip().strip("{}")
        filepath = prefix + new_path + suffix
        logger.debug(f"New path: {filepath}")

        # TODO: This has some bugs, which I need to come back to.
        return filepath

    def get_file_changes(self) -> List[FileChange]:
        """Get list of file changes with their types and sizes."""
        # Get file status changes
        status_result = subprocess.run(
            ["git", "diff", "--name-status", self._change_specifier],
            capture_output=True,
            text=True,
            check=True,
        )

        # Get file size changes
        stats_result = subprocess.run(
            ["git", "diff", "--numstat", self._change_specifier],
            capture_output=True,
            text=True,
            check=True,
        )

        # Parse stats into a dictionary for easy lookup
        stats: Dict[str, Tuple[int, int]] = {}
        for line in stats_result.stdout.splitlines():
            try:
                additions, deletions, filepath = self._parse_numstat_line(line)
            except ValueError:
                logger.warning(f"Failed to parse stats line: {line}")
            else:
                stats[filepath] = (additions, deletions)

        # Combine status and stats into FileChange objects
        changes: List[FileChange] = []
        for line in status_result.stdout.splitlines():
            if not line:
                continue

            parts = line.split()

            status = parts[0]
            # Handle different git diff output formats
            if status.startswith("R"):
                # Rename format: R100  old_file  new_file
                filepath = parts[2]  # Use the new file path
                status = "R"
            else:
                # Other formats: A/M/D  filepath
                filepath = parts[1]

            if status == "A":
                change_type = ChangeType.ADDED
            elif status == "D":
                change_type = ChangeType.DELETED
            elif status == "R":
                change_type = ChangeType.RENAMED
            elif status == "M":
                change_type = ChangeType.MODIFIED
            else:
                logger.warning(f"Unknown status {status} for {filepath}")
                continue

            # Get stats for the file, defaulting to (0,0) if not found
            if filepath not in stats:
                logger.warning(f"No stats found for {filepath}")
                continue

            additions, deletions = stats[filepath]
            changes.append(
                FileChange(
                    type=change_type,
                    filepath=filepath,
                    additions=additions,
                    deletions=deletions,
                )
            )

        return changes

    def get_applicable_check(self, change: FileChange) -> Rule | ExceptionLimit | None:
        """Find the first matching rule for a file change, considering exceptions."""
        # Check exceptions first
        if change.filepath in self._exceptions:
            self._used_exceptions.add(change.filepath)
            return self._exceptions[change.filepath]

        return self._get_applicable_rule(change)

    def _get_applicable_rule(self, change: FileChange) -> Rule | None:
        # Then check rules
        for rule in self._rules:
            if rule.change_type and rule.change_type != change.type:
                continue
            if any(
                Path(change.filepath).full_match(pattern)  # type: ignore
                for pattern in rule.pattern
            ):
                return rule

        return None

    def validate_changes(self, changes: List[FileChange]) -> List[str]:
        """Validate changes against rules and exceptions."""
        errors = []

        for change in changes:
            check = self.get_applicable_check(change)

            if isinstance(check, ExceptionLimit):
                # logger.info(f"Exception found for {change.filepath}: {rule}")
                if self._allow_exception_removal and (
                    rule := self._get_applicable_rule(change)
                ):
                    rule_errors = self._check_change_against_rule(
                        change, rule, is_simulation=True
                    )
                    if not rule_errors:
                        # If the rule passes, we can remove the exception.
                        logger.info(
                            f"Removing useless exception for {change.filepath}: {check} -> {rule}"
                        )
                        del self._exceptions[change.filepath]
                        continue

                sub_errors = self._check_change_against_exception(change, check)
                errors.extend(sub_errors)
            elif isinstance(check, Rule):
                # logger.info(f"Rule found for {change.filepath}: {rule}")
                sub_errors = self._check_change_against_rule(change, check)
                errors.extend(sub_errors)
            else:
                errors.append(f"No rule found for {change.filepath}")

        return errors

    def _check_change_against_exception(
        self, change: FileChange, exception: ExceptionLimit
    ) -> List[str]:
        """Check if a change is allowed by an exception."""

        errors = []
        if exception.additions == _inf:
            # If the exception was explicitly set to inf, leave it alone.
            pass
        elif exception.additions is not None and change.additions > exception.additions:
            if self._allow_exception_loosening:
                logger.info(
                    f"Loosening exception for {change.filepath}: {exception.additions} -> {change.additions}"
                )
                exception.additions = (
                    _inf if change.type == ChangeType.ADDED else change.additions
                )
            else:
                errors.append(
                    f"Too many additions for {change.filepath}: {change.additions} > {exception.additions}"
                )
        elif (
            self._allow_exception_tightening
            and exception.additions is not None
            and change.additions < exception.additions
        ):
            logger.info(
                f"Tightening exception for {change.filepath}: {exception.additions} -> {change.additions}"
            )
            exception.additions = change.additions

        if exception.deletions == _inf:
            # If the exception was explicitly set to inf, leave it alone.
            pass
        elif exception.deletions is not None and change.deletions > exception.deletions:
            if self._allow_exception_loosening:
                logger.info(
                    f"Loosening exception for {change.filepath}: {exception.deletions} -> {change.deletions}"
                )
                exception.deletions = (
                    _inf if change.type == ChangeType.DELETED else change.deletions
                )
            else:
                errors.append(
                    f"Too many deletions for {change.filepath}: {change.deletions} > {exception.deletions}"
                )
        elif (
            self._allow_exception_tightening
            and exception.deletions is not None
            and change.deletions < exception.deletions
        ):
            logger.info(
                f"Tightening exception for {change.filepath}: {exception.deletions} -> {change.deletions}"
            )
            exception.deletions = change.deletions

        return errors

    def _check_change_against_rule(
        self, change: FileChange, rule: Rule, is_simulation: bool = False
    ) -> List[str]:
        """Check if a change is allowed by a rule."""
        errors = []
        limits = rule.limits

        # Check addition limits
        if limits.max_additions is not None and change.additions > limits.max_additions:
            errors.append(
                f"Too many additions for {change.filepath}: "
                f"{change.additions} > {limits.max_additions}"
            )

        # Check deletion limits
        if limits.max_deletions is not None and change.deletions > limits.max_deletions:
            errors.append(
                f"Too many deletions for {change.filepath}: "
                f"{change.deletions} > {limits.max_deletions}"
            )

        # Check total change limits
        if limits.max_total is not None and change.total_changes > limits.max_total:
            errors.append(
                f"Too many total changes for {change.filepath}: "
                f"{change.total_changes} > {limits.max_total} "
                f"({change.additions} additions, {change.deletions} deletions)"
            )

        if not is_simulation and self._allow_new_exceptions and errors:
            if change.type == ChangeType.ADDED:
                exception = ExceptionLimit(
                    filepath=change.filepath, additions=_inf, deletions=None
                )
            elif change.type == ChangeType.DELETED:
                exception = ExceptionLimit(
                    filepath=change.filepath, additions=None, deletions=_inf
                )
            else:
                exception = ExceptionLimit(
                    filepath=change.filepath,
                    additions=(
                        change.additions
                        if (
                            limits.max_additions is not None
                            or limits.max_total is not None
                        )
                        else None
                    ),
                    deletions=(
                        change.deletions
                        if (
                            limits.max_deletions is not None
                            or limits.max_total is not None
                        )
                        else None
                    ),
                )
            self._exceptions[change.filepath] = exception
            return []  # No errors, because we just added an exception

        return errors

    def run(self) -> None:
        """Run the validation process."""
        # Get all changes
        changes = self.get_file_changes()

        # Print some summary stats - n added, n deleted, n modified.
        logger.info(f"Found changes to {len(changes)} files")
        counts = collections.Counter(change.type for change in changes)
        logger.info(f"  {counts[ChangeType.ADDED]} added")
        logger.info(f"  {counts[ChangeType.DELETED]} deleted")
        logger.info(f"  {counts[ChangeType.MODIFIED]} modified")
        logger.info(f"  {counts[ChangeType.RENAMED]} renamed")

        # Validate changes
        errors = self.validate_changes(changes)
        if errors:
            logger.error(f"Found {len(errors)} diff violations:")
            for message in errors:
                logger.error(f"  - {message}")
            exit(1)

        if self._allow_exception_tightening:
            self._remove_unused_exceptions()

        if self._allow_exception_changes:
            self._print_exception_change_summary()
        else:
            logger.info("Success: no new diff violations")

    def _remove_unused_exceptions(self) -> None:
        # TODO: Tricky - in order to fully implement exception removal, we'd need
        # to check the file against the rules to see if the file would pass
        # without the exception.
        unused_exceptions = set(self._exceptions) - self._used_exceptions
        logger.info(f"Removing {len(unused_exceptions)} unused exceptions")
        for filepath in unused_exceptions:
            logger.debug(f"Removing unused exception: {filepath}")
            del self._exceptions[filepath]

    def _print_exception_change_summary(self) -> None:
        new_exceptions = set(self._exceptions) - set(self._original_exceptions)
        removed_exceptions = set(self._original_exceptions) - set(self._exceptions)

        modified_exceptions = set()
        for filepath in set(self._exceptions) & set(self._original_exceptions):
            if self._exceptions[filepath] != self._original_exceptions[filepath]:
                modified_exceptions.add(filepath)

        logger.info("Summary of exception changes:")
        logger.info(f"  {len(new_exceptions)} new exceptions")
        logger.info(f"  {len(removed_exceptions)} exceptions removed")
        logger.info(f"  {len(modified_exceptions)} exceptions modified")

    def get_updated_exceptions(self) -> Dict[str, ExceptionLimit]:
        return self._exceptions


@app.command()
def check(
    change_specifier: str = _default_change_specifier,
    loosen: bool = False,
    tighten: bool = False,
):
    rules = load_rules(_rules_file)
    logger.info(f"Loaded {len(rules)} rules")
    logger.debug(f"Rules: {rules}")
    exceptions = load_exceptions(_exceptions_file)
    logger.info(f"Loaded {len(exceptions)} exceptions")
    validator = DiffValidator(
        change_specifier=change_specifier,
        rules=rules,
        exceptions=exceptions,
        allow_new_exceptions=loosen,
        allow_exception_loosening=loosen,
        allow_exception_tightening=tighten,
        allow_exception_removal=tighten,
    )
    validator.run()

    if loosen or tighten:
        updated_exceptions = validator.get_updated_exceptions()
        save_exceptions(_exceptions_file, updated_exceptions)


@app.command()
def show_diff(
    filepath: str,
    change_specifier: str = _default_change_specifier,
):
    subprocess.run(["git", "diff", change_specifier, "--", filepath], check=True)


@app.command()
def restore_from_oss(filepath: str, oss_branch: str = _default_oss_branch):
    subprocess.run(["git", "checkout", oss_branch, "--", filepath], check=True)


def _smart_split_path(filepath: str, max_parts: int) -> list[str]:
    raw_parts = filepath.split("/")

    parts: list[str] = []
    merge_into_prev = False
    i = 0
    while i < len(raw_parts):
        if not merge_into_prev and len(parts) > max_parts:
            parts.append("/".join(raw_parts[i:]))
            break
        else:
            part = raw_parts[i]
            if merge_into_prev:
                parts[-1] = parts[-1] + "/" + raw_parts[i]
                merge_into_prev = False
            else:
                parts.append(part)
            if part in {
                "src",
                "tests",
                "test",
                "main",
                "java",
                "com",
                "linkedin",
                "datahub",
                "cypress",
            }:
                merge_into_prev = True

        i += 1

    return parts


def _build_sunburst_chart(exceptions: Dict[str, ExceptionLimit]) -> go.Figure:
    """Build a DataFrame with directory levels for the sunburst chart."""

    def coerce(x: int | Literal["inf"] | None) -> int:
        if x is None:
            return 0
        if x == "inf":
            return 5
        return x

    rows = []

    for filepath, limits in exceptions.items():
        parts = _smart_split_path(filepath, max_parts=3)

        additions = limits.additions
        deletions = limits.deletions
        row = {
            "dir0": "<root>",
            "dir1": parts[0],
            "dir2": parts[1] if len(parts) > 1 else None,
            "dir3": parts[2] if len(parts) > 2 else None,
            "remaining": "/".join(parts[3:]) if len(parts) > 3 else None,
            "filepath": filepath,
            # Harshal's hacked-together heuristic for diff "badness".
            "value": max(1, math.log2(max(1, coerce(additions) + coerce(deletions)))),
            "additions": additions,
            "deletions": deletions,
        }
        rows.append(row)

    df = pd.DataFrame(rows)

    return px.sunburst(
        df,
        path=["dir0", "dir1", "dir2", "dir3", "remaining"],
        values="value",
        hover_data=["additions", "deletions", "filepath"],
        title="Diffs with OSS",
    )


@app.command()
def ui(
    debug: bool = False,
    change_specifier: str = _default_change_specifier,
) -> None:
    # Create Dash app
    app = dash.Dash(
        __name__,
        external_scripts=[
            "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.11.1/build/highlight.min.js",
            "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.11.1/build/languages/diff.min.js",
        ],
        external_stylesheets=[
            "https://unpkg.com/mvp.css",
            "https://cdn.jsdelivr.net/gh/highlightjs/cdn-release@11.11.1/build/styles/default.min.css",
        ],
    )

    # Create figure
    exceptions = load_exceptions(_exceptions_file)
    sunburst = _build_sunburst_chart(exceptions)
    # fig.update_layout(margin=dict(t=50, l=0, r=0, b=0), height=800)
    # fig.show()

    # Set up layout
    app.layout = html.Main(
        [
            dcc.Markdown(
                f"# OSS/SaaS Diff Viewer\n\nShowing {len(exceptions)} diff rule violations"
            ),
            dcc.Graph(id="sunburst", figure=sunburst, style={"height": "800px"}),
            html.Div(id="selected-file", style={"margin-top": "20px"}),
        ]
    )

    @app.callback(
        dash.Output(component_id="selected-file", component_property="children"),
        dash.Input(component_id="sunburst", component_property="clickData"),
    )
    def show_file_diff(clickData):
        if clickData is None:
            return dash.no_update

        # logger.info(f"Click data: {clickData}")
        filepath = clickData["points"][0]["customdata"][2]
        if filepath == "(?)":
            return dash.no_update

        logger.info(f"Showing diff for {filepath}")

        diff = subprocess.check_output(
            ["git", "diff", change_specifier, "--", filepath],
            text=True,
            cwd=_repo_root,
        )
        # logger.debug(f"Diff for {filepath}: {diff}")
        return dcc.Markdown(f"## Diff for `{filepath}`:\n\n```diff\n{diff}\n```")

    # Run app
    app.run(debug=debug)


if __name__ == "__main__":
    app()
