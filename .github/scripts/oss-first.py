#!/usr/bin/env -S uv run
# /// script
# requires-python = ">=3.13"
# dependencies = ["loguru", "PyYAML", "typer", "pydantic"]
# ///

import collections
import copy
import json
import subprocess
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Dict, List, Literal, Optional, Tuple

import typer
import yaml
from loguru import logger

_script_dir = Path(__file__).parent
_rules_file = _script_dir / "oss-first-rules.yml"
_exceptions_file = _script_dir / "oss-first-exceptions.json"

app = typer.Typer()

_oss_branch = "master"


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
    ignore_whitespace: bool = False


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
        limits_dict = {}
        for key, value in rule_dict.items():
            if key in ("pattern", "change_type"):
                continue
            if key == "max_removals":
                limits_dict["max_deletions"] = value
            elif key == "whitespace":
                limits_dict["ignore_whitespace"] = value
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
        allow_exception_loosening: bool = False,
        allow_exception_tightening: bool = False,
    ):
        self._change_specifier = change_specifier
        self._rules = rules

        self._original_exceptions = exceptions
        self._used_exceptions = set[str]()
        self._exceptions = copy.deepcopy(exceptions)

        self._allow_new_exceptions = allow_new_exceptions
        self._allow_exception_loosening = allow_exception_loosening
        self._allow_exception_tightening = allow_exception_tightening

    @property
    def _allow_exception_changes(self) -> bool:
        return (
            self._allow_new_exceptions
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

    def get_applicable_rule(self, change: FileChange) -> Rule | ExceptionLimit | None:
        """Find the first matching rule for a file change, considering exceptions."""
        # Check exceptions first
        if change.filepath in self._exceptions:
            self._used_exceptions.add(change.filepath)
            return self._exceptions[change.filepath]

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
            rule = self.get_applicable_rule(change)

            # TODO: When in tightening mode, we should be able to
            # remove exceptions that are no longer needed.

            if isinstance(rule, ExceptionLimit):
                # logger.info(f"Exception found for {change.filepath}: {rule}")
                sub_errors = self._check_change_against_exception(change, rule)
                errors.extend(sub_errors)
            elif isinstance(rule, Rule):
                # logger.info(f"Rule found for {change.filepath}: {rule}")
                sub_errors = self._check_change_against_rule(change, rule)
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

    def _check_change_against_rule(self, change: FileChange, rule: Rule) -> List[str]:
        """Check if a change is allowed by a rule."""
        errors = []
        limits = rule.limits

        # Skip validation if whitespace changes are ignored
        if limits.ignore_whitespace:
            # TODO: Implement whitespace-only change detection
            raise NotImplementedError(
                "Whitespace-only change detection not implemented"
            )

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

        if self._allow_new_exceptions and errors:
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
            logger.error(f"Found {len(errors)} diff size violations:")
            for message in errors:
                logger.error(f"  - {message}")
            exit(1)

        if self._allow_exception_tightening:
            self._remove_unused_exceptions()

        if self._allow_exception_changes:
            self._print_exception_change_summary()
        else:
            logger.info("Success: no new diff size violations")

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
    change_specifier: str = f"{_oss_branch}...",
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
    )
    validator.run()

    if loosen or tighten:
        updated_exceptions = validator.get_updated_exceptions()
        save_exceptions(_exceptions_file, updated_exceptions)


@app.command()
def show_diff(
    filepath: str,
    change_specifier: str = f"{_oss_branch}...",
):
    subprocess.run(["git", "diff", change_specifier, "--", filepath], check=True)


@app.command()
def restore_oss(filepath: str):
    subprocess.run(["git", "checkout", _oss_branch, "--", filepath], check=True)


if __name__ == "__main__":
    app()
