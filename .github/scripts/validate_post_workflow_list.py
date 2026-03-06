#!/usr/bin/env python3
"""Validate that post-workflow-actions.yml lists all workflows with push/PR triggers.

GitHub Actions' workflow_run.workflows does not support wildcards, so every workflow
that should be monitored must be listed explicitly. This script checks that the list
is complete and contains no stale entries.

Usage:
    python .github/scripts/validate_post_workflow_list.py
"""

import os
import pathlib
import sys
from typing import Any, cast

import yaml

WORKFLOWS_DIR = pathlib.Path(".github/workflows")
POST_WORKFLOW_FILE = WORKFLOWS_DIR / "post-workflow-actions.yml"
PR_PUSH_TRIGGERS = {"push", "pull_request", "pull_request_target"}

IGNORED_WORKFLOWS = {
    "Create Linear Ticket for PR & Issue Review",
    "PR & Issue Routing (Linear Shadow Mode)",
    "PR Comment",
    "Pull Request Labeler"
}


def get_on_value(data: dict[Any, Any]) -> Any:
    # PyYAML 5.x parses bare 'on' as the boolean True (YAML 1.1 legacy).
    return data.get(True) or data.get("on") or {}


def has_pr_or_push_trigger(on_value: Any) -> bool:
    if isinstance(on_value, str):
        return on_value in PR_PUSH_TRIGGERS
    if isinstance(on_value, list):
        return bool(set(on_value) & PR_PUSH_TRIGGERS)
    if isinstance(on_value, dict):
        return bool(set(on_value.keys()) & PR_PUSH_TRIGGERS)
    return False


SUMMARY_FILE = pathlib.Path("post-workflow-validation-summary.md")


def _plain_output(missing: set[str], extra: set[str], listed: set[str]) -> str:
    lines: list[str] = []
    if not missing and not extra:
        lines.append(f"OK: post-workflow-actions.yml is in sync ({len(listed)} workflows listed).")
    else:
        if missing:
            lines.append("ERROR: The following workflows have push/PR triggers but are NOT listed in post-workflow-actions.yml:")
            for name in sorted(missing):
                lines.append(f"  - {name}")
        if extra:
            lines.append("ERROR: The following names are listed but have no push/PR trigger (stale or renamed?):")
            for name in sorted(extra):
                lines.append(f"  - {name}")
        lines.append("To fix: update the 'workflows:' list in .github/workflows/post-workflow-actions.yml.")
    return "\n".join(lines)


def _markdown_output(missing: set[str], extra: set[str], listed: set[str]) -> str:
    lines: list[str] = ["## post-workflow-actions.yml Validation\n"]
    if not missing and not extra:
        lines.append(f":white_check_mark: In sync — {len(listed)} workflows listed.\n")
    else:
        if missing:
            lines.append(":x: **Missing** — have push/PR triggers but are NOT listed:\n")
            for name in sorted(missing):
                lines.append(f"- `{name}`")
            lines.append("")
        if extra:
            lines.append(":x: **Extra** — listed but have no push/PR trigger (stale or renamed?):\n")
            for name in sorted(extra):
                lines.append(f"- `{name}`")
            lines.append("")
        lines.append("> To fix: update the `workflows:` list in `post-workflow-actions.yml`.")
    return "\n".join(lines)


def main() -> int:
    is_ci = bool(os.environ.get("CI"))

    if not POST_WORKFLOW_FILE.exists():
        raise FileNotFoundError(f"ERROR: {POST_WORKFLOW_FILE} not found.")

    post_data = yaml.safe_load(POST_WORKFLOW_FILE.read_text())
    on_value = get_on_value(post_data)
    listed: set[str] = set(on_value.get("workflow_run", {}).get("workflows", []))

    expected: set[str] = set()
    for workflow_file in sorted(WORKFLOWS_DIR.glob("*.yml")):
        if workflow_file.resolve() == POST_WORKFLOW_FILE.resolve():
            continue
        try:
            data: Any = yaml.safe_load(workflow_file.read_text())
        except yaml.YAMLError as e:
            print(f"WARNING: Could not parse {workflow_file.name}: {e}")
            continue

        if not isinstance(data, dict):
            continue

        typed_data = cast(dict[str, Any], data)
        if has_pr_or_push_trigger(get_on_value(typed_data)):
            name = typed_data.get("name")
            if name:
                if name not in IGNORED_WORKFLOWS:
                    expected.add(str(name))
                else:
                    print(f"INFO: {workflow_file.name} is ignored")
            else:
                print(f"WARNING: {workflow_file.name} has a push/PR trigger but no 'name' field")

    missing = expected - listed
    extra = listed - expected

    if is_ci:
        markdown = _markdown_output(missing, extra, listed)
        print(markdown)
        SUMMARY_FILE.write_text(markdown)
    else:
        print(_plain_output(missing, extra, listed))

    return 0 if not missing and not extra else 1


if __name__ == "__main__":
    sys.exit(main())