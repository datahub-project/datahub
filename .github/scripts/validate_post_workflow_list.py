#!/usr/bin/env python3
"""Validate that post-workflow-actions.yml lists all workflows with push/PR triggers.

GitHub Actions' workflow_run.workflows does not support wildcards, so every workflow
that should be monitored must be listed explicitly. This script checks that the list
is complete and contains no stale entries.

Usage:
    python .github/scripts/validate_post_workflow_list.py
"""

import pathlib
import sys
from typing import Any

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


def main() -> int:
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
            data: dict[Any, Any] = yaml.safe_load(workflow_file.read_text())
        except yaml.YAMLError as e:
            print(f"WARNING: Could not parse {workflow_file.name}: {e}")
            continue

        if not isinstance(data, dict):
            continue

        if has_pr_or_push_trigger(get_on_value(data)):
            name = data.get("name")
            if name:
                if name not in IGNORED_WORKFLOWS:
                    expected.add(str(name))
                else:
                    print(f"{workflow_file.name} is ignored")
            else:
                print(
                    f"WARNING: {workflow_file.name} has a push/PR trigger but no 'name' field"
                )

    missing = expected - listed
    extra = listed - expected

    lines: list[str] = ["## post-workflow-actions.yml Validation\n"]

    if not missing and not extra:
        lines.append(
            f":white_check_mark: In sync — {len(listed)} workflows listed.\n"
        )
    else:
        if missing:
            lines.append(
                ":x: **Missing** — have push/PR triggers but are NOT listed:\n"
            )
            for name in sorted(missing):
                lines.append(f"- `{name}`")
            lines.append("")
        if extra:
            lines.append(
                ":x: **Extra** — listed but have no push/PR trigger (stale or renamed?):\n"
            )
            for name in sorted(extra):
                lines.append(f"- `{name}`")
            lines.append("")
        lines.append(
            "> To fix: update the `workflows:` list in `post-workflow-actions.yml`."
        )

    summary = "\n".join(lines)
    print(summary)
    SUMMARY_FILE.write_text(summary)

    return 0 if not missing and not extra else 1


if __name__ == "__main__":
    sys.exit(main())