#!/usr/bin/env python3
"""Render the PR comment that explains which tests this run selected.

Two steps, both in docker-unified:

* ``pytest``: run by ``smoke_test_matrix`` after the classifier, the plan and the
  matrix are decided. Reads the environment, so a missing classifier output or
  plan degrades to a shorter section rather than none, and writes the pytest
  section to ``COMMENT_FILE``.
* ``compose``: run by ``comment_smart_testing`` with the pytest section and the
  Playwright tier plan (either may be absent) to write the whole comment to
  ``COMMENT_FILE``, which that job upserts onto the PR, found by ``MARKER``.
"""

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from smart_testing_constants import (
    FAILSAFE_TEXT,
    PLAYWRIGHT_OPT_OUT_LABEL,
    PYTEST_OPT_OUT_LABEL,
    SELECTION_TEXT,
    FailSafeReason,
)

MARKER = "<!-- smart-testing-selection -->"
REASONING_MAX_CHARS = 300
MAX_LISTED_SCOPES = 100


def sanitize(text: str) -> str:
    """One line, bounded, with @mentions and HTML defanged.

    The reasoning is model output derived from the PR's diff, so it is untrusted.
    """
    flat = re.sub(r"\s+", " ", text).strip()
    if len(flat) > REASONING_MAX_CHARS:
        flat = flat[: REASONING_MAX_CHARS - 1].rstrip() + "…"
    return flat.replace("@", "@​").replace("<", "&lt;").replace(">", "&gt;")


def parse_tags(raw: str) -> Dict[str, List[str]]:
    try:
        data = json.loads(raw) if raw else {}
    except ValueError:
        return {}
    return {
        dim: [str(t) for t in tags]
        for dim, tags in data.items()
        if dim != "confidence" and isinstance(tags, list) and tags
    }


def load_plan(path: Optional[str]) -> Optional[Dict[str, Any]]:
    if not path:
        return None
    try:
        plan = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return None
    return plan if isinstance(plan, dict) else None


def describe_plan(plan: Dict[str, Any]) -> str:
    batches = int(plan.get("batch_count", 0))
    text = (
        f"{plan.get('test_count', '?')} of {plan.get('candidate_test_count', '?')} tests"
        f" in {batches} batch{'' if batches == 1 else 'es'}"
    )
    wall = plan.get("max_predicted_wall")
    if isinstance(wall, (int, float)) and batches:
        text += f", predicted longest batch ~{round(wall / 60)} min"
    return text


def planned_scopes(plan: Dict[str, Any]) -> List[str]:
    scopes = {
        str(scope)
        for batch in plan.get("batches") or []
        if isinstance(batch, dict)
        for scope in batch.get("scopes") or []
    }
    return sorted(scopes)


def render_scopes(scopes: List[str], test_count: Any) -> List[str]:
    shown = scopes[:MAX_LISTED_SCOPES]
    lines = [
        "",
        "<details>",
        f"<summary>Selected scopes ({len(scopes)} files or classes, {test_count} tests)</summary>",
        "",
    ]
    lines += [f"- `{scope}`" for scope in shown]
    if len(scopes) > len(shown):
        lines.append(
            f"- … and {len(scopes) - len(shown)} more (see the `pytest-smoke-plan` artifact)"
        )
    lines += ["", "</details>"]
    return lines


def describe_failsafe(reason: str) -> str:
    try:
        return FAILSAFE_TEXT[FailSafeReason(reason)]
    except (ValueError, KeyError):
        return reason


def render_pytest_section(env: Mapping[str, str]) -> str:
    mode = env.get("SMART_MODE", "")
    planned = env.get("SMART_SELECTION", "")
    applied = env.get("SELECTION_APPLIED", "")
    failsafe = env.get("FAILSAFE_REASON", "")
    confidence = env.get("SMART_CONFIDENCE", "") or "unknown"
    tags = parse_tags(env.get("SMART_TAGS", ""))
    plan = (
        load_plan(env.get("PLAN_FILE"))
        if env.get("PLAN_OUTCOME") == "success"
        else None
    )

    lines = ["### pytest smoke tests", ""]

    if mode == "active":
        lines.append(
            "**Mode: active.** Only the selection below runs for the pytest smoke tests "
            f"on this PR (applied: `{applied or 'unknown'}`)."
        )
    else:
        lines.append(
            "**Mode: shadow.** The selection below is computed but not applied; "
            f"this PR still runs the default matrix (`{applied or 'unknown'}`)."
        )
    lines.append("")

    if planned:
        what = SELECTION_TEXT.get(planned, planned)
        lines.append(f"- **Selection:** `{planned}`: {what}")
    else:
        lines.append("- **Selection:** none, the plan did not complete")
    if plan:
        lines.append(f"- **Size:** {describe_plan(plan)}")

    if tags:
        lines.append(
            "- **Tags:** "
            + "; ".join(
                f"{dim}: {', '.join(f'`{t}`' for t in values)}"
                for dim, values in sorted(tags.items())
            )
        )
    else:
        lines.append("- **Tags:** none")
    changed = env.get("CHANGED_FILES", "")
    lines.append(
        f"- **Confidence:** {confidence}"
        + (f" ({changed} changed files)" if changed else "")
    )

    reasoning = sanitize(env.get("SMART_REASONING", ""))
    if reasoning:
        lines.append(f"- **Classifier reasoning:** {reasoning}")
    if failsafe:
        lines.append(
            f"- **Fell back to the fail-safe because:** {describe_failsafe(failsafe)}"
        )

    if confidence == "MEDIUM":
        lines += [
            "",
            "Medium confidence: a wrong tag means tests are skipped, so check the tags above against what this PR touches.",
        ]
    if plan:
        scopes = planned_scopes(plan)
        if scopes:
            lines += render_scopes(scopes, plan.get("test_count", "?"))

    lines += [
        "",
        f"Add the `{PYTEST_OPT_OUT_LABEL}` label to run the full pytest suite on this PR.",
    ]

    return "\n".join(lines) + "\n"


# plan_playwright_tier.py reason for specs that only run because they carry @p0.
P0_REASON = "p0"


def render_playwright_section(plan: Dict[str, Any]) -> str:
    run_mode = plan.get("runMode", "unknown")
    selection = plan.get("selectionMode", "unknown")
    lines = ["### Playwright E2E tests", ""]
    if plan.get("tierRequested"):
        lines.append(f"**This run:** `{run_mode}`")
    else:
        lines.append(
            "**Shadow.** The tier below is computed but not applied; this PR still runs "
            "the full Playwright suite."
        )
    lines += [
        "",
        f"- **P0 tier selection:** `{selection}`: {sanitize(str(plan.get('selectionReason', '')))}",
    ]

    specs = plan.get("specs") or []
    if selection == "p0" and specs:
        selected = sum(int(s.get("selectedTests", 0)) for s in specs)
        minutes = round(float(plan.get("predictedSeconds", 0)) / 60)
        lines.append(
            f"- **Size:** {selected} tests in {len(specs)} spec file{'' if len(specs) == 1 else 's'}, "
            f"~{minutes} min predicted on {plan.get('totalShards', '?')} shard(s)"
        )
        # Most specs are the @p0 baseline every PR runs; only the others say
        # anything about this PR.
        affected = [
            s for s in specs if [str(r) for r in s.get("reasons", [])] != [P0_REASON]
        ]
        baseline = len(specs) - len(affected)
        lines.append(
            f"- **Baseline:** {baseline} spec file{'' if baseline == 1 else 's'} run for every PR (`@p0`)"
        )
        if affected:
            lines += [
                "",
                "<details>",
                f"<summary>Specs selected because of this PR ({len(affected)})</summary>",
                "",
            ]
            lines += ["| Spec | Tests | Why |", "|---|---:|---|"]
            for spec in affected[:MAX_LISTED_SCOPES]:
                # Code spans: reasons such as the @p0 tag must not render as mentions.
                why = ", ".join(
                    f"`{str(r).replace('`', '')}`" for r in spec.get("reasons", [])
                )
                lines.append(
                    f"| `{spec.get('path', '?')}` | {spec.get('selectedTests', '?')}/{spec.get('totalTests', '?')} | {why} |"
                )
            if len(affected) > MAX_LISTED_SCOPES:
                lines.append(
                    f"| … and {len(affected) - MAX_LISTED_SCOPES} more (see the `playwright-tier-plan` artifact) | | |"
                )
            lines += ["", "</details>"]
        else:
            lines.append("- No spec was selected because of this PR's changes.")

    lines += [
        "",
        f"Add the `{PLAYWRIGHT_OPT_OUT_LABEL}` label to run the full Playwright suite on this PR.",
    ]
    return "\n".join(lines) + "\n"


def compose(
    pytest_section: str,
    playwright_plan: Optional[Dict[str, Any]],
    env: Mapping[str, str],
) -> str:
    sections = [pytest_section] if pytest_section.strip() else []
    if playwright_plan:
        sections.append(render_playwright_section(playwright_plan))
    body = [MARKER, "## Smart test selection", ""]
    body.append(
        "\n".join(sections)
        if sections
        else "No test selection was computed for this run.\n"
    )

    sha = env.get("SHORT_SHA", "")
    run_url = env.get("RUN_URL", "")
    footer = [
        f"commit `{sha}`" if sha else "",
        f"[workflow run]({run_url})" if run_url else "",
    ]
    footer_text = " · ".join(part for part in footer if part)
    if footer_text:
        body += ["", f"<sub>{footer_text}</sub>"]
    return "\n".join(body) + "\n"


def read_text(path: Optional[str]) -> str:
    try:
        return Path(path).read_text() if path else ""
    except OSError:
        return ""


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("pytest")
    compose_parser = sub.add_parser("compose")
    compose_parser.add_argument("--pytest-section")
    compose_parser.add_argument("--playwright-plan")
    args = parser.parse_args()

    if args.command == "pytest":
        body = render_pytest_section(os.environ)
        summary = os.environ.get("GITHUB_STEP_SUMMARY")
        if summary:
            with open(summary, "a") as f:
                f.write(body)
    else:
        plan = load_plan(args.playwright_plan)
        body = compose(read_text(args.pytest_section), plan, os.environ)

    out = os.environ.get("COMMENT_FILE")
    if out:
        Path(out).parent.mkdir(parents=True, exist_ok=True)
        Path(out).write_text(body)
    print(body)


if __name__ == "__main__":
    main()
