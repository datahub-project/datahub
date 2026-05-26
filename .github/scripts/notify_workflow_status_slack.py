#!/usr/bin/env python3
"""Post a Slack notification when a CI workflow completes on a protected branch.

Sends a commit-scoped root message on the first notification, then threads all
subsequent notifications for the same commit as replies. Thread state is persisted
via the --thread-ts-file argument, which is backed by GitHub Actions cache so
multiple post-workflow-actions runs for the same commit share it.
Omit --thread-ts-file to always post a standalone root message.

Root message (posted once per commit SHA):
  🔴/🟢 GitHub Action <conclusion> in <repo> on <branch> for <commit> for event: <event>
  Repo | Branch | Commit | Author | Event

Thread reply (posted per workflow):
  🔴/🟢 <workflow-name> <conclusion-verb> — <link to run>
  Repo | Branch | Commit | Event | Duration
  [Failed Jobs section if applicable]
  [Extra fields as additional sections]
"""

import argparse
import dataclasses
import json
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from utils.slack_notifier import blockkit, slack_client

_CONCLUSION_VERB: dict[str, str] = {
    "failure": "failed",
    "timed_out": "timed out",
    "cancelled": "was cancelled",
    "success": "succeeded",
}

_CONCLUSION_COLOR: dict[str, blockkit.Status] = {
    "failure": "failure",
    "timed_out": "failure",
    "cancelled": "warning",
    "success": "success",
}

_CONCLUSION_EMOJI: dict[str, str] = {
    "failure": ":red_circle:",
    "timed_out": ":red_circle:",
    "cancelled": ":large_yellow_circle:",
    "success": ":large_green_circle:",
}

_FAILED_CONCLUSIONS = {"failure", "timed_out"}


@dataclass
class WorkflowDetails:
    # Required — must be present in the input JSON
    workflow_name: str
    head_branch: str
    head_sha: str
    display_title: str
    actor_login: str
    run_url: str
    event: str
    conclusion: str
    repo: str

    # Optional — explicitly rendered in the Slack message
    duration_seconds: int | None = None
    failed_jobs: list[str] = field(default_factory=list)

    # Passthrough — any extra keys from the JSON not listed above
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_file(cls, path: str) -> "WorkflowDetails":
        data: dict[str, Any] = json.loads(Path(path).read_text())

        all_field_names = {f.name for f in dataclasses.fields(cls)}
        known_field_names = all_field_names - {"extra"}

        required_names = {
            f.name
            for f in dataclasses.fields(cls)
            if f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING  # type: ignore[misc]
        }
        missing = required_names - data.keys()
        if missing:
            raise ValueError(f"Missing required fields in workflow details JSON: {sorted(missing)}")

        known = {k: v for k, v in data.items() if k in known_field_names}
        extra = {k: v for k, v in data.items() if k not in known_field_names}
        return cls(**known, extra=extra)


def _fmt_duration(seconds: int) -> str:
    m, s = divmod(seconds, 60)
    return f"{m}m {s}s" if m else f"{s}s"


def main() -> int:
    args = _parse_args()

    token = os.environ.get("SLACK_BOT_TOKEN")
    if not token:
        print("ERROR: SLACK_BOT_TOKEN environment variable is required.", file=sys.stderr)
        return 2

    details = WorkflowDetails.from_file(args.input_file)

    thread_ts_map: dict[str, str] = _load_thread_ts_map(args.thread_ts_file)
    short_sha = details.head_sha[:7]
    repo_url = f"https://github.com/{details.repo}"
    commit_url = f"{repo_url}/commit/{details.head_sha}"
    conclusion = details.conclusion
    conclusion_verb = _CONCLUSION_VERB.get(conclusion, conclusion)
    conclusion_color: blockkit.Status = _CONCLUSION_COLOR.get(conclusion, "failure")
    conclusion_emoji = _CONCLUSION_EMOJI.get(conclusion, "\U0001f534")

    any_new_thread = False
    for channel in args.channel:
        existing_ts = thread_ts_map.get(channel)
        if existing_ts:
            _post_thread_reply(
                channel=channel,
                thread_ts=existing_ts,
                details=details,
                short_sha=short_sha,
                commit_url=commit_url,
                conclusion_verb=conclusion_verb,
                conclusion_color=conclusion_color,
                conclusion_emoji=conclusion_emoji,
                token=token,
            )
        else:
            ts = _post_root_message(
                channel=channel,
                details=details,
                repo_url=repo_url,
                short_sha=short_sha,
                commit_url=commit_url,
                conclusion=conclusion,
                conclusion_color=conclusion_color,
                conclusion_emoji=conclusion_emoji,
                token=token,
            )
            thread_ts_map[channel] = ts
            any_new_thread = True
            _post_thread_reply(
                channel=channel,
                thread_ts=ts,
                details=details,
                short_sha=short_sha,
                commit_url=commit_url,
                conclusion_verb=conclusion_verb,
                conclusion_color=conclusion_color,
                conclusion_emoji=conclusion_emoji,
                token=token,
            )

    if any_new_thread and args.thread_ts_file:
        Path(args.thread_ts_file).write_text(json.dumps(thread_ts_map))

    return 0


def _load_thread_ts_map(path: str | None) -> dict[str, str]:
    if not path:
        print("No thread-ts-file specified; each run will post a new root message.")
        return {}
    ts_path = Path(path)
    if not ts_path.exists():
        print(f"Thread-ts file not found at {ts_path}; will post a new root message.")
        return {}
    if ts_path.stat().st_size == 0:
        print(f"Thread-ts file at {ts_path} is empty; will post a new root message.")
        return {}
    result: dict[str, str] = json.loads(ts_path.read_text())
    print(f"Loaded thread-ts map from {ts_path}: {result}")
    return result


def _post_root_message(
    *,
    channel: str,
    details: WorkflowDetails,
    repo_url: str,
    short_sha: str,
    commit_url: str,
    conclusion: str,
    conclusion_color: blockkit.Status,
    conclusion_emoji: str,
    token: str,
) -> str:
    commit_ref = blockkit.link(short_sha, commit_url)
    display_title = details.display_title
    if display_title:
        title_body = f"({commit_ref}): {display_title}"
    else:
        title_body = f"({commit_ref})"
    title = f"*{conclusion_emoji} {title_body}"
    root_fields: dict[str, str] = {
        "Repo": blockkit.link(details.repo, repo_url),
        "Branch": details.head_branch,
        "Commit": f"{blockkit.link(short_sha, commit_url)} by @{details.actor_login}",
        "Event": details.event,
    }
    if details.display_title:
        root_fields["Commit Message"] = details.display_title
    blocks: list[dict[str, Any]] = [
        blockkit.section(title),
        blockkit.fields_section(root_fields),
    ]
    return slack_client.post_message(
        channel=channel,
        blocks=[],
        attachments=[blockkit.color_attachment(conclusion_color, blocks)],
        token=token,
        text=(
            f"GitHub Action {conclusion} in {details.repo} on {details.head_branch}"
            f" for {short_sha} (event: {details.event})"
        ),
    )


def _post_thread_reply(
    *,
    channel: str,
    thread_ts: str,
    details: WorkflowDetails,
    short_sha: str,
    commit_url: str,
    conclusion_verb: str,
    conclusion_color: blockkit.Status,
    conclusion_emoji: str,
    token: str,
) -> None:
    metadata_fields: dict[str, str] = {
        "Repo": details.repo,
        "Branch": details.head_branch,
        "Commit": f"{blockkit.link(short_sha, commit_url)} by @{details.actor_login}",
        "Event": details.event,
    }
    if details.display_title:
        metadata_fields["Commit Message"] = details.display_title
    if details.duration_seconds is not None:
        metadata_fields["Duration"] = _fmt_duration(details.duration_seconds)

    reply_blocks: list[dict[str, Any]] = [
        blockkit.section(
            f"{conclusion_emoji} *{details.workflow_name}* {conclusion_verb}"
            f" — {blockkit.link('view run', details.run_url)}"
        ),
        blockkit.fields_section(metadata_fields),
    ]

    if details.failed_jobs and details.conclusion in _FAILED_CONCLUSIONS:
        bullet_list = "\n".join(f"• {name}" for name in details.failed_jobs)
        reply_blocks.append(blockkit.section(f"*Failed Jobs*\n{bullet_list}"))

    for key, value in details.extra.items():
        reply_blocks.append(blockkit.fields_section({key: str(value)}))

    slack_client.post_thread_reply(
        channel=channel,
        thread_ts=thread_ts,
        blocks=[],
        attachments=[blockkit.color_attachment(conclusion_color, reply_blocks)],
        token=token,
        text=(
            f"{details.workflow_name} {conclusion_verb} in {details.repo}"
            f" on {details.head_branch} ({short_sha}) [event: {details.event}]"
        ),
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input-file",
        required=True,
        metavar="FILE",
        help="Path to JSON file produced by fetch_workflow_details.py",
    )
    parser.add_argument(
        "--channel",
        action="append",
        required=True,
        metavar="CHANNEL",
        help="Slack channel (repeatable, e.g. --channel '#ci-failures' --channel '#releases')",
    )
    parser.add_argument(
        "--thread-ts-file",
        help="Path to JSON file storing {channel: thread_ts} map (read + written in-place)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    sys.exit(main())
