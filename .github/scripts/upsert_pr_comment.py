#!/usr/bin/env python3
"""Upsert a pull-request comment identified by a unique HTML marker in the body.

Usage: upsert_pr_comment.py <marker> <body-file> <pr-number> <owner/repo>
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def main() -> int:
    if len(sys.argv) != 5:
        sys.stderr.write("usage: upsert_pr_comment.py <marker> <body-file> <pr-number> <owner/repo>\n")
        return 2
    marker, body_file, pr_number, repo = sys.argv[1:]
    body = Path(body_file).read_text(encoding="utf-8")
    if marker not in body:
        body = f"{marker}\n{body}"

    raw = subprocess.check_output(
        [
            "gh",
            "api",
            "--paginate",
            "--slurp",
            f"repos/{repo}/issues/{pr_number}/comments?per_page=100",
        ],
        text=True,
    )
    comments = [comment for page in json.loads(raw) for comment in page]
    existing_id = next((c.get("id") for c in comments if marker in (c.get("body") or "")), None)

    payload = json.dumps({"body": body})
    if existing_id:
        url = f"repos/{repo}/issues/comments/{existing_id}"
        method = "PATCH"
    else:
        url = f"repos/{repo}/issues/{pr_number}/comments"
        method = "POST"
    subprocess.run(
        ["gh", "api", "-X", method, url, "--input", "-"],
        input=payload,
        text=True,
        check=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
