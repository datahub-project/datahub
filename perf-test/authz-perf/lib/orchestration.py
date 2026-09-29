from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

from lib.metadata import GitMetadata
from lib.results import utc_now_iso


@dataclass
class ManifestRunEntry:
    target_name: str
    build_slug: str
    gms_url: str
    gms_host: str
    gms_version: Optional[str]
    gms_commit: Optional[str]
    git: dict[str, Any]
    output_jsonl: str
    output_summary: str
    status: str
    error: Optional[str] = None
    view_authorization_enabled: Optional[bool] = None


def write_manifest(
    output_dir: Path,
    *,
    orchestration_id: str,
    harness_args: dict[str, Any],
    fixture_version: str,
    runs: list[ManifestRunEntry],
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "manifest.json"
    payload = {
        "orchestration_id": orchestration_id,
        "generated_at": utc_now_iso(),
        "fixture_version": fixture_version,
        "harness_args": harness_args,
        "runs": [
            {
                "target_name": entry.target_name,
                "build_slug": entry.build_slug,
                "gms_url": entry.gms_url,
                "gms_host": entry.gms_host,
                "gms_version": entry.gms_version,
                "gms_commit": entry.gms_commit,
                "git": entry.git,
                "output_jsonl": entry.output_jsonl,
                "output_summary": entry.output_summary,
                "status": entry.status,
                "authorization": {
                    "view_enabled": entry.view_authorization_enabled,
                },
                **({"error": entry.error} if entry.error else {}),
            }
            for entry in runs
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def git_dict(git: GitMetadata) -> dict[str, Any]:
    return {
        "commit": git.commit,
        "commit_full": git.commit_full,
        "branch": git.branch,
        "tag": git.tag,
        "dirty": git.dirty,
    }
