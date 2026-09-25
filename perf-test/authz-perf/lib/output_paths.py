from __future__ import annotations

import re
from pathlib import Path

from lib.metadata import GitMetadata
from lib.variants import RunVariant


def sanitize_slug(value: str) -> str:
    slug = re.sub(r"[^\w.-]+", "-", value.strip())
    slug = slug.strip("-")
    return slug[:96] if slug else "unknown"


def build_slug(
    variant: RunVariant,
    git: GitMetadata,
    gms_version: str | None,
) -> str:
    if variant.run_label:
        return sanitize_slug(variant.run_label)
    if variant.docker_tag:
        return sanitize_slug(variant.docker_tag)
    if git.tag:
        return sanitize_slug(git.tag)
    if git.commit and git.commit != "unknown":
        return sanitize_slug(f"commit-{git.commit}")
    if gms_version and gms_version != "unknown":
        return sanitize_slug(gms_version)
    return "unknown"


def result_paths(
    output_dir: Path,
    target_name: str,
    build_slug_value: str,
    orchestration_id: str,
    *,
    multi_cell: bool,
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    if multi_cell:
        stem = f"{sanitize_slug(target_name)}-{build_slug_value}-{orchestration_id}"
    elif target_name == "local":
        stem = build_slug_value
    else:
        stem = f"{sanitize_slug(target_name)}-{build_slug_value}"

    jsonl = output_dir / f"{stem}.jsonl"
    summary = output_dir / f"{stem}.summary.json"
    return jsonl, summary
