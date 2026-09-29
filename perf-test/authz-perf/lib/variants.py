from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class RunVariant:
    run_label: Optional[str] = None
    docker_tag: Optional[str] = None


def parse_variants(
    run_labels: list[str],
    docker_tags: list[str],
) -> list[RunVariant]:
    if run_labels and docker_tags:
        raise SystemExit("Use --run-label or --docker-tag for variants, not both.")

    if run_labels:
        return [RunVariant(run_label=label) for label in run_labels]
    if docker_tags:
        return [RunVariant(docker_tag=tag) for tag in docker_tags]
    return [RunVariant()]


def variant_matrix_size(target_count: int, variant_count: int) -> int:
    return target_count * variant_count
