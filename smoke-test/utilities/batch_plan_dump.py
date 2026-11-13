"""Opt-in pytest plugin: dump the CI batch pack plan without running tests.

Load only with ``-p batch_plan_dump`` from the smoke-test directory. Do not
add this module to ``pytest_plugins`` — ordinary pytest / CI jobs must not
load it.

Write JSON to ``SMOKE_DUMP_BATCH_PLAN`` when that env is set (overrides the
CLI path), else ``--dump-batch-plan PATH``, else stdout.

Use ``--collect-only`` so session fixtures and GMS never start. Collection
still imports the suite.

With ``--size-batches MAX`` (CI's ``smoke_test_matrix`` job, via
``TEST_STRATEGY=plan ./smoke.sh``) the plugin also applies smart selection from
``SMOKE_SMART_TAGS`` and picks the fewest batches (at most MAX) whose predicted
long pole meets ``TARGET_BATCH_WALL_SECONDS`` (see ``shard_pack.size_batches``).
Each batch job then runs only its planned scopes (``SMOKE_BATCH_PLAN``).
"""

from __future__ import annotations

import json
import logging
import sys
from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional, Protocol, Sequence, TypeVar

import pytest
from _pytest.mark import MarkMatcher
from _pytest.mark.expression import Expression
from _pytest.mark.structures import Mark
from _pytest.nodes import Item

from shard_pack import (
    TARGET_BATCH_WALL_SECONDS,
    BatchPlan,
    PackedSuite,
    loadscope_key,
    plan_collected_items,
    size_batches,
    slowest_scope_seconds,
)
from smart_testing import collection as smart_testing
from smart_testing.taxonomy import PRTags, load_tag_map
from utilities import env_vars
from utilities.domains import domains_of, is_selected, parse_requested_domains


class _HasIterMarkers(Protocol):
    def iter_markers(self) -> Iterable[Mark]: ...


_T = TypeVar("_T", bound=_HasIterMarkers)

logger = logging.getLogger(__name__)


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--dump-batch-plan",
        action="store",
        default=None,
        metavar="PATH",
        help=(
            "Write the smoke-test batch pack plan as JSON to PATH. "
            "SMOKE_DUMP_BATCH_PLAN overrides this when set. "
            "Stdout if neither is set. Requires -p batch_plan_dump."
        ),
    )
    parser.addoption(
        "--size-batches",
        action="store",
        type=int,
        default=None,
        metavar="MAX",
        help=(
            "Apply smart selection (SMOKE_SMART_TAGS) and pick the fewest batches, "
            "at most MAX, whose predicted long pole meets the per-batch target. "
            "Ignores BATCH_COUNT. Requires -p batch_plan_dump."
        ),
    )


def resolve_dump_path(
    cli_path: Optional[str], env_path: Optional[str]
) -> Optional[str]:
    """Return the dump file path, or None to write stdout.

    Env overrides CLI so CI can pin an artifact path without changing argv.
    """
    if env_path and env_path.strip():
        return env_path.strip()
    if cli_path and cli_path.strip():
        return cli_path.strip()
    return None


def domain_filtered_items(config: pytest.Config, items: Sequence[Item]) -> list[Item]:
    """Apply ``--domain`` to a copy; leave the live collection list unchanged."""
    requested = parse_requested_domains(config.getoption("--domain"))
    if not requested:
        return list(items)
    return [
        item
        for item in items
        if is_selected(domains_of(item.get_closest_marker("domain")), requested)
    ]


def markexpr_filtered_items(markexpr: str, items: Sequence[_T]) -> list[_T]:
    """Apply pytest ``-m`` / ``markexpr`` to a copy; do not mutate ``items``.

    This plugin's collection hook is not trylast, so it runs before pytest's
    own mark-deselection hook. Pack from the same subset ``-m`` would keep.
    """
    copied = list(items)
    expr = (markexpr or "").strip()
    if not expr:
        return copied
    compiled = Expression.compile(expr)
    return [
        item
        for item in copied
        if compiled.evaluate(MarkMatcher.from_markers(item.iter_markers()))
    ]


def serialize_batch_plans(
    plans: list[BatchPlan],
    *,
    batch_count: int,
    batch_number: int,
    xdist_workers: int,
    test_count: int,
    scope_test_counts: dict[str, int],
    missing_weight_count: int,
    markexpr: str,
    domains: list[str],
) -> dict[str, Any]:
    batches = []
    walls: list[float] = []
    for index, plan in enumerate(plans):
        tests = sum(scope_test_counts.get(scope, 0) for scope in plan.module_paths)
        walls.append(plan.predicted_wall)
        batches.append(
            {
                "batch": index,
                "predicted_wall": plan.predicted_wall,
                "phase1_makespan": plan.phase1_makespan,
                "serial_seconds": plan.serial_seconds,
                "test_count": tests,
                "scope_count": len(plan.module_paths),
                "scopes": list(plan.module_paths),
            }
        )
    max_wall = max(walls) if walls else 0.0
    min_wall = min(walls) if walls else 0.0
    return {
        "batch_count": batch_count,
        "batch_number": batch_number,
        "xdist_workers": xdist_workers,
        "markexpr": markexpr,
        "domains": domains,
        "test_count": test_count,
        "scope_count": len(scope_test_counts),
        "missing_weight_count": missing_weight_count,
        "max_predicted_wall": max_wall,
        "imbalance": max_wall - min_wall,
        "batches": batches,
    }


def write_dump(payload: dict[str, Any], path: Optional[str]) -> None:
    text = json.dumps(payload, indent=2) + "\n"
    if path is None:
        sys.stdout.write(text)
        return
    out = Path(path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text)


def size_selected(
    items: Sequence[Item],
    *,
    max_batches: int,
    xdist_workers: int,
    raw_pr_tags: str,
    changed_tests: Sequence[str],
) -> tuple[PackedSuite, dict[str, Any]]:
    """Smart-select ``items``, then pick the fewest batches that meet the target.

    Empty ``raw_pr_tags`` means no smart filtering (CI's p0 fail-safe). Bad tags
    or an unreadable tag map raise: CI treats a failed plan as "fall back to
    the fixed p0 matrix", never as "silently run a different set".
    """
    if max_batches < 1:
        raise pytest.UsageError(f"--size-batches must be >= 1, got {max_batches}")
    chosen = list(items)
    if raw_pr_tags:
        pr = PRTags.from_json(raw_pr_tags)
        tag_map = load_tag_map()
        # conftest only applies YAML tags when SMOKE_SMART_TESTING != off; apply
        # them here so the plan does not depend on that env. Re-applying is a
        # no-op for selection (tags union per module).
        for item in items:
            smart_testing.apply_markers(item, tag_map)
        chosen = smart_testing.selected_items(items, tag_map, pr, changed_tests)
    packed = size_batches(chosen, max_batches, xdist_workers)
    return packed, {
        "max_batches": max_batches,
        "target_wall": TARGET_BATCH_WALL_SECONDS,
        "slowest_scope_wall": slowest_scope_seconds(packed),
        "candidate_test_count": len(items),
        "candidate_scope_count": len({loadscope_key(item.nodeid) for item in items}),
        "smart_tags_applied": bool(raw_pr_tags),
        # Lets a later reader of this plan tell whether its batches ran it
        # (active) or it was only computed (shadow).
        "smart_mode": env_vars.get_smoke_plan_smart_mode(),
    }


def pytest_collection_modifyitems(
    session: pytest.Session, config: pytest.Config, items: list[Item]
) -> None:
    """Pack the full selected set for BATCH_COUNT before conftest slices BATCH_NUMBER.

    Not trylast: conftest's packing hook is trylast=True and mutates ``items``.
    """
    if not config.option.collectonly:
        logger.warning(
            "batch_plan_dump is loaded without --collect-only; selected tests "
            "are cleared so they will not run. Prefer --collect-only."
        )

    selected = markexpr_filtered_items(
        str(getattr(config.option, "markexpr", "") or ""),
        domain_filtered_items(config, items),
    )
    batch_number = env_vars.get_batch_number()
    xdist_workers = env_vars.get_pytest_xdist_workers()
    max_batches = config.getoption("--size-batches")
    sizing: dict[str, Any] = {}
    if max_batches is None:
        batch_count = max(1, env_vars.get_batch_count())
        packed = plan_collected_items(selected, batch_count, xdist_workers)
    else:
        packed, sizing = size_selected(
            selected,
            max_batches=max_batches,
            xdist_workers=xdist_workers,
            raw_pr_tags=env_vars.get_smart_testing_tags(),
            changed_tests=env_vars.get_smoke_changed_tests(),
        )
        batch_count = len(packed.plans)
        selected = [item for scope in packed.items_by_scope.values() for item in scope]
    markexpr = str(getattr(config.option, "markexpr", "") or "")
    domains = list(config.getoption("--domain") or [])
    payload = serialize_batch_plans(
        packed.plans,
        batch_count=batch_count,
        batch_number=batch_number,
        xdist_workers=xdist_workers,
        test_count=len(selected),
        scope_test_counts={
            scope_key: len(scope_items)
            for scope_key, scope_items in packed.items_by_scope.items()
        },
        missing_weight_count=len(packed.missing_weight_ids),
        markexpr=markexpr,
        domains=domains,
    )
    payload.update(sizing)
    path = resolve_dump_path(
        config.getoption("--dump-batch-plan"),
        env_vars.get_smoke_dump_batch_plan(),
    )
    write_dump(payload, path)
    logger.info(
        "Wrote batch plan dump (%s tests, %s batches, xdist_workers=%s) to %s",
        len(selected),
        batch_count,
        xdist_workers,
        path or "stdout",
    )
    # Backstop if --collect-only is omitted: loading this plugin is dump-only.
    items.clear()
