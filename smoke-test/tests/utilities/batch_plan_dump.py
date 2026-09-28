"""Opt-in pytest plugin: dump the CI batch pack plan without running tests.

Load only with ``-p batch_plan_dump`` from the smoke-test directory. Do not
add this module to ``pytest_plugins`` — ordinary pytest / CI jobs must not
load it.

Write JSON to ``SMOKE_DUMP_BATCH_PLAN`` when that env is set (overrides the
CLI path), else ``--dump-batch-plan PATH``, else stdout.

Use ``--collect-only`` so session fixtures and GMS never start. Collection
still imports the suite.
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

from shard_pack import BatchPlan, plan_collected_items
from tests.utilities import env_vars
from tests.utilities.domains import domains_of, is_selected, parse_requested_domains


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
    batch_count = max(1, env_vars.get_batch_count())
    batch_number = env_vars.get_batch_number()
    xdist_workers = env_vars.get_pytest_xdist_workers()
    packed = plan_collected_items(selected, batch_count, xdist_workers)
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
