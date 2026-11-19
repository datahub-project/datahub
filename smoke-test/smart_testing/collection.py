"""Pytest-facing smart-testing helpers, called from tests/e2e/conftest.py.

Mode ``off`` (the default) does no work at all: the tag map is never read and
no markers are applied. ``shadow`` applies markers and records the would-be
selection as a JUnit ``smart_selected`` property without deselecting anything,
and stamps ``smart_outcome`` on the final attempt so a rerun-then-pass is not
a miss. ``active`` deselects the modules the PR does not touch.
"""

import logging
from collections import defaultdict
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Optional, Sequence, Set, Tuple

import pytest
import yaml
from _pytest.nodes import Item

from smart_testing.shadow_report import OUTCOME_PROPERTY, SELECTED_PROPERTY
from smart_testing.taxonomy import (
    Dimension,
    PRTags,
    TagMap,
    TagSet,
    load_tag_map,
    select_modules,
    split_known,
    tag_value,
)

logger = logging.getLogger(__name__)


class Mode(str, Enum):
    OFF = "off"
    SHADOW = "shadow"
    ACTIVE = "active"


def parse_mode(raw: str) -> Mode:
    try:
        return Mode(raw)
    except ValueError:
        raise ValueError(
            f"Unknown SMOKE_SMART_TESTING={raw!r}; expected one of "
            f"{', '.join(m.value for m in Mode)}"
        ) from None


def load_for_mode(
    mode: Mode, loader: Callable[[], TagMap] = load_tag_map
) -> Optional[TagMap]:
    """The tag map for ``mode``; ``None`` (and nothing read) when off.

    A broken tag map disables smart testing rather than failing the run: shadow
    mode must never break CI.
    """
    if mode is Mode.OFF:
        return None
    try:
        return loader()
    except (OSError, ValueError, KeyError, yaml.YAMLError):
        logger.exception("Smart testing: cannot load tag map; smart testing disabled")
        return None


def module_path(item: Item) -> str:
    """Module path relative to the smoke-test rootdir, e.g. ``tests/e2e/x.py``."""
    return Path(item.path).relative_to(item.config.rootpath).as_posix()


def apply_markers(item: Item, tag_map: TagMap) -> None:
    tags = tag_map.tags_for(module_path(item))
    for dim, values in (tags or {}).items():
        item.add_marker(getattr(pytest.mark, dim.marker)(*sorted(values)))


def markers_of(item: Item) -> Optional[TagSet]:
    """Tags from the item's smart markers (applied or hand-written); None if untagged."""
    tags: TagSet = {}
    for dim in Dimension:
        values = {
            tag_value(arg)
            for marker in item.iter_markers(dim.marker)
            for arg in marker.args
        }
        if values:
            tags[dim] = values
    return tags or None


def _module_tags(items: Sequence[Item]) -> Dict[str, Optional[TagSet]]:
    """Per-module union of item tags; a module is untagged only if all its items are."""
    by_module: Dict[str, Optional[TagSet]] = {}
    for item in items:
        path = module_path(item)
        tags = markers_of(item)
        current = by_module.get(path)
        if tags is None:
            by_module.setdefault(path, None)
        elif current is None:
            by_module[path] = {dim: set(v) for dim, v in tags.items()}
        else:
            for dim, values in tags.items():
                current.setdefault(dim, set()).update(values)
    return by_module


def _report_unknown(module_tags: Dict[str, Optional[TagSet]]) -> None:
    offenders: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    for module, tags in module_tags.items():
        if tags is None:
            continue
        _, unknown = split_known(tags)
        for dim, values in unknown.items():
            for value in values:
                offenders[(dim.value, value)].append(module)
    for (dim_value, value), modules in sorted(offenders.items()):
        logger.error(
            "Smart testing: unknown %s tag %r (not in taxonomy) on %s module(s): %s",
            dim_value,
            value,
            len(modules),
            ", ".join(sorted(modules)[:5]),
        )


def _select(
    module_tags: Dict[str, Optional[TagSet]],
    tag_map: TagMap,
    pr: PRTags,
    changed_tests: Sequence[str],
) -> Set[str]:
    # A PR's own changed test modules always run; CI passes repo-relative paths.
    always: Set[str] = set(tag_map.always_run) | {
        module
        for module in module_tags
        if any(path.endswith(module) for path in changed_tests)
    }
    return select_modules(
        module_tags, pr, always, plumbing_sample=tag_map.plumbing_sample
    )


def selected_items(
    items: Sequence[Item],
    tag_map: TagMap,
    pr: PRTags,
    changed_tests: Sequence[str],
) -> List[Item]:
    """The items active mode would keep; used by the CI batch planner."""
    selected = _select(_module_tags(items), tag_map, pr, changed_tests)
    return [item for item in items if module_path(item) in selected]


def filter_items(
    config: pytest.Config,
    items: List[Item],
    mode: Mode,
    tag_map: Optional[TagMap],
    raw_pr_tags: str,
    changed_tests: Sequence[str],
) -> None:
    """Validate smart markers, then record (shadow) or apply (active) the selection."""
    if mode is Mode.OFF or tag_map is None:
        return

    module_tags = _module_tags(items)
    _report_unknown(module_tags)

    if not raw_pr_tags:
        logger.info("Smart testing (%s): no PR tags; not filtering", mode.value)
        return
    try:
        pr = PRTags.from_json(raw_pr_tags)
    except ValueError as exc:
        logger.error(
            "Smart testing: unreadable SMOKE_SMART_TAGS (%s); not filtering", exc
        )
        return

    selected = _select(module_tags, tag_map, pr, changed_tests)
    chosen = [m for m in module_tags if m in selected]
    logger.info(
        "Smart testing (%s): %s %s of %s module(s)",
        mode.value,
        "would select" if mode is Mode.SHADOW else "selected",
        len(chosen),
        len(module_tags),
    )

    if mode is Mode.SHADOW:
        for item in items:
            flag = "true" if module_path(item) in selected else "false"
            item.user_properties.append((SELECTED_PROPERTY, flag))
        return

    kept = [item for item in items if module_path(item) in selected]
    deselected = [item for item in items if module_path(item) not in selected]
    if deselected:
        config.hook.pytest_deselected(items=deselected)
    items[:] = kept


class FinalOutcomeRecorder:
    """Stamp ``smart_outcome`` on the final attempt's teardown report.

    pytest-rerunfailures rewrites a retry to outcome ``rerun`` after
    ``pytest_runtest_makereport`` and before ``pytest_runtest_logreport``, so
    the outcome has to be read from the log report. JUnit copies
    ``user_properties`` off the teardown report when it finalizes the testcase.
    """

    def __init__(self) -> None:
        self._setup_failed: Dict[str, bool] = {}
        self._call_failed: Dict[str, bool] = {}
        self._call_passed: Dict[str, bool] = {}

    @pytest.hookimpl(tryfirst=True)
    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.outcome == "rerun":
            return
        nodeid = report.nodeid
        if report.when == "setup":
            self._setup_failed[nodeid] = report.failed
            return
        if report.when == "call":
            self._call_failed[nodeid] = report.failed
            self._call_passed[nodeid] = report.passed
            return
        if report.when != "teardown":
            return
        failed = (
            self._setup_failed.get(nodeid, False)
            or self._call_failed.get(nodeid, False)
            or report.failed
        )
        if failed:
            _stamp_outcome(report, "failed")
        elif self._call_passed.get(nodeid, False):
            _stamp_outcome(report, "passed")
        self._setup_failed.pop(nodeid, None)
        self._call_failed.pop(nodeid, None)
        self._call_passed.pop(nodeid, None)


def _stamp_outcome(report: pytest.TestReport, value: str) -> None:
    report.user_properties = [
        (name, existing)
        for name, existing in report.user_properties
        if name != OUTCOME_PROPERTY
    ]
    report.user_properties.append((OUTCOME_PROPERTY, value))
