"""CPU-only tests for smart test selection (smart_testing/)."""

from __future__ import annotations

import fnmatch
import importlib.util
import os
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import yaml

from smart_testing.collection import Mode, load_for_mode
from smart_testing.escape_report import check, scope_classname
from smart_testing.shadow_report import (
    OUTCOME_PROPERTY,
    SELECTED_PROPERTY,
    false_negatives,
)
from smart_testing.taxonomy import (
    KNOWN_TAGS,
    NARROW_THRESHOLD,
    SAFETY_FLOOR,
    TAG_ENUMS,
    Dimension,
    PRTags,
    TagMap,
    TagRule,
    load_tag_map,
    select_modules,
    split_known,
)

SMOKE_ROOT = Path(__file__).resolve().parents[2]
TAXONOMY_YAML = SMOKE_ROOT.parent / ".github/smart-testing/taxonomy.yaml"
TAXONOMY_OVERLAY_YAML = TAXONOMY_YAML.with_name("taxonomy.overlay.yaml")
TAGS_SCRIPT = SMOKE_ROOT.parent / ".claude/skills/datahub-test-taxonomy/scripts/tags.py"

F = Dimension.FEATURE_USER
T = Dimension.TECHNICAL_LAYER
C = Dimension.CHANGE_TYPE


def test_taxonomy_py_matches_taxonomy_yaml() -> None:
    from_yaml: dict = {}
    for path in (TAXONOMY_YAML, TAXONOMY_OVERLAY_YAML):
        if not path.exists():
            continue
        for dim, body in yaml.safe_load(path.read_text())["dimensions"].items():
            from_yaml.setdefault(Dimension(dim), set()).update(
                entry["tag"] for entry in body["tags"]
            )
    assert from_yaml == {dim: set(tags) for dim, tags in KNOWN_TAGS.items()}


def test_generated_enum_block_is_current() -> None:
    # The enums are generated from taxonomy.yaml by the datahub-test-taxonomy skill; a
    # hand edit to either side, or a skipped `tags.py enums --write`, fails here.
    spec = importlib.util.spec_from_file_location("tags", TAGS_SCRIPT)
    assert spec is not None and spec.loader is not None
    tags = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tags)
    assert tags.cmd_enums(write=False) == "unchanged"


def test_checked_in_tag_map_uses_only_taxonomy_tags() -> None:
    tag_map = load_tag_map()
    unknown = {
        rule.pattern: found
        for rule in tag_map.rules
        if (found := split_known(rule.tags)[1])
    }
    assert unknown == {}


def test_base_tag_map_uses_only_base_taxonomy_tags() -> None:
    # The base files are shared with repositories that have no overlay, so they
    # must not depend on overlay tags.
    base_tags = {dim: {tag.value for tag in enum} for dim, enum in TAG_ENUMS.items()}
    tag_map = load_tag_map(overlay=None)
    overlay_only = {
        rule.pattern: tags
        for rule in tag_map.rules
        if (
            tags := {
                d: v - base_tags[d] for d, v in rule.tags.items() if v - base_tags[d]
            }
        )
    }
    assert overlay_only == {}


def test_load_tag_map_merges_overlay(tmp_path: Path) -> None:
    base = tmp_path / "test_tags.yaml"
    base.write_text(
        "always_run: [tests/e2e/a.py]\n"
        "tests:\n  - path: tests/e2e/a.py\n    tags:\n      feature-user: [search]\n"
    )
    overlay = tmp_path / "test_tags.overlay.yaml"
    overlay.write_text(
        "tests:\n  - path: tests/e2e/b.py\n    tags:\n      feature-user: [lineage]\n"
    )
    merged = load_tag_map(base, overlay)
    assert merged.always_run == {"tests/e2e/a.py"}
    assert merged.tags_for("tests/e2e/b.py") == {F: {"lineage"}}
    assert (
        load_tag_map(base, tmp_path / "missing.yaml").tags_for("tests/e2e/b.py") is None
    )


def test_checked_in_tag_map_paths_exist() -> None:
    tag_map = load_tag_map()
    files = [
        p.relative_to(SMOKE_ROOT).as_posix()
        for p in (SMOKE_ROOT / "tests/e2e").rglob("*.py")
    ]
    stale = [
        rule.pattern
        for rule in tag_map.rules
        if not any(fnmatch.fnmatch(f, rule.pattern) for f in files)
    ]
    assert stale == []
    assert all((SMOKE_ROOT / path).is_file() for path in tag_map.always_run)
    assert tag_map.plumbing_sample
    assert all((SMOKE_ROOT / path).is_file() for path in tag_map.plumbing_sample)


def test_checked_in_tag_map_never_tags_tests_with_change_type() -> None:
    # change-type describes PRs. On a test it would select nothing and only
    # mislead the next reader.
    tag_map = load_tag_map()
    assert [r.pattern for r in tag_map.rules if Dimension.CHANGE_TYPE in r.tags] == []


def test_tags_for_exact_beats_glob_and_longest_glob_wins() -> None:
    tag_map = TagMap(
        always_run=frozenset(),
        plumbing_sample=frozenset(),
        rules=(
            TagRule("tests/e2e/*.py", {F: {"search"}}),
            TagRule("tests/e2e/lineage/*.py", {F: {"lineage"}}),
            TagRule("tests/e2e/lineage/test_a.py", {F: {"ownership"}}),
        ),
    )
    assert tag_map.tags_for("tests/e2e/lineage/test_a.py") == {F: {"ownership"}}
    assert tag_map.tags_for("tests/e2e/lineage/test_b.py") == {F: {"lineage"}}
    assert tag_map.tags_for("tests/other/test_c.py") is None


def test_select_modules_on_feature_overlap() -> None:
    modules = {
        "lineage.py": {F: {"lineage"}},
        "search.py": {F: {"search"}},
        "untagged.py": None,
        "layer_only.py": {T: {"graphql"}},
    }
    pr = PRTags(tags={F: {"lineage"}})
    assert select_modules(
        modules, pr, ["canary.py"], plumbing_sample=["sample.py"]
    ) == {
        "lineage.py",
        "untagged.py",
        "canary.py",
    }


def test_select_modules_without_feature_tags_runs_always_run_only() -> None:
    modules = {"lineage.py": {F: {"lineage"}}, "untagged.py": None}
    selected = select_modules(
        modules, PRTags(tags={}), ["canary.py"], plumbing_sample=["sample.py"]
    )
    assert selected == {"canary.py"}


def test_select_modules_change_type_alone_selects_no_tests() -> None:
    modules = {"lineage.py": {F: {"lineage"}}, "untagged.py": None}
    pr = PRTags(tags={C: {"ci", "test-infra"}})
    assert select_modules(
        modules, pr, ["canary.py"], plumbing_sample=["sample.py"]
    ) == {"canary.py"}


def test_select_modules_adds_plumbing_sample_for_harness_changes() -> None:
    modules = {"lineage.py": {F: {"lineage"}}, "search.py": {F: {"search"}}}
    harness_only = PRTags(tags={C: {"pytest-plumbing", "test-infra"}})
    assert select_modules(
        modules, harness_only, ["canary.py"], plumbing_sample=["sample.py"]
    ) == {"canary.py", "sample.py"}

    # Feature tags still select as before; the sample is added on top.
    mixed = PRTags(tags={F: {"lineage"}, C: {"pytest-plumbing"}})
    assert select_modules(
        modules, mixed, ["canary.py"], plumbing_sample=["sample.py"]
    ) == {"lineage.py", "canary.py", "sample.py"}


def test_select_modules_narrows_broad_selection_above_floor() -> None:
    count = NARROW_THRESHOLD + 1
    modules = {f"m{i}.py": {F: {"search"}, T: {"openapi"}} for i in range(count)}
    for i in range(SAFETY_FLOOR):
        modules[f"m{i}.py"][T] = {"graphql"}
    pr = PRTags(tags={F: {"search"}, T: {"graphql"}})
    assert len(select_modules(modules, pr, [], plumbing_sample=[])) == SAFETY_FLOOR

    # One module fewer than the floor: narrowing is abandoned.
    modules["m0.py"][T] = {"openapi"}
    assert len(select_modules(modules, pr, [], plumbing_sample=[])) == count


def test_off_mode_never_reads_tag_map() -> None:
    def loader() -> TagMap:
        raise AssertionError("tag map read in off mode")

    assert load_for_mode(Mode.OFF, loader) is None


def test_broken_tag_map_disables_smart_testing() -> None:
    def loader() -> TagMap:
        raise yaml.YAMLError("bad yaml")

    assert load_for_mode(Mode.SHADOW, loader) is None


def test_pr_tags_round_trip() -> None:
    pr = PRTags(tags={F: {"lineage", "search"}}, confidence="HIGH")
    assert PRTags.from_json(pr.to_json()) == pr


def test_shadow_report_lists_failed_tests_that_would_be_skipped(tmp_path: Path) -> None:
    def case(name: str, selected: str, failed: bool) -> str:
        failure = "<failure message='boom'/>" if failed else ""
        return (
            f"<testcase classname='tests.e2e.mod' name='{name}'><properties>"
            f"<property name='smart_selected' value='{selected}'/></properties>"
            f"{failure}</testcase>"
        )

    junit = tmp_path / "junit.smoke.xml"
    junit.write_text(
        "<testsuites><testsuite>"
        + case("test_missed", "false", failed=True)
        + case("test_caught", "true", failed=True)
        + case("test_skipped_ok", "false", failed=False)
        + "</testsuite></testsuites>"
    )
    assert false_negatives([str(junit)]) == ["tests.e2e.mod::test_missed"]


def _case(
    name: str,
    selected: str,
    *,
    failure: str = "",
    outcome: str | None = None,
) -> str:
    properties = f"<property name='{SELECTED_PROPERTY}' value='{selected}'/>"
    if outcome is not None:
        properties += f"<property name='{OUTCOME_PROPERTY}' value='{outcome}'/>"
    return (
        f"<testcase classname='tests.e2e.mod' name='{name}'>"
        f"<properties>{properties}</properties>{failure}</testcase>"
    )


def _write_junit(tmp_path: Path, body: str) -> str:
    junit = tmp_path / "junit.xml"
    junit.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
    return str(junit)


def test_shadow_report_ignores_passed_outcome_and_rerun_tags(tmp_path: Path) -> None:
    junit = _write_junit(
        tmp_path,
        _case(
            "test_retried",
            "false",
            failure="<failure message='boom'/>",
            outcome="passed",
        )
        + _case("test_rerun_only", "false", failure="<rerun message='boom'/>")
        + _case(
            "test_final", "false", failure="<error message='boom'/>", outcome="failed"
        )
        + _case("test_selected", "true", outcome="failed"),
    )
    assert false_negatives([junit]) == ["tests.e2e.mod::test_final"]


def test_shadow_report_uses_the_last_testcase_for_a_rerun(tmp_path: Path) -> None:
    junit = _write_junit(
        tmp_path,
        _case("test_flaky", "false", failure="<failure message='boom'/>")
        + _case("test_flaky", "false", outcome="passed")
        + _case("test_still_bad", "false", outcome="passed")
        + _case("test_still_bad", "false", outcome="failed"),
    )
    assert false_negatives([junit]) == ["tests.e2e.mod::test_still_bad"]


def test_rerun_then_pass_is_not_a_final_failure(tmp_path: Path) -> None:
    (tmp_path / "conftest.py").write_text(
        "from smart_testing.collection import FinalOutcomeRecorder\n"
        "from smart_testing.shadow_report import SELECTED_PROPERTY\n"
        "\n"
        "def pytest_configure(config):\n"
        "    config.pluginmanager.register(\n"
        "        FinalOutcomeRecorder(), 'smart-testing-final-outcome'\n"
        "    )\n"
        "\n"
        "def pytest_collection_modifyitems(config, items):\n"
        "    for item in items:\n"
        "        item.user_properties.append((SELECTED_PROPERTY, 'false'))\n"
    )
    (tmp_path / "test_sample.py").write_text(
        "from pathlib import Path\n"
        "\n"
        "_ATTEMPTS = Path(__file__).with_name('attempts.txt')\n"
        "\n"
        "def test_passes_on_rerun():\n"
        "    seen = int(_ATTEMPTS.read_text()) if _ATTEMPTS.exists() else 0\n"
        "    _ATTEMPTS.write_text(str(seen + 1))\n"
        "    assert seen >= 1\n"
        "\n"
        "def test_always_fails():\n"
        "    assert False\n"
    )
    junit = tmp_path / "junit.xml"
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [str(SMOKE_ROOT), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "test_sample.py",
            f"--confcutdir={tmp_path}",
            "--reruns",
            "1",
            f"--junit-xml={junit}",
            "-p",
            "no:cacheprovider",
            "-o",
            "addopts=",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert junit.is_file(), completed.stdout + completed.stderr
    outcomes = {
        case.get("name"): next(
            (
                prop.get("value")
                for prop in case.iter("property")
                if prop.get("name") == OUTCOME_PROPERTY
            ),
            None,
        )
        for case in ET.parse(junit).iter("testcase")
    }
    details = completed.stdout + completed.stderr
    assert outcomes["test_passes_on_rerun"] == "passed", details
    assert outcomes["test_always_fails"] == "failed", details
    assert false_negatives([str(junit)]) == ["test_sample::test_always_fails"]


def test_scope_classname_matches_junit_classname() -> None:
    assert scope_classname("tests/e2e/a.py") == "tests.e2e.a"
    assert scope_classname("tests/e2e/a.py::TestX") == "tests.e2e.a.TestX"
    assert scope_classname("tests/e2e/a.py::TestX::TestY") == "tests.e2e.a.TestX.TestY"


def _junit_file(path: Path, *cases: tuple[str, str, str]) -> str:
    """Write junit with (classname, name, child element) testcases."""
    body = "".join(
        f"<testcase classname='{classname}' name='{name}'>{child}</testcase>"
        for classname, name, child in cases
    )
    path.write_text(f"<testsuites><testsuite>{body}</testsuite></testsuites>")
    return str(path)


FAILED = "<failure message='boom'/>"


def _plan(smart_mode: str = "active", smart_tags_applied: bool = True) -> dict:
    return {
        "smart_mode": smart_mode,
        "smart_tags_applied": smart_tags_applied,
        "test_count": 5,
        "batches": [{"scopes": ["tests/e2e/planned.py::TestP"]}],
    }


def test_escape_check_flags_failures_that_did_not_run_on_the_pr(
    tmp_path: Path,
) -> None:
    post_merge = _junit_file(
        tmp_path / "post_merge.xml",
        ("tests.e2e.skipped", "test_escaped", FAILED),
        ("tests.e2e.ran", "test_ran", FAILED),
        ("tests.e2e.planned.TestP", "test_planned", FAILED),
        ("tests.e2e.skipped", "test_passed", ""),
        ("tests.e2e.skipped", "test_flaky", FAILED),
        ("tests.e2e.skipped", "test_flaky", ""),
        ("", "tests.e2e.planned", "<error message='collection'/>"),
        ("tests.e2e.ran", "TestNew::test_new", FAILED),
    )
    pr = _junit_file(tmp_path / "pr.xml", ("tests.e2e.ran", "test_other", ""))
    # The module-level collection error on "tests.e2e.planned" matches the
    # planned class scope of that module.

    result = check(_plan(), [post_merge], [pr])

    assert result is not None
    assert result.post_merge_failures == [
        "::tests.e2e.planned",
        "tests.e2e.planned.TestP::test_planned",
        "tests.e2e.ran::TestNew::test_new",
        "tests.e2e.ran::test_ran",
        "tests.e2e.skipped::test_escaped",
    ]
    assert result.escapes == ["tests.e2e.skipped::test_escaped"]
    assert result.smart_tags_applied is True and result.tests_selected == 5


def test_escape_check_covers_the_p0_fallback(tmp_path: Path) -> None:
    post_merge = _junit_file(
        tmp_path / "post_merge.xml", ("tests.e2e.not_p0", "test_x", FAILED)
    )
    result = check(_plan(smart_tags_applied=False), [post_merge], [])
    assert result is not None
    assert result.escapes == ["tests.e2e.not_p0::test_x"]
    assert result.smart_tags_applied is False


def test_escape_check_reports_a_clean_post_merge_run(tmp_path: Path) -> None:
    post_merge = _junit_file(tmp_path / "post_merge.xml", ("tests.e2e.a", "test_a", ""))
    result = check(_plan(), [post_merge], [])
    assert result is not None
    assert result.post_merge_failures == [] and result.escapes == []


def test_escape_check_skips_a_shadow_plan(tmp_path: Path) -> None:
    post_merge = _junit_file(
        tmp_path / "post_merge.xml", ("tests.e2e.a", "test_a", FAILED)
    )
    assert check(_plan(smart_mode="shadow"), [post_merge], []) is None
