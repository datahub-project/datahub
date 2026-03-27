"""Tests for bump_schema_versions.py"""

import argparse
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bump_schema_versions as bsv


# ---------------------------------------------------------------------------
# PDL fixture helpers
# ---------------------------------------------------------------------------


def write_pdl(root: Path, rel_path: str, content: str) -> Path:
    """Write content to root/rel_path, creating parent dirs. Returns the Path."""
    p = root / rel_path
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(content, encoding="utf-8")
    return p


def aspect_pdl(name: str, includes: str = "", schema_version: int | None = None) -> str:
    fields = [f'"name": "{name}"']
    if schema_version is not None:
        fields.append(f'"schemaVersion": {schema_version}')
    annotation = "{\n" + ",\n".join(f"  {f}" for f in fields) + "\n}"
    includes_clause = f" includes {includes}" if includes else ""
    return f"@Aspect = {annotation}\nrecord {name.title()}{includes_clause} {{ field: string }}"


def record_pdl(namespace: str, name: str, imports: list[str] | None = None, includes: str = "") -> str:
    lines = [f"namespace {namespace}"]
    for imp in imports or []:
        lines.append(f"import {imp}")
    includes_clause = f" includes {includes}" if includes else ""
    lines.append(f"record {name}{includes_clause} {{ field: string }}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# parse_pdl_header
# ---------------------------------------------------------------------------


def test_parse_pdl_header_extracts_namespace():
    ns, imports = bsv.parse_pdl_header("namespace com.linkedin.dataset\n\nrecord Foo {}")
    assert ns == "com.linkedin.dataset"
    assert imports == {}


def test_parse_pdl_header_extracts_imports():
    content = (
        "namespace com.linkedin.dataset\n"
        "import com.linkedin.timeseries.TimeseriesAspectBase\n"
        "import com.linkedin.common.Ownership\n"
        "\nrecord Foo {}"
    )
    ns, imports = bsv.parse_pdl_header(content)
    assert imports["TimeseriesAspectBase"] == "com.linkedin.timeseries.TimeseriesAspectBase"
    assert imports["Ownership"] == "com.linkedin.common.Ownership"


def test_parse_pdl_header_no_namespace_or_imports():
    ns, imports = bsv.parse_pdl_header("record Foo {}")
    assert ns == ""
    assert imports == {}


# ---------------------------------------------------------------------------
# parse_includes
# ---------------------------------------------------------------------------


def test_parse_includes_single_name():
    assert bsv.parse_includes("record Foo includes Bar { field: string }") == ["Bar"]


def test_parse_includes_multiple_names():
    result = bsv.parse_includes("record Foo includes Bar, Baz, Qux { field: string }")
    assert result == ["Bar", "Baz", "Qux"]


def test_parse_includes_multiple_record_definitions():
    # Regression for review comment #3 — re.search only caught the first clause
    content = (
        "record Foo includes Bar { f: string }\n"
        "record Boo includes Zap { g: string }\n"
    )
    result = bsv.parse_includes(content)
    assert "Bar" in result
    assert "Zap" in result
    assert len(result) == 2


def test_parse_includes_no_includes():
    assert bsv.parse_includes("record Foo { field: string }") == []


# ---------------------------------------------------------------------------
# resolve_includes
# ---------------------------------------------------------------------------


def test_resolve_includes_via_import():
    content = (
        "namespace com.linkedin.dataset\n"
        "import com.linkedin.timeseries.TimeseriesAspectBase\n"
        "\nrecord Foo includes TimeseriesAspectBase { field: string }"
    )
    assert bsv.resolve_includes(content) == ["com.linkedin.timeseries.TimeseriesAspectBase"]


def test_resolve_includes_via_namespace_fallback():
    content = (
        "namespace com.linkedin.dataset\n"
        "\nrecord Foo includes LocalRecord { field: string }"
    )
    assert bsv.resolve_includes(content) == ["com.linkedin.dataset.LocalRecord"]


def test_resolve_includes_mixed():
    content = (
        "namespace com.linkedin.dataset\n"
        "import com.linkedin.common.Ownership\n"
        "\nrecord Foo includes Ownership, LocalRecord { field: string }"
    )
    result = bsv.resolve_includes(content)
    assert "com.linkedin.common.Ownership" in result
    assert "com.linkedin.dataset.LocalRecord" in result


def test_resolve_includes_unknown_name_omitted():
    # No namespace, no imports — unresolvable name is dropped
    assert bsv.resolve_includes("record Foo includes Unknown { field: string }") == []


# ---------------------------------------------------------------------------
# find_aspect_annotation_bounds
# ---------------------------------------------------------------------------


def test_find_aspect_annotation_bounds_basic():
    content = '@Aspect = {\n  "name": "foo"\n}\nrecord Foo {}'
    bounds = bsv.find_aspect_annotation_bounds(content)
    assert bounds is not None
    extracted = content[bounds[0] : bounds[1]]
    assert extracted.startswith("{") and extracted.endswith("}")


def test_find_aspect_annotation_bounds_nested_braces():
    content = '@Aspect = {\n  "name": "foo",\n  "nested": {"key": "val"}\n}\nrecord Foo {}'
    bounds = bsv.find_aspect_annotation_bounds(content)
    assert bounds is not None
    assert '"nested"' in content[bounds[0] : bounds[1]]


def test_find_aspect_annotation_bounds_absent():
    assert bsv.find_aspect_annotation_bounds("record Foo { field: string }") is None


# ---------------------------------------------------------------------------
# parse_annotation
# ---------------------------------------------------------------------------


def test_parse_annotation_valid():
    result = bsv.parse_annotation('{"name": "foo", "schemaVersion": 3}')
    assert result == {"name": "foo", "schemaVersion": 3}


def test_parse_annotation_trailing_comma():
    result = bsv.parse_annotation('{"name": "foo", "schemaVersion": 3,}')
    assert result["schemaVersion"] == 3


def test_parse_annotation_malformed_raises():
    with pytest.raises(ValueError):
        bsv.parse_annotation("{not valid json")


# ---------------------------------------------------------------------------
# get_schema_version
# ---------------------------------------------------------------------------


def test_get_schema_version_explicit():
    content = '@Aspect = {\n  "name": "foo",\n  "schemaVersion": 5\n}\nrecord Foo {}'
    assert bsv.get_schema_version(content) == 5


def test_get_schema_version_absent_defaults_to_1():
    content = '@Aspect = {\n  "name": "foo"\n}\nrecord Foo {}'
    assert bsv.get_schema_version(content) == 1


def test_get_schema_version_no_aspect_defaults_to_1():
    assert bsv.get_schema_version("record Foo { field: string }") == 1


# ---------------------------------------------------------------------------
# is_aspect
# ---------------------------------------------------------------------------


def test_is_aspect_true():
    assert bsv.is_aspect('@Aspect = {\n  "name": "foo"\n}\nrecord Foo {}') is True


def test_is_aspect_false():
    assert bsv.is_aspect("record Foo { field: string }") is False


# ---------------------------------------------------------------------------
# update_schema_version
# ---------------------------------------------------------------------------


def test_update_schema_version_replaces_existing():
    content = '@Aspect = {\n  "name": "foo",\n  "schemaVersion": 1\n}\nrecord Foo {}'
    result = bsv.update_schema_version(content, 2)
    assert '"schemaVersion": 2' in result
    assert '"schemaVersion": 1' not in result


def test_update_schema_version_inserts_when_absent():
    content = '@Aspect = {\n  "name": "foo"\n}\nrecord Foo {}'
    result = bsv.update_schema_version(content, 2)
    assert bsv.get_schema_version(result) == 2


def test_update_schema_version_no_aspect_unchanged():
    content = "record Foo { field: string }"
    assert bsv.update_schema_version(content, 2) == content


def test_update_schema_version_trailing_comma_style():
    # Timeseries aspects use trailing commas before the closing brace
    content = '@Aspect = {\n  "name": "timeseries",\n  "type": "timeseries",\n}\nrecord Foo {}'
    result = bsv.update_schema_version(content, 2)
    assert bsv.get_schema_version(result) == 2


# ---------------------------------------------------------------------------
# build_reverse_include_graph
# ---------------------------------------------------------------------------


def test_build_reverse_include_graph_basic(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    base = write_pdl(tmp_path, "com/linkedin/common/Base.pdl",
                     record_pdl("com.linkedin.common", "Base"))
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl",
                       "namespace com.linkedin.dataset\n"
                       "import com.linkedin.common.Base\n"
                       + aspect_pdl("myAspect", includes="Base"))

    graph = bsv.build_reverse_include_graph([base, aspect])
    assert str(aspect) in graph["com.linkedin.common.Base"]


def test_build_reverse_include_graph_two_files_same_include(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    base = write_pdl(tmp_path, "com/linkedin/common/Base.pdl",
                     record_pdl("com.linkedin.common", "Base"))
    a1 = write_pdl(tmp_path, "com/linkedin/dataset/A1.pdl",
                   "namespace com.linkedin.dataset\nimport com.linkedin.common.Base\n"
                   + aspect_pdl("a1", includes="Base"))
    a2 = write_pdl(tmp_path, "com/linkedin/dataset/A2.pdl",
                   "namespace com.linkedin.dataset\nimport com.linkedin.common.Base\n"
                   + aspect_pdl("a2", includes="Base"))

    graph = bsv.build_reverse_include_graph([base, a1, a2])
    assert str(a1) in graph["com.linkedin.common.Base"]
    assert str(a2) in graph["com.linkedin.common.Base"]


def test_build_reverse_include_graph_no_includes(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    solo = write_pdl(tmp_path, "com/linkedin/common/Solo.pdl",
                     record_pdl("com.linkedin.common", "Solo"))
    graph = bsv.build_reverse_include_graph([solo])
    assert len(graph) == 0


# ---------------------------------------------------------------------------
# find_transitively_affected_aspects
# ---------------------------------------------------------------------------


def test_directly_changed_aspect_included(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl",
                       aspect_pdl("myAspect"))

    graph = bsv.build_reverse_include_graph([aspect])
    result = bsv.find_transitively_affected_aspects([str(aspect)], graph, [aspect])
    assert str(aspect) in result


def test_changed_non_aspect_bumps_including_aspect(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    base = write_pdl(tmp_path, "com/linkedin/common/Base.pdl",
                     record_pdl("com.linkedin.common", "Base"))
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl",
                       "namespace com.linkedin.dataset\nimport com.linkedin.common.Base\n"
                       + aspect_pdl("myAspect", includes="Base"))

    graph = bsv.build_reverse_include_graph([base, aspect])
    result = bsv.find_transitively_affected_aspects([str(base)], graph, [base, aspect])
    assert str(aspect) in result
    assert str(base) not in result  # base is not an aspect


def test_multi_hop_transitive_chain(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    a = write_pdl(tmp_path, "com/linkedin/common/A.pdl",
                  record_pdl("com.linkedin.common", "A"))
    b = write_pdl(tmp_path, "com/linkedin/common/B.pdl",
                  record_pdl("com.linkedin.common", "B",
                              imports=["com.linkedin.common.A"], includes="A"))
    c = write_pdl(tmp_path, "com/linkedin/dataset/C.pdl",
                  "namespace com.linkedin.dataset\nimport com.linkedin.common.B\n"
                  + aspect_pdl("cAspect", includes="B"))

    graph = bsv.build_reverse_include_graph([a, b, c])
    result = bsv.find_transitively_affected_aspects([str(a)], graph, [a, b, c])
    assert str(c) in result


def test_only_including_aspects_are_affected(tmp_path, monkeypatch):
    # base changes; aspect_a includes it (affected); aspect_b does not (unaffected)
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    base = write_pdl(tmp_path, "com/linkedin/common/Base.pdl",
                     record_pdl("com.linkedin.common", "Base"))
    aspect_a = write_pdl(tmp_path, "com/linkedin/dataset/AspectA.pdl",
                         "namespace com.linkedin.dataset\nimport com.linkedin.common.Base\n"
                         + aspect_pdl("aspectA", includes="Base"))
    aspect_b = write_pdl(tmp_path, "com/linkedin/dataset/AspectB.pdl",
                         aspect_pdl("aspectB"))  # no includes — unrelated

    graph = bsv.build_reverse_include_graph([base, aspect_a, aspect_b])
    result = bsv.find_transitively_affected_aspects([str(base)], graph, [base, aspect_a, aspect_b])

    assert str(aspect_a) in result
    assert str(aspect_b) not in result


def test_changed_file_with_no_dependents_not_in_result_if_not_aspect(tmp_path, monkeypatch):
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))

    solo = write_pdl(tmp_path, "com/linkedin/common/Solo.pdl",
                     record_pdl("com.linkedin.common", "Solo"))

    graph = bsv.build_reverse_include_graph([solo])
    result = bsv.find_transitively_affected_aspects([str(solo)], graph, [solo])
    assert str(solo) not in result


# ---------------------------------------------------------------------------
# detect_default_branch
# ---------------------------------------------------------------------------


def _make_proc(returncode: int, stdout: str = "") -> MagicMock:
    m = MagicMock()
    m.returncode = returncode
    m.stdout = stdout
    return m


def test_detect_default_branch_from_symbolic_ref(monkeypatch):
    monkeypatch.setattr(subprocess, "run",
                        lambda *a, **kw: _make_proc(0, "refs/remotes/origin/custom-main\n"))
    assert bsv.detect_default_branch() == "custom-main"


def test_detect_default_branch_from_env_var(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _make_proc(1))
    monkeypatch.setenv("MAIN_BRANCH", "custom-main")
    assert bsv.detect_default_branch() == "custom-main"


def test_detect_default_branch_fallback_to_master(monkeypatch):
    monkeypatch.setattr(subprocess, "run", lambda *a, **kw: _make_proc(1))
    monkeypatch.delenv("MAIN_BRANCH", raising=False)
    assert bsv.detect_default_branch() == "master"


# ---------------------------------------------------------------------------
# Version bump scenarios (via main())
#
# These tests exercise the version calculation logic in main():
#   base_version = get_schema_version(base_content) if base_content else 0
#   new_version  = base_version + 1
#   skip if current_version >= new_version
# ---------------------------------------------------------------------------


def _run_main(tmp_path, monkeypatch, changed_files, base_content_by_path):
    """
    Run main() with filesystem and git calls mocked out.

    changed_files        — list of Path objects treated as "changed"
    base_content_by_path — dict mapping str(path) → content string on base branch,
                           or None to simulate a new file not present on base
    """
    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["bump_schema_versions.py", "--base-branch", "master"])
    monkeypatch.setattr(bsv, "get_changed_pdl_files", lambda _branch: [str(f) for f in changed_files])
    monkeypatch.setattr(bsv, "get_file_at_branch",
                        lambda path, _branch: base_content_by_path.get(path))
    return bsv.main()


def test_version_bump_existing_aspect_no_schema_version(tmp_path, monkeypatch):
    # Base has @Aspect with no schemaVersion (defaults to 1) → should bump to 2
    base_content = aspect_pdl("myAspect")  # no schemaVersion
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl", base_content)

    rc = _run_main(tmp_path, monkeypatch, [aspect], {str(aspect): base_content})

    assert rc == 0
    assert bsv.get_schema_version(aspect.read_text()) == 2


def test_version_bump_existing_aspect_with_schema_version(tmp_path, monkeypatch):
    # Base has schemaVersion=3 → should bump to 4
    base_content = aspect_pdl("myAspect", schema_version=3)
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl", base_content)

    rc = _run_main(tmp_path, monkeypatch, [aspect], {str(aspect): base_content})

    assert rc == 0
    assert bsv.get_schema_version(aspect.read_text()) == 4


def test_version_bump_skips_if_already_at_target(tmp_path, monkeypatch):
    # Base has schemaVersion=1 → new_version=2; current file already at 2 → skip
    base_content = aspect_pdl("myAspect", schema_version=1)
    current_content = aspect_pdl("myAspect", schema_version=2)
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl", current_content)

    rc = _run_main(tmp_path, monkeypatch, [aspect], {str(aspect): base_content})

    assert rc == 0
    assert bsv.get_schema_version(aspect.read_text()) == 2  # unchanged


def test_version_bump_new_file_stays_at_1(tmp_path, monkeypatch):
    # File not on base branch (base_content=None) → base_version=0, new_version=1
    # Default schemaVersion is already 1 → skip (already at target)
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/NewAspect.pdl",
                       aspect_pdl("newAspect"))  # no schemaVersion → defaults to 1

    rc = _run_main(tmp_path, monkeypatch, [aspect], {str(aspect): None})

    assert rc == 0
    assert bsv.get_schema_version(aspect.read_text()) == 1


def test_version_bump_dry_run_does_not_write(tmp_path, monkeypatch):
    base_content = aspect_pdl("myAspect")
    aspect = write_pdl(tmp_path, "com/linkedin/dataset/MyAspect.pdl", base_content)
    original = aspect.read_text()

    monkeypatch.setenv("PDL_ROOTS", str(tmp_path))
    monkeypatch.setattr(sys, "argv", ["bump_schema_versions.py", "--base-branch", "master", "--dry-run"])
    monkeypatch.setattr(bsv, "get_changed_pdl_files", lambda _branch: [str(aspect)])
    monkeypatch.setattr(bsv, "get_file_at_branch", lambda path, _branch: base_content)

    rc = bsv.main()

    assert rc == 0
    assert aspect.read_text() == original  # file untouched
