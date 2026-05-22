"""Tests for report_aspect_changes.py"""

import subprocess
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import report_aspect_changes as rac


# ---------------------------------------------------------------------------
# resolve_base — acryl mode (mode passed explicitly to bypass auto-detect)
# ---------------------------------------------------------------------------


def test_resolve_base_returns_latest_stable_cloud_tag_from_global_list():
    """Happy path: `git tag --list` returns version-sorted tags; first non-rc
    `-cloud` tag is returned. Verifies the global (ancestry-independent)
    lookup — stable cloud releases are typically on parallel hotfix branches,
    not ancestors of acryl-main, so an ancestry-based `git describe` would
    miss them.
    """
    # Simulated `git tag --list 'v*-cloud' --sort=-v:refname` output: a mix of
    # rc and stable tags, version-sorted descending. The filter should skip
    # the rc tags at the top and return the first stable below them.
    tag_list = "v1.1.0rc3-cloud\nv1.1.0rc2-cloud\nv1.1.0rc1-cloud\nv1.0.1-cloud\nv1.0.0-cloud\n"
    with patch("subprocess.check_output", return_value=tag_list) as m:
        assert rac.resolve_base(mode="acryl") == "v1.0.1-cloud"
    # Only one git invocation needed when stable lookup succeeds.
    assert m.call_count == 1


def test_resolve_base_invokes_git_tag_list_with_sort_flag():
    """First-pass call must use `git tag --list` (not `git describe`) with the
    `-cloud` match pattern and version-sort flag — the ancestry-independent
    enumeration is core to the fix.
    """
    with patch("subprocess.check_output", return_value="v1.0.1-cloud\n") as m:
        rac.resolve_base(mode="acryl")
    args = m.call_args[0][0]
    assert args[:3] == ["git", "tag", "--list"]
    assert "v*-cloud" in args
    assert "--sort=-v:refname" in args


def test_resolve_base_falls_back_to_rc_when_no_stable_cloud():
    """When no stable `-cloud` tag exists in the repo, fall back to the latest
    `-cloud` tag of any kind (rc accepted). Handles the early-cycle case
    before any stable release has shipped.
    """
    rc_only_list = "v1.1.0rc3-cloud\nv1.1.0rc2-cloud\nv1.1.0rc1-cloud\n"
    # First call: same list, but rc filter excludes everything → empty result.
    # Second call: no filter, returns the latest rc-cloud tag.
    with patch(
            "subprocess.check_output",
            side_effect=[rc_only_list, rc_only_list],
    ) as m:
        assert rac.resolve_base(mode="acryl") == "v1.1.0rc3-cloud"
    # First lookup exhausted (all rc), second lookup succeeded without filter.
    assert m.call_count == 2


def test_resolve_base_raises_when_no_cloud_tag_in_repo():
    """Both lookups return empty → SystemExit with diagnostic listing what
    was tried.
    """
    with patch("subprocess.check_output", side_effect=["", ""]):
        with pytest.raises(SystemExit) as exc:
            rac.resolve_base(mode="acryl")
    msg = str(exc.value)
    assert "Could not find" in msg
    assert "--base" in msg
    # Diagnostic should mention the git command tried so user can reproduce.
    assert "v*-cloud" in msg


def test_resolve_base_rc_substring_filter_skips_rc_tags_only():
    """The Python-side filter must skip rc tags but not stable tags. Verifies
    the filtering logic (since git tag --list returns everything matching the
    glob; the rc filter is done in Python, not by git).
    """
    # Mixed list — rc tags first (newest by version), stable tags after.
    tag_list = (
        "v1.1.0rc1-cloud\n"  # skip: contains "rc"
        "v1.0.2rc2-cloud\n"  # skip: contains "rc"
        "v1.0.1-cloud\n"  # ← first stable, should be picked
        "v1.0.0-cloud\n"
    )
    with patch("subprocess.check_output", return_value=tag_list):
        assert rac.resolve_base(mode="acryl") == "v1.0.1-cloud"


# ---------------------------------------------------------------------------
# _detect_mode — repo-flavor auto-detection
# ---------------------------------------------------------------------------


def test_detect_mode_returns_acryl_when_acryl_main_ref_exists():
    """`git rev-parse --verify --quiet acryl-main` succeeds → acryl mode."""
    with patch("subprocess.check_output", return_value="abc123\n") as m:
        assert rac._detect_mode() == "acryl"
    # Verify the probe command — should be `git rev-parse --verify` against
    # `acryl-main` specifically. Other refs would give false positives.
    args = m.call_args[0][0]
    assert args[:3] == ["git", "rev-parse", "--verify"]
    assert "acryl-main" in args


def test_detect_mode_returns_oss_when_acryl_main_ref_missing():
    """`git rev-parse --verify acryl-main` exits non-zero in the OSS repo →
    oss mode. Triggers when the ref doesn't exist in any namespace.
    """
    err = subprocess.CalledProcessError(returncode=1, cmd=["git"])
    with patch("subprocess.check_output", side_effect=err):
        assert rac._detect_mode() == "oss"


# ---------------------------------------------------------------------------
# _default_head — per-mode head ref default
# ---------------------------------------------------------------------------


def test_default_head_acryl_mode_returns_acryl_main():
    assert rac._default_head("acryl") == "acryl-main"


def test_default_head_oss_mode_returns_master():
    """OSS mode returns `master` when the local ref resolves. Mocked so the
    result doesn't depend on whether the test environment is a fork checkout
    (where only `origin/master` exists) or an OSS checkout.
    """
    with patch("subprocess.check_output", return_value="abc123\n"):
        assert rac._default_head("oss") == "master"


def test_default_head_auto_detects_when_mode_not_passed():
    """When mode=None, falls back to _detect_mode(). Mocking subprocess so the
    test doesn't depend on the local repo flavor.
    """
    err = subprocess.CalledProcessError(returncode=1, cmd=["git"])
    with patch("subprocess.check_output", side_effect=err):
        assert rac._default_head() == "master"


# ---------------------------------------------------------------------------
# resolve_base — OSS mode
# ---------------------------------------------------------------------------


def test_resolve_base_oss_returns_latest_stable_non_cloud_tag():
    """OSS mode: latest stable v* tag, excluding rc and -cloud. The exclude
    filter must skip -cloud tags even when they sort newest, since a checkout
    with both OSS and fork tags should still pick the right release line.
    """
    # Mixed list as `git tag --list 'v*' --sort=-v:refname` would return on a
    # repo with both OSS and cloud tags. rc tags skipped by exclude_substr;
    # -cloud tags skipped by exclude_extra; first surviving entry wins.
    tag_list = (
        "v1.6.0rc2\n"  # skip: rc
        "v1.6.0rc1\n"  # skip: rc
        "v1.5.0.7-cloud\n"  # skip: -cloud
        "v1.5.0.7\n"  # ← first stable non-cloud, should be picked
        "v1.5.0.6\n"
        "v1.5.0\n"
    )
    with patch("subprocess.check_output", return_value=tag_list) as m:
        assert rac.resolve_base(mode="oss") == "v1.5.0.7"
    assert m.call_count == 1


def test_resolve_base_oss_invokes_git_tag_list_with_v_star_match():
    """OSS mode probes `v*` (not `v*-cloud`) so OSS tags without the cloud
    suffix are returned. The -cloud filter happens Python-side.
    """
    with patch("subprocess.check_output", return_value="v1.5.0\n") as m:
        rac.resolve_base(mode="oss")
    args = m.call_args[0][0]
    assert args[:3] == ["git", "tag", "--list"]
    assert "v*" in args
    assert "v*-cloud" not in args  # would over-narrow and miss OSS tags


def test_resolve_base_oss_falls_back_to_rc_when_no_stable():
    """OSS mode: when no stable v* tag exists (all are rc), fall back to
    accepting rc tags (while still excluding -cloud).
    """
    rc_only_list = "v1.6.0rc2\nv1.6.0rc1\n"
    with patch(
            "subprocess.check_output",
            side_effect=[rc_only_list, rc_only_list],
    ) as m:
        assert rac.resolve_base(mode="oss") == "v1.6.0rc2"
    assert m.call_count == 2


def test_resolve_base_oss_raises_when_no_tag_in_repo():
    """OSS mode: both lookups empty → SystemExit. Diagnostic mentions the
    glob used and the mode for the user to reproduce.
    """
    with patch("subprocess.check_output", side_effect=["", ""]):
        with pytest.raises(SystemExit) as exc:
            rac.resolve_base(mode="oss")
    msg = str(exc.value)
    assert "Could not find" in msg
    assert "--base" in msg
    assert "mode=oss" in msg


def test_resolve_base_rejects_unknown_mode():
    """Defensive: an invalid mode string should raise loudly rather than
    silently picking one branch.
    """
    with pytest.raises(ValueError, match="unknown mode"):
        rac.resolve_base(mode="nonsense")


# ---------------------------------------------------------------------------
# resolve_base — auto-detect (mode=None)
# ---------------------------------------------------------------------------


def test_resolve_base_auto_detect_uses_oss_when_no_acryl_main():
    """When mode is not passed, resolve_base() auto-detects via _detect_mode.
    With both `acryl-main` and `origin/acryl-main` missing, OSS path runs and
    picks a non-cloud tag.
    """
    err = subprocess.CalledProcessError(returncode=1, cmd=["git"])
    oss_tag_list = "v1.5.0.7-cloud\nv1.5.0.7\nv1.5.0\n"
    with patch("subprocess.check_output", side_effect=[err, err, oss_tag_list]) as m:
        assert rac.resolve_base() == "v1.5.0.7"
    # First two calls: detection probes (acryl-main, origin/acryl-main); third: tag list.
    assert m.call_count == 3


# ---------------------------------------------------------------------------
# PDL parsing primitives
# ---------------------------------------------------------------------------


ASPECT_PDL = """
namespace com.linkedin.dataset

@Aspect = {
  "name": "datasetProperties",
  "schemaVersion": 3
}
record DatasetProperties {
  description: optional string
  customProperties: map[string, string] = {}
  externalUrl: optional string
}
"""

ENUM_PDL = """
namespace com.linkedin.common

enum FabricType {
  PROD
  CORP
  DEV
}
"""

RENAMED_PDL = """
namespace com.linkedin.dataset

@renamedFrom = "OldDatasetProperties"
record DatasetPropertiesV2 {
  description: optional string
}
"""


def test_aspect_meta_extracts_name_and_schema_version():
    meta = rac.aspect_meta(ASPECT_PDL)
    assert meta == {"name": "datasetProperties", "schemaVersion": 3, "type": None}


def test_aspect_meta_returns_none_for_non_aspect():
    assert rac.aspect_meta(ENUM_PDL) is None


def test_aspect_meta_handles_missing_schema_version():
    src = '@Aspect = {"name": "foo"}\nrecord Foo { x: string }'
    assert rac.aspect_meta(src) == {"name": "foo", "schemaVersion": None, "type": None}


def test_record_name_returns_top_level_record():
    assert rac.record_name(ASPECT_PDL) == "DatasetProperties"


def test_record_name_returns_none_for_enum_only_file():
    assert rac.record_name(ENUM_PDL) is None


def test_renamed_from_returns_old_name():
    assert rac.renamed_from(RENAMED_PDL) == "OldDatasetProperties"


def test_renamed_from_returns_none_when_absent():
    assert rac.renamed_from(ASPECT_PDL) is None


def test_fields_returns_name_to_metadata_map():
    fs = rac.fields(ASPECT_PDL)
    assert set(fs.keys()) == {"description", "customProperties", "externalUrl"}
    assert fs["description"]["optional"] is True
    assert fs["customProperties"]["optional"] is False
    assert "string" in fs["description"]["type"]


def test_fields_ignores_nested_record_fields():
    src = """
    record Outer {
      a: string
      nested: record Inner { b: string }
    }
    """
    fs = rac.fields(src)
    assert "a" in fs
    assert "b" not in fs  # b belongs to Inner, not Outer


def test_enums_returns_symbols_per_enum():
    es = rac.enums(ENUM_PDL)
    assert es == {"FabricType": ["PROD", "CORP", "DEV"]}


def test_enums_returns_empty_when_no_enum_present():
    assert rac.enums(ASPECT_PDL) == {}


# ---------------------------------------------------------------------------
# Per-file classifier
# ---------------------------------------------------------------------------


def test_analyze_pure_addition_is_additive_only(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string\n y: optional string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "abc1234 add y")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.is_aspect
    assert any("added optional field: y" in line for line in f.additive)
    assert f.breaking == []


def test_analyze_removed_field_is_breaking(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string\n y: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("removed field: y" in line for line in f.breaking)


def test_analyze_required_to_optional_is_noisy(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: optional string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("required→optional" in line for line in f.noisy)
    assert f.breaking == []


def test_analyze_optional_to_required_is_breaking(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: optional string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("optional→required" in line for line in f.breaking)


def test_analyze_type_change_is_breaking(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = (
        'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: int }'
    )

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("type change on x: string→int" in line for line in f.breaking)


def test_analyze_enum_removal_is_breaking(monkeypatch):
    old = "namespace x\nenum E { A B C }"
    new = "namespace x\nenum E { A B }"

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("enum E: removed value C" in line for line in f.breaking)


def test_analyze_record_rename_without_renamedFrom_is_breaking(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a"}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a"}\nrecord B { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("renamed A→B with NO renamedFrom" in line for line in f.breaking)


def test_analyze_record_rename_with_renamedFrom_is_noisy(monkeypatch):
    # Renames are structural changes, so a proper rename PR also bumps
    # schemaVersion (which is what bump_schema_versions would do). Test
    # the well-formed case: rename annotation present AND version bumped.
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\n@renamedFrom = "A"\nrecord B { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("renamed A→B (renamedFrom set)" in line for line in f.noisy)
    assert f.breaking == []


def test_analyze_rename_with_renamedFrom_AND_schema_bump_is_bump_done(monkeypatch):
    """A properly annotated rename IS a structural change; pairing it with a
    schemaVersion bump should classify as bump_done (legitimate), not as
    bump_spurious (auto-bumper side-effect)."""
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\n@renamedFrom = "A"\nrecord B { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *args: [])

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.BUMP_DONE
    # The "schemaVersion bumped with NO structural change" noisy entry must
    # NOT fire here — the rename IS the structural change.
    assert not any("bumped with NO structural change" in line for line in f.noisy)


def test_analyze_deleted_aspect_renders_as_aspect_not_nested(monkeypatch):
    """When an aspect file is deleted at head, is_aspect must remain True
    (sourced from the base content) so the report renders the finding as
    [ASPECT] in the BREAKING section, not as [nested]."""
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else "")
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *args: [])

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.is_aspect
    assert f.aspect_name == "a"
    assert any("file deleted" in line for line in f.breaking)


def test_analyze_schema_version_bump_without_structural_change_is_noisy(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    # identical body, just a version bump and a comment
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string /* doc */ }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any(
        "schemaVersion 1→2 bumped with NO structural change" in line for line in f.noisy
    )
    assert f.breaking == []


def test_analyze_structural_change_without_schema_version_bump_is_breaking(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string\n y: string }'
    # y removed but schemaVersion NOT bumped
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'

    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")

    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert any("removed field: y" in line for line in f.breaking)
    assert any(
        "structural change without schemaVersion bump" in line for line in f.breaking
    )


# ---------------------------------------------------------------------------
# Transitive aspect surfacing
# ---------------------------------------------------------------------------


def test_find_transitive_aspects_resolves_via_bump_schema_helpers(
        monkeypatch, tmp_path
):
    # Simulate: changed non-aspect file NonAspect.pdl is depended on by
    # one aspect AspectA.pdl. find_transitive_aspects must return AspectA.
    non_aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/NonAspect.pdl"
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"

    fake_reverse = {"com.linkedin.x.NonAspect": {aspect_path}}
    fake_all = [Path(non_aspect_path), Path(aspect_path)]

    monkeypatch.setattr(rac.bsv, "find_all_pdl_files", lambda: fake_all)
    monkeypatch.setattr(rac.bsv, "build_reverse_include_graph", lambda _: fake_reverse)

    # Use the real find_transitively_affected_aspects, but stub Path.exists/read_text
    # because the helper opens the files to check is_aspect.
    class _FakePath(type(Path())):
        def exists(self):
            return True

        def read_text(self, encoding="utf-8"):
            if str(self).endswith("AspectA.pdl"):
                return '@Aspect = {"name": "a"}\nrecord A { x: string }'
            return "record NonAspect { y: string }"

    monkeypatch.setattr(rac.bsv, "Path", _FakePath)

    affected = rac.find_transitive_aspects([non_aspect_path])
    assert aspect_path in affected


# ---------------------------------------------------------------------------
# Markdown rendering
# ---------------------------------------------------------------------------


def _finding(
        path: str,
        breaking=None,
        additive=None,
        noisy=None,
        is_aspect=True,
        aspect_name="a",
        head_commit="abc1234 msg",
        affected_via=None,
):
    return rac.FileFinding(
        path=path,
        is_aspect=is_aspect,
        aspect_name=aspect_name,
        head_commit=head_commit,
        affected_via=affected_via,
        breaking=breaking or [],
        additive=additive or [],
        noisy=noisy or [],
    )


def test_render_empty_diff_emits_no_changes_message():
    out = rac.render_report(
        [], base="v1.0.1-cloud", head="acryl-main", head_sha="deadbeef"
    )
    assert "No aspect changes" in out
    assert "v1.0.1-cloud" in out and "acryl-main" in out


def test_upstream_attribution_for_transitive_aggregates_across_non_aspects(
        monkeypatch,
):
    """For purely-transitive aspects (the file itself has no commits in the
    window), the helper should pull PRs/owner/date from the changed non-aspect
    record(s) — not from the aspect's own (empty) history."""
    src_data = {
        "non/A.pdl": {
            "prs": ["9100", "9200"],
            "author": "Alice",
            "date": "2026-05-01",
        },
        "non/B.pdl": {
            "prs": ["9300"],
            "author": "Bob",
            "date": "2026-05-10",
        },
    }
    monkeypatch.setattr(
        rac, "pr_numbers_for_file", lambda ref, p, base: src_data[p]["prs"]
    )
    monkeypatch.setattr(
        rac, "last_author_for_file", lambda ref, p, base: src_data[p]["author"]
    )
    monkeypatch.setattr(
        rac, "latest_commit_date_for_file", lambda ref, p, base: src_data[p]["date"]
    )

    prs, author, date = rac.upstream_attribution_for_transitive(
        ["non/A.pdl", "non/B.pdl"], "HEAD", "BASE"
    )
    # PRs aggregated and sorted ascending
    assert prs == ["9100", "9200", "9300"]
    # Author/date from the MOST RECENT upstream commit (B, 2026-05-10)
    assert author == "Bob"
    assert date == "2026-05-10"


def test_upstream_attribution_handles_empty_non_aspect_list(monkeypatch):
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *a: [])
    monkeypatch.setattr(rac, "last_author_for_file", lambda *a: None)
    monkeypatch.setattr(rac, "latest_commit_date_for_file", lambda *a: None)
    prs, author, date = rac.upstream_attribution_for_transitive([], "HEAD", "BASE")
    assert prs == []
    assert author is None
    assert date is None


def test_upstream_attribution_dedupes_prs_across_sources(monkeypatch):
    """Two upstream non-aspects touched by the same PR should yield ONE entry."""
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda ref, p, base: ["9465"])
    monkeypatch.setattr(rac, "last_author_for_file", lambda *a: "david-leifker")
    monkeypatch.setattr(rac, "latest_commit_date_for_file", lambda *a: "2026-05-11")
    prs, _, _ = rac.upstream_attribution_for_transitive(
        ["non/A.pdl", "non/B.pdl"], "HEAD", "BASE"
    )
    assert prs == ["9465"]


def test_aggregate_per_pr_verdict_priority_order():
    """Highest-priority verdict wins: spurious > needed > done > bump_not_needed."""
    assert (
            rac._aggregate_per_pr_verdict(
                [
                    {"bump_status": rac.BUMP_DONE},
                    {"bump_status": rac.BUMP_SPURIOUS},
                    {"bump_status": rac.BUMP_NEEDED},
                ]
            )
            == rac.BUMP_SPURIOUS
    )
    assert (
            rac._aggregate_per_pr_verdict(
                [
                    {"bump_status": rac.BUMP_DONE},
                    {"bump_status": rac.BUMP_NEEDED},
                ]
            )
            == rac.BUMP_NEEDED
    )
    assert (
            rac._aggregate_per_pr_verdict(
                [
                    {"bump_status": rac.BUMP_DONE},
                    {"bump_status": rac.BUMP_DONE},
                ]
            )
            == rac.BUMP_DONE
    )
    # bump_not_needed (formerly not_sure / n/a) is the lowest-priority fallback
    assert (
            rac._aggregate_per_pr_verdict(
                [
                    {"bump_status": rac.BUMP_DONE},
                    {"bump_status": rac.BUMP_NOT_NEEDED},
                ]
            )
            == rac.BUMP_DONE
    )
    assert rac._aggregate_per_pr_verdict([]) == rac.BUMP_NOT_NEEDED


def test_aggregator_ignores_needed_when_caught_up_by_later_slice():
    """A BUMP_NEEDED entry whose PR was paid by a later catch-up bump is
    no longer treated as unreconciled — the aggregator returns BUMP_DONE."""
    entries = [
        {"pr": "A", "bump_status": rac.BUMP_NEEDED},
        {
            "pr": "B",
            "bump_status": rac.BUMP_DONE,
            "catch_up_for_prs": ["A"],
        },
    ]
    assert rac._aggregate_per_pr_verdict(entries) == rac.BUMP_DONE


def test_aggregator_keeps_spurious_even_when_needed_was_caught_up():
    """A truly-spurious slice (no debt to pay) still wins over a
    catch-up-reconciled NEEDED: the file lands in bump_spurious."""
    entries = [
        {"pr": "A", "bump_status": rac.BUMP_NEEDED},
        {
            "pr": "B",
            "bump_status": rac.BUMP_DONE,
            "catch_up_for_prs": ["A"],
        },
        {"pr": "C", "bump_status": rac.BUMP_SPURIOUS},
    ]
    assert rac._aggregate_per_pr_verdict(entries) == rac.BUMP_SPURIOUS


def test_aggregator_returns_needed_when_some_needed_slices_unreconciled():
    """If a NEEDED PR was caught up but another NEEDED PR was NOT, the
    file is still bump_needed for the unreconciled one."""
    entries = [
        {"pr": "A", "bump_status": rac.BUMP_NEEDED},
        {
            "pr": "B",
            "bump_status": rac.BUMP_DONE,
            "catch_up_for_prs": ["A"],
        },
        {"pr": "C", "bump_status": rac.BUMP_NEEDED},  # not in catch_up_for_prs
    ]
    assert rac._aggregate_per_pr_verdict(entries) == rac.BUMP_NEEDED


def test_main_cumulative_bucket_follows_per_pr_spurious_slice(monkeypatch, tmp_path):
    """The cumulative bucket follows the per-PR aggregator (post-catch-up).
    A file with cumulative bump_done but an unreconciled spurious per-PR
    slice lands in `bump_spurious` — not bump_done. Reviewers see the
    Per-PR breakdown table to identify which specific PR was at fault."""
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"
    aspect_old = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    aspect_new = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string\n y: optional string }'

    # Cumulative diff has a real change → cumulative classifier would say bump_done.
    monkeypatch.setattr(rac, "changed_pdls", lambda b, h: [aspect_path])
    monkeypatch.setattr(
        rac, "file_at", lambda ref, p: aspect_old if ref == "BASE" else aspect_new
    )
    monkeypatch.setattr(rac, "resolve_base", lambda *a, **kw: "BASE")
    monkeypatch.setattr(rac, "_head_sha", lambda ref: "sha123")
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "abc1 feat (#1234)")
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *a: ["1234"])
    monkeypatch.setattr(rac, "last_author_for_file", lambda *a: "Alice")
    monkeypatch.setattr(rac, "find_transitive_aspects", lambda non_aspect: set())
    monkeypatch.setattr(rac, "find_transitive_aspects_per_source", lambda srcs: {})
    monkeypatch.setattr(rac, "find_mutators_added_in_window", lambda *a: [])
    monkeypatch.setattr(rac, "discover_mutator_hierarchy", lambda: set())
    # Per-PR audit shows one real-change PR (#9555 = bump_done) and one
    # truly-spurious PR (#9579, no debt to pay). Per-PR truth drives the
    # cumulative bucket → bump_spurious wins.
    monkeypatch.setattr(
        rac,
        "per_pr_audit",
        lambda base, head: {
            aspect_path: [
                {
                    "sha": "aaa1111111",
                    "pr": "9555",
                    "subject": "feat: real (#9555)",
                    "bump_status": rac.BUMP_DONE,
                    "author": "Carol",
                    "aspect_name": "a",
                },
                {
                    "sha": "bbb2222222",
                    "pr": "9579",
                    "subject": "feat: spurious (#9579)",
                    "bump_status": rac.BUMP_SPURIOUS,
                    "author": "Dave",
                    "aspect_name": "a",
                },
            ]
        },
    )

    out_file = tmp_path / "report.md"
    rc = rac.main(["--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()
    # Per-PR truth wins: file lands in bump_spurious, not bump_done.
    assert "0 bump_done" in text
    assert "1 bump_spurious" in text
    assert "### `bump_spurious` (1)" in text
    assert "AspectA.pdl" in text
    # No misalignment annotation any longer — bump_done has clean bucket text
    # because misaligned files are now in bump_spurious.
    assert "bump and change rode in different PRs" not in text


def test_main_cumulative_bucket_clean_when_all_slices_are_done(monkeypatch, tmp_path):
    """Sanity: cumulative bump_done with all per-PR slices also bump_done
    yields a clean bump_done row."""
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"
    aspect_old = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    aspect_new = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string\n y: optional string }'

    monkeypatch.setattr(rac, "changed_pdls", lambda b, h: [aspect_path])
    monkeypatch.setattr(
        rac, "file_at", lambda ref, p: aspect_old if ref == "BASE" else aspect_new
    )
    monkeypatch.setattr(rac, "resolve_base", lambda *a, **kw: "BASE")
    monkeypatch.setattr(rac, "_head_sha", lambda ref: "sha123")
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "abc1 feat (#1234)")
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *a: ["1234"])
    monkeypatch.setattr(rac, "last_author_for_file", lambda *a: "Alice")
    monkeypatch.setattr(rac, "find_transitive_aspects", lambda non_aspect: set())
    monkeypatch.setattr(rac, "find_transitive_aspects_per_source", lambda srcs: {})
    monkeypatch.setattr(rac, "find_mutators_added_in_window", lambda *a: [])
    monkeypatch.setattr(rac, "discover_mutator_hierarchy", lambda: set())
    monkeypatch.setattr(
        rac,
        "per_pr_audit",
        lambda base, head: {
            aspect_path: [
                {
                    "sha": "aaa1111111",
                    "pr": "9555",
                    "subject": "feat: real (#9555)",
                    "bump_status": rac.BUMP_DONE,
                    "author": "Carol",
                    "aspect_name": "a",
                },
            ]
        },
    )

    out_file = tmp_path / "report.md"
    rc = rac.main(["--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()
    assert "1 bump_done" in text
    assert "### `bump_done` (1)" in text
    assert "AspectA.pdl" in text


def test_main_cumulative_mode_still_overrides_when_promoting_to_needed_or_spurious(
        monkeypatch, tmp_path
):
    """The fix only protects cumulative bump_done from being downgraded by
    per-PR spurious slices. Other cumulative→per-PR transitions still flow
    through normally — e.g., cumulative bump_not_needed with a per-PR
    spurious slice should still surface as bump_spurious (the original
    PR #9579 motivating case where no real change exists in the window)."""
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"
    # No structural change cumulatively, no bump cumulatively → bump_not_needed.
    aspect_same = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'

    monkeypatch.setattr(rac, "changed_pdls", lambda b, h: [aspect_path])
    monkeypatch.setattr(rac, "file_at", lambda ref, p: aspect_same)
    monkeypatch.setattr(rac, "resolve_base", lambda *a, **kw: "BASE")
    monkeypatch.setattr(rac, "_head_sha", lambda ref: "sha123")
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "abc1 feat (#1234)")
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda *a: ["1234"])
    monkeypatch.setattr(rac, "last_author_for_file", lambda *a: "Alice")
    monkeypatch.setattr(rac, "find_transitive_aspects", lambda non_aspect: set())
    monkeypatch.setattr(rac, "find_transitive_aspects_per_source", lambda srcs: {})
    monkeypatch.setattr(rac, "find_mutators_added_in_window", lambda *a: [])
    monkeypatch.setattr(rac, "discover_mutator_hierarchy", lambda: set())
    # Per-PR audit: only spurious slices. Cumulative bump_not_needed should
    # be overridden to bump_spurious (existing behavior preserved).
    monkeypatch.setattr(
        rac,
        "per_pr_audit",
        lambda base, head: {
            aspect_path: [
                {
                    "sha": "bbb2222222",
                    "pr": "9579",
                    "subject": "feat: spurious (#9579)",
                    "bump_status": rac.BUMP_SPURIOUS,
                    "author": "Dave",
                    "aspect_name": "a",
                },
            ]
        },
    )

    out_file = tmp_path / "report.md"
    rc = rac.main(["--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()
    assert "**1 bump_spurious**" in text
    assert "### `bump_spurious` (1)" in text


def test_render_all_pdls_section_appears_before_bump_status_breakdown():
    """The 'All changed PDL files' section is the first H2 section in the body —
    above the Bump status line and the breakdown tables."""
    f = _finding("a.pdl", additive=["x"])
    out = rac.render_report([f], base="B", head="H", head_sha="sha")
    assert out.index("## All changed PDL files") < out.index("**Bump status:**")
    assert out.index("## All changed PDL files") < out.index(
        "**Bump status breakdown:**"
    )


def test_render_all_pdls_section_lists_every_finding():
    """The All-PDLs section enumerates every finding (aspect, nested, and
    transitively-affected) with PR + owner + Date + Why attribution, sorted
    by Date DESC. Per-row Why matches the file's bump bucket Why."""
    f_aspect = _finding(
        "metadata-models/.../AspectA.pdl",
        breaking=["removed field: y"],
        head_commit="abc1 feat (#9555)",
    )
    f_aspect.pr_numbers = ["9555"]
    f_aspect.last_author = "Alice"
    f_aspect.last_commit_date = "2026-05-05"
    f_aspect.has_structural = True
    f_aspect.bump_status = rac.BUMP_NEEDED  # structural change without bump
    f_nested = _finding(
        "metadata-models/.../NestedX.pdl",
        is_aspect=False,
        additive=["new field"],
        head_commit="def2 feat (#9579)",
    )
    f_nested.pr_numbers = ["9579"]
    f_nested.last_author = "Bob"
    f_nested.last_commit_date = "2026-05-15"
    f_nested.bump_reason = "non-aspect record (no schemaVersion concept applies)"
    f_transitive = _finding(
        "metadata-models/.../TransitiveZ.pdl",
        noisy=["transitively affected"],
        affected_via="ChangedRecord",
    )
    f_transitive.bump_status = rac.BUMP_NEEDED
    f_transitive.is_purely_transitive = True
    # transitive finding has no direct edits → no PR, no owner, no date
    out = rac.render_report(
        [f_aspect, f_nested, f_transitive], base="B", head="H", head_sha="sha"
    )
    assert "## All changed PDL files (3)" in out
    assert "| File | Kind | PR(s) | Owner | Date | Why |" in out
    # Sorted by Date DESC — NestedX (2026-05-15) before AspectA (2026-05-05) before
    # TransitiveZ (no date, sorts last)
    nested_idx = out.index("NestedX.pdl")
    aspect_idx = out.index("AspectA.pdl")
    transitive_idx = out.index("TransitiveZ.pdl")
    assert nested_idx < aspect_idx < transitive_idx
    # NestedX row uses the bump_reason from the classifier (non-aspect record)
    assert (
            "| NestedX.pdl | nested | #9579 | Bob | 2026-05-15 | "
            "non-aspect record (no schemaVersion concept applies) |" in out
    )
    # AspectA's Why reflects bump_needed (direct schema change, no bump)
    assert (
            "| AspectA.pdl | ASPECT | #9555 | Alice | 2026-05-05 | "
            "direct schema change in this file — schemaVersion was NOT bumped |" in out
    )
    # TransitiveZ row marks the transitive-via reason via bump_needed Why
    assert (
            "| TransitiveZ.pdl | ASPECT (transitive) | (no-PR) "
            "| _(no owner — file wasn't directly touched)_ "
            "| _(unknown)_ | "
            "transitively affected via `ChangedRecord` (file's own schema unchanged) "
            "— schemaVersion was NOT bumped |" in out
    )


def test_all_pdls_kind_label_distinguishes_pure_transitive_from_direct_plus_transitive():
    """Kind column logic for the All-PDLs section:
    - ASPECT — directly edited aspect (regardless of also being BFS-reached)
    - ASPECT (transitive) — purely transitive (file not directly edited)
    - nested — non-aspect record
    """
    # Direct + transitive: file was edited AND reached by BFS. Should be plain ASPECT.
    f_direct_plus_transitive = _finding(
        "metadata-models/.../DirectAndTransitive.pdl",
        breaking=["removed field: x"],
        head_commit="abc1 feat (#9555)",
    )
    f_direct_plus_transitive.pr_numbers = ["9555"]
    f_direct_plus_transitive.last_author = "Alice"
    f_direct_plus_transitive.last_commit_date = "2026-05-10"
    f_direct_plus_transitive.has_structural = True
    f_direct_plus_transitive.affected_via = "ChangedRecord"
    f_direct_plus_transitive.is_purely_transitive = False  # was directly edited
    f_direct_plus_transitive.bump_status = rac.BUMP_DONE

    # Purely transitive: file NOT directly edited. Should be ASPECT (transitive).
    f_pure_transitive = _finding(
        "metadata-models/.../PureTransitive.pdl",
        affected_via="ChangedRecord",
    )
    f_pure_transitive.is_purely_transitive = True
    f_pure_transitive.bump_status = rac.BUMP_NEEDED

    out = rac.render_report(
        [f_direct_plus_transitive, f_pure_transitive],
        base="B",
        head="H",
        head_sha="sha",
    )
    # Direct + transitive aspect: plain ASPECT, no suffix.
    assert "| DirectAndTransitive.pdl | ASPECT |" in out
    assert "| DirectAndTransitive.pdl | ASPECT (transitive) |" not in out
    # Purely transitive aspect: ASPECT (transitive).
    assert "| PureTransitive.pdl | ASPECT (transitive) |" in out


def test_render_per_finding_sections_are_all_commented_out():
    """All per-finding section() calls in render_report are commented out. The
    summary line + bump_status breakdown tables remain the only per-aspect
    surfaces. This test guards against accidental un-commenting."""
    fs = [
        _finding("a.pdl", additive=["added optional field: y"]),
        _finding("b.pdl", breaking=["removed field: x"]),
        _finding(
            "c.pdl", noisy=["transitively affected"], affected_via="ChangedRecord"
        ),
    ]
    out = rac.render_report(fs, base="B", head="H", head_sha="sha")
    assert "## ⚠️ BREAKING" not in out
    assert "## Transitively affected aspects" not in out
    assert "## Additive" not in out
    assert "## Noisy" not in out
    assert "## No logical change" not in out
    # Summary line still names the file count
    assert "**Summary:** 3 PDL files changed" in out


def test_render_summary_counts_are_correct():
    fs = [
        _finding("a.pdl", breaking=["x"]),
        _finding("b.pdl", additive=["y"]),
        _finding("c.pdl", noisy=["z"]),
        _finding("d.pdl"),
    ]
    fs[0].bump_status = rac.BUMP_DONE
    fs[1].bump_status = rac.BUMP_NEEDED
    fs[2].bump_status = rac.BUMP_SPURIOUS
    fs[3].bump_status = rac.BUMP_NOT_NEEDED
    out = rac.render_report(fs, base="B", head="H", head_sha="sha")
    # Summary line shows file count + bump-bucket roll-up
    assert (
            "**Summary:** 4 PDL files changed · "
            "1 bump_done · 1 bump_needed · 1 bump_spurious · 1 bump_not_needed" in out
    )


# ---------------------------------------------------------------------------
# Bump-status classification
# ---------------------------------------------------------------------------


def test_bump_status_bump_done_when_version_increases(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string\n y: optional string }'
    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.BUMP_DONE


def test_bump_status_bump_needed_when_structural_change_without_bump(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string\n y: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.BUMP_NEEDED


def test_bump_status_na_when_no_change_and_no_bump(monkeypatch):
    same = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    monkeypatch.setattr(rac, "file_at", lambda ref, p: same)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.BUMP_NA


def test_bump_status_not_sure_when_version_decreases(monkeypatch):
    old = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 3}\nrecord A { x: string }'
    new = 'namespace x\n@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string }'
    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.NOT_SURE


def test_bump_status_na_for_non_aspect(monkeypatch):
    old = "namespace x\nrecord A { x: string }"
    new = "namespace x\nrecord A { x: string\n y: optional string }"
    monkeypatch.setattr(rac, "file_at", lambda ref, p: old if ref == "BASE" else new)
    monkeypatch.setattr(rac, "latest_commit", lambda ref, p, base: "")
    f = rac.analyze_file("path.pdl", "BASE", "HEAD")
    assert f.bump_status == rac.BUMP_NA


def test_classify_bump_missing_schema_version_treated_as_one():
    """Per bump_schema_versions.md: missing schemaVersion is treated as 1.
    Here the version goes 1→2 but content is identical, so it's bump_spurious."""
    old_no_version = '@Aspect = {"name": "a"}\nrecord A { x: string }'
    new_v2 = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string }'
    assert (
            rac._classify_bump(True, old_no_version, new_v2, has_structural=False)
            == rac.BUMP_SPURIOUS
    )


def test_classify_bump_spurious_when_version_increments_without_change():
    """PR #9579 pattern: lone schemaVersion bump with no schema modifications."""
    old = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string }'
    new = '@Aspect = {"name": "a", "schemaVersion": 3}\nrecord A { x: string }'
    assert rac._classify_bump(True, old, new, has_structural=False) == rac.BUMP_SPURIOUS


def test_classify_bump_done_only_when_version_increments_AND_change_exists():
    """A bump WITH a real change is bump_done (legitimate)."""
    old = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    new = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord A { x: string\n y: optional string }'
    assert rac._classify_bump(True, old, new, has_structural=True) == rac.BUMP_DONE


def test_extract_pr_number_from_squash_commit_subject():
    assert rac._extract_pr_number("feat(telemetry): some change (#9579)") == "9579"
    assert rac._extract_pr_number("fix: nothing referencing a PR") is None
    assert rac._extract_pr_number("") is None


def test_render_bump_status_summary_includes_spurious_count():
    f1 = _finding("a.pdl")
    f1.bump_status = rac.BUMP_SPURIOUS
    f2 = _finding("b.pdl")
    f2.bump_status = rac.BUMP_DONE
    out = rac.render_report([f1, f2], base="B", head="H", head_sha="sha")
    assert "1 bump_done" in out
    assert "1 bump_spurious" in out


def test_render_includes_bump_status_legend():
    fs = [_finding("a.pdl", breaking=["x"])]
    out = rac.render_report(fs, base="B", head="H", head_sha="sha")
    assert "Bump status legend" in out
    assert "auto-bumper side-effect" in out  # description of bump_spurious


def test_render_pr_number_appears_in_bump_breakdown_table():
    """Per-finding `- PR: #N` bullets are no longer rendered (sections commented
    out). PR attribution survives via the bump_status breakdown table, which
    renders `| PDL file | PR(s) | Owner | Date | Why |` rows."""
    f = _finding(
        "a.pdl",
        additive=["added optional field: y"],
        head_commit="abc1234 feat: change (#9579)",
    )
    f.pr_numbers = ["9579"]
    f.bump_status = rac.BUMP_DONE
    out = rac.render_report([f], base="B", head="H", head_sha="sha")
    # PR shows up in the breakdown table cell, not as a bullet line
    assert "#9579" in out
    # The bullet-format "- PR: #9579" should NOT appear (sections are commented out)
    assert "- PR:" not in out


def test_bump_breakdown_groups_by_pr():
    f1 = _finding("path/to/A.pdl")
    f1.bump_status = rac.BUMP_SPURIOUS
    f1.pr_numbers = ["9579"]
    f2 = _finding("path/to/B.pdl")
    f2.bump_status = rac.BUMP_SPURIOUS
    f2.pr_numbers = ["9579"]
    f3 = _finding("path/to/C.pdl")
    f3.bump_status = rac.BUMP_SPURIOUS
    f3.pr_numbers = ["9591"]
    out = rac._bump_breakdown([f1, f2, f3], rac.BUMP_SPURIOUS)
    # Two PR groups, sorted ascending: 9579 then 9591
    assert out == "(#9579: A.pdl, B.pdl), (#9591: C.pdl)"


def test_bump_breakdown_groups_file_under_multiple_prs_when_touched_by_each():
    """A file touched by two PRs in the window appears in BOTH breakdown groups."""
    f = _finding("path/to/Shared.pdl")
    f.bump_status = rac.BUMP_DONE
    f.pr_numbers = ["9000", "9579"]
    out = rac._bump_breakdown([f], rac.BUMP_DONE)
    assert out == "(#9000: Shared.pdl), (#9579: Shared.pdl)"


def test_bump_breakdown_returns_none_for_empty_bucket():
    assert rac._bump_breakdown([], rac.BUMP_SPURIOUS) == "(none)"


def test_bump_breakdown_handles_findings_without_pr_reference():
    f = _finding("path/to/X.pdl")
    f.bump_status = rac.BUMP_NEEDED
    f.pr_numbers = []
    out = rac._bump_breakdown([f], rac.BUMP_NEEDED)
    assert out == "(no-PR: X.pdl)"


def test_main_routes_two_hop_transitive_aspect_to_transitive_section(
        monkeypatch, tmp_path
):
    """A directly-edited aspect that depends on a changed non-aspect via a
    multi-hop chain (so the proximate dep is NOT named in the aspect's content)
    should still route to the Transitively affected section with a generic
    'transitive dependency' affected_via label, and its noisy section should
    NOT carry the misleading 'bumped with NO structural change' entry."""
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"
    # Intermediate.pdl exists between AspectA and Changed but isn't named in the
    # test fixtures — its role is documented in the dependency chain comment below.
    changed_nested_path = "metadata-models/src/main/pegasus/com/linkedin/x/Changed.pdl"

    # AspectA references Intermediate (NOT Changed). Intermediate is unchanged here.
    # Only Changed was modified in the diff. The dependency chain is:
    # AspectA → Intermediate → Changed
    # So AspectA's content does NOT contain the string "Changed".
    aspect_old = '@Aspect = {"name": "a"}\nrecord AspectA { x: Intermediate }'
    aspect_new = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord AspectA { x: Intermediate }'
    nested_old = "record Changed { y: string }"
    nested_new = "record Changed { y: string\n z: optional string }"

    def fake_changed(base, head):
        return [aspect_path, changed_nested_path]

    def fake_file_at(ref, path):
        if path == aspect_path:
            return aspect_old if ref == "BASE" else aspect_new
        return nested_old if ref == "BASE" else nested_new

    monkeypatch.setattr(rac, "changed_pdls", fake_changed)
    monkeypatch.setattr(rac, "file_at", fake_file_at)
    monkeypatch.setattr(rac, "resolve_base", lambda *a, **kw: "BASE")
    monkeypatch.setattr(rac, "_head_sha", lambda ref: "sha123")
    monkeypatch.setattr(
        rac, "latest_commit", lambda ref, p, base: "abc1234 feat (#9999)"
    )
    monkeypatch.setattr(rac, "pr_numbers_for_file", lambda ref, p, base: ["9999"])
    # find_transitive_aspects returns AspectA — bsv's BFS found the chain
    monkeypatch.setattr(
        rac, "find_transitive_aspects", lambda non_aspect: {aspect_path}
    )
    # Per-source variant: the single changed non-aspect (Changed.pdl) reaches AspectA
    monkeypatch.setattr(
        rac,
        "find_transitive_aspects_per_source",
        lambda sources: {changed_nested_path: {aspect_path}},
    )
    # Cumulative mode now also runs per_pr_audit; stub it for this test which
    # exercises only the transitive-reclassification + section-routing logic.
    monkeypatch.setattr(rac, "per_pr_audit", lambda base, head: {})
    monkeypatch.setattr(rac, "find_mutators_added_in_window", lambda *a: [])
    monkeypatch.setattr(rac, "discover_mutator_hierarchy", lambda: set())

    out_file = tmp_path / "report.md"
    rc = rac.main(["--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()

    # Sections are commented out — verify the reclassification surfaces via
    # the bump_status counts in the summary line + breakdown table instead.
    # bump_done because the bump was justified by the transitive change
    assert "1 bump_done" in text
    # The misleading noisy entry was stripped (verifiable by absence)
    assert "bumped with NO structural change" not in text


def test_pr_numbers_for_file_returns_all_prs_in_window(monkeypatch):
    """When multiple PRs touch a file in base..head, ALL get returned."""
    log_output = (
        "abc1234 feat: thing (#9700)\n"
        "def5678 fix: other thing (#9579)\n"
        "9876fed chore: tweak (#9579)\n"  # duplicate PR
        "1111111 commit with no pr reference\n"
    )
    monkeypatch.setattr(rac, "_git", lambda *args: log_output)
    prs = rac.pr_numbers_for_file("acryl-main", "path.pdl", "v1.0.1-cloud")
    assert prs == ["9579", "9700"]  # sorted ascending, deduped


def test_last_author_for_file_returns_most_recent_author(monkeypatch):
    monkeypatch.setattr(rac, "_git", lambda *args: "Alice Example\n")
    assert (
            rac.last_author_for_file("acryl-main", "path.pdl", "v1.0.0rc1-cloud")
            == "Alice Example"
    )


def test_last_author_for_file_returns_none_when_no_commits(monkeypatch):
    # `git log -1 --format=%an base..ref -- path` produces empty stdout when
    # there are no matching commits in the range (e.g. a transitively-affected
    # aspect whose file itself wasn't edited).
    monkeypatch.setattr(rac, "_git", lambda *args: "")
    assert rac.last_author_for_file("acryl-main", "path.pdl", "v1.0.0rc1-cloud") is None


def test_render_owner_appears_in_bump_breakdown_table():
    """Per-finding `- owner: <name>` bullets are no longer rendered (sections
    commented out). Owner attribution survives via the bump_status breakdown
    table's Owner column."""
    f = _finding(
        "a.pdl",
        additive=["added optional field: y"],
        head_commit="abc1234 feat (#1234)",
    )
    f.last_author = "Alice Example"
    f.bump_status = rac.BUMP_DONE
    out = rac.render_report([f], base="B", head="H", head_sha="sha")
    # Owner appears in the breakdown table row, not as a bullet
    assert "Alice Example" in out
    # The bullet-format "- owner: Alice Example" should NOT appear
    assert "- owner:" not in out


def test_render_includes_per_status_breakdown():
    f = _finding("metadata-models/.../ActionRequestInfo.pdl")
    f.bump_status = rac.BUMP_SPURIOUS
    f.pr_numbers = ["9579"]
    f.last_author = "Nick Adams"
    out = rac.render_report([f], base="B", head="H", head_sha="sha")
    assert "**Bump status breakdown:**" in out
    # Headers per status with counts
    assert "### `bump_spurious` (1)" in out
    assert "### `bump_done` (0)" in out
    # Table header for populated bucket (now PDL file + Date + Why columns)
    assert "| PDL file | PR(s) | Owner | Date | Why |" in out
    # The single row for our finding
    assert "| ActionRequestInfo.pdl | #9579 | Nick Adams |" in out
    # Empty buckets render as italicized "(none)"
    assert "_(none)_" in out


def test_bump_breakdown_table_renders_rows_with_owner_and_pr():
    f1 = _finding("path/to/A.pdl")
    f1.bump_status = rac.BUMP_NEEDED
    f1.pr_numbers = ["9242"]
    f1.last_author = "John Joyce"
    f1.last_commit_date = "2026-05-01"
    f2 = _finding("path/to/B.pdl")
    f2.bump_status = rac.BUMP_NEEDED
    f2.pr_numbers = ["9272"]
    f2.last_author = "Chris Collins"
    f2.last_commit_date = "2026-05-10"
    rows = rac._bump_breakdown_table([f1, f2], rac.BUMP_NEEDED)
    assert rows[0] == "| PDL file | PR(s) | Owner | Date | Why |"
    assert rows[1] == "| --- | --- | --- | --- | --- |"
    # Sorted by date DESC — B (2026-05-10) before A (2026-05-01)
    assert (
            rows[2]
            == "| B.pdl | #9272 | Chris Collins | 2026-05-10 | "
            + rac._BUMP_WHY[rac.BUMP_NEEDED]
            + " |"
    )
    assert (
            rows[3]
            == "| A.pdl | #9242 | John Joyce | 2026-05-01 | "
            + rac._BUMP_WHY[rac.BUMP_NEEDED]
            + " |"
    )


def test_bump_breakdown_table_uses_no_pr_and_no_owner_placeholders():
    f = _finding("path/to/X.pdl")
    f.bump_status = rac.BUMP_NEEDED
    # pr_numbers stays empty, last_author stays None — transitively-affected case
    rows = rac._bump_breakdown_table([f], rac.BUMP_NEEDED)
    assert rows[2] == (
            "| X.pdl | (no-PR) | _(no owner — file wasn't directly touched)_ "
            "| _(unknown)_ | " + rac._BUMP_WHY[rac.BUMP_NEEDED] + " |"
    )


def test_bump_breakdown_table_returns_none_marker_for_empty_bucket():
    assert rac._bump_breakdown_table([], rac.BUMP_SPURIOUS) == ["_(none)_"]


def test_catchup_reclassifies_spurious_bump_after_unbumped_change():
    """The Status.pdl pattern: PR1 makes a change without bumping, PR2 bumps
    without making a change → PR2 is reclassified as catch-up (bump_done)."""
    entries = [
        {
            "pr": "9192",
            "date": "2026-05-13",
            "bump_status": rac.BUMP_NEEDED,
        },
        {
            "pr": "9534",
            "date": "2026-05-14",
            "bump_status": rac.BUMP_SPURIOUS,
        },
    ]
    rac._apply_catchup_reclassification(entries)
    statuses = {e["pr"]: e["bump_status"] for e in entries}
    assert statuses["9192"] == rac.BUMP_NEEDED  # missing-bump signal preserved
    assert statuses["9534"] == rac.BUMP_DONE  # catch-up reclassification
    catch_up = next(e for e in entries if e["pr"] == "9534")["catch_up_for_prs"]
    assert catch_up == ["9192"]


def test_catchup_keeps_spurious_when_no_pending_debt():
    """A spurious-bump PR with no prior unbumped change stays spurious."""
    entries = [
        {
            "pr": "9579",
            "date": "2026-05-15",
            "bump_status": rac.BUMP_SPURIOUS,
        },
    ]
    rac._apply_catchup_reclassification(entries)
    assert entries[0]["bump_status"] == rac.BUMP_SPURIOUS
    assert "catch_up_for_prs" not in entries[0]


def test_catchup_only_first_spurious_after_needed_is_reclassified():
    """The Status.pdl 3-PR pattern: only the FIRST spurious bump catches up;
    further bumps without a change remain spurious."""
    entries = [
        {
            "pr": "9192",
            "date": "2026-05-13",
            "bump_status": rac.BUMP_NEEDED,
        },
        {
            "pr": "9534",
            "date": "2026-05-14",
            "bump_status": rac.BUMP_SPURIOUS,
        },
        {
            "pr": "9579",
            "date": "2026-05-15",
            "bump_status": rac.BUMP_SPURIOUS,
        },
    ]
    rac._apply_catchup_reclassification(entries)
    statuses = {e["pr"]: e["bump_status"] for e in entries}
    assert statuses["9192"] == rac.BUMP_NEEDED
    assert statuses["9534"] == rac.BUMP_DONE
    assert statuses["9579"] == rac.BUMP_SPURIOUS  # no debt left to catch up


def test_catchup_single_bump_clears_multiple_pending_needed_prs():
    """Three unbumped real-change PRs followed by ONE catch-up bump: the
    single bump clears all accumulated debt and records every paid PR."""
    entries = [
        {"pr": "1", "date": "2026-05-01", "bump_status": rac.BUMP_NEEDED},
        {"pr": "2", "date": "2026-05-02", "bump_status": rac.BUMP_NEEDED},
        {"pr": "3", "date": "2026-05-03", "bump_status": rac.BUMP_NEEDED},
        {"pr": "4", "date": "2026-05-04", "bump_status": rac.BUMP_SPURIOUS},
        {"pr": "5", "date": "2026-05-05", "bump_status": rac.BUMP_SPURIOUS},
    ]
    rac._apply_catchup_reclassification(entries)
    statuses = {e["pr"]: e["bump_status"] for e in entries}
    assert statuses["1"] == rac.BUMP_NEEDED
    assert statuses["2"] == rac.BUMP_NEEDED
    assert statuses["3"] == rac.BUMP_NEEDED
    assert statuses["4"] == rac.BUMP_DONE
    assert statuses["5"] == rac.BUMP_SPURIOUS  # debt was cleared by #4
    catch_up = next(e for e in entries if e["pr"] == "4")["catch_up_for_prs"]
    assert catch_up == ["1", "2", "3"]


def test_catchup_bump_done_slice_also_clears_pending_debt():
    """A slice with its own change AND its own bump implicitly clears prior
    unbumped-change debt — the bump goes with this slice but conventionally
    covers all earlier missed bumps too."""
    entries = [
        {"pr": "1", "date": "2026-05-01", "bump_status": rac.BUMP_NEEDED},
        {"pr": "2", "date": "2026-05-02", "bump_status": rac.BUMP_DONE},
        {"pr": "3", "date": "2026-05-03", "bump_status": rac.BUMP_SPURIOUS},
    ]
    rac._apply_catchup_reclassification(entries)
    statuses = {e["pr"]: e["bump_status"] for e in entries}
    assert statuses["1"] == rac.BUMP_NEEDED
    assert statuses["2"] == rac.BUMP_DONE
    # #2's bump_done implicitly cleared #1's debt → #3 has no debt to pay
    assert statuses["3"] == rac.BUMP_SPURIOUS
    # #2 records that it absorbed #1's debt
    assert next(e for e in entries if e["pr"] == "2")["catch_up_for_prs"] == ["1"]


def test_catchup_sorts_by_date_not_input_order():
    """Reclassification respects chronological order regardless of input order."""
    entries = [
        # Input order is intentionally newest-first to verify we sort.
        {"pr": "9579", "date": "2026-05-15", "bump_status": rac.BUMP_SPURIOUS},
        {"pr": "9534", "date": "2026-05-14", "bump_status": rac.BUMP_SPURIOUS},
        {"pr": "9192", "date": "2026-05-13", "bump_status": rac.BUMP_NEEDED},
    ]
    rac._apply_catchup_reclassification(entries)
    statuses = {e["pr"]: e["bump_status"] for e in entries}
    # Chronological order: #9192 (needed) → #9534 (catch-up done) → #9579 (still spurious)
    assert statuses["9192"] == rac.BUMP_NEEDED
    assert statuses["9534"] == rac.BUMP_DONE
    assert statuses["9579"] == rac.BUMP_SPURIOUS


def test_describe_per_pr_slice_renders_catchup_annotation():
    """The 'What happened' prose names the PRs whose debt this slice cleared."""
    entry = {
        "bump_status": rac.BUMP_DONE,
        "old_v": 1,
        "new_v": 2,
        "additive": [],
        "breaking": [],
        "noisy": [],
        "catch_up_for_prs": ["9192"],
    }
    text = rac._describe_per_pr_slice(entry)
    assert "bumped schemaVersion 1→2" in text
    assert "catch-up for unbumped change(s) in #9192" in text


def test_release_test_pdl_file_lists_done_needed_spurious_with_mutator_column():
    """The standalone `release_test_pdl_changes.md` content is a flat union
    of bump_done + bump_needed + bump_spurious entries with a Mutator
    column that names any AspectMigrationMutator subclass targeting the
    same aspect. bump_not_needed rows are excluded entirely."""
    f_done = _finding("path/Done.pdl")
    f_done.bump_status = rac.BUMP_DONE
    f_done.aspect_name = "done"
    f_done.last_commit_date = "2026-05-10"
    f_done.pr_numbers = ["1001"]
    f_done.last_author = "Alice"

    f_needed = _finding("path/Needed.pdl")
    f_needed.bump_status = rac.BUMP_NEEDED
    f_needed.aspect_name = "needed"
    f_needed.last_commit_date = "2026-05-09"
    f_needed.pr_numbers = ["1002"]
    f_needed.last_author = "Bob"

    f_spurious = _finding("path/Spurious.pdl")
    f_spurious.bump_status = rac.BUMP_SPURIOUS
    f_spurious.aspect_name = "spurious"
    f_spurious.last_commit_date = "2026-05-08"
    f_spurious.pr_numbers = ["1003"]
    f_spurious.last_author = "Carol"

    f_not_needed = _finding("path/NotNeeded.pdl")
    f_not_needed.bump_status = rac.BUMP_NOT_NEEDED
    f_not_needed.bump_reason = "non-aspect record (no schemaVersion concept applies)"
    f_not_needed.last_commit_date = "2026-05-07"

    mutators = [
        {"class_name": "DoneMutator", "target_aspect": "done"},
        {"class_name": "BaseMutator", "target_aspect": None},  # abstract base
    ]
    out = rac._render_release_test_pdl_file(
        [f_done, f_needed, f_spurious, f_not_needed],
        mutators,
        base="B",
        head="H",
        head_sha="sha",
    )
    # Standalone file header with metadata.
    assert "# PDL files for integration test cases" in out
    assert "**Base:** `B`" in out
    assert "**Head:** `H` (sha: `sha`)" in out
    assert "**Total aspects:** 3" in out
    # 5-column header (PDL file | PR(s) | Owner | Date | Mutator) — no Why.
    assert "| PDL file | PR(s) | Owner | Date | Mutator |" in out
    assert "Why" not in out
    # Rows sorted by Date DESC.
    done_idx = out.index("Done.pdl")
    needed_idx = out.index("Needed.pdl")
    spurious_idx = out.index("Spurious.pdl")
    assert done_idx < needed_idx < spurious_idx
    # Done aspect's row carries its matching mutator class name.
    assert "| Done.pdl | #1001 | Alice | 2026-05-10 | `DoneMutator` |" in out
    # Needed / Spurious aspects don't match any mutator → (none).
    assert "| Needed.pdl | #1002 | Bob | 2026-05-09 | (none) |" in out
    assert "| Spurious.pdl | #1003 | Carol | 2026-05-08 | (none) |" in out
    # bump_not_needed excluded.
    assert "NotNeeded.pdl" not in out


def test_release_test_pdl_file_returns_empty_when_no_relevant_findings():
    """Empty input or all-bump_not_needed → return empty string so the caller
    skips writing the sidecar file."""
    assert (
            rac._render_release_test_pdl_file([], [], base="B", head="H", head_sha="sha")
            == ""
    )
    f = _finding("path/X.pdl")
    f.bump_status = rac.BUMP_NOT_NEEDED
    assert (
            rac._render_release_test_pdl_file([f], [], base="B", head="H", head_sha="sha")
            == ""
    )


def test_extract_mutator_target_aspect_string_literal():
    """`getAspectName() { return "foo"; }` → 'foo'."""
    java = """
        public class FooMutator extends Base {
          public String getAspectName() {
            return "fooAspect";
          }
        }
    """
    assert rac._extract_mutator_target_aspect(java, {}) == "fooAspect"


def test_extract_mutator_target_aspect_constant_reference():
    """`getAspectName() { return DATAHUB_VIEW_INFO_ASPECT_NAME; }` resolves
    via the Constants.java map."""
    java = """
        public class ViewInfoMutator extends Base {
          public String getAspectName() {
            return DATAHUB_VIEW_INFO_ASPECT_NAME;
          }
        }
    """
    constants = {"DATAHUB_VIEW_INFO_ASPECT_NAME": "dataHubViewInfo"}
    assert rac._extract_mutator_target_aspect(java, constants) == "dataHubViewInfo"


def test_extract_mutator_target_aspect_returns_none_for_abstract_base():
    """A class that doesn't override `getAspectName()` → None."""
    java = """
        public abstract class BaseMutator extends AspectMigrationMutator {
          // no getAspectName override
        }
    """
    assert rac._extract_mutator_target_aspect(java, {}) is None


def test_extract_mutator_target_aspect_returns_none_when_constant_unknown():
    """Constant reference that's not in the resolved map → None (don't
    fabricate)."""
    java = """
        public class FooMutator extends Base {
          public String getAspectName() {
            return SOME_UNKNOWN_CONSTANT;
          }
        }
    """
    assert rac._extract_mutator_target_aspect(java, {}) is None


def test_describe_per_pr_slice_renders_new_aspect_file_lifecycle():
    """A BUMP_NOT_NEEDED slice whose reason is 'new aspect file' renders as
    a dedicated creator-PR row — the file was added in this PR."""
    entry = {
        "bump_status": rac.BUMP_NOT_NEEDED,
        "bump_reason": "new aspect file (default schemaVersion = 1; no prior version to bump from)",
        "additive": ["new file"],
        "breaking": [],
        "noisy": [],
        "old_v": 1,
        "new_v": 1,
    }
    text = rac._describe_per_pr_slice(entry)
    assert text == "added new aspect file (default schemaVersion = 1)"


def test_describe_per_pr_slice_renders_deleted_aspect_file_lifecycle():
    """A BUMP_NOT_NEEDED slice whose reason is 'file deleted at head' renders
    as a dedicated deleter-PR row."""
    entry = {
        "bump_status": rac.BUMP_NOT_NEEDED,
        "bump_reason": "file deleted at head",
        "additive": [],
        "breaking": ["file deleted"],
        "noisy": [],
    }
    text = rac._describe_per_pr_slice(entry)
    assert text == "deleted aspect file"


def test_per_pr_audit_keeps_lifecycle_not_needed_slices(monkeypatch):
    """per_pr_audit should KEEP BUMP_NOT_NEEDED slices whose bump_reason is a
    lifecycle event (new aspect file, file deleted, schemaVersion regressed).
    The previous filter dropped all BUMP_NOT_NEEDED slices, which hid the
    creator-PR for newly-added aspects (e.g. EvalRunEvent.pdl missing #9242).
    """
    aspect_path = "metadata-models/.../EvalRunEvent.pdl"

    # Stub: enumerate two PR commits — #9242 (creates the aspect, BUMP_NOT_NEEDED
    # with 'new aspect file' reason) and #9555 (later bump_done slice).
    monkeypatch.setattr(
        rac,
        "enumerate_pdl_pr_commits",
        lambda b, h: [
            ("bbb2222222", "feat: later bump (#9555)"),
            ("aaa1111111", "feat: create aspect (#9242)"),
        ],
    )
    monkeypatch.setattr(
        rac,
        "_commit_date",
        lambda sha: "2026-05-14" if sha == "bbb2222222" else "2026-04-30",
    )

    def fake_classify(parent, sha):
        if sha == "aaa1111111":
            f = rac.FileFinding(
                path=aspect_path,
                is_aspect=True,
                aspect_name="evalRunEvent",
                head_commit="aaa1 feat (#9242)",
                additive=["new file"],
                bump_status=rac.BUMP_NOT_NEEDED,
                bump_reason="new aspect file (default schemaVersion = 1; no prior version to bump from)",
            )
            return [f]
        f = rac.FileFinding(
            path=aspect_path,
            is_aspect=True,
            aspect_name="evalRunEvent",
            head_commit="bbb2 feat (#9555)",
            bump_status=rac.BUMP_DONE,
        )
        f.old_v = 1
        f.new_v = 2
        return [f]

    monkeypatch.setattr(rac, "_classify_window", fake_classify)
    monkeypatch.setattr(rac, "_git", lambda *a, **kw: "Some Author\n")
    monkeypatch.setattr(
        rac, "_extract_pr_number", lambda s: "9555" if "9555" in s else "9242"
    )

    audit = rac.per_pr_audit("BASE", "HEAD")
    assert aspect_path in audit
    entries = audit[aspect_path]
    prs = {e["pr"] for e in entries}
    # Both PRs preserved — the creator AND the later bumper.
    assert prs == {"9242", "9555"}


def test_per_pr_audit_still_drops_non_lifecycle_not_needed_slices(monkeypatch):
    """Sanity: BUMP_NOT_NEEDED slices that are pure noise (file touched but
    no schema change) are still filtered out — only lifecycle events leak
    through the relaxed filter."""
    aspect_path = "metadata-models/.../UnchangedAspect.pdl"
    monkeypatch.setattr(
        rac,
        "enumerate_pdl_pr_commits",
        lambda b, h: [("aaa1111111", "feat: whitespace tweak (#1234)")],
    )
    monkeypatch.setattr(rac, "_commit_date", lambda sha: "2026-05-01")

    def fake_classify(parent, sha):
        f = rac.FileFinding(
            path=aspect_path,
            is_aspect=True,
            aspect_name="unchanged",
            head_commit="aaa1 feat (#1234)",
            bump_status=rac.BUMP_NOT_NEEDED,
            bump_reason="no schema change in this file",
        )
        return [f]

    monkeypatch.setattr(rac, "_classify_window", fake_classify)
    monkeypatch.setattr(rac, "_git", lambda *a, **kw: "Some Author\n")
    monkeypatch.setattr(rac, "_extract_pr_number", lambda s: "1234")

    audit = rac.per_pr_audit("BASE", "HEAD")
    # No-schema-change slices are filtered out — no entries for the aspect.
    assert audit == {}


def test_bump_needed_why_for_transitive_change_names_the_upstream_record():
    f = _finding("path/to/RecommendationModule.pdl")
    f.bump_status = rac.BUMP_NEEDED
    f.affected_via = "Criterion"
    f.has_structural = False
    f.pr_numbers = ["9465"]
    f.last_author = "david-leifker"
    f.last_commit_date = "2026-05-11"
    rows = rac._bump_breakdown_table([f], rac.BUMP_NEEDED)
    assert "transitively affected via `Criterion`" in rows[2]
    assert "file's own schema unchanged" in rows[2]
    assert "schemaVersion was NOT bumped" in rows[2]


def test_bump_needed_why_for_direct_change_says_direct():
    f = _finding("path/to/DirectAspect.pdl")
    f.bump_status = rac.BUMP_NEEDED
    f.affected_via = None
    f.has_structural = True
    f.pr_numbers = ["9999"]
    f.last_author = "Alice"
    f.last_commit_date = "2026-05-01"
    rows = rac._bump_breakdown_table([f], rac.BUMP_NEEDED)
    assert "direct schema change in this file" in rows[2]
    assert "schemaVersion was NOT bumped" in rows[2]
    assert "transitively" not in rows[2]


def test_bump_needed_why_for_combo_direct_and_transitive_names_both():
    f = _finding("path/to/Combo.pdl")
    f.bump_status = rac.BUMP_NEEDED
    f.affected_via = "SomeRecord"
    f.has_structural = True
    f.pr_numbers = ["1234"]
    f.last_author = "Bob"
    f.last_commit_date = "2026-05-02"
    rows = rac._bump_breakdown_table([f], rac.BUMP_NEEDED)
    assert "direct schema change AND transitively affected via `SomeRecord`" in rows[2]
    assert "schemaVersion was NOT bumped" in rows[2]


def test_bump_needed_why_falls_back_to_bucket_text_when_no_context():
    # No has_structural and no affected_via — degenerate case, keep bucket text.
    f = _finding("path/to/Mystery.pdl")
    f.bump_status = rac.BUMP_NEEDED
    f.affected_via = None
    f.has_structural = False
    rows = rac._bump_breakdown_table([f], rac.BUMP_NEEDED)
    assert rac._BUMP_WHY[rac.BUMP_NEEDED] in rows[2]


def test_bump_breakdown_table_renders_multiple_prs_per_aspect():
    f = _finding("path/to/Shared.pdl")
    f.bump_status = rac.BUMP_DONE
    f.pr_numbers = ["9000", "9579"]
    f.last_author = "Some Author"
    f.last_commit_date = "2026-05-15"
    rows = rac._bump_breakdown_table([f], rac.BUMP_DONE)
    assert rows[2] == (
            "| Shared.pdl | #9000, #9579 | Some Author | 2026-05-15 | "
            + rac._BUMP_WHY[rac.BUMP_DONE]
            + " |"
    )


def test_main_reclassifies_aspect_bump_as_done_when_transitively_affected(
        monkeypatch, tmp_path
):
    """An aspect that was bumped AND has a changed non-aspect dependency in the
    same diff should be bump_done, not bump_spurious."""
    aspect_path = "metadata-models/src/main/pegasus/com/linkedin/x/AspectA.pdl"
    nested_path = "metadata-models/src/main/pegasus/com/linkedin/x/Nested.pdl"

    aspect_old = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord AspectA includes Nested { }'
    aspect_new = '@Aspect = {"name": "a", "schemaVersion": 2}\nrecord AspectA includes Nested { }'
    nested_old = "record Nested { x: string }"
    nested_new = "record Nested { x: string\n y: optional string }"

    def fake_changed(base, head):
        return [aspect_path, nested_path]

    def fake_file_at(ref, path):
        if path == aspect_path:
            return aspect_old if ref == "BASE" else aspect_new
        return nested_old if ref == "BASE" else nested_new

    def fake_resolve(mode=None):
        return "BASE"

    def fake_head_sha(ref):
        return "sha123"

    def fake_find_transitive(non_aspect_changed):
        # Simulates the BFS: nested is the changed non-aspect, AspectA depends on it.
        return {aspect_path}

    def fake_find_transitive_per_source(sources):
        # Per-source variant: each source's BFS downstream set.
        return {nested_path: {aspect_path}}

    monkeypatch.setattr(rac, "changed_pdls", fake_changed)
    monkeypatch.setattr(rac, "file_at", fake_file_at)
    monkeypatch.setattr(rac, "resolve_base", fake_resolve)
    monkeypatch.setattr(rac, "_head_sha", fake_head_sha)
    monkeypatch.setattr(
        rac, "latest_commit", lambda ref, p, base: "abc1234 feat (#1234)"
    )
    monkeypatch.setattr(rac, "find_transitive_aspects", fake_find_transitive)
    monkeypatch.setattr(
        rac, "find_transitive_aspects_per_source", fake_find_transitive_per_source
    )
    # The cumulative mode now also runs per_pr_audit to enrich bump_status with
    # per-PR-aware verdicts. Stub it out for this test (the test exercises only
    # the cumulative classifier's transitive-reclassification path).
    monkeypatch.setattr(rac, "per_pr_audit", lambda base, head: {})
    monkeypatch.setattr(rac, "find_mutators_added_in_window", lambda *a: [])
    monkeypatch.setattr(rac, "discover_mutator_hierarchy", lambda: set())

    out_file = tmp_path / "report.md"
    rc = rac.main(["--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()

    # AspectA bumped 1→2 AND has a sibling non-aspect change → bump_done, not bump_spurious
    assert "1 bump_done" in text
    assert "bump_spurious`: (none)" in text or "0 bump_spurious" in text


def test_classify_bump_transitive_affected_with_no_version_change_is_bump_needed():
    same = '@Aspect = {"name": "a", "schemaVersion": 1}\nrecord A { x: string }'
    assert (
            rac._classify_bump(
                True, same, same, has_structural=False, transitively_affected=True
            )
            == rac.BUMP_NEEDED
    )


def test_render_includes_bump_status_summary_line():
    fs = [
        _finding("a.pdl", breaking=["x"]),
        _finding("b.pdl", additive=["y"]),
        _finding("c.pdl"),
    ]
    # Manually set bump statuses (the renderer doesn't compute them, just counts)
    fs[0].bump_status = rac.BUMP_NEEDED
    fs[1].bump_status = rac.BUMP_DONE
    fs[2].bump_status = rac.BUMP_NOT_NEEDED
    out = rac.render_report(fs, base="B", head="H", head_sha="sha")
    assert "**Bump status:**" in out
    assert "1 bump_done" in out
    assert "1 bump_needed" in out
    assert "1 bump_not_needed" in out


def test_render_bump_status_appears_in_summary_and_breakdown():
    """Per-finding `- bump: <status>` bullets are no longer rendered (sections
    commented out). The bump status surfaces via the summary line counts and
    the per-status breakdown tables."""
    f = _finding("a.pdl", additive=["added optional field: y"])
    f.bump_status = rac.BUMP_NEEDED
    f.pr_numbers = ["1234"]
    f.last_author = "Author"
    out = rac.render_report([f], base="B", head="H", head_sha="sha")
    # Summary count present
    assert "1 bump_needed" in out
    # Breakdown table for bump_needed exists and lists the aspect
    assert "### `bump_needed` (1)" in out
    assert "a.pdl" in out
    # The bullet-format "- bump: bump_needed" should NOT appear
    assert "- bump:" not in out


# ---------------------------------------------------------------------------
# Per-PR audit mode
# ---------------------------------------------------------------------------


def test_enumerate_pdl_pr_commits_parses_log_output(monkeypatch):
    log_output = (
        "abc123fff feat: change one (#9555)\ndef456eee feat: change two (#9579)\n"
    )
    monkeypatch.setattr(rac, "_git", lambda *args: log_output)
    rows = rac.enumerate_pdl_pr_commits("v1.0", "HEAD")
    assert rows == [
        ("abc123fff", "feat: change one (#9555)"),
        ("def456eee", "feat: change two (#9579)"),
    ]


def test_enumerate_pdl_pr_commits_skips_blank_lines(monkeypatch):
    monkeypatch.setattr(rac, "_git", lambda *args: "\n\nabc12345 only entry\n\n")
    rows = rac.enumerate_pdl_pr_commits("base", "head")
    assert rows == [("abc12345", "only entry")]


def test_render_per_pr_report_buckets_aspects_correctly():
    audit = {
        "metadata-models/.../AssertionInfo.pdl": [
            {
                "sha": "abc1234567",
                "pr": "9555",
                "subject": "feat: real change (#9555)",
                "bump_status": rac.BUMP_DONE,
                "author": "Alice",
                "aspect_name": "assertionInfo",
            },
            {
                "sha": "def4567890",
                "pr": "9579",
                "subject": "feat: telemetry (#9579)",
                "bump_status": rac.BUMP_SPURIOUS,
                "author": "Bob",
                "aspect_name": "assertionInfo",
            },
        ],
        "metadata-models/.../DataHubTaskInfo.pdl": [
            {
                "sha": "111aaa2222",
                "pr": "9242",
                "subject": "feat: evals (#9242)",
                "bump_status": rac.BUMP_NEEDED,
                "author": "Carol",
                "aspect_name": "dataHubTaskInfo",
            },
        ],
        "metadata-models/.../GoodAspect.pdl": [
            {
                "sha": "999fff8888",
                "pr": "9000",
                "subject": "feat: good (#9000)",
                "bump_status": rac.BUMP_DONE,
                "author": "Dan",
                "aspect_name": "goodAspect",
            },
        ],
    }
    out = rac.render_per_pr_report(audit, base="B", head="H", head_sha="sha123")
    # Header
    assert "per-PR audit" in out
    assert "**Base:** `B`" in out
    # Bucket headers + counts
    assert "Aspects with at least one unjustified bump" in out
    assert "Aspects with at least one missing bump" in out
    assert "Aspects with all bumps justified" in out
    # AssertionInfo lands in unjustified bucket (because it has a bump_spurious entry)
    assert "### `AssertionInfo.pdl`" in out
    # The spurious row appears
    assert (
            "| #9579 | `def4567890` | `bump_spurious` | Bob | feat: telemetry (#9579) |"
            in out
    )
    # DataHubTaskInfo lands in needed bucket
    assert "### `DataHubTaskInfo.pdl`" in out
    # GoodAspect lands in the justified summary table
    assert "| GoodAspect.pdl | 1 | #9000 |" in out


def test_render_per_pr_report_handles_empty_audit():
    out = rac.render_per_pr_report({}, base="B", head="H", head_sha="sha")
    assert "No aspects with bump-relevant PR-slice verdicts" in out


def test_main_per_pr_flag_writes_per_pr_report(tmp_path, monkeypatch):
    """--per-pr swaps the renderer to render_per_pr_report. Stub out the
    git+audit machinery so the test is hermetic."""
    monkeypatch.setattr(rac, "resolve_base", lambda *a, **kw: "BASE")
    monkeypatch.setattr(rac, "_head_sha", lambda ref: "sha123")
    monkeypatch.setattr(
        rac,
        "per_pr_audit",
        lambda base, head: {
            "metadata-models/.../X.pdl": [
                {
                    "sha": "abc1234567",
                    "pr": "9579",
                    "subject": "feat (#9579)",
                    "bump_status": rac.BUMP_SPURIOUS,
                    "author": "Bob",
                    "aspect_name": "x",
                },
            ],
        },
    )
    out_file = tmp_path / "audit.md"
    rc = rac.main(["--head", "HEAD", "--per-pr", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()
    assert "per-PR audit" in text
    assert "Aspects with at least one unjustified bump" in text
    assert "X.pdl" in text


# ---------------------------------------------------------------------------
# Mutator detection (subclasses of AspectMigrationMutator)
# ---------------------------------------------------------------------------


def test_extract_class_and_parent_simple_case():
    src = "public class FooMutator extends BarBase {\n  // ...\n}"
    assert rac._extract_class_and_parent(src) == ("FooMutator", "BarBase")


def test_extract_class_and_parent_with_generics():
    src = "public final class FooMutator<T> extends BarBase<T> {\n}"
    assert rac._extract_class_and_parent(src) == ("FooMutator", "BarBase")


def test_extract_class_and_parent_returns_none_for_no_extends():
    src = "public class FooMutator {\n  // no parent\n}"
    assert rac._extract_class_and_parent(src) is None


def test_find_mutators_added_uses_hierarchy(monkeypatch):
    """Files that extend a known mutator class should be included; unrelated
    .java additions and files extending non-mutator classes should be filtered."""
    log_output = (
        "COMMIT abc123fff feat: add mutator (#9999)\n"
        "metadata-service/factories/src/main/java/com/example/MyNewMutator.java\n"
        "metadata-service/factories/src/main/java/com/example/UnrelatedClass.java\n"
        "metadata-service/factories/src/main/java/com/example/AnotherMutator.java\n"
        "COMMIT def456eee chore: noise (#9998)\n"
        "metadata-service/factories/src/test/java/com/example/TestFile.java\n"
    )

    def fake_git(*args):
        if args[0] == "log":
            return log_output
        if args[0] == "show":
            # The ref looks like "abc123fff:path/to/file.java"
            ref = args[1]
            if "MyNewMutator" in ref:
                return "public class MyNewMutator extends CriterionFilterMutatorBase {}"
            if "UnrelatedClass" in ref:
                return "public class UnrelatedClass extends Object {}"
            if "AnotherMutator" in ref:
                return "public class AnotherMutator extends AspectMigrationMutator {}"
            return ""
        # author lookup
        if args[:2] == ("log", "-1"):
            return "Alice Example\n"
        return ""

    monkeypatch.setattr(rac, "_git", fake_git)
    hierarchy = {"AspectMigrationMutator", "CriterionFilterMutatorBase"}
    results = rac.find_mutators_added_in_window("v1.0", "HEAD", hierarchy)
    class_names = {r["class_name"] for r in results}
    assert class_names == {"MyNewMutator", "AnotherMutator"}
    # Test files filtered out
    assert not any("/test/" in r["path"] for r in results)
    # PR + author + parent attribution
    by_class = {r["class_name"]: r for r in results}
    assert by_class["MyNewMutator"]["parent"] == "CriterionFilterMutatorBase"
    assert by_class["MyNewMutator"]["pr"] == "9999"
    assert by_class["AnotherMutator"]["parent"] == "AspectMigrationMutator"


def test_render_mutator_section_empty_returns_empty_list():
    assert rac._render_mutator_section([]) == []


def test_render_mutator_section_emits_table_with_date_sorted_rows():
    mutators = [
        {
            "sha": "bbbbbbbbbb",
            "pr": "9579",
            "path": "p/Beta.java",
            "class_name": "BetaMutator",
            "parent": "BaseMutator",
            "author": "Bob",
            "subject": "feat (#9579)",
            "commit_date": "2026-05-15",
        },
        {
            "sha": "aaaaaaaaaa",
            "pr": "9555",
            "path": "p/Alpha.java",
            "class_name": "AlphaMutator",
            "parent": "AspectMigrationMutator",
            "author": "Alice",
            "subject": "feat (#9555)",
            "commit_date": "2026-05-20",
        },
    ]
    section = rac._render_mutator_section(mutators)
    text = "\n".join(section)
    # Header
    assert "## Mutators introduced in this window" in text
    # Table header now has Date + Why columns
    assert "| PR | sha | Mutator class | Extends | Author | Date | Why |" in text
    # Sorted by date DESC — AlphaMutator (2026-05-20) appears before BetaMutator (2026-05-15)
    alpha_idx = text.index("AlphaMutator")
    beta_idx = text.index("BetaMutator")
    assert alpha_idx < beta_idx
    assert (
            "| #9555 | `aaaaaaaaaa` | `AlphaMutator` | `AspectMigrationMutator` "
            "| Alice | 2026-05-20 | feat (#9555) |" in text
    )
    assert (
            "| #9579 | `bbbbbbbbbb` | `BetaMutator` | `BaseMutator` | Bob "
            "| 2026-05-15 | feat (#9579) |" in text
    )


def test_render_mutator_section_uses_no_pr_placeholder():
    mutators = [
        {
            "sha": "abc1234567",
            "pr": None,
            "path": "p/X.java",
            "class_name": "XMutator",
            "parent": "AspectMigrationMutator",
            "author": "Eve",
            "subject": "noref",
            "commit_date": "2026-05-12",
        }
    ]
    text = "\n".join(rac._render_mutator_section(mutators))
    assert (
            "| (no-PR) | `abc1234567` | `XMutator` | `AspectMigrationMutator` "
            "| Eve | 2026-05-12 | noref |" in text
    )


# ---------------------------------------------------------------------------
# End-to-end smoke test against the real repo
# ---------------------------------------------------------------------------


def test_main_runs_against_real_repo_and_emits_markdown(tmp_path, capsys):
    """Invokes main() with --base=HEAD --head=HEAD (zero diff). Verifies:
    - returns 0
    - emits markdown header and 'No aspect changes' line
    - does not raise on the real repo
    """
    rc = rac.main(["--base", "HEAD", "--head", "HEAD"])
    assert rc == 0
    captured = capsys.readouterr()
    assert "# @Aspect PDL change report" in captured.out
    assert "No aspect changes" in captured.out


def test_main_writes_to_output_file(tmp_path):
    out_file = tmp_path / "report.md"
    rc = rac.main(["--base", "HEAD", "--head", "HEAD", "--output", str(out_file)])
    assert rc == 0
    text = out_file.read_text()
    assert "# @Aspect PDL change report" in text
