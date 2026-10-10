"""Tests for rollback_analysis.py"""

import json
import subprocess
import sys
from pathlib import Path
from contextlib import ExitStack, contextmanager
from unittest.mock import patch

import pytest


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import bump_schema_versions as bsv
import report_aspect_changes as rac
from rollback import cli, java_scan, model, pdl_parser, pdl_rules, pipeline, repo, report

# ---------------------------------------------------------------------------
# RollbackFinding construction helpers
# ---------------------------------------------------------------------------


def _finding(
    dimension=model.DIM_PDL_SCHEMA,
    risk=model.SAFE,
    path="test.pdl",
    aspect_name=None,
    summary="test finding",
    **kwargs,
):
    return model.RollbackFinding(
        dimension=dimension,
        risk=risk,
        path=path,
        aspect_name=aspect_name,
        summary=summary,
        **kwargs,
    )


# ---------------------------------------------------------------------------
# Verdict computation
# ---------------------------------------------------------------------------


class TestComputeVerdict:
    def test_empty_findings_is_feasible(self):
        assert model.compute_verdict([]) == model.VERDICT_FEASIBLE

    def test_all_safe_is_feasible(self):
        findings = [_finding(risk=model.SAFE) for _ in range(3)]
        assert model.compute_verdict(findings) == model.VERDICT_FEASIBLE

    def test_attention_without_blockers_is_manual(self):
        findings = [
            _finding(risk=model.SAFE),
            _finding(risk=model.REQUIRES_ATTENTION),
        ]
        assert model.compute_verdict(findings) == model.VERDICT_MANUAL

    def test_any_blocker_is_not_recommended(self):
        findings = [
            _finding(risk=model.SAFE),
            _finding(risk=model.REQUIRES_ATTENTION),
            _finding(risk=model.BLOCKS_ROLLBACK),
        ]
        assert model.compute_verdict(findings) == model.VERDICT_NOT_RECOMMENDED

    def test_blocker_alone_is_not_recommended(self):
        findings = [_finding(risk=model.BLOCKS_ROLLBACK)]
        assert model.compute_verdict(findings) == model.VERDICT_NOT_RECOMMENDED


# ---------------------------------------------------------------------------
# PDL classification
# ---------------------------------------------------------------------------

_ASPECT_V1 = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: optional string
  bar: int
}
"""

_ASPECT_V1_ADDED_OPTIONAL = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: optional string
  bar: int
  baz: optional string
}
"""

_ASPECT_V1_ADDED_REQUIRED = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: optional string
  bar: int
  required_field: string
}
"""

_ASPECT_V1_REMOVED_FIELD = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: optional string
}
"""

_ASPECT_V1_TYPE_CHANGE = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: optional string
  bar: long
}
"""

_ASPECT_V1_OPT_TO_REQ = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 1
}
record TestAspect {
  foo: string
  bar: int
}
"""

_ASPECT_V2 = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect",
  "schemaVersion": 2
}
record TestAspect {
  foo: optional string
  bar: int
}
"""


def _mock_file_at(content_map):
    """Return a file_at mock that returns content based on (ref, path)."""
    def _file_at(ref, path):
        return content_map.get((ref, path), "")
    return _file_at


class TestClassifyPdlForRollback:
    def test_new_file_in_n_is_expected_loss(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == model.EXPECTED_LOSS
        assert "New file" in findings[0].summary

    def test_deleted_file_in_n_requires_attention(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == model.REQUIRES_ATTENTION
        assert "deleted" in findings[0].summary.lower()

    def test_added_optional_field_is_expected_loss(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_ADDED_OPTIONAL,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=["123"]), \
             patch.object(rac, "last_author_for_file", return_value="dev"):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        lost = [f for f in findings if f.risk == model.EXPECTED_LOSS]
        assert any("baz" in f.summary for f in lost)

    def test_added_required_field_is_expected_loss(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_ADDED_REQUIRED,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        lost = [f for f in findings if f.risk == model.EXPECTED_LOSS]
        assert any("required_field" in f.summary for f in lost)

    def _removed_bar(self, n_minus_1):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_REMOVED_FIELD,
            ("N-1", "test.pdl"): n_minus_1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        removed = [f for f in findings if "`bar`" in f.summary]
        assert len(removed) == 1
        return removed[0], findings

    def test_removed_required_field_without_default_blocks_rollback(self):
        f, findings = self._removed_bar(_ASPECT_V1)  # bar: int
        assert f.risk == model.BLOCKS_ROLLBACK
        assert (f.read_impact, f.write_impact, f.data_loss) == ("API fails", "fails", "yes")
        assert model.compute_verdict(findings) == model.VERDICT_NOT_RECOMMENDED

    def test_removed_required_field_with_default_requires_attention(self):
        f, _ = self._removed_bar(_ASPECT_V1.replace("bar: int", "bar: int = 0"))
        assert f.risk == model.REQUIRES_ATTENTION
        assert (f.read_impact, f.write_impact, f.data_loss) == ("ok", "ok", "yes")

    def test_removed_optional_field_requires_attention(self):
        f, _ = self._removed_bar(_ASPECT_V1.replace("bar: int", "bar: optional int"))
        assert f.risk == model.REQUIRES_ATTENTION
        assert (f.read_impact, f.write_impact, f.data_loss) == ("ok", "ok", "yes")

    def _bar_change(self, n, n_minus_1=None):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): n,
            ("N-1", "test.pdl"): n_minus_1 or _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        changed = [f for f in findings if "`bar`" in f.summary]
        assert len(changed) == 1
        return changed[0]

    def test_widened_number_may_truncate_on_n1(self):
        f = self._bar_change(_ASPECT_V1_TYPE_CHANGE)  # int -> long
        assert f.risk == model.REQUIRES_ATTENTION
        assert (f.read_impact, f.write_impact, f.data_loss) == ("ok, may truncate", "ok", "if out of range")
        assert f.reindex_required is False

    def test_number_conversion_loss_depends_on_kind(self):
        def loss(n1_type, n_type):
            f = self._bar_change(_ASPECT_V1.replace("bar: int", f"bar: {n_type}"),
                                 n_minus_1=_ASPECT_V1.replace("bar: int", f"bar: {n1_type}"))
            return f.data_loss
        assert loss("int", "long") == "if out of range"
        assert loss("int", "double") == "drops fractions"
        assert loss("double", "long") == "rounds large values"
        assert loss("float", "double") == "rounds large values"

    def test_narrowed_number_is_safe(self):
        f = self._bar_change(_ASPECT_V1, n_minus_1=_ASPECT_V1_TYPE_CHANGE)  # long -> int
        assert f.risk == model.SAFE

    def test_non_numeric_type_change_fails_on_n1(self):
        f = self._bar_change(_ASPECT_V1.replace("bar: int", "bar: string"))
        assert f.risk == model.REQUIRES_ATTENTION
        assert (f.read_impact, f.write_impact) == ("API fails", "fails")

    def test_search_mapping_change_is_a_reindex_trigger(self):
        n = _ASPECT_V1.replace("bar: int", '@Searchable = { "fieldType": "KEYWORD" }\n  bar: int')
        n1 = _ASPECT_V1.replace("bar: int", '@Searchable = { "fieldType": "TEXT" }\n  bar: int')
        f = self._bar_change(n, n_minus_1=n1)
        assert f.summary.startswith("Search mapping changed")
        assert f.reindex_required is True

    def test_formatting_only_annotation_edit_is_ignored(self):
        n = _ASPECT_V1.replace("bar: int", '@Searchable = {"fieldType": "TEXT",}\n  bar: int')
        n1 = _ASPECT_V1.replace("bar: int", '@Searchable = { "fieldType": "TEXT" }\n  bar: int')
        with patch.object(rac, "file_at", _mock_file_at({("N", "test.pdl"): n, ("N-1", "test.pdl"): n1})), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            assert pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1") == []

    def test_optional_to_required_is_safe(self):
        """N always writes a field it requires, so N-1 can read it."""
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_OPT_TO_REQ,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        foo = [f for f in findings if "foo" in f.summary]
        assert len(foo) == 1
        assert foo[0].risk == model.SAFE
        assert model.compute_verdict(findings) == model.VERDICT_FEASIBLE

    def test_required_to_optional_requires_attention(self):
        """N may omit a field it made optional, which N-1 still requires."""
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
            ("N-1", "test.pdl"): _ASPECT_V1_OPT_TO_REQ,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        foo = [f for f in findings if "foo" in f.summary]
        assert len(foo) == 1
        assert foo[0].risk == model.REQUIRES_ATTENTION
        assert model.compute_verdict(findings) == model.VERDICT_MANUAL

    def test_both_empty_returns_nothing(self):
        with patch.object(rac, "file_at", _mock_file_at({})), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert findings == []


# ---------------------------------------------------------------------------
# Enum changes
# ---------------------------------------------------------------------------

_ASPECT_ENUM_BASE = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
record TestAspect {
  status: enum Status { ACTIVE INACTIVE }
}
"""

_ASPECT_ENUM_ADDED = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
record TestAspect {
  status: enum Status { ACTIVE INACTIVE ARCHIVED }
}
"""

_ASPECT_ENUM_REMOVED = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
record TestAspect {
  status: enum Status { ACTIVE }
}
"""


class TestEnumChanges:
    def test_added_enum_value_is_expected_loss(self):
        """N-1 can't trim an unknown enum symbol the way it trims unknown fields."""
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_ENUM_ADDED,
            ("N-1", "test.pdl"): _ASPECT_ENUM_BASE,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        archived = [f for f in findings if f.summary.startswith("Enum `Status`: added value `ARCHIVED`")]
        assert len(archived) == 1
        assert archived[0].risk == model.EXPECTED_LOSS
        assert model.compute_verdict(findings) == model.VERDICT_EXPECTED_LOSS

    def test_removed_enum_value_is_not_reported(self):
        """N never writes a value it removed, so N-1 is unaffected."""
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_ENUM_REMOVED,
            ("N-1", "test.pdl"): _ASPECT_ENUM_BASE,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert not any("INACTIVE" in f.summary for f in findings)


# ---------------------------------------------------------------------------
# Record rename
# ---------------------------------------------------------------------------

_RENAMED_WITH_ANNOTATION = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
@renamedFrom = "OldRecord"
record NewRecord {
  foo: optional string
}
"""

_RENAMED_WITHOUT_ANNOTATION = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
record NewRecord {
  foo: optional string
}
"""

_ORIGINAL_RECORD = """
namespace com.linkedin.test

@Aspect = {
  "name": "testAspect"
}
record OldRecord {
  foo: optional string
}
"""


class TestRecordRename:
    """Aspects are stored by aspect name, so a record rename is safe either way."""

    def _rename_findings(self, renamed):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): renamed,
            ("N-1", "test.pdl"): _ORIGINAL_RECORD,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        return [f for f in findings if "renamed" in f.summary.lower()]

    def test_rename_with_annotation_is_safe(self):
        renamed = self._rename_findings(_RENAMED_WITH_ANNOTATION)
        assert len(renamed) == 1
        assert renamed[0].risk == model.SAFE

    def test_rename_without_annotation_is_safe(self):
        renamed = self._rename_findings(_RENAMED_WITHOUT_ANNOTATION)
        assert len(renamed) == 1
        assert renamed[0].risk == model.SAFE


# ---------------------------------------------------------------------------
# Mutator classification (retention + Kafka replay model)
# ---------------------------------------------------------------------------


class TestClassifyMutatorsForRollback:
    def test_new_mutator_is_requires_attention(self):
        mutator_java = """
        public class MyMutator extends AspectMigrationMutator {
            public String getAspectName() { return "testAspect"; }
            public long getSourceVersion() { return 1L; }
            public long getTargetVersion() { return 2L; }
            public RecordTemplate transform(RecordTemplate r) { return r; }
        }
        """
        mock_mutators = [{
            "path": "src/MyMutator.java",
            "class_name": "MyMutator",
            "target_aspect": "testAspect",
            "pr": "123",
            "author": "dev",
        }]
        with patch.object(rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(java_scan, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(rac, "file_at", return_value=mutator_java):
            findings = java_scan.classify_mutators_for_rollback("N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == model.REQUIRES_ATTENTION
        assert findings[0].dimension == model.DIM_MUTATOR
        assert "MyMutator" in findings[0].summary
        assert findings[0].detail is None  # filled in by run() from field changes

    def test_mutator_with_version_hop_in_summary(self):
        mutator_java = """
        public class FooMutator extends AspectMigrationMutator {
            public long getSourceVersion() { return 3L; }
            public long getTargetVersion() { return 5L; }
        }
        """
        mock_mutators = [{
            "path": "src/FooMutator.java",
            "class_name": "FooMutator",
            "target_aspect": "fooAspect",
            "pr": None,
            "author": None,
        }]
        with patch.object(rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(java_scan, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(rac, "file_at", return_value=mutator_java):
            findings = java_scan.classify_mutators_for_rollback("N", "N-1")
        assert len(findings) == 1
        assert "v3→v5" in findings[0].summary

    def test_duplicate_mutator_merged_with_prs(self):
        mutator_java = """
        public class MyMutator extends AspectMigrationMutator {
            public long getSourceVersion() { return 1L; }
            public long getTargetVersion() { return 2L; }
        }
        """
        mock_mutators = [
            {"path": "src/MyMutator.java", "class_name": "MyMutator",
             "target_aspect": "testAspect", "pr": "100", "author": "dev"},
            {"path": "src/MyMutator.java", "class_name": "MyMutator",
             "target_aspect": "testAspect", "pr": "200", "author": "dev"},
        ]
        with patch.object(rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(java_scan, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(rac, "file_at", return_value=mutator_java):
            findings = java_scan.classify_mutators_for_rollback("N", "N-1")
        assert len(findings) == 1
        assert "100" in findings[0].pr_number
        assert "200" in findings[0].pr_number

    def test_no_mutators_yields_empty(self):
        with patch.object(rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(java_scan, "find_mutators_added_in_window",
                          return_value=[]):
            findings = java_scan.classify_mutators_for_rollback("N", "N-1")
        assert findings == []

    def test_mutator_with_missing_content_skipped(self):
        mock_mutators = [{
            "path": "src/Gone.java",
            "class_name": "GoneMutator",
            "target_aspect": "x",
            "pr": None,
            "author": None,
        }]
        with patch.object(rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(java_scan, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(rac, "file_at", return_value=None):
            findings = java_scan.classify_mutators_for_rollback("N", "N-1")
        assert findings == []


# ---------------------------------------------------------------------------
# Extract method return int
# ---------------------------------------------------------------------------


class TestExtractMethodReturnInt:
    def test_extracts_source_version(self):
        java = "public long getSourceVersion() { return 1; }"
        assert java_scan._extract_method_return_int(java, "getSourceVersion") == 1

    def test_extracts_target_version(self):
        java = "public long getTargetVersion() { return 2; }"
        assert java_scan._extract_method_return_int(java, "getTargetVersion") == 2

    def test_extracts_long_literal_with_suffix(self):
        java = "public long getSourceVersion() { return 1L; }"
        assert java_scan._extract_method_return_int(java, "getSourceVersion") == 1

    def test_returns_none_when_missing(self):
        java = "public String getName() { return \"test\"; }"
        assert java_scan._extract_method_return_int(java, "getSourceVersion") is None


# ---------------------------------------------------------------------------
# Schema version gap analysis
# ---------------------------------------------------------------------------


class TestAnalyzeSchemaVersionGaps:
    def test_gap_detected(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V2,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 1
        assert findings[0].dimension == model.DIM_SCHEMA_VERSION
        # N-1 reads higher versions fine and writes its own; field-level
        # changes are reported separately.
        assert findings[0].risk == model.SAFE
        assert (findings[0].read_impact, findings[0].write_impact, findings[0].data_loss) == ("ok", "ok", "no")
        assert "v1" in findings[0].summary and "v2" in findings[0].summary

    def test_no_gap_when_same_version(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })):
            findings = pdl_rules.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0

    def test_no_gap_for_non_aspect(self):
        non_aspect = "namespace com.linkedin.test\nrecord Foo { bar: int }"
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): non_aspect,
            ("N-1", "test.pdl"): non_aspect,
        })):
            findings = pdl_rules.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0

    def test_skips_when_one_side_missing(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V2,
        })):
            findings = pdl_rules.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# Markdown report rendering
# ---------------------------------------------------------------------------


class TestRenderRollbackReport:
    def test_empty_findings_report(self):
        md = report.render_rollback_report(
            [], "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "Feasible as-is" in md
        assert "0 changes analyzed" in md
        assert "No schema" in md

    def test_blocker_report_has_blockers_section(self):
        findings = [
            _finding(
                risk=model.BLOCKS_ROLLBACK,
                summary="Added required field `x`",
                aspect_name="testAspect",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Verdict: 🛑 Not feasible until blockers are fixed" in md
        assert "## Blockers" in md
        assert "testAspect" in md

    def test_attention_report(self):
        findings = [
            _finding(
                risk=model.REQUIRES_ATTENTION,
                dimension=model.DIM_UPGRADE_STEP,
                summary="New BlockingSystemUpgrade: `MyStep`",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Verdict: ⚠️ Feasible after review" in md
        assert "## Requires Attention" in md

    def test_safe_changes_in_details_block(self):
        findings = [_finding(risk=model.SAFE, summary="safe change")]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "<details>" in md
        assert "Safe Changes (1)" in md

    def test_mutator_section_rendered(self):
        findings = [
            _finding(
                dimension=model.DIM_MUTATOR,
                risk=model.REQUIRES_ATTENTION,
                summary="New mutator `FooMutator` (v1→v2) — verify retention/replay coverage",
                aspect_name="fooAspect",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Mutators in Window" in md
        assert "FooMutator" in md
        assert "Version Hop" in md

    def test_reindex_section_rendered(self):
        findings = [
            _finding(
                dimension=model.DIM_PDL_SCHEMA,
                risk=model.REQUIRES_ATTENTION,
                summary="Type change on `name`: `string`→`int`",
                detail="Type change requires reindex after rollback",
                reindex_required=True,
                aspect_name="datasetProperties",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Reindex Triggers" in md
        assert "datasetProperties" in md
        assert "Reason" in md

    def test_expected_loss_section_lists_what_rollback_removes(self):
        findings = [
            _finding(risk=model.EXPECTED_LOSS, aspect_name="a", subject="E",
                     summary="Enum `E`: added value `V` — N-1 doesn't know it"),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="a", subject="f", record="Rec",
                     summary="In `Rec`: Added field `f` — N-1 ignores unknown fields"),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="a", subject="entities",
                     summary="Graph relationship on `entities` gained target types `chart`"),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="newAspect",
                     summary="New file in N — absent in N-1 (N-1 rejects writes to it)"),
        ]
        md = report.render_rollback_report(findings, "v2.0", "v1.0", "abc1234567", "def1234567")
        assert "## Verdict: ✅ Feasible, with expected loss" in md
        assert "- `a`: enum value `E.V`, field `Rec.f`, `chart` targets on `entities`" in md
        assert "- `newAspect`: the whole aspect" in md
        assert "## Schema Version Gaps" not in md

    def test_pr_number_in_table(self):
        findings = [
            _finding(
                risk=model.BLOCKS_ROLLBACK,
                pr_number="456",
                summary="test",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "#456" in md


# ---------------------------------------------------------------------------
# JSON report rendering
# ---------------------------------------------------------------------------


class TestRenderJsonReport:
    def test_valid_json(self):
        findings = [
            _finding(risk=model.SAFE),
            _finding(risk=model.BLOCKS_ROLLBACK),
        ]
        raw = report.render_json_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        data = json.loads(raw)
        assert data["verdict"] == model.VERDICT_NOT_RECOMMENDED
        assert data["summary"]["total"] == 2
        assert data["summary"]["safe"] == 1
        assert data["summary"]["blocks_rollback"] == 1
        assert len(data["findings"]) == 2

    def test_empty_findings_json(self):
        raw = report.render_json_report(
            [], "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        data = json.loads(raw)
        assert data["verdict"] == model.VERDICT_FEASIBLE
        assert data["summary"]["total"] == 0

    def test_internal_fields_stay_out_of_json(self):
        f = _finding(subject="bar", record="Foo", hop="v1→v2",
                     relationship="OwnedBy", rel_change="renamed", rel_new="AdminOf")
        raw = report.render_json_report([f], "v2.0", "v1.0", "abc1234567", "def1234567")
        keys = set(json.loads(raw)["findings"][0])
        assert not keys & {"subject", "record", "hop", "relationship", "rel_change", "rel_new"}
        assert {"summary", "read_impact", "affected_aspects"} <= keys


# ---------------------------------------------------------------------------
# Upgrade step detection
# ---------------------------------------------------------------------------


class TestUpgradeStepDetection:
    def test_implements_blocking_detected(self):
        java = """
        public class MyStep implements BlockingSystemUpgrade {
            public void execute() {}
        }
        """
        m = java_scan.IMPLEMENTS_STEP_RE.search(java)
        assert m is not None
        assert "BlockingSystemUpgrade" in m.group(0)

    def test_implements_non_blocking_detected(self):
        java = """
        public class MyStep implements NonBlockingSystemUpgrade {
            public void execute() {}
        }
        """
        m = java_scan.IMPLEMENTS_STEP_RE.search(java)
        assert m is not None
        assert "NonBlockingSystemUpgrade" in m.group(0)

    def test_no_match_for_unrelated_class(self):
        java = """
        public class MyService implements SomeOtherInterface {
            public void run() {}
        }
        """
        m = java_scan.IMPLEMENTS_STEP_RE.search(java)
        assert m is None


# ---------------------------------------------------------------------------
# Step section rendering
# ---------------------------------------------------------------------------


class TestRenderStepSection:
    def test_blocking_step_labeled_correctly(self):
        findings = [
            _finding(
                dimension=model.DIM_UPGRADE_STEP,
                risk=model.REQUIRES_ATTENTION,
                summary="New BlockingSystemUpgrade: `MyStep`",
            ),
        ]
        lines = report._render_step_section(findings)
        table = "\n".join(lines)
        assert "| Blocking |" in table

    def test_non_blocking_step_labeled_correctly(self):
        findings = [
            _finding(
                dimension=model.DIM_UPGRADE_STEP,
                risk=model.REQUIRES_ATTENTION,
                summary="New NonBlockingSystemUpgrade: `MyAsyncStep`",
                subject="MyAsyncStep",
            ),
        ]
        lines = report._render_step_section(findings)
        table = "\n".join(lines)
        assert "| Non-blocking |" in table
        assert "MyAsyncStep" in table


# ---------------------------------------------------------------------------
# Summary line grammar
# ---------------------------------------------------------------------------


class TestSummaryGrammar:
    def test_singular_blocker_uses_blocks(self):
        findings = [_finding(risk=model.BLOCKS_ROLLBACK)]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "1 blocks rollback" in md

    def test_plural_blockers_uses_block(self):
        findings = [
            _finding(risk=model.BLOCKS_ROLLBACK, summary="a"),
            _finding(risk=model.BLOCKS_ROLLBACK, summary="b"),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "2 block rollback" in md


# ---------------------------------------------------------------------------
# Mixed-dimension integration
# ---------------------------------------------------------------------------


class TestMixedFindings:
    def test_mixed_verdict_picks_worst(self):
        findings = [
            _finding(dimension=model.DIM_PDL_SCHEMA, risk=model.SAFE),
            _finding(dimension=model.DIM_REINDEX, risk=model.REQUIRES_ATTENTION),
            _finding(dimension=model.DIM_MUTATOR, risk=model.BLOCKS_ROLLBACK),
        ]
        assert model.compute_verdict(findings) == model.VERDICT_NOT_RECOMMENDED

    def test_mutator_attention_with_pdl_blocker(self):
        findings = [
            _finding(dimension=model.DIM_PDL_SCHEMA, risk=model.BLOCKS_ROLLBACK),
            _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION),
        ]
        assert model.compute_verdict(findings) == model.VERDICT_NOT_RECOMMENDED

    def test_mutator_attention_only_is_manual(self):
        findings = [
            _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION),
        ]
        assert model.compute_verdict(findings) == model.VERDICT_MANUAL

    def test_report_has_all_sections(self):
        findings = [
            _finding(dimension=model.DIM_PDL_SCHEMA, risk=model.SAFE, summary="safe"),
            _finding(
                dimension=model.DIM_MUTATOR,
                risk=model.REQUIRES_ATTENTION,
                summary="New mutator `X` (v1→v2) — verify retention/replay coverage",
                aspect_name="a",
            ),
            _finding(
                dimension=model.DIM_UPGRADE_STEP,
                risk=model.REQUIRES_ATTENTION,
                summary="New BlockingSystemUpgrade: `Y`",
            ),
            _finding(
                dimension=model.DIM_PDL_SCHEMA,
                risk=model.REQUIRES_ATTENTION,
                summary="Removed field `z` — N-1 expects it",
                detail="Field deletion requires reindex after rollback",
                reindex_required=True,
                aspect_name="b",
            ),
            _finding(
                dimension=model.DIM_SCHEMA_VERSION,
                risk=model.REQUIRES_ATTENTION,
                summary="Schema version gap: v1→v2 (1 hop)",
                aspect_name="c",
                detail="detail",
            ),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Requires Attention" in md
        assert "<details>" in md
        assert "## Mutators in Window" in md
        assert "## Upgrade Steps in Window" in md
        assert "## Reindex Triggers" in md


class TestMainTargetDefault:
    def test_json_output_path_is_rejected_with_json_flag(self, tmp_path):
        with patch.object(pipeline, "run") as run, pytest.raises(SystemExit) as exc:
            cli.main(["--current", "a", "--target", "b", "--json",
                      "--output", str(tmp_path / "report.json")])
        assert exc.value.code == 2
        run.assert_not_called()

    def test_target_defaults_to_latest_release(self, tmp_path):
        out = tmp_path / "report.md"
        with patch.object(
            rac, "resolve_base", return_value="v1.2.0"
        ) as resolve, patch.object(
            pipeline, "run", return_value=([], "abc1234567", "def1234567")
        ) as run:
            cli.main(["--current", "abc123", "--output", str(out)])
        resolve.assert_called_once()
        assert run.call_args.args[:2] == ("abc123", "v1.2.0")
        assert "v1.2.0" in out.read_text()

    def test_explicit_target_skips_resolution(self, tmp_path):
        out = tmp_path / "report.md"
        with patch.object(rac, "resolve_base") as resolve, patch.object(
            pipeline, "run", return_value=([], "abc1234567", "def1234567")
        ) as run:
            cli.main(
                ["--current", "abc123", "--target", "v1.1.0", "--output", str(out)]
            )
        resolve.assert_not_called()
        assert run.call_args.args[:2] == ("abc123", "v1.1.0")


    def test_repo_url_flag_overrides_detection(self, tmp_path):
        out = str(tmp_path / "report.md")
        with patch.object(repo, "repo_url", return_value="https://github.com/o/detected"), \
             patch.object(pipeline, "run", return_value=([], "abc1234567", "def1234567")) as run:
            cli.main(["--current", "a", "--target", "b", "--output", out])
            assert run.call_args.args[2] == "https://github.com/o/detected"
            cli.main(["--current", "a", "--target", "b", "--output", out, "--repo-url", "https://github.com/o/given"])
            assert run.call_args.args[2] == "https://github.com/o/given"

    def test_schema_diff_reaches_both_outputs(self, tmp_path):
        out = tmp_path / "report.md"
        diff = {"aspects_added": [{"aspect": "newAspect", "entities": ["dataset"]}]}
        with patch.object(pipeline, "run", return_value=([_finding()], "abc1234567", "def1234567")), \
             patch.object(pipeline, "schema_diff", return_value=diff):
            cli.main(["--current", "a", "--target", "b", "--output", str(out), "--json"])
        assert "| `newAspect` | ❌ missing | ✅ added | `dataset` |" in out.read_text()
        assert json.loads((tmp_path / "report.json").read_text())["schema_diff"] == diff

    def test_unavailable_schema_diff_is_reported(self, tmp_path, capsys):
        out = tmp_path / "report.md"
        with patch.object(pipeline, "run", return_value=([_finding()], "abc1234567", "def1234567")), \
             patch.object(pipeline, "schema_diff", return_value=None):
            cli.main(["--current", "a", "--target", "b", "--output", str(out), "--json"])
        assert "schema diff skipped" in capsys.readouterr().err
        assert "## Schema Diff (N vs N-1)\n\n_Not available:" in out.read_text()
        assert "error" in json.loads((tmp_path / "report.json").read_text())["schema_diff"]

    def test_json_report_never_overwrites_markdown(self, tmp_path):
        out = tmp_path / "report"
        with patch.object(
            pipeline, "run", return_value=([], "abc1234567", "def1234567")
        ):
            cli.main(
                ["--current", "abc123", "--target", "v1.1.0",
                 "--output", str(out), "--json"]
            )
        assert out.read_text().startswith("# Rollback Compatibility Report")
        assert json.loads((tmp_path / "report.json").read_text())["target"] == "v1.1.0"


class TestOrderWarning:
    def test_release_versions_in_order(self):
        assert cli.order_warning("v1.7.0.1", "v1.7.0", "a" * 10, "b" * 10) is None
        assert cli.order_warning("v2.3.0-cloud", "v2.2.3-cloud", "a" * 10, "b" * 10) is None
        assert cli.order_warning("releases/v1.8.0", "v1.7.0.1", "a" * 10, "b" * 10) is None

    def test_release_versions_reversed(self):
        assert cli.order_warning("v1.6.0", "v1.7.0", "a" * 10, "b" * 10)
        assert cli.order_warning("v1.7.0rc1", "v1.7.0.1", "a" * 10, "b" * 10)

    def test_same_commit_never_warns(self):
        assert cli.order_warning("v1.6.0", "v1.7.0", "a" * 10, "a" * 10) is None

    def test_non_release_refs_fall_back_to_commit_dates(self):
        times = {"newsha": "200\n", "oldsha": "100\n"}
        with patch.object(rac, "_git", side_effect=lambda *a: times[a[-1]]):
            assert cli.order_warning("master", "v1.7.0", "newsha", "oldsha") is None
            assert cli.order_warning("master", "v1.7.0", "oldsha", "newsha")

    def test_warning_is_shown_in_reports(self):
        md = report.render_rollback_report(
            [], "v1.6.0", "v1.7.0", "abc1234567", "def1234567", "swapped"
        )
        assert "**Warning:** swapped" in md
        data = json.loads(
            report.render_json_report(
                [], "v1.6.0", "v1.7.0", "abc1234567", "def1234567", "swapped"
            )
        )
        assert data["warning"] == "swapped"


class TestFindMutatorsAddedInWindow:
    def test_skips_files_already_in_base(self):
        """A root commit in the window re-adds files that already existed at
        base; only files that are new between the base and head trees count."""
        new_path = "metadata-service/factories/src/main/java/com/example/NewMutator.java"
        old_path = "metadata-service/factories/src/main/java/com/example/OldMutator.java"
        log_output = (
            "COMMIT abc123fff feat: add mutator (#9999)\n"
            f"{new_path}\n"
            "COMMIT def456eee Stacked merge of clean commits\n"
            f"{old_path}\n"
        )
        shown: list[str] = []

        def fake_git(*args):
            if args[:2] == ("log", "-1"):
                return "Alice Example\n"
            if args[0] == "log":
                return log_output
            if args[0] == "diff":
                return f"{new_path}\n"
            if args[0] == "show":
                shown.append(args[1])
                name = args[1].rsplit("/", 1)[-1].removesuffix(".java")
                return f"public class {name} extends AspectMigrationMutator {{}}"
            return ""

        with patch.object(rac, "_git", side_effect=fake_git), \
             patch.object(rac, "_load_aspect_name_constants", return_value={}):
            results = java_scan.find_mutators_added_in_window(
                "v1.0", "HEAD", {"AspectMigrationMutator"}
            )
        assert [r["class_name"] for r in results] == ["NewMutator"]
        assert results[0]["pr"] == "9999"
        assert not any(old_path in ref for ref in shown)


class TestWhyColumn:
    def test_attention_and_safe_tables_show_detail(self):
        findings = [
            _finding(
                risk=model.REQUIRES_ATTENTION,
                summary="Required→optional flip on `foo` — N-1 requires it",
                detail="N may omit it | check\nstored records first",
            ),
            _finding(risk=model.SAFE, summary="Added field `bar`"),
        ]
        md = report.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        attention = md.split("## Requires Attention", 1)[1].split("<details>", 1)[0]
        safe = md.split("<details>", 1)[1]
        assert "| Why / action |" in attention
        assert "N may omit it \\| check stored records first" in attention
        assert "Why / action" in safe
        assert f"_{report.TABLE_DESCRIPTIONS['Requires Attention']}_" in attention
        assert f"_{report.TABLE_DESCRIPTIONS['Safe Changes']}_" in safe


class TestImpact:
    def _classify(self, n, n1):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): n, ("N-1", "test.pdl"): n1,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            return pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")

    def test_required_to_optional_fails_read_and_write(self):
        f = [x for x in self._classify(_ASPECT_V1, _ASPECT_V1_OPT_TO_REQ) if "foo" in x.summary][0]
        assert (f.read_impact, f.write_impact, f.data_loss) == ("API fails", "fails", "no")

    def test_added_field_is_dropped_on_n1_write(self):
        f = [x for x in self._classify(_ASPECT_V1_ADDED_OPTIONAL, _ASPECT_V1) if "baz" in x.summary][0]
        assert f.risk == model.EXPECTED_LOSS
        assert (f.read_impact, f.write_impact, f.data_loss) == ("ok", model.DROPS_NEW_FIELD, "no")

    def test_report_shows_read_write_data_loss_columns(self):
        findings = [_finding(risk=model.REQUIRES_ATTENTION, **model.impact("fails", "fails", "no"))]
        md = report.render_rollback_report(findings, "v2.0", "v1.0", "abc1234567", "def1234567")
        assert "| N-1 read | N-1 write | N-1 data loss |" in md
        assert "| fails | fails | no |" in md
        assert "<br>" not in md
        data = json.loads(report.render_json_report(findings, "v2.0", "v1.0", "abc1234567", "def1234567"))
        assert data["findings"][0]["read_impact"] == "fails"


class TestComparableType:
    def test_inline_enum_equals_named_reference(self):
        assert pdl_parser.comparable_type("enum Card { ONE N }") == pdl_parser.comparable_type("Card")

    def test_default_only_change_is_not_a_type_change(self):
        assert pdl_parser.comparable_type('EvalType = "METADATA"') == pdl_parser.comparable_type('EvalType = "SQL"')

    def test_real_type_change_is_still_detected(self):
        assert pdl_parser.comparable_type("string") != pdl_parser.comparable_type("int")

    def test_change_after_nested_default_is_a_type_change(self):
        a = "record Inner { x: int = 1, y: string }"
        b = "record Inner { x: int = 1, y: long }"
        assert pdl_parser.comparable_type(a) != pdl_parser.comparable_type(b)

    def test_map_default_is_stripped(self):
        assert pdl_parser.comparable_type("map[string, string] = { }") == "map[string, string]"

    def test_enum_addition_is_reported_once_not_as_type_change(self):
        with patch.object(rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_ENUM_ADDED,
            ("N-1", "test.pdl"): _ASPECT_ENUM_BASE,
        })), patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert not any(f.summary.startswith("Type change") for f in findings)
        assert not any(f.reindex_required for f in findings)


class TestHasDefault:
    def test_default_after_inline_record_on_later_line(self):
        pdl = 'record A {\n  inner: record I {\n    x: int\n  } = { "x": 1 }\n  other: int\n}'
        assert pdl_parser.field_has_default(pdl, "inner", "A") is True
        assert pdl_parser.field_has_default(pdl, "other", "A") is False

    def test_default_on_same_named_field_in_another_record_is_ignored(self):
        pdl = 'record A {\n  x: string\n}\nrecord B {\n  x: string = "d"\n}'
        assert pdl_parser.field_has_default(pdl, "x", "A") is False
        assert pdl_parser.field_has_default(pdl, "x", "B") is True

    def test_default_inside_inline_nested_record_is_ignored(self):
        pdl = 'record A {\n  inner: record I {\n    x: string = "d"\n  }\n  x: string\n}'
        assert pdl_parser.field_has_default(pdl, "x", "A") is False

    def test_removed_required_field_still_blocks_when_other_record_has_default(self):
        n1 = _ASPECT_V1 + '\nrecord Other {\n  bar: int = 0\n}\n'
        n = _ASPECT_V1_REMOVED_FIELD + '\nrecord Other {\n  bar: int = 0\n}\n'
        with patch.object(rac, "file_at", _mock_file_at({("N", "test.pdl"): n, ("N-1", "test.pdl"): n1})), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        removed = [f for f in findings if f.summary.startswith("Removed field `bar`")]
        assert len(removed) == 1 and removed[0].risk == model.BLOCKS_ROLLBACK


class TestMutatorImpact:
    def test_mutator_takes_worst_impact_of_its_aspect_fields(self):
        mutator = _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION, aspect_name="a")
        findings = [
            mutator,
            _finding(aspect_name="a", **model.impact("ok", model.DROPS_NEW_FIELD, "no")),
            _finding(aspect_name="a", **model.impact("API fails", "fails", "no")),
            _finding(aspect_name="other", **model.impact("ok", "ok", "yes")),
        ]
        pipeline.set_mutator_impact(findings)
        assert (mutator.read_impact, mutator.write_impact, mutator.data_loss) == ("API fails", "fails", "no")

    def test_mutator_without_schema_change_is_unknown(self):
        mutator = _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION, aspect_name="a")
        pipeline.set_mutator_impact([mutator])
        assert (mutator.read_impact, mutator.write_impact, mutator.data_loss) == ("unknown", "unknown", "unknown")
        assert "check its transform" in mutator.detail


class TestMutatorDetail:
    def test_detail_names_the_field_change_and_its_effect(self):
        mutator = _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION, aspect_name="a")
        added = _finding(aspect_name="a", summary="Added field `parent` — N-1 ignores unknown fields",
                         **model.impact("ok", model.DROPS_NEW_FIELD, "no"))
        pipeline.set_mutator_impact([mutator, added])
        assert mutator.detail.startswith("Converts records to N's shape: added field `parent`.")
        assert "N-1 drops the new field when it saves a record." in mutator.detail
        assert "ASPECT_MIGRATION_MUTATOR_ENABLED" in mutator.detail
        assert "Option F restore" in mutator.detail
        assert mutator.risk == model.REQUIRES_ATTENTION


class TestEnumParser:
    def test_commas_and_annotations_are_not_symbols(self):
        src = 'record R { s: enum E {\n A,\n @deprecated = "Use B instead."\n C,\n /** doc */ B\n} }'
        assert pdl_parser.enum_symbols(src) == {"E": ["A", "C", "B"]}

    def test_annotation_with_braces_inside_enum(self):
        assert pdl_parser.enum_symbols('enum F { X Y @symbolDocs = {"X": "x"} Z }') == {"F": ["X", "Y", "Z"]}


class TestUnexplainedVersionGap:
    def test_gap_without_field_changes_requires_attention(self):
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="a", **model.impact("ok", "ok", "no"))
        pipeline.flag_unexplained_version_gaps([gap])
        assert gap.risk == model.REQUIRES_ATTENTION
        assert gap.read_impact == "not analysed"
        assert "records it uses" in gap.detail

    def test_gap_with_field_changes_stays_safe(self):
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="a", **model.impact("ok", "ok", "no"))
        pipeline.flag_unexplained_version_gaps([gap, _finding(aspect_name="a", summary="Added field `x`")])
        assert gap.risk == model.SAFE


class TestFindUpgradeStepsAddedInWindow:
    def test_implements_only_step_is_found(self):
        path = "datahub-upgrade/src/main/java/com/example/MyStep.java"

        def fake_git(*args):
            if args[:2] == ("log", "-1"):
                return "Alice\n"
            if args[0] == "log":
                return f"COMMIT abc123fff feat: add step (#4242)\n{path}\n"
            if args[0] == "diff":
                return f"{path}\n"
            if args[0] == "show":
                return "public class MyStep implements NonBlockingSystemUpgrade { }"
            return ""

        with patch.object(rac, "_git", side_effect=fake_git), \
             patch.object(java_scan, "discover_upgrade_step_hierarchy", return_value={}):
            steps = java_scan.find_upgrade_steps_added_in_window("v1", "v2")
        assert [(s["class_name"], s["step_type"], s["pr"]) for s in steps] == [
            ("MyStep", "NonBlockingSystemUpgrade", "4242")
        ]

    def test_step_made_by_a_later_commit_is_found(self):
        path = "datahub-upgrade/src/main/java/com/example/MyStep.java"

        def fake_git(*args):
            if args[:2] == ("log", "-1"):
                return "Alice\n"
            if args[0] == "log":
                return f"COMMIT abc123fff add helper (#1)\n{path}\n"
            if args[0] == "diff":
                return f"{path}\n"
            if args[0] == "show":
                # Plain class when added; a later commit made it a step.
                if args[1].startswith("abc123fff:"):
                    return "public class MyStep { }"
                return "public class MyStep implements BlockingSystemUpgrade { }"
            return ""

        with patch.object(rac, "_git", side_effect=fake_git), \
             patch.object(java_scan, "discover_upgrade_step_hierarchy", return_value={}):
            steps = java_scan.find_upgrade_steps_added_in_window("v1", "v2")
        assert [s["class_name"] for s in steps] == ["MyStep"]

    def test_implements_among_other_interfaces(self):
        src = "public class S implements Foo, BlockingSystemUpgrade {"
        assert java_scan.IMPLEMENTS_STEP_RE.search(src).group(1) == "BlockingSystemUpgrade"


_POLICY_V1 = """
namespace com.linkedin.test
record Criterion {
  field: string
}
"""
_POLICY_V2 = _POLICY_V1.replace("field: string", "field: string\n  extra: optional string")
_ASPECT_USING = """
namespace com.linkedin.test
@Aspect = { "name": "policyAspect" }
record PolicyAspect {
  criteria: array[Criterion]
}
"""
_P = "metadata-models/src/main/pegasus/com/linkedin/test/"


@contextmanager
def _mock_repo(files: dict, all_paths: tuple = ()):
    """Patch git access so `files` ({(ref, path): content}) is the repo.
    `all_paths` are the PDLs listed at ref N (for dependency lookups)."""
    with ExitStack() as stack:
        stack.enter_context(patch.object(rac, "file_at", lambda ref, path: files.get((ref, path), "")))
        stack.enter_context(patch.object(rac, "pr_numbers_for_file", return_value=[]))
        stack.enter_context(patch.object(rac, "last_author_for_file", return_value=None))
        stack.enter_context(patch.object(repo, "first_pr", return_value=None))
        stack.enter_context(patch.object(repo, "file_author", return_value=None))
        if all_paths:
            stack.enter_context(patch.object(rac, "_git", return_value="\n".join(all_paths) + "\n"))
            stack.enter_context(patch.object(
                repo, "read_files_at", return_value={p: files[("N", p)] for p in all_paths}
            ))
        yield


class TestNestedChanges:
    def test_aspects_using_follows_field_types_transitively(self):
        contents = {
            _P + "Criterion.pdl": _POLICY_V1,
            _P + "Middle.pdl": "namespace com.linkedin.test\nrecord Middle { c: Criterion }",
            _P + "PolicyAspect.pdl": _ASPECT_USING.replace("array[Criterion]", "Middle"),
        }
        users = pdl_rules.aspects_using({"com.linkedin.test.Criterion"}, contents)
        assert users["com.linkedin.test.Criterion"] == {"policyAspect"}

    def test_nested_added_field_is_attributed_to_using_aspect(self):
        files = {("N", _P + "Criterion.pdl"): _POLICY_V2, ("N-1", _P + "Criterion.pdl"): _POLICY_V1}
        with patch.object(rac, "file_at", lambda ref, path: files.get((ref, path), "")), \
             patch.object(rac, "_git", return_value=_P + "Criterion.pdl\n" + _P + "PolicyAspect.pdl\n"), \
             patch.object(repo, "read_files_at", return_value={
                 _P + "Criterion.pdl": _POLICY_V2, _P + "PolicyAspect.pdl": _ASPECT_USING}), \
             patch.object(repo, "first_pr", return_value="77"), \
             patch.object(repo, "file_author", return_value=None):
            findings = pdl_rules.analyze_nested_changes("N", "N-1", [_P + "Criterion.pdl"])
        assert [f.summary.split(" — ")[0] for f in findings] == ["In `Criterion`: Added field `extra`"]
        assert findings[0].affected_aspects == ["policyAspect"]
        assert "Used by: policyAspect." in findings[0].detail

    def test_version_gap_explained_by_nested_change_stays_safe(self):
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="policyAspect")
        nested = _finding(summary="In `Criterion`: Added field `extra`", affected_aspects=["policyAspect"])
        pipeline.flag_unexplained_version_gaps([gap, nested])
        assert gap.risk == model.SAFE


_INNER_V1 = """
namespace com.linkedin.test
@Aspect = { "name": "innerAspect", "schemaVersion": 2 }
record InnerAspect {
  @Relationship = { "/*": { "name": "On", "entityTypes": [ "dataset" ] } }
  entities: array[string]
}
"""
_INNER_V2 = _INNER_V1.replace('"schemaVersion": 2', '"schemaVersion": 3').replace(
    '[ "dataset" ]', '[ "dataset", "chart" ]'
)
_OUTER = """
namespace com.linkedin.test
@Aspect = { "name": "outerAspect", "schemaVersion": 3 }
record OuterAspect {
  info: InnerAspect
}
"""


class TestEmbeddedAspectChanges:
    def test_aspect_change_explains_embedding_aspect_version_gap(self):
        inner, outer = _P + "InnerAspect.pdl", _P + "OuterAspect.pdl"
        files = {
            ("N", inner): _INNER_V2, ("N-1", inner): _INNER_V1,
            ("N", outer): _OUTER, ("N-1", outer): _OUTER.replace('"schemaVersion": 3', '"schemaVersion": 2'),
        }
        with patch.object(rac, "file_at", lambda ref, path: files.get((ref, path), "")), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None), \
             patch.object(rac, "_git", return_value=f"{inner}\n{outer}\n"), \
             patch.object(repo, "read_files_at", return_value={inner: _INNER_V2, outer: _OUTER}):
            findings = pdl_rules.classify_pdl_for_rollback(inner, "N", "N-1")
            pdl_rules.attribute_embedded_aspect_changes(findings, "N", "N-1", [inner, outer])
            findings.extend(pdl_rules.analyze_schema_version_gaps("N", "N-1", [inner, outer]))
        pipeline.flag_unexplained_version_gaps(findings)

        rel = [f for f in findings if f.summary.startswith("Graph relationship on `entities` gained")]
        assert len(rel) == 1 and rel[0].risk == model.EXPECTED_LOSS and rel[0].write_impact == "fails"
        assert rel[0].aspect_name == "innerAspect"
        assert rel[0].affected_aspects == ["outerAspect"]
        assert "Also embedded in: outerAspect." in rel[0].detail
        gap = next(f for f in findings if f.dimension == model.DIM_SCHEMA_VERSION and f.aspect_name == "outerAspect")
        assert gap.risk == model.SAFE and gap.read_impact != "not analysed"


class TestRelationshipAndIncludes:
    def test_removed_relationship_is_rebuilt_by_restore_indices(self):
        n1 = _ASPECT_V1.replace("bar: int", '@Relationship = { "name": "OwnedBy", "entityTypes": [ "corpuser" ] }\n  bar: int')
        with patch.object(rac, "file_at", _mock_file_at({("N", "test.pdl"): _ASPECT_V1, ("N-1", "test.pdl"): n1})), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        [rel] = [f for f in findings if f.summary.startswith("Graph relationship `OwnedBy` removed from `bar`")]
        # No other aspect builds OwnedBy here, so the rollback's restore-indices rebuilds it.
        assert rel.risk == model.SAFE and pdl_rules.RESTORE_REBUILDS in rel.detail

    def test_new_include_adds_its_fields(self):
        n1 = _ASPECT_V1
        n = _ASPECT_V1.replace("record TestAspect {", "record TestAspect includes Extra {")
        extra = "namespace com.linkedin.test\nrecord Extra {\n  customProperties: map[string, string] = { }\n}"
        files = {("N", "test.pdl"): n, ("N-1", "test.pdl"): n1, ("N", _P + "Extra.pdl"): extra}
        with patch.object(rac, "file_at", lambda ref, path: files.get((ref, path), "")), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        added = [f for f in findings if f.summary.startswith("Added field `customProperties` (via includes `Extra`)")]
        assert len(added) == 1 and added[0].risk == model.EXPECTED_LOSS


class TestUpgradeStepImpact:
    def _step(self):
        return _finding(dimension=model.DIM_UPGRADE_STEP, risk=model.REQUIRES_ATTENTION,
                        path="datahub-upgrade/x/MyStep.java", **model.impact("unknown", "unknown", "unknown"))

    def test_step_aspects_from_constants_and_docs(self):
        files = {
            "datahub-upgrade/x/MyStep.java": "class MyStep { String a = POLICY_ASPECT_NAME; }",
            "datahub-upgrade/x/MyStepStep.java": "/** writes a denormalized {@code dataProducts} aspect */",
            "datahub-upgrade/x/Other.java": "String b = IGNORED_ASPECT_NAME;",
        }
        with patch.object(rac, "_git", return_value="\n".join(files)), \
             patch.object(repo, "read_files_at", lambda ref, paths: {p: files[p] for p in paths}):
            got = java_scan.step_aspects("datahub-upgrade/x/MyStep.java", "N", {
                "POLICY_ASPECT_NAME": "dataHubPolicyInfo", "IGNORED_ASPECT_NAME": "other"})
        assert got == {"dataHubPolicyInfo", "dataProducts"}

    def _run(self, aspects, n1_aspects, findings):
        step = self._step()
        with patch.object(rac, "_load_aspect_name_constants", return_value={}), \
             patch.object(repo, "aspect_names_at", return_value=n1_aspects), \
             patch.object(java_scan, "step_aspects", return_value=aspects):
            pipeline.set_upgrade_step_impact([step, *findings], "N", "N-1")
        return step

    def test_aspect_unknown_to_n1_fails_on_n1(self):
        step = self._run({"dataProducts", "dataHubUpgradeResult"}, {"dataHubUpgradeResult"}, [])
        # N-1 reads entities without an aspect it doesn't know; writing it fails.
        assert (step.read_impact, step.write_impact, step.data_loss) == ("ok", "fails", "no")
        assert "`dataProducts` (not in N-1, which reads its entities without it)" in step.detail
        assert "dataHubUpgradeResult" not in step.detail

    def test_aspect_of_new_entity_type_fails_reads(self):
        with patch.object(pdl_rules, "entity_owners", return_value=pdl_rules.EntityOwners(["feedback"], [])):
            step = self._run({"feedbackInfo"}, set(), [])
        assert step.read_impact == model.API_FAILS
        assert "`feedbackInfo` (part of the new entity type feedback)" in step.detail

    def test_unchanged_known_aspect_is_ok(self):
        step = self._run({"aliases"}, {"aliases"}, [])
        assert (step.read_impact, step.write_impact, step.data_loss) == ("ok", "ok", "no")

    def test_unexplained_version_gap_is_not_analysed(self):
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="aliases",
                       **model.impact("not analysed", "not analysed", "not analysed"))
        step = self._run({"aliases"}, {"aliases"}, [gap])
        assert step.read_impact == "not analysed"
        assert "`aliases` (changed in N)" in step.detail

    def test_explained_version_gap_adds_nothing(self):
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="aliases",
                       **model.impact("ok", "ok", "no"))
        step = self._run({"aliases"}, {"aliases"}, [gap])
        assert (step.read_impact, step.write_impact, step.data_loss) == ("ok", "ok", "no")
        assert "changed in N" not in step.detail

    def test_no_aspects_found_stays_unknown(self):
        step = self._run(set(), set(), [])
        assert step.read_impact == "unknown"
        assert "Couldn't tell" in step.detail


_BASE = "namespace com.linkedin.test\nrecord Base {\n  a: string\n}\n"
_ASP_INCLUDES = 'namespace com.linkedin.test\n@Aspect = { "name": "asp" }\nrecord Asp includes Base {\n  b: string\n}\n'
_ASP_INLINED = 'namespace com.linkedin.test\n@Aspect = { "name": "asp" }\nrecord Asp {\n  a: string\n  b: string\n}\n'


class TestEffectiveFields:
    def _classify(self, n, n1):
        files = {("N", "asp.pdl"): n, ("N-1", "asp.pdl"): n1, ("N", _P + "Base.pdl"): _BASE, ("N-1", _P + "Base.pdl"): _BASE}
        with _mock_repo(files):
            return pdl_rules.classify_pdl_for_rollback("asp.pdl", "N", "N-1")

    def test_inlining_an_include_is_not_a_change(self):
        assert self._classify(_ASP_INLINED, _ASP_INCLUDES) == []

    def test_moving_fields_into_an_include_is_not_a_change(self):
        assert self._classify(_ASP_INCLUDES, _ASP_INLINED) == []

    def test_included_field_keeps_its_own_default(self):
        base = "namespace com.linkedin.test\nrecord Base {\n  a: string = \"x\"\n}\n"
        files = {("N-1", _P + "Base.pdl"): base}
        with patch.object(rac, "file_at", lambda ref, path: files.get((ref, path), "")):
            fields = pdl_parser.effective_fields(_ASP_INCLUDES, "N-1")
        assert fields["a"]["has_default"] is True and fields["a"]["via"] == "Base"
        assert fields["b"]["has_default"] is False and fields["b"]["via"] is None


class TestRequiredToOptionalWithDefault:
    def test_n1_default_makes_flip_safe(self):
        n1 = _ASPECT_V1.replace("bar: int", "bar: int = 0")
        n = _ASPECT_V1.replace("bar: int", "bar: optional int")
        with patch.object(rac, "file_at", _mock_file_at({("N", "test.pdl"): n, ("N-1", "test.pdl"): n1})), \
             patch.object(rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(rac, "last_author_for_file", return_value=None):
            findings = pdl_rules.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        flip = [f for f in findings if "flip on `bar`" in f.summary]
        assert len(flip) == 1 and flip[0].risk == model.SAFE
        assert (flip[0].read_impact, flip[0].write_impact) == ("ok", "ok")


class TestReindexSection:
    def test_nested_finding_shows_record_and_field(self):
        f = _finding(summary="In `Foo`: Search mapping changed on `bar`", aspect_name="a | b",
                     detail="x | y", reindex_required=True, record="Foo", subject="bar")
        row = report._render_reindex_section([f])[4]
        assert "`Foo.bar`" in row
        assert "a \\| b" in row and "x \\| y" in row

    def test_top_level_finding_shows_field(self):
        f = _finding(summary="Search mapping changed on `title`", reindex_required=True)
        assert "`title`" in report._render_reindex_section([f])[4]


class TestResolveRefName:
    def test_falls_back_to_remote_tracking_branch(self):
        resolves = {"origin/releases/v1.3.0"}
        with patch.object(rac, "_resolve_ref", lambda *c: next((r for r in c if r in resolves), None)):
            assert repo.resolve_ref_name("releases/v1.3.0") == "origin/releases/v1.3.0"
            assert repo.resolve_ref_name("v9.9.9") == "v9.9.9"


_U_V1 = "namespace com.linkedin.test\ntyperef U = union[string, int]\n"
_U_V2 = "namespace com.linkedin.test\ntyperef U = union[string, int, long]\n"
_ASP_U = 'namespace com.linkedin.test\n@Aspect = { "name": "uAspect" }\nrecord UAspect {\n  u: U\n}\n'


class TestFullReviewFixes:
    def _nested(self, files, pdl_paths, all_files):
        with _mock_repo(files, tuple(all_files)):
            return pdl_rules.analyze_nested_changes("N", "N-1", pdl_paths)

    def test_union_member_added_in_shared_typeref_is_expected_loss(self):
        u, asp = _P + "U.pdl", _P + "UAspect.pdl"
        files = {("N", u): _U_V2, ("N-1", u): _U_V1, ("N", asp): _ASP_U, ("N-1", asp): _ASP_U}
        findings = self._nested(files, [u], [u, asp])
        assert [f.summary.split(" — ")[0] for f in findings] == ["Union `U`: added member `long`"]
        f = findings[0]
        assert f.risk == model.EXPECTED_LOSS and (f.read_impact, f.write_impact) == ("API fails", "fails")
        assert f.affected_aspects == ["uAspect"]
        assert model.compute_verdict(findings) == model.VERDICT_EXPECTED_LOSS

    def test_comment_only_change_in_typeref_file_is_not_reported(self):
        u, asp = _P + "U.pdl", _P + "UAspect.pdl"
        files = {("N", u): "/** new doc */\n" + _U_V1, ("N-1", u): _U_V1, ("N", asp): _ASP_U, ("N-1", asp): _ASP_U}
        assert self._nested(files, [u], [u, asp]) == []

    def test_file_the_parser_cannot_read_requires_attention(self):
        bad_v1 = "namespace com.linkedin.test\nrecord U {\n  a: string\n"
        bad_v2 = bad_v1 + "  b: string\n"
        u, asp = _P + "U.pdl", _P + "UAspect.pdl"
        files = {("N", u): bad_v2, ("N-1", u): bad_v1, ("N", asp): _ASP_U, ("N-1", asp): _ASP_U}
        findings = self._nested(files, [u], [u, asp])
        assert len(findings) == 1
        f = findings[0]
        assert "couldn't be analysed" in f.summary and f.risk == model.REQUIRES_ATTENTION
        assert (f.read_impact, f.write_impact, f.data_loss) == ("not analysed",) * 3
        assert f.affected_aspects == ["uAspect"]
        assert model.compute_verdict(findings) == model.VERDICT_MANUAL

    def test_include_change_not_repeated_for_aspect_whose_file_changed(self):
        base, asp = _P + "Base.pdl", _P + "Asp.pdl"
        base_v2 = _BASE.replace("a: string", "a: string\n  x: optional string")
        files = {("N", base): base_v2, ("N-1", base): _BASE, ("N", asp): _ASP_INCLUDES, ("N-1", asp): _ASP_INCLUDES}
        # Aspect file changed too (e.g. version bump): its own diff covers Base.
        assert self._nested(files, [base, asp], [base, asp]) == []
        # Aspect file unchanged: the nested finding is still reported for it.
        reported = self._nested(files, [base], [base, asp])
        assert [f.summary.split(" — ")[0] for f in reported] == ["In `Base`: Added field `x`"]

    def test_upgrade_step_added_twice_is_reported_once(self):
        step = {"path": "datahub-upgrade/x/S.java", "class_name": "S", "step_type": "NonBlockingSystemUpgrade", "author": None}
        with patch.object(java_scan, "find_upgrade_steps_added_in_window",
                          return_value=[{**step, "pr": "1"}, {**step, "pr": "2"}]):
            findings = java_scan.classify_upgrade_steps_for_rollback("N", "N-1")
        assert len(findings) == 1 and findings[0].pr_number == "1, 2"


_BASE_ENUM_V1 = "namespace com.linkedin.test\nrecord Base {\n  status: enum Status { A B }\n}\n"
_BASE_ENUM_V2 = _BASE_ENUM_V1.replace("{ A B }", "{ A B C }")
_ASP_INC_V1 = 'namespace com.linkedin.test\n@Aspect = { "name": "asp", "schemaVersion": 1 }\nrecord Asp includes Base {\n  b: string\n}\n'
_ASP_INC_V2 = _ASP_INC_V1.replace('"schemaVersion": 1', '"schemaVersion": 2')


class TestIncludedRecordChanges:
    def _run(self, files, changed):
        with _mock_repo(files, (_P + "Base.pdl", _P + "Asp.pdl")):
            findings = []
            for p in changed:
                findings += pdl_rules.classify_pdl_for_rollback(p, "N", "N-1")
            return findings + pdl_rules.analyze_nested_changes("N", "N-1", changed)

    def test_enum_added_in_included_record_is_reported_when_aspect_also_changed(self):
        files = {("N", _P + "Base.pdl"): _BASE_ENUM_V2, ("N-1", _P + "Base.pdl"): _BASE_ENUM_V1,
                 ("N", _P + "Asp.pdl"): _ASP_INC_V2, ("N-1", _P + "Asp.pdl"): _ASP_INC_V1}
        findings = self._run(files, [_P + "Base.pdl", _P + "Asp.pdl"])
        enum = [f for f in findings if f.summary.startswith("Enum `Status`: added value `C`")]
        assert len(enum) == 1 and enum[0].affected_aspects == ["asp"]

    def test_unreadable_included_file_is_reported_when_aspect_also_changed(self):
        bad = "namespace com.linkedin.test\nrecord Base {\n  status: string\n"
        files = {("N", _P + "Base.pdl"): bad + "  extra: string\n", ("N-1", _P + "Base.pdl"): bad,
                 ("N", _P + "Asp.pdl"): _ASP_INC_V2, ("N-1", _P + "Asp.pdl"): _ASP_INC_V1}
        findings = self._run(files, [_P + "Base.pdl", _P + "Asp.pdl"])
        unread = [f for f in findings if "couldn't be analysed" in f.summary]
        assert len(unread) == 1 and unread[0].affected_aspects == ["asp"]
        assert unread[0].risk == model.REQUIRES_ATTENTION

    def test_typeref_change_in_included_file_is_reported_when_aspect_also_changed(self):
        u1 = "namespace com.linkedin.test\ntyperef Base = union[string, int]\n"
        files = {("N", _P + "Base.pdl"): u1.replace("int]", "int, long]"), ("N-1", _P + "Base.pdl"): u1,
                 ("N", _P + "Asp.pdl"): _ASP_INC_V2, ("N-1", _P + "Asp.pdl"): _ASP_INC_V1}
        findings = self._run(files, [_P + "Base.pdl", _P + "Asp.pdl"])
        assert any(f.summary.startswith("Union `Base`: added member `long`") and f.affected_aspects == ["asp"]
                   for f in findings)


class TestCachedReader:
    def test_each_file_is_read_once_per_run(self):
        calls = []
        with patch.object(rac, "file_at", lambda ref, path: calls.append((ref, path)) or "x"):
            read = repo.cached_reader()
            read("N", "a.pdl"), read("N", "a.pdl"), read("N-1", "a.pdl")
        assert calls == [("N", "a.pdl"), ("N-1", "a.pdl")]


class TestDiffFieldsRequiresDefaultFlag:
    def test_missing_has_default_is_an_error_not_a_silent_no(self):
        removed = {"x": {"optional": False, "type": "string", "annotations": {}}}
        with pytest.raises(KeyError):
            pdl_rules.diff_fields({}, removed, {}, {}, None, "p", "a", None, None)


class TestTyperefs:
    def test_split_typerefs_leaves_records_parseable(self):
        pdl = 'namespace a\n/** doc */\ntyperef U = union[\n  string,\n  @x = "y"\n  int\n]\nrecord R { a: string }\nfixed MD5 16\n'
        typerefs, fixed, rest = pdl_parser.split_typerefs(pdl)
        assert pdl_parser.union_members(typerefs["U"]) == {"string", "int"}
        assert fixed == {"MD5": 16}
        assert set(bsv.parse_top_level_defs(rest)) == {"R"}

    def test_union_members_without_commas(self):
        # PDL commas are optional; named members are often newline-separated.
        assert pdl_parser.union_members("union[\n  costId: double\n  costCode: string\n]") == {
            "costId: double", "costCode: string"}
        assert pdl_parser.union_members("union[array[string] map[string, int] long]") == {
            "array[string]", "map[string, int]", "long"}

    def test_inline_record_with_includes_is_one_union_member(self):
        assert pdl_parser.union_members("union[a: record R includes Base { x: int } b: long]") == {
            "a: record R includes Base { x: int }", "b: long"}

    def test_reordered_union_is_not_a_change(self):
        old = "typeref U = union[\n a: int\n b: long\n]"
        new = "typeref U = union[\n b: long\n a: int\n]"
        assert pdl_rules.typeref_findings(new, old, "p", None, None) == []

    def test_typeref_text_inside_a_string_is_ignored(self):
        old = 'namespace a\n@doc = "typeref Display = long"\nrecord R { x: int }\n'
        new = 'namespace a\n@doc = "typeref Display = string"\nrecord R { x: int }\n'
        assert pdl_rules.typeref_findings(new, old, "p", None, None) == []

    def test_removed_union_member_is_not_reported(self):
        assert pdl_rules.typeref_findings("typeref U = union[string]", "typeref U = union[string, int]", "p", None, None) == []

    def test_typeref_target_and_fixed_size_changes(self):
        summaries = [f.summary for f in pdl_rules.typeref_findings(
            "typeref T = long\nfixed H 32", "typeref T = string\nfixed H 16", "p", None, None)]
        assert summaries == ["Type change on `T`: `string`→`long`", "Fixed `H`: size 16→32"]


class TestUnknownImpactCarriesThrough:
    def test_worst_prefers_unknown_over_any_known_value(self):
        assert model.worst(["ok", "not analysed"], model.READ_SEVERITY) == "not analysed"
        assert model.worst(["ok"], model.READ_SEVERITY) == "ok"

    def test_mutator_with_unanalysed_change_is_not_reported_as_fine(self):
        mutator = _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION, aspect_name="a")
        unparsed = _finding(summary="`U` changed but couldn't be analysed", affected_aspects=["a"],
                            **model.impact("not analysed", "not analysed", "not analysed"))
        pipeline.set_mutator_impact([mutator, unparsed])
        assert mutator.read_impact == "not analysed"
        assert "couldn't be analysed" in mutator.detail and "fine" not in mutator.detail

    def test_mutator_on_aspect_with_unexplained_version_gap(self):
        mutator = _finding(dimension=model.DIM_MUTATOR, risk=model.REQUIRES_ATTENTION, aspect_name="a")
        gap = _finding(dimension=model.DIM_SCHEMA_VERSION, aspect_name="a",
                       summary="Schema version gap: v1→v2 (1 hop)")
        pipeline.combine_findings([mutator, gap], "N", "N-1", [])
        assert gap.read_impact == "not analysed"
        assert mutator.read_impact == "not analysed"
        assert "schema version gap: v1→v2" in mutator.detail


class TestMainRecord:
    def test_record_is_read_even_when_file_has_typeref(self):
        pdl = "namespace a\ntyperef T = string\nrecord R includes Base {\n  x: T\n}\n"
        rdef = pdl_parser.main_record(pdl)
        assert rdef is not None and rdef["includes"] == {"Base"} and set(rdef["fields"]) == {"x"}


def _rel(cur, tgt):
    origin = model.Origin("p", "a", None, None)
    return pdl_rules._relationship_findings(
        origin, "f", pdl_parser.normalized_annotation(cur), pdl_parser.normalized_annotation(tgt), None
    )


class TestRelationshipRules:
    def test_gained_target_type_is_expected_loss_and_writes_fail(self):
        [f] = _rel('{ "name": "On", "entityTypes": [ "dataset", "chart" ] }',
                        '{ "name": "On", "entityTypes": [ "dataset" ] }')
        assert f.risk == model.EXPECTED_LOSS and f.write_impact == model.FAILS
        assert "`chart`" in f.summary and "restore-indices doesn't remove" in f.detail

    def test_gained_type_with_renamed_relationship(self):
        risks = {f.risk for f in _rel('{ "name": "OnV2", "entityTypes": [ "dataset", "chart" ] }',
                                           '{ "name": "On", "entityTypes": [ "dataset" ] }')}
        assert risks == {model.EXPECTED_LOSS, model.SAFE}

    def test_relationship_added_in_n_leaves_extra_edges(self):
        [f] = _rel('{ "name": "On", "entityTypes": [ "dataset" ] }', None)
        assert f.risk == model.REQUIRES_ATTENTION and "never updates or removes them" in f.detail
        assert (f.relationship, f.rel_change) == ("On", "added")

    def test_empty_path_spec_does_not_crash(self):
        kinds = {f.rel_change for f in _rel('{ "/*": { "name": "On", "entityTypes": [ "dataset" ] } }', '{ "/x": {} }')}
        assert kinds == {"added", "removed"}

    def test_non_dict_path_setting_is_unreadable(self):
        assert pdl_parser.annotation_specs('{"/*": "OwnedBy"}') is None

    def test_relationship_removed_in_n_is_rebuilt_by_restore_indices(self):
        [f] = _rel(None, '{ "name": "On", "entityTypes": [ "dataset" ] }')
        assert f.risk == model.SAFE and pdl_rules.RESTORE_REBUILDS in f.detail


class TestCommitLinks:
    def test_findings_link_to_first_parent_commits_of_this_repo(self):
        f = _finding(path="x.pdl", pr_number="19367")
        log = "aaaaaaaaaaaa1\tai-auto-merge: upstream → acryl-main (#11686)\nbbbbbbbbbbbb2\tdirect fix\n"
        with patch.object(repo, "git", return_value=log) as git:
            pipeline.attach_commits([f], "N", "N-1", "https://github.com/o/r")
        assert "--first-parent" in git.call_args.args
        assert f.pr_number == "11686"
        cell = report._format_changes(f)
        assert cell == "[#11686](https://github.com/o/r/pull/11686), [`bbbbbbbbbb`](https://github.com/o/r/commit/bbbbbbbbbbbb2)"

    def test_no_commits_keeps_existing_pr(self):
        f = _finding(path="x.pdl", pr_number="7")
        with patch.object(repo, "git", side_effect=subprocess.CalledProcessError(1, "git")):
            pipeline.attach_commits([f], "N", "N-1", None)
        assert report._format_changes(f) == "#7"

    def test_repo_url_from_actions_env_or_remote(self, monkeypatch):
        monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
        monkeypatch.setenv("GITHUB_REPOSITORY", "acme/fork")
        assert repo.repo_url() == "https://github.com/acme/fork"
        monkeypatch.delenv("GITHUB_SERVER_URL")
        for remote in ("git@github.com:acme/fork.git", "https://token@github.com/acme/fork.git",
                       "ssh://git@github.com:22/acme/fork.git", "git://github.com/acme/fork.git"):
            with patch.object(repo, "git", return_value=remote + "\n"):
                assert repo.repo_url() == "https://github.com/acme/fork"


class TestVerdictDetails:
    def test_one_line_per_risk_level_with_its_action(self):
        findings = [
            _finding(risk=model.REQUIRES_ATTENTION, dimension=model.DIM_UPGRADE_STEP),
            _finding(risk=model.REQUIRES_ATTENTION),
            _finding(risk=model.EXPECTED_LOSS),
            _finding(risk=model.SAFE),
        ]
        lines = report._verdict_details(findings)
        assert lines[0].startswith("- **2 need a decision:** 1 schema change, 1 upgrade step.")
        assert lines[1].startswith("- **1 expected loss:**")
        assert lines[2].startswith("- **1 safe:**")
        assert model.compute_verdict(findings) == model.VERDICT_MANUAL


class TestRequiredFieldRemediation:
    def _field(self, optional):
        return {"type": "string", "optional": optional, "annotations": {}, "has_default": False, "via": None}

    def test_blocker_and_flip_both_say_delete_then_reemit(self):
        removed = pdl_rules.diff_fields({}, {"f": self._field(False)}, {}, {}, None, "p", "a", None, None)
        flipped = pdl_rules.diff_fields({"f": self._field(True)}, {"f": self._field(False)}, {}, {}, None, "p", "a", None, None)
        assert [f.risk for f in removed] == [model.BLOCKS_ROLLBACK]
        assert [f.risk for f in flipped] == [model.REQUIRES_ATTENTION]
        for f in removed + flipped:
            assert "delete this aspect" in f.detail and "re-emit" in f.detail


_REGISTRY_N1 = """entities:
  - name: dataset
    keyAspect: datasetKey
    aspects:
      - datasetProperties
events:
  - name: entityChangeEvent
"""
_REGISTRY_N = _REGISTRY_N1.replace("events:", """  - name: feedback
    keyAspect: feedbackKey
    aspects:
      - feedbackInfo
events:""").replace("      - datasetProperties", "      - datasetProperties\n      - newDatasetAspect")
_REG = "metadata-models/src/main/resources/entity-registry.yml"


class TestNewEntityTypes:
    def test_registry_reads_only_the_entities_section(self):
        reg = pdl_parser.entity_registry(_REGISTRY_N)
        assert reg == {"dataset": {"datasetKey", "datasetProperties", "newDatasetAspect"},
                       "feedback": {"feedbackKey", "feedbackInfo"}}

    def _new_aspect(self, name):
        pdl = f'namespace com.linkedin.test\n@Aspect = {{ "name": "{name}" }}\nrecord R {{\n  a: string\n}}\n'
        files = {("N", "a.pdl"): pdl, ("N", _REG): _REGISTRY_N, ("N-1", _REG): _REGISTRY_N1}
        with _mock_repo(files):
            [f] = pdl_rules.classify_pdl_for_rollback("a.pdl", "N", "N-1")
        return f

    def test_aspect_of_a_new_entity_type_says_the_entity_is_new(self):
        f = self._new_aspect("feedbackInfo")
        assert f.risk == model.EXPECTED_LOSS and "`feedback`, an entity type new in N" in f.summary
        assert "can't read, write or index these entities at all" in f.detail
        assert report._removed_item(f) == "the whole `feedback` entity (new in N)"

    def test_new_aspect_of_an_existing_entity_keeps_aspect_wording(self):
        f = self._new_aspect("newDatasetAspect")
        assert "entity type new in N" not in f.summary and "reads these entities without this aspect" in f.detail
        assert "skips these rows one by one" in f.detail and f.read_impact == model.OK
        assert "only requests that name this aspect" in f.detail

    def test_aspect_on_new_and_existing_entities_says_both(self):
        reg_n = _REGISTRY_N.replace("      - newDatasetAspect", "      - newDatasetAspect\n      - feedbackInfo")
        pdl = 'namespace com.linkedin.test\n@Aspect = { "name": "feedbackInfo" }\nrecord R {\n  a: string\n}\n'
        files = {("N", "a.pdl"): pdl, ("N", _REG): reg_n, ("N-1", _REG): _REGISTRY_N1}
        with _mock_repo(files):
            [f] = pdl_rules.classify_pdl_for_rollback("a.pdl", "N", "N-1")
        assert f.read_impact == model.API_FAILS and "N-1 reads `dataset` entities" in f.detail
        assert report._removed_item(f) == "the whole aspect, and the whole `feedback` entity (new in N)"

    def test_removed_item_names_every_new_entity(self):
        f = _finding(summary="New file in N — part of `a`, `b`, an entity type new in N", subject="a, b")
        assert report._removed_item(f) == "the whole `a`, `b` entities (new in N)"


_MXE = "metadata-models/src/main/pegasus/com/linkedin/mxe/"
_EVT_TYPE_V1 = "namespace com.linkedin.mxe\nenum EvtType {\n  A\n}\n"
_EVT = "namespace com.linkedin.mxe\nrecord Evt {\n  eventType: EvtType\n}\n"


class TestEventSchemaChanges:
    def test_type_used_only_by_an_event_is_reported_for_review(self):
        t, e = _MXE + "EvtType.pdl", _MXE + "Evt.pdl"
        files = {("N", t): _EVT_TYPE_V1.replace("A\n", "A\n  B\n"), ("N-1", t): _EVT_TYPE_V1,
                 ("N", e): _EVT, ("N-1", e): _EVT}
        with _mock_repo(files, (t, e)):
            [f] = pdl_rules.analyze_nested_changes("N", "N-1", [t])
        assert f.dimension == model.DIM_EVENT_SCHEMA and f.risk == model.REQUIRES_ATTENTION
        assert f.summary.startswith("Enum `EvtType`: added value `B`")
        assert f.aspect_name == "Evt (event)" and f.read_impact == model.NOT_ANALYSED
        assert "Kafka event `Evt`" in f.detail

    def test_type_also_used_by_an_aspect_stays_an_aspect_change(self):
        t, e, asp = _MXE + "EvtType.pdl", _MXE + "Evt.pdl", _P + "UsesEvt.pdl"
        aspect = 'namespace com.linkedin.test\nimport com.linkedin.mxe.EvtType\n@Aspect = { "name": "usesEvt" }\nrecord UsesEvt {\n  t: EvtType\n}\n'
        files = {("N", t): _EVT_TYPE_V1.replace("A\n", "A\n  B\n"), ("N-1", t): _EVT_TYPE_V1,
                 ("N", e): _EVT, ("N", asp): aspect}
        with _mock_repo(files, (t, e, asp)):
            [f] = pdl_rules.analyze_nested_changes("N", "N-1", [t])
        assert f.dimension == model.DIM_PDL_SCHEMA and f.aspect_name == "usesEvt"


class TestRelationshipKinds:
    def test_only_islineage_changed_is_safe(self):
        [f] = _rel('{ "name": "ModeledBy", "entityTypes": [ "semanticModel" ] }',
                        '{ "name": "ModeledBy", "entityTypes": [ "semanticModel" ], "isLineage": true }')
        assert f.risk == model.SAFE and "`isLineage`" in f.summary

    def test_narrowed_target_types_are_safe(self):
        [f] = _rel('{ "name": "On", "entityTypes": [ "dataset" ] }',
                        '{ "name": "On", "entityTypes": [ "dataset", "chart" ] }')
        assert f.risk == model.SAFE

    def test_path_keyed_relationship_removed(self):
        [f] = _rel(None, '{ "/*": { "name": "Contains", "entityTypes": [ "dataset" ], "isLineage": true } }')
        assert f.rel_change == "removed" and f.relationship == "Contains"
        assert f.risk == model.SAFE and pdl_rules.RESTORE_REBUILDS in f.detail

    def test_renamed_keeps_old_name_for_restore(self):
        [f] = _rel('{ "name": "DownstreamOfV2" }', '{ "name": "DownstreamOf" }')
        assert (f.rel_change, f.relationship, f.rel_new) == ("renamed", "DownstreamOf", "DownstreamOfV2")
        assert f.risk == model.SAFE


_GROUP_REGISTRY = """entities:
  - name: corpGroup
    keyAspect: corpGroupKey
    aspects:
      - corpGroupInfo
      - ownership
"""
_PL = "metadata-models/src/main/pegasus/com/linkedin/x/"
_GROUP_INFO = ('namespace com.linkedin.x\n@Aspect = { "name": "corpGroupInfo" }\nrecord CorpGroupInfo {\n'
               '  @Relationship = { "/*": { "name": "OwnedBy", "entityTypes": [ "corpuser" ] } }\n  admins: array[string]\n}\n')
_OWNERSHIP = 'namespace com.linkedin.x\n@Aspect = { "name": "ownership" }\nrecord Ownership {\n  owners: array[Owner]\n}\n'
_OWNER = ('namespace com.linkedin.x\nrecord Owner {\n  @Relationship = { "name": "OwnedBy", "entityTypes": [ "corpuser" ] }\n'
          '  owner: string\n}\n')


def _refine(f, pdls):
    with patch.object(rac, "file_at", lambda ref, path: _GROUP_REGISTRY if path.endswith("entity-registry.yml") else ""), \
         patch.object(pdl_rules, "_all_pdls_at", return_value=pdls):
        pdl_rules.refine_relationship_findings([f], "N-1")
    return f


class TestRestoreIndicesForSharedRelationships:

    def _removed(self):
        return pdl_rules._relationship_removed(model.Origin("p", "corpGroupInfo", None, None), "admins", "OwnedBy", None)

    def test_restore_does_not_rebuild_when_another_aspect_builds_the_same_edges(self):
        f = _refine(self._removed(), {_PL + "CorpGroupInfo.pdl": _GROUP_INFO,
                                           _PL + "Ownership.pdl": _OWNERSHIP, _PL + "Owner.pdl": _OWNER})
        assert f.risk == model.REQUIRES_ATTENTION and "`ownership`" in f.detail
        assert (f.read_impact, f.data_loss) == (model.STALE, model.LOSS_GRAPH_ONLY)
        assert "the last one replacing the others" in f.detail

    def test_restore_rebuilds_when_no_other_aspect_builds_them(self):
        f = _refine(self._removed(), {_PL + "CorpGroupInfo.pdl": _GROUP_INFO,
                                           _PL + "Ownership.pdl": _OWNERSHIP.replace("Owner]", "string]")})
        assert pdl_rules.RESTORE_REBUILDS in f.detail

    def test_added_relationship_known_to_n1_shows_extra_edges(self):
        f = pdl_rules._relationship_added(model.Origin("p", "corpGroupInfo", None, None), "leads", "OwnedBy", None)
        _refine(f, {_PL + "Ownership.pdl": _OWNERSHIP, _PL + "Owner.pdl": _OWNER})
        assert f.risk == model.REQUIRES_ATTENTION and f.read_impact == model.STALE and "extra edges" in f.detail

    def test_added_relationship_unknown_to_n1_is_safe(self):
        f = pdl_rules._relationship_added(model.Origin("p", "corpGroupInfo", None, None), "leads", "LedBy", None)
        _refine(f, {_PL + "Ownership.pdl": _OWNERSHIP, _PL + "Owner.pdl": _OWNER})
        assert f.risk == model.SAFE and "views don't show them" in f.detail


class TestSearchKinds:
    def _search(self, cur, tgt):
        origin = model.Origin("p", "a", None, None)
        def norm(v):
            return {"Searchable": pdl_parser.normalized_annotation(v)} if v else {}

        return pdl_rules._search_findings(origin, "f", norm(cur), norm(tgt), None)

    def test_added_is_safe(self):
        [f] = self._search('{ "fieldType": "TEXT" }', None)
        assert f.risk == model.SAFE

    def test_removed_leaves_search_documents_without_the_field(self):
        [f] = self._search(None, '{ "/*": { "fieldName": "datasets", "fieldType": "URN" } }')
        assert f.risk == model.SAFE and "restore-indices rebuilds it" in f.detail
        assert not f.reindex_required

    def test_query_only_settings_are_safe(self):
        [f] = self._search('{ "fieldType": "TEXT", "boostScore": 2.0 }', '{ "fieldType": "TEXT", "boostScore": 1.0 }')
        assert f.risk == model.SAFE and "`boostScore`" in f.summary

    def test_autocomplete_only_is_query_time(self):
        [f] = self._search('{ "fieldType": "TEXT", "enableAutocomplete": true }', '{ "fieldType": "TEXT" }')
        assert f.risk == model.SAFE and not f.reindex_required

    def test_mapping_change_keeps_n_mapping_after_rollback(self):
        [f] = self._search('{ "fieldType": "KEYWORD" }', '{ "fieldType": "TEXT" }')
        assert f.risk == model.SAFE and f.reindex_required and "new DATAHUB_REVISION" in f.detail


class TestSearchDefaults:
    def _search(self, cur, tgt, field_type="string"):
        origin = model.Origin("p", "a", None, None)
        return pdl_rules._search_findings(
            origin, "type", {"Searchable": pdl_parser.normalized_annotation(cur)},
            {"Searchable": pdl_parser.normalized_annotation(tgt)}, None, field_type, field_type)

    def test_spelling_out_the_default_is_not_a_change(self):
        assert self._search('{ "fieldName": "type", "fieldType": "TEXT" }', "{}") == []
        assert self._search('{ "fieldType": "COUNT" }', "{}", "int") == []

    def test_explicit_type_differing_from_the_default_is_a_mapping_change(self):
        [f] = self._search('{ "fieldType": "KEYWORD" }', "{}")
        assert f.summary.startswith("Search mapping changed") and f.reindex_required

    def test_renamed_search_field_names_both(self):
        [f] = self._search('{ "fieldName": "fieldAssertionType", "fieldType": "KEYWORD" }', "{}")
        assert f.summary == "Search field renamed on `type`: `type` → `fieldAssertionType`"
        assert f.risk == model.SAFE



class TestRefineScope:
    def test_event_rows_keep_their_classification(self):
        f = pdl_rules._relationship_added(model.Origin("p", "corpGroupInfo", None, None), "admins", "OwnedBy", None)
        f.dimension, f.risk, f.aspect_name = model.DIM_EVENT_SCHEMA, model.REQUIRES_ATTENTION, "Evt (event)"
        _refine(f, {_PL + "CorpGroupInfo.pdl": _GROUP_INFO})
        assert f.risk == model.REQUIRES_ATTENTION

    def test_embedded_aspect_is_not_its_own_other_builder(self):
        # ownership embeds corpGroupInfo; nothing else builds OwnedBy.
        f = pdl_rules._relationship_removed(model.Origin("p", "corpGroupInfo", None, None), "admins", "OwnedBy", None)
        f.affected_aspects = ["ownership"]
        assert _refine(f, {_PL + "CorpGroupInfo.pdl": _GROUP_INFO}).risk == model.SAFE

    def test_embedded_aspect_still_checks_its_own_entity(self):
        f = pdl_rules._relationship_removed(model.Origin("p", "corpGroupInfo", None, None), "admins", "OwnedBy", None)
        f.affected_aspects = ["someEmbedder"]
        _refine(f, {_PL + "CorpGroupInfo.pdl": _GROUP_INFO, _PL + "Ownership.pdl": _OWNERSHIP, _PL + "Owner.pdl": _OWNER})
        assert f.risk == model.REQUIRES_ATTENTION and "`ownership`" in f.detail


class TestRolledBackRenamedRelationship:
    def test_new_name_n1_builds_elsewhere_shows_extra_edges(self):
        f = pdl_rules._relationship_renamed(model.Origin("p", "corpGroupInfo", None, None), "admins", "AdminOf", "OwnedBy", None)
        _refine(f, {_PL + "Ownership.pdl": _OWNERSHIP, _PL + "Owner.pdl": _OWNER})
        assert f.risk == model.REQUIRES_ATTENTION and "extra edges" in f.detail



class TestN1Summary:
    def test_summary_matches_the_rows(self):
        findings = [
            _finding(risk=model.EXPECTED_LOSS, aspect_name="docInfo", summary="Enum `T`: added value `X` — N-1 doesn't know it",
                     **model.impact("UI/API fails", "fails", "no")),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="incidentInfo", affected_aspects=["incidentActivityEvent"],
                     summary="Graph relationship on `entities` gained target types `x`", **model.impact("ok", "fails", "no")),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="a", summary="Added field `f` — N-1 ignores unknown fields",
                     **model.impact("ok", model.DROPS_NEW_FIELD, "no")),
            _finding(risk=model.EXPECTED_LOSS, aspect_name="feedbackInfo", subject="feedback",
                     summary="New file in N — part of `feedback`, an entity type new in N", **model.impact("API fails", "fails", "no")),
        ]
        text = "\n".join(report._n1_summary(findings))
        assert "- **Read:** works, except entities whose records use N's new values or lack a field N-1 requires (`docInfo`) and the new entity type `feedback`." in text
        assert "`incidentInfo`, `incidentActivityEvent`" in text and "`feedbackInfo`" not in text.split("Write")[1].split("Data loss")[0]
        assert "N's new field is dropped when N-1 saves those records. N's new aspects and entity types stay in the database." in text


    def test_graph_only_and_not_analysed_lines(self):
        findings = [
            _finding(risk=model.REQUIRES_ATTENTION, aspect_name="corpGroupInfo",
                     summary="Graph relationship `OwnedBy` removed from `admins`", **model.impact("ok, stale", "ok", "graph only")),
            _finding(risk=model.REQUIRES_ATTENTION, dimension=model.DIM_EVENT_SCHEMA, aspect_name="Evt (event)",
                     summary="Enum `E`: added value `X`", **model.impact("not analysed", "not analysed", "not analysed")),
        ]
        text = "\n".join(report._n1_summary(findings))
        assert "some graph edges from `corpGroupInfo` are missing until N-1 saves those records" in text
        assert "- **Not analysed:** 1 item" in text


class TestSchemaDiff:
    def test_single_aspect_labels(self):
        assert model.single_aspect("moreInfo") == "moreInfo"
        assert model.single_aspect("a, b, c +2 more") is None
        assert model.single_aspect("Evt (event)") is None

    def test_lists_only_differences(self):
        reg_n1 = "entities:\n  - name: dataset\n    keyAspect: datasetKey\n    aspects:\n      - status\n"
        reg_n = (reg_n1 + "      - forms\n      - dataProducts\n"
                 "  - name: feedback\n    keyAspect: feedbackKey\n    aspects:\n      - feedbackInfo\n")
        def aspect(name, version=1):
            return f'namespace a\n@Aspect = {{ "name": "{name}", "schemaVersion": {version} }}\nrecord R {{\n  x: string\n}}\n'
        n1 = {"s.pdl": aspect("status"), "f.pdl": aspect("forms"), "old.pdl": aspect("oldAspect")}
        n = {"s.pdl": aspect("status", 2), "f.pdl": aspect("forms"), "d.pdl": aspect("dataProducts"),
             "fi.pdl": aspect("feedbackInfo"), "fk.pdl": aspect("feedbackKey")}
        changed = _finding(aspect_name="status", summary="Added field `x` — N-1 ignores unknown fields",
                           risk=model.EXPECTED_LOSS)
        with patch.object(rac, "file_at", lambda ref, path: reg_n if ref == "N" else reg_n1), \
             patch.object(pdl_rules, "_all_pdls_at", lambda ref: n if ref == "N" else n1):
            diff = pdl_rules.schema_diff("N", "N-1", [changed])
        assert [e["entity"] for e in diff["entities_added"]] == ["feedback"]
        assert diff["entities_added"][0]["key_aspect"] == "feedbackKey"
        assert [a["aspect"] for a in diff["aspects_added"]] == ["dataProducts", "feedbackInfo", "feedbackKey"]
        assert diff["aspects_removed"] == ["oldAspect"]
        assert diff["entity_aspects_changed"] == [{"entity": "dataset", "aspects_added": ["forms"], "aspects_removed": []}]
        assert diff["aspects_changed"] == [{"aspect": "status", "schema_version": [1, 2],
                                            "changes": ["added field `x`"], "worst_risk": "expected_loss"}]
        md = "\n".join(report._render_schema_diff(diff))
        assert "### Entities only in N (1): added in N, missing in N-1" in md
        assert "| `feedback` | ❌ missing | ✅ added | `feedbackKey` |" in md
        assert "| `status` | v1 | v2 | added field `x` | expected_loss |" in md

    def test_nested_field_changes_are_shortened(self):
        assert report._short_change("in `Rec`: Added field `f` (via includes `X`)") == "added field `Rec.f`"
        assert report._short_change("enum `E`: added value `V`") == "enum `E`: added value `V`"
