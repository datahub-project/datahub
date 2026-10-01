"""Tests for rollback_analysis.py"""

import json
import sys
from pathlib import Path
from unittest.mock import patch


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import rollback_analysis as ra

# ---------------------------------------------------------------------------
# RollbackFinding construction helpers
# ---------------------------------------------------------------------------


def _finding(
    dimension=ra.DIM_PDL_SCHEMA,
    risk=ra.SAFE,
    path="test.pdl",
    aspect_name=None,
    summary="test finding",
    **kwargs,
):
    return ra.RollbackFinding(
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
        assert ra.compute_verdict([]) == ra.VERDICT_FEASIBLE

    def test_all_safe_is_feasible(self):
        findings = [_finding(risk=ra.SAFE) for _ in range(3)]
        assert ra.compute_verdict(findings) == ra.VERDICT_FEASIBLE

    def test_attention_without_blockers_is_manual(self):
        findings = [
            _finding(risk=ra.SAFE),
            _finding(risk=ra.REQUIRES_ATTENTION),
        ]
        assert ra.compute_verdict(findings) == ra.VERDICT_MANUAL

    def test_any_blocker_is_not_recommended(self):
        findings = [
            _finding(risk=ra.SAFE),
            _finding(risk=ra.REQUIRES_ATTENTION),
            _finding(risk=ra.BLOCKS_ROLLBACK),
        ]
        assert ra.compute_verdict(findings) == ra.VERDICT_NOT_RECOMMENDED

    def test_blocker_alone_is_not_recommended(self):
        findings = [_finding(risk=ra.BLOCKS_ROLLBACK)]
        assert ra.compute_verdict(findings) == ra.VERDICT_NOT_RECOMMENDED


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
    def test_new_file_in_n_is_safe(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == ra.SAFE
        assert "New file" in findings[0].summary

    def test_deleted_file_in_n_requires_attention(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == ra.REQUIRES_ATTENTION
        assert "deleted" in findings[0].summary.lower()

    def test_added_optional_field_is_safe(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_ADDED_OPTIONAL,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=["123"]), \
             patch.object(ra.rac, "last_author_for_file", return_value="dev"):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        safe = [f for f in findings if f.risk == ra.SAFE]
        assert any("baz" in f.summary for f in safe)

    def test_added_required_field_is_safe(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_ADDED_REQUIRED,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        safe = [f for f in findings if f.risk == ra.SAFE]
        assert any("required_field" in f.summary for f in safe)

    def test_removed_field_requires_attention_and_reindex(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_REMOVED_FIELD,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        attention = [f for f in findings if f.risk == ra.REQUIRES_ATTENTION]
        removed = [f for f in attention if "bar" in f.summary]
        assert len(removed) == 1
        assert removed[0].reindex_required is True

    def test_type_change_requires_attention_and_reindex(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_TYPE_CHANGE,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        attention = [f for f in findings if f.risk == ra.REQUIRES_ATTENTION]
        changed = [f for f in attention if "bar" in f.summary]
        assert len(changed) == 1
        assert changed[0].reindex_required is True

    def test_optional_to_required_requires_attention(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1_OPT_TO_REQ,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        attention = [f for f in findings if f.risk == ra.REQUIRES_ATTENTION]
        assert any("foo" in f.summary for f in attention)

    def test_required_to_optional_is_safe(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
            ("N-1", "test.pdl"): _ASPECT_V1_OPT_TO_REQ,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        safe = [f for f in findings if f.risk == ra.SAFE]
        assert any("foo" in f.summary for f in safe)

    def test_both_empty_returns_nothing(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({})), \
             patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
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
    def test_added_enum_value_is_safe(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_ENUM_ADDED,
            ("N-1", "test.pdl"): _ASPECT_ENUM_BASE,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        safe = [f for f in findings if f.risk == ra.SAFE]
        assert any("ARCHIVED" in f.summary for f in safe)

    def test_removed_enum_value_requires_attention(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_ENUM_REMOVED,
            ("N-1", "test.pdl"): _ASPECT_ENUM_BASE,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        attention = [f for f in findings if f.risk == ra.REQUIRES_ATTENTION]
        assert any("INACTIVE" in f.summary for f in attention)


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
    def test_rename_with_annotation_requires_attention(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _RENAMED_WITH_ANNOTATION,
            ("N-1", "test.pdl"): _ORIGINAL_RECORD,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        attention = [f for f in findings if f.risk == ra.REQUIRES_ATTENTION]
        assert any("renamed" in f.summary.lower() for f in attention)

    def test_rename_without_annotation_blocks_rollback(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _RENAMED_WITHOUT_ANNOTATION,
            ("N-1", "test.pdl"): _ORIGINAL_RECORD,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.classify_pdl_for_rollback("test.pdl", "N", "N-1")
        blockers = [f for f in findings if f.risk == ra.BLOCKS_ROLLBACK]
        assert any("renamed" in f.summary.lower() for f in blockers)


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
        with patch.object(ra.rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(ra.rac, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(ra.rac, "file_at", return_value=mutator_java):
            findings = ra.classify_mutators_for_rollback("N", "N-1")
        assert len(findings) == 1
        assert findings[0].risk == ra.REQUIRES_ATTENTION
        assert findings[0].dimension == ra.DIM_MUTATOR
        assert "MyMutator" in findings[0].summary
        assert "retention" in findings[0].summary.lower() or \
               "retention" in (findings[0].detail or "").lower()

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
        with patch.object(ra.rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(ra.rac, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(ra.rac, "file_at", return_value=mutator_java):
            findings = ra.classify_mutators_for_rollback("N", "N-1")
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
        with patch.object(ra.rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(ra.rac, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(ra.rac, "file_at", return_value=mutator_java):
            findings = ra.classify_mutators_for_rollback("N", "N-1")
        assert len(findings) == 1
        assert "100" in findings[0].pr_number
        assert "200" in findings[0].pr_number

    def test_no_mutators_yields_empty(self):
        with patch.object(ra.rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(ra.rac, "find_mutators_added_in_window",
                          return_value=[]):
            findings = ra.classify_mutators_for_rollback("N", "N-1")
        assert findings == []

    def test_mutator_with_missing_content_skipped(self):
        mock_mutators = [{
            "path": "src/Gone.java",
            "class_name": "GoneMutator",
            "target_aspect": "x",
            "pr": None,
            "author": None,
        }]
        with patch.object(ra.rac, "discover_mutator_hierarchy", return_value={}), \
             patch.object(ra.rac, "find_mutators_added_in_window",
                          return_value=mock_mutators), \
             patch.object(ra.rac, "file_at", return_value=None):
            findings = ra.classify_mutators_for_rollback("N", "N-1")
        assert findings == []


# ---------------------------------------------------------------------------
# Extract method return int
# ---------------------------------------------------------------------------


class TestExtractMethodReturnInt:
    def test_extracts_source_version(self):
        java = "public long getSourceVersion() { return 1; }"
        assert ra._extract_method_return_int(java, "getSourceVersion") == 1

    def test_extracts_target_version(self):
        java = "public long getTargetVersion() { return 2; }"
        assert ra._extract_method_return_int(java, "getTargetVersion") == 2

    def test_extracts_long_literal_with_suffix(self):
        java = "public long getSourceVersion() { return 1L; }"
        assert ra._extract_method_return_int(java, "getSourceVersion") == 1

    def test_returns_none_when_missing(self):
        java = "public String getName() { return \"test\"; }"
        assert ra._extract_method_return_int(java, "getSourceVersion") is None


# ---------------------------------------------------------------------------
# Schema version gap analysis
# ---------------------------------------------------------------------------


class TestAnalyzeSchemaVersionGaps:
    def test_gap_detected(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V2,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })), patch.object(ra.rac, "pr_numbers_for_file", return_value=[]), \
             patch.object(ra.rac, "last_author_for_file", return_value=None):
            findings = ra.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 1
        assert findings[0].dimension == ra.DIM_SCHEMA_VERSION
        assert findings[0].risk == ra.REQUIRES_ATTENTION
        assert "v1" in findings[0].summary and "v2" in findings[0].summary

    def test_no_gap_when_same_version(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V1,
            ("N-1", "test.pdl"): _ASPECT_V1,
        })):
            findings = ra.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0

    def test_no_gap_for_non_aspect(self):
        non_aspect = "namespace com.linkedin.test\nrecord Foo { bar: int }"
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): non_aspect,
            ("N-1", "test.pdl"): non_aspect,
        })):
            findings = ra.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0

    def test_skips_when_one_side_missing(self):
        with patch.object(ra.rac, "file_at", _mock_file_at({
            ("N", "test.pdl"): _ASPECT_V2,
        })):
            findings = ra.analyze_schema_version_gaps("N", "N-1", ["test.pdl"])
        assert len(findings) == 0


# ---------------------------------------------------------------------------
# Markdown report rendering
# ---------------------------------------------------------------------------


class TestRenderRollbackReport:
    def test_empty_findings_report(self):
        md = ra.render_rollback_report(
            [], "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "Feasible as-is" in md
        assert "0 changes analyzed" in md
        assert "No schema" in md

    def test_blocker_report_has_blockers_section(self):
        findings = [
            _finding(
                risk=ra.BLOCKS_ROLLBACK,
                summary="Added required field `x`",
                aspect_name="testAspect",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "Not recommended" in md
        assert "## Blockers" in md
        assert "testAspect" in md

    def test_attention_report(self):
        findings = [
            _finding(
                risk=ra.REQUIRES_ATTENTION,
                dimension=ra.DIM_UPGRADE_STEP,
                summary="New BlockingSystemUpgrade: `MyStep`",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "manual intervention" in md
        assert "## Requires Attention" in md

    def test_safe_changes_in_details_block(self):
        findings = [_finding(risk=ra.SAFE, summary="safe change")]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "<details>" in md
        assert "Safe Changes (1)" in md

    def test_mutator_section_rendered(self):
        findings = [
            _finding(
                dimension=ra.DIM_MUTATOR,
                risk=ra.REQUIRES_ATTENTION,
                summary="New mutator `FooMutator` (v1→v2) — verify retention/replay coverage",
                aspect_name="fooAspect",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Mutators in Window" in md
        assert "FooMutator" in md
        assert "Version Hop" in md

    def test_reindex_section_rendered(self):
        findings = [
            _finding(
                dimension=ra.DIM_PDL_SCHEMA,
                risk=ra.REQUIRES_ATTENTION,
                summary="Type change on `name`: `string`→`int`",
                detail="Type change requires reindex after rollback",
                reindex_required=True,
                aspect_name="datasetProperties",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Reindex Triggers" in md
        assert "datasetProperties" in md
        assert "Reason" in md

    def test_version_section_rendered(self):
        findings = [
            _finding(
                dimension=ra.DIM_SCHEMA_VERSION,
                risk=ra.REQUIRES_ATTENTION,
                summary="Schema version gap: v1→v2 (1 hop)",
                aspect_name="testAspect",
                detail="N-1 expects version 1; N writes version 2.",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Schema Version Gaps" in md

    def test_pr_number_in_table(self):
        findings = [
            _finding(
                risk=ra.BLOCKS_ROLLBACK,
                pr_number="456",
                summary="test",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "#456" in md


# ---------------------------------------------------------------------------
# JSON report rendering
# ---------------------------------------------------------------------------


class TestRenderJsonReport:
    def test_valid_json(self):
        findings = [
            _finding(risk=ra.SAFE),
            _finding(risk=ra.BLOCKS_ROLLBACK),
        ]
        raw = ra.render_json_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        data = json.loads(raw)
        assert data["verdict"] == ra.VERDICT_NOT_RECOMMENDED
        assert data["summary"]["total"] == 2
        assert data["summary"]["safe"] == 1
        assert data["summary"]["blocks_rollback"] == 1
        assert len(data["findings"]) == 2

    def test_empty_findings_json(self):
        raw = ra.render_json_report(
            [], "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        data = json.loads(raw)
        assert data["verdict"] == ra.VERDICT_FEASIBLE
        assert data["summary"]["total"] == 0


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
        m = ra._IMPLEMENTS_STEP_RE.search(java)
        assert m is not None
        assert "BlockingSystemUpgrade" in m.group(0)

    def test_implements_non_blocking_detected(self):
        java = """
        public class MyStep implements NonBlockingSystemUpgrade {
            public void execute() {}
        }
        """
        m = ra._IMPLEMENTS_STEP_RE.search(java)
        assert m is not None
        assert "NonBlockingSystemUpgrade" in m.group(0)

    def test_no_match_for_unrelated_class(self):
        java = """
        public class MyService implements SomeOtherInterface {
            public void run() {}
        }
        """
        m = ra._IMPLEMENTS_STEP_RE.search(java)
        assert m is None


# ---------------------------------------------------------------------------
# Step section rendering
# ---------------------------------------------------------------------------


class TestRenderStepSection:
    def test_blocking_step_labeled_correctly(self):
        findings = [
            _finding(
                dimension=ra.DIM_UPGRADE_STEP,
                risk=ra.REQUIRES_ATTENTION,
                summary="New BlockingSystemUpgrade: `MyStep`",
            ),
        ]
        lines = ra._render_step_section(findings)
        table = "\n".join(lines)
        assert "| Blocking |" in table

    def test_non_blocking_step_labeled_correctly(self):
        findings = [
            _finding(
                dimension=ra.DIM_UPGRADE_STEP,
                risk=ra.REQUIRES_ATTENTION,
                summary="New NonBlockingSystemUpgrade: `MyAsyncStep`",
            ),
        ]
        lines = ra._render_step_section(findings)
        table = "\n".join(lines)
        assert "| Non-blocking |" in table
        assert "MyAsyncStep" in table


# ---------------------------------------------------------------------------
# Summary line grammar
# ---------------------------------------------------------------------------


class TestSummaryGrammar:
    def test_singular_blocker_uses_blocks(self):
        findings = [_finding(risk=ra.BLOCKS_ROLLBACK)]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "1 blocks rollback" in md

    def test_plural_blockers_uses_block(self):
        findings = [
            _finding(risk=ra.BLOCKS_ROLLBACK, summary="a"),
            _finding(risk=ra.BLOCKS_ROLLBACK, summary="b"),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "2 block rollback" in md


# ---------------------------------------------------------------------------
# Mixed-dimension integration
# ---------------------------------------------------------------------------


class TestMixedFindings:
    def test_mixed_verdict_picks_worst(self):
        findings = [
            _finding(dimension=ra.DIM_PDL_SCHEMA, risk=ra.SAFE),
            _finding(dimension=ra.DIM_REINDEX, risk=ra.REQUIRES_ATTENTION),
            _finding(dimension=ra.DIM_MUTATOR, risk=ra.BLOCKS_ROLLBACK),
        ]
        assert ra.compute_verdict(findings) == ra.VERDICT_NOT_RECOMMENDED

    def test_mutator_attention_with_pdl_blocker(self):
        findings = [
            _finding(dimension=ra.DIM_PDL_SCHEMA, risk=ra.BLOCKS_ROLLBACK),
            _finding(dimension=ra.DIM_MUTATOR, risk=ra.REQUIRES_ATTENTION),
        ]
        assert ra.compute_verdict(findings) == ra.VERDICT_NOT_RECOMMENDED

    def test_mutator_attention_only_is_manual(self):
        findings = [
            _finding(dimension=ra.DIM_MUTATOR, risk=ra.REQUIRES_ATTENTION),
        ]
        assert ra.compute_verdict(findings) == ra.VERDICT_MANUAL

    def test_report_has_all_sections(self):
        findings = [
            _finding(dimension=ra.DIM_PDL_SCHEMA, risk=ra.SAFE, summary="safe"),
            _finding(
                dimension=ra.DIM_MUTATOR,
                risk=ra.REQUIRES_ATTENTION,
                summary="New mutator `X` (v1→v2) — verify retention/replay coverage",
                aspect_name="a",
            ),
            _finding(
                dimension=ra.DIM_UPGRADE_STEP,
                risk=ra.REQUIRES_ATTENTION,
                summary="New BlockingSystemUpgrade: `Y`",
            ),
            _finding(
                dimension=ra.DIM_PDL_SCHEMA,
                risk=ra.REQUIRES_ATTENTION,
                summary="Removed field `z` — N-1 expects it",
                detail="Field deletion requires reindex after rollback",
                reindex_required=True,
                aspect_name="b",
            ),
            _finding(
                dimension=ra.DIM_SCHEMA_VERSION,
                risk=ra.REQUIRES_ATTENTION,
                summary="Schema version gap: v1→v2 (1 hop)",
                aspect_name="c",
                detail="detail",
            ),
        ]
        md = ra.render_rollback_report(
            findings, "v2.0", "v1.0", "abc1234567", "def1234567"
        )
        assert "## Requires Attention" in md
        assert "<details>" in md
        assert "## Mutators in Window" in md
        assert "## Upgrade Steps in Window" in md
        assert "## Reindex Triggers" in md
        assert "## Schema Version Gaps" in md


class TestMainTargetDefault:
    def test_target_defaults_to_latest_release(self, tmp_path):
        out = tmp_path / "report.md"
        with patch.object(
            ra.rac, "resolve_base", return_value="v1.2.0"
        ) as resolve, patch.object(
            ra, "run", return_value=([], "abc1234567", "def1234567")
        ) as run:
            ra.main(["--current", "abc123", "--output", str(out)])
        resolve.assert_called_once()
        run.assert_called_once_with("abc123", "v1.2.0")
        assert "v1.2.0" in out.read_text()

    def test_explicit_target_skips_resolution(self, tmp_path):
        out = tmp_path / "report.md"
        with patch.object(ra.rac, "resolve_base") as resolve, patch.object(
            ra, "run", return_value=([], "abc1234567", "def1234567")
        ) as run:
            ra.main(
                ["--current", "abc123", "--target", "v1.1.0", "--output", str(out)]
            )
        resolve.assert_not_called()
        run.assert_called_once_with("abc123", "v1.1.0")


class TestOrderWarning:
    def test_release_versions_in_order(self):
        assert ra.order_warning("v1.7.0.1", "v1.7.0", "a" * 10, "b" * 10) is None
        assert ra.order_warning("v2.3.0-cloud", "v2.2.3-cloud", "a" * 10, "b" * 10) is None
        assert ra.order_warning("releases/v1.8.0", "v1.7.0.1", "a" * 10, "b" * 10) is None

    def test_release_versions_reversed(self):
        assert ra.order_warning("v1.6.0", "v1.7.0", "a" * 10, "b" * 10)
        assert ra.order_warning("v1.7.0rc1", "v1.7.0.1", "a" * 10, "b" * 10)

    def test_same_commit_never_warns(self):
        assert ra.order_warning("v1.6.0", "v1.7.0", "a" * 10, "a" * 10) is None

    def test_non_release_refs_fall_back_to_commit_dates(self):
        times = {"newsha": "200\n", "oldsha": "100\n"}
        with patch.object(ra.rac, "_git", side_effect=lambda *a: times[a[-1]]):
            assert ra.order_warning("master", "v1.7.0", "newsha", "oldsha") is None
            assert ra.order_warning("master", "v1.7.0", "oldsha", "newsha")

    def test_warning_is_shown_in_reports(self):
        md = ra.render_rollback_report(
            [], "v1.6.0", "v1.7.0", "abc1234567", "def1234567", "swapped"
        )
        assert "**Warning:** swapped" in md
        data = json.loads(
            ra.render_json_report(
                [], "v1.6.0", "v1.7.0", "abc1234567", "def1234567", "swapped"
            )
        )
        assert data["warning"] == "swapped"
