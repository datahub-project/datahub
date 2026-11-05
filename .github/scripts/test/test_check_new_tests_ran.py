#!/usr/bin/env python3
"""Unit tests for check_new_tests_ran (run by test-github-scripts.yml: pytest .github/scripts/test)."""

import io
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import check_new_tests_ran as cntr  # noqa: E402

TEST_DIR = "mod/src/test/java/com/x"


def junit(cases: Sequence[Tuple[str, str, bool]]) -> str:
    """JUnit XML in Gradle's shape: (classname, test name, skipped)."""
    body = "".join(
        f'<testcase name="{n}" classname="{c}" time="0.1">'
        + ("<skipped/>" if s else "")
        + "</testcase>"
        for c, n, s in cases
    )
    return f'<?xml version="1.0"?><testsuite name="s" tests="{len(cases)}">{body}</testsuite>'


def results_from(cases: Sequence[Tuple[str, str, bool]]) -> cntr.Results:
    with tempfile.TemporaryDirectory() as d:
        Path(d, "TEST-x.xml").write_text(junit(cases))
        return cntr.load_results([d])


def run_eval(
    files: Dict[str, str],
    cases: Sequence[Tuple[str, str, bool]],
    changed: Optional[List[str]] = None,
    optouts: Optional[Dict[str, str]] = None,
    languages: Sequence[str] = ("java", "python"),
) -> cntr.Report:
    read = files.get
    return cntr.evaluate(
        changed if changed is not None else list(files),
        read,
        results_from(cases),
        optouts or {},
        set(languages),
        cntr.SourceIndex(list(files), read),
    )


def java(name: str, body: str = "@Test public void t() {}", head: str = "public class") -> str:
    return (
        f"package com.x;\n\nimport org.testng.annotations.Test;\n\n{head} {name} {{\n  {body}\n}}\n"
    )


class ParseTests(unittest.TestCase):
    def test_package_and_top_level_test_class(self) -> None:
        pkg, decls = cntr.parse_jvm_types(java("FooTest"))
        self.assertEqual(pkg, "com.x")
        self.assertEqual(
            [(d.name, d.has_tests, d.abstract) for d in decls], [("FooTest", True, False)]
        )

    def test_annotation_in_comment_or_string_is_not_a_test(self) -> None:
        body = (
            "// @Test\n  /* @Test { */\n  String s = \"@Test {\";\n  char c = '{';\n  void t() {}"
        )
        _, decls = cntr.parse_jvm_types(java("Helper", body))
        self.assertEqual([(d.name, d.has_tests) for d in decls], [("Helper", False)])

    def test_text_block_braces_do_not_break_scan(self) -> None:
        body = 'String j = """\n  { "a": "}" }\n  """;\n  @Test void t() {}'
        _, decls = cntr.parse_jvm_types(java("JsonTest", body) + "\nclass Second {}\n")
        self.assertEqual(
            [(d.name, d.has_tests) for d in decls], [("JsonTest", True), ("Second", False)]
        )

    def test_lookalike_annotations_are_not_tests(self) -> None:
        body = "@TestConfiguration static class C {}\n  @SpringBootTest void x() {}\n  @Testcontainers void y() {}"
        _, decls = cntr.parse_jvm_types(java("Config", body))
        self.assertFalse(decls[0].has_tests)

    def test_junit5_and_qualified_annotations_are_tests(self) -> None:
        for ann in (
            "@ParameterizedTest",
            "@org.junit.jupiter.api.Test",
            "@RepeatedTest(3)",
            "@TestFactory",
        ):
            _, decls = cntr.parse_jvm_types(java("T", f"{ann} void t() {{}}"))
            self.assertTrue(decls[0].has_tests, ann)

    def test_class_level_testng_annotation(self) -> None:
        _, decls = cntr.parse_jvm_types(
            java("T", "public void t() {}", head='@Test(groups = "g")\npublic class')
        )
        self.assertTrue(decls[0].has_tests)

    def test_nested_types_are_not_top_level(self) -> None:
        body = "static class Inner { @Test void t() {} }\n  enum E { A }"
        _, decls = cntr.parse_jvm_types(java("Outer", body))
        self.assertEqual([d.name for d in decls], ["Outer"])
        self.assertTrue(decls[0].has_tests)

    def test_abstract_interface_and_generic_supertypes(self) -> None:
        src = (
            "package com.x;\n"
            "public abstract class Base<T extends Comparable<T>> extends Root<T> implements A, B<T> {}\n"
            "interface I {}\n"
        )
        _, decls = cntr.parse_jvm_types(src)
        self.assertTrue(decls[0].abstract)
        self.assertEqual(decls[0].supertypes, ("Root", "A", "B"))
        self.assertTrue(decls[1].abstract)

    def test_kotlin_class(self) -> None:
        src = "package com.k\n\nimport org.junit.jupiter.api.Test\n\nclass KTest : Base() {\n  @Test fun t() {}\n}\n"
        pkg, decls = cntr.parse_jvm_types(src, kotlin=True)
        self.assertEqual(pkg, "com.k")
        self.assertEqual(
            (decls[0].name, decls[0].has_tests, decls[0].supertypes), ("KTest", True, ("Base",))
        )

    def test_kotlin_primary_constructor_is_not_a_supertype(self) -> None:
        src = "package com.k\n\nclass KTest(val x: Int) : BaseTest() {\n  @Test fun t() {}\n}\n"
        _, decls = cntr.parse_jvm_types(src, kotlin=True)
        self.assertEqual(decls[0].supertypes, ("BaseTest",))

    def test_test_source_paths(self) -> None:
        self.assertTrue(cntr.is_jvm_test_source("a/src/test/java/X.java"))
        self.assertTrue(cntr.is_jvm_test_source("a/b/src/integTest/java/X.java"))
        self.assertTrue(cntr.is_jvm_test_source("datahub-frontend/test/app/X.java"))
        self.assertTrue(cntr.is_jvm_test_source("a/src/sparkSmoke4/java/X.java"))
        self.assertFalse(cntr.is_jvm_test_source("a/src/main/java/X.java"))
        self.assertFalse(cntr.is_jvm_test_source("a/src/testFixtures/java/X.java"))
        self.assertTrue(cntr.is_python_test_source("metadata-ingestion/tests/unit/test_a.py"))
        self.assertTrue(cntr.is_python_test_source("x/tests/a_test.py"))
        self.assertFalse(cntr.is_python_test_source("metadata-ingestion/tests/unit/conftest.py"))
        self.assertFalse(cntr.is_python_test_source("metadata-ingestion/src/test_a.py"))
        self.assertEqual(
            cntr.python_module_name(
                "metadata-ingestion/tests/integration/hive-metastore/test_h.py"
            ),
            "tests.integration.hive-metastore.test_h",
        )


class EvaluateTests(unittest.TestCase):
    def test_ran_class_passes_and_counts_parameterized_cases(self) -> None:
        files = {f"{TEST_DIR}/FooTest.java": java("FooTest")}
        report = run_eval(
            files, [("com.x.FooTest", "t[0]", False), ("com.x.FooTest", "t[1]", False)]
        )
        self.assertEqual(report.failures, [])
        self.assertEqual(report.checked[0].detail, "2 executed")

    def test_class_missing_from_results_fails(self) -> None:
        # The suite-filter case: the class compiled, but no suite selected it.
        files = {f"{TEST_DIR}/NewTest.java": java("NewTest")}
        report = run_eval(files, [("com.x.OtherTest", "t", False)])
        self.assertEqual([c.name for c in report.failures], ["com.x.NewTest"])

    def test_all_skipped_fails(self) -> None:
        files = {f"{TEST_DIR}/FooTest.java": java("FooTest")}
        report = run_eval(files, [("com.x.FooTest", "t", True)])
        self.assertEqual(report.failures[0].detail, "0 executed, all 1 skipped")

    def test_prefix_collision_does_not_count(self) -> None:
        files = {f"{TEST_DIR}/FooTest.java": java("FooTest")}
        report = run_eval(
            files, [("com.x.FooTestHelper", "t", False), ("com.x.FooTest2", "t", False)]
        )
        self.assertEqual(len(report.failures), 1)

    def test_nested_class_results_count_for_outer(self) -> None:
        files = {
            f"{TEST_DIR}/OuterTest.java": java(
                "OuterTest", "@Nested class In { @Test void t() {} }"
            )
        }
        report = run_eval(files, [("com.x.OuterTest$In", "t", False)])
        self.assertEqual(report.failures, [])

    def test_helper_without_tests_is_ignored(self) -> None:
        files = {f"{TEST_DIR}/Utils.java": java("Utils", "static int x() { return 1; }")}
        report = run_eval(files, [])
        self.assertEqual((report.checked, len(report.ignored)), ([], 1))

    def test_abstract_base_passes_when_a_concrete_subclass_ran(self) -> None:
        files = {
            f"{TEST_DIR}/BaseTest.java": java("BaseTest", head="public abstract class"),
            f"{TEST_DIR}/MidTest.java": java(
                "MidTest", "", head="abstract class MidTest extends BaseTest {}\nclass Unused"
            ),
            f"{TEST_DIR}/LeafTest.java": java(
                "LeafTest", "", head="public class LeafTest extends MidTest {}\nclass Unused2"
            ),
        }
        report = run_eval(
            files, [("com.x.LeafTest", "t", False)], changed=[f"{TEST_DIR}/BaseTest.java"]
        )
        self.assertEqual(report.failures, [])
        self.assertIn("com.x.LeafTest", report.checked[0].detail)

    def test_abstract_base_fails_when_no_subclass_ran(self) -> None:
        files = {
            f"{TEST_DIR}/BaseTest.java": java("BaseTest", head="public abstract class"),
            f"{TEST_DIR}/LeafTest.java": "package com.x;\npublic class LeafTest extends BaseTest {}\n",
        }
        report = run_eval(files, [], changed=[f"{TEST_DIR}/BaseTest.java"])
        self.assertIn("none of its concrete subclasses", report.failures[0].detail)

    def test_concrete_subclass_with_only_inherited_tests_is_checked(self) -> None:
        # A per-backend subclass that only overrides setup is what a suite split drops.
        files = {
            f"{TEST_DIR}/SearchTestBase.java": java("SearchTestBase", head="public abstract class"),
            f"{TEST_DIR}/SearchEs8Test.java": (
                "package com.x;\npublic class SearchEs8Test extends SearchTestBase<Foo> {\n"
                "  @Override protected int port() { return 1; }\n}\n"
            ),
        }
        changed = [f"{TEST_DIR}/SearchEs8Test.java"]
        report = run_eval(files, [("com.x.SearchOsTest", "t", False)], changed=changed)
        self.assertEqual([c.name for c in report.failures], ["com.x.SearchEs8Test"])
        report = run_eval(files, [("com.x.SearchEs8Test", "t", False)], changed=changed)
        self.assertEqual((report.failures, len(report.checked)), ([], 1))

    def test_optout_by_name_glob_and_path(self) -> None:
        files = {
            f"{TEST_DIR}/ATest.java": java("ATest"),
            "mod/src/test/java/com/it/BTest.java": java("BTest").replace("com.x", "com.it"),
            f"{TEST_DIR}/CTest.java": java("CTest"),
        }
        optouts = {"com.x.ATest": "r1", "com.it.*": "r2", f"{TEST_DIR}/CTest.java": "r3"}
        report = run_eval(files, [], optouts=optouts)
        self.assertEqual(report.failures, [])
        self.assertEqual(
            sorted(c.opted_out_reason or "" for c in report.checked), ["r1", "r2", "r3"]
        )

    def test_python_module_and_class_cases(self) -> None:
        files = {
            "metadata-ingestion/tests/unit/test_a.py": "def test_x():\n    pass\n",
            "metadata-ingestion/tests/unit/test_b.py": "class TestB:\n    async def test_y(self):\n        pass\n",
            "metadata-ingestion/tests/unit/test_c.py": "def test_z():\n    pass\n",
        }
        cases = [
            ("tests.unit.test_a", "test_x", False),
            ("tests.unit.test_b.TestB", "test_y", False),
            ("tests.unit.test_c", "test_z", True),
        ]
        report = run_eval(files, cases)
        self.assertEqual([c.name for c in report.failures], ["tests.unit.test_c"])

    def test_language_filter(self) -> None:
        files = {
            "x/tests/test_a.py": "def test_x(): pass\n",
            f"{TEST_DIR}/FooTest.java": java("FooTest"),
        }
        report = run_eval(files, [], languages=["java"])
        self.assertEqual([c.name for c in report.checked], ["com.x.FooTest"])


class ResultsLoadingTests(unittest.TestCase):
    def test_non_junit_and_broken_xml(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            Path(d, "a.xml").write_text(junit([("com.x.A", "t", False)]))
            Path(d, "sub").mkdir()
            Path(d, "sub", "b.xml").write_text(
                '<testsuites><testsuite><testcase classname="com.x.B" name="t"/></testsuite></testsuites>'
            )
            Path(d, "pom.xml").write_text("<project/>")
            Path(d, "broken.xml").write_text("<testsuite><testcase")
            results = cntr.load_results([d])
        self.assertEqual(results.junit_files, 2)
        self.assertEqual(set(results.by_classname), {"com.x.A", "com.x.B"})
        self.assertEqual(len(results.unreadable_files), 1)

    def test_pytest_xfail_counts_as_executed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            Path(d, "junit.xml").write_text(
                '<testsuites><testsuite><testcase classname="tests.t" name="a">'
                '<skipped type="pytest.xfail" message="known"/></testcase>'
                '<testcase classname="tests.t" name="b"><skipped type="pytest.skip"/></testcase>'
                "</testsuite></testsuites>"
            )
            counts = cntr.load_results([d]).by_classname["tests.t"]
        self.assertEqual((counts.executed, counts.skipped), (1, 1))

    def test_optout_reason_required(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            p = Path(d, "o.json")
            p.write_text(json.dumps({"com.x.A": " "}))
            with self.assertRaises(ValueError):
                cntr.load_optouts(p)
            p.write_text(json.dumps({"_comment": "c", "com.x.A": "why"}))
            self.assertEqual(cntr.load_optouts(p), {"com.x.A": "why"})


class MainTests(unittest.TestCase):
    """End to end through git: only files changed since the merge base are checked."""

    def _git(self, repo: Path, *args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(repo), "-c", "user.email=t@t", "-c", "user.name=t", *args],
            check=True,
            capture_output=True,
            text=True,
        ).stdout

    def _main(self, *args: str) -> Tuple[int, str]:
        out = io.StringIO()
        with redirect_stdout(out):
            rc = cntr.main(list(args))
        return rc, out.getvalue()

    def test_base_diff_head_rev_and_exit_codes(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            repo, res = Path(d, "repo"), Path(d, "res")
            res.mkdir()
            (repo / TEST_DIR).mkdir(parents=True)
            self._git(repo, "init", "-q", "-b", "main")
            (repo / TEST_DIR / "OldTest.java").write_text(java("OldTest"))
            self._git(repo, "add", ".")
            self._git(repo, "commit", "-qm", "base")
            self._git(repo, "checkout", "-qb", "pr")
            (repo / TEST_DIR / "NewTest.java").write_text(java("NewTest"))
            self._git(repo, "add", ".")
            self._git(repo, "commit", "-qm", "add test")
            # The base branch moves on after the PR branched: its change to OldTest is not the PR's.
            self._git(repo, "checkout", "-q", "main")
            (repo / TEST_DIR / "OldTest.java").write_text(
                java("OldTest", "@Test public void t2() {}")
            )
            self._git(repo, "commit", "-qam", "base moves on")
            self._git(repo, "checkout", "-q", "pr")

            # OldTest did not run either, but the PR did not touch it, so only NewTest is checked.
            (res / "TEST-a.xml").write_text(junit([("com.x.Unrelated", "t", False)]))
            rc, out = self._main(
                "--repo-root", str(repo), "--base", "main", "--results-dir", str(res)
            )
            self.assertEqual(rc, 1)
            self.assertIn("NOT RUN com.x.NewTest", out)
            self.assertNotIn("OldTest", out)

            (res / "TEST-b.xml").write_text(junit([("com.x.NewTest", "t", False)]))
            # --head reads sources from the commit, not the (now deleted) working tree file.
            (repo / TEST_DIR / "NewTest.java").unlink()
            rc, out = self._main(
                "--repo-root",
                str(repo),
                "--base",
                "main",
                "--head",
                "pr",
                "--results-dir",
                str(res),
            )
            self.assertEqual(rc, 0, out)
            self.assertIn("RAN     com.x.NewTest (1 executed)", out)

    def test_missing_results_dir_is_usage_error(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            files = Path(d, "files.txt")
            files.write_text("")
            rc, _ = self._main(
                "--repo-root",
                d,
                "--changed-files-from",
                str(files),
                "--results-dir",
                str(Path(d, "nope")),
            )
        self.assertEqual(rc, 2)

    def test_unknown_head_is_usage_error(self) -> None:
        # Otherwise every file reads as missing and the check passes vacuously.
        with tempfile.TemporaryDirectory() as d:
            repo = Path(d)
            self._git(repo, "init", "-q")
            self._git(repo, "commit", "-q", "--allow-empty", "-m", "c")
            files = repo / "files.txt"
            files.write_text(f"{TEST_DIR}/FooTest.java\n")
            rc, _ = self._main(
                "--repo-root",
                d,
                "--changed-files-from",
                str(files),
                "--head",
                "0123456789abcdef0123456789abcdef01234567",
                "--results-dir",
                d,
            )
        self.assertEqual(rc, 2)

    def test_non_ascii_path_in_git_diff(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            repo, res = Path(d, "repo"), Path(d, "res")
            res.mkdir()
            src = repo / "m/src/test/java/é"
            src.mkdir(parents=True)
            self._git(repo, "init", "-q", "-b", "main")
            self._git(repo, "commit", "-q", "--allow-empty", "-m", "base")
            (src / "ÉTest.java").write_text(java("ÉTest").replace("com.x", "é"))
            self._git(repo, "add", ".")
            self._git(repo, "commit", "-qm", "add")
            rc, out = self._main(
                "--repo-root",
                str(repo),
                "--base",
                "HEAD~1",
                "--head",
                "HEAD",
                "--results-dir",
                str(res),
            )
        self.assertEqual(rc, 1, out)
        self.assertIn("NOT RUN é.ÉTest", out)


if __name__ == "__main__":
    unittest.main()
