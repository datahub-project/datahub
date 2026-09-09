"""Unit tests for parse_failed_pytest_tests (run by test-github-scripts.yml)."""

from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import parse_failed_pytest_tests as parser  # noqa: E402

SCRIPT = Path(__file__).resolve().parent.parent / "parse_failed_pytest_tests.py"

COLLECTION_ERROR_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="1" failures="0" skipped="0" tests="1" time="0.1">
    <testcase classname="" name="tests/foo/test_bar.py" time="0">
      <error message="collection failure">ImportError: cannot import name X</error>
    </testcase>
  </testsuite>
</testsuites>
"""

COLLECTION_ERROR_WITH_NODEID_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="1" failures="0" skipped="0" tests="1" time="0.1">
    <testcase classname="" name="tests/foo/test_bar.py::test_something" time="0">
      <error message="collection failure">ImportError: cannot import name X</error>
    </testcase>
  </testsuite>
</testsuites>
"""

MIXED_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="1" failures="0" skipped="0" tests="2" time="0.2">
    <testcase classname="tests.ok_module" name="test_ok" time="0.01"/>
    <testcase classname="" name="tests/foo/test_broken.py" time="0">
      <error message="collection failure">ImportError: boom</error>
    </testcase>
  </testsuite>
</testsuites>
"""

CLASS_BASED_FAILURE_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="1" skipped="0" tests="1" time="0.2">
    <testcase classname="tests.my_module.MyTest" name="test_it" time="0.1">
      <failure message="assert False">assert False</failure>
    </testcase>
  </testsuite>
</testsuites>
"""

ALL_PASS_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="0" skipped="1" tests="2" time="0.2">
    <testcase classname="tests.ok_module" name="test_ok" time="0.01"/>
    <testcase classname="tests.ok_module" name="test_skip" time="0.0">
      <skipped message="skip"/>
    </testcase>
  </testsuite>
</testsuites>
"""

UNMAPPED_SUITE_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="1" failures="0" skipped="0" tests="0" time="0.1">
  </testsuite>
</testsuites>
"""

UNMAPPED_EMPTY_NAME_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="1" failures="0" skipped="0" tests="1" time="0.1">
    <testcase classname="" name="" time="0">
      <error message="collection failure">ImportError: boom</error>
    </testcase>
  </testsuite>
</testsuites>
"""


def _write_junit(directory: Path, contents: str, name: str = "junit.smoke.xml") -> Path:
    path = directory / name
    path.write_text(contents)
    return path


class ParseFailedModulesTests(unittest.TestCase):
    def test_collection_error_maps_file_from_name(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), COLLECTION_ERROR_XML)
            self.assertEqual(
                parser.parse_failed_modules(Path(d)),
                {"tests/foo/test_bar.py"},
            )

    def test_collection_error_strips_nodeid_suffix(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), COLLECTION_ERROR_WITH_NODEID_XML)
            self.assertEqual(
                parser.parse_failed_modules(Path(d)),
                {"tests/foo/test_bar.py"},
            )

    def test_mixed_pass_and_collection_error(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), MIXED_XML)
            self.assertEqual(
                parser.parse_failed_modules(Path(d)),
                {"tests/foo/test_broken.py"},
            )

    def test_class_based_failure_strips_class_name(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), CLASS_BASED_FAILURE_XML)
            self.assertEqual(
                parser.parse_failed_modules(Path(d)),
                {"tests/my_module.py"},
            )

    def test_all_pass_returns_empty_set(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), ALL_PASS_XML)
            self.assertEqual(parser.parse_failed_modules(Path(d)), set())

    def test_unmapped_suite_errors_raise(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), UNMAPPED_SUITE_XML)
            with self.assertRaises(parser.UnmappedFailuresError):
                parser.parse_failed_modules(Path(d))

    def test_unmapped_empty_classname_and_name_raise(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), UNMAPPED_EMPTY_NAME_XML)
            with self.assertRaises(parser.UnmappedFailuresError):
                parser.parse_failed_modules(Path(d))

    def test_no_xml_files_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            self.assertIsNone(parser.parse_failed_modules(Path(d)))

    def test_unparsed_xml_returns_none(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "junit.broken.xml").write_text("not xml <<<")
            self.assertIsNone(parser.parse_failed_modules(Path(d)))


class MainExitCodeTests(unittest.TestCase):
    def _run(self, input_dir: Path) -> subprocess.CompletedProcess[str]:
        output = input_dir / "failed.txt"
        return subprocess.run(
            [
                sys.executable,
                str(SCRIPT),
                "--input-dir",
                str(input_dir),
                "--output",
                str(output),
            ],
            capture_output=True,
            text=True,
        )

    def test_collection_error_exits_has_failures(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), COLLECTION_ERROR_XML)
            result = self._run(Path(d))
            self.assertEqual(result.returncode, 0)
            self.assertEqual(
                (Path(d) / "failed.txt").read_text(),
                "tests/foo/test_bar.py\n",
            )

    def test_all_pass_exits_all_passed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), ALL_PASS_XML)
            result = self._run(Path(d))
            self.assertEqual(result.returncode, 2)

    def test_unmapped_suite_exits_error_not_all_passed(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            _write_junit(Path(d), UNMAPPED_SUITE_XML)
            result = self._run(Path(d))
            self.assertEqual(result.returncode, 1)

    def test_no_xml_exits_no_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            result = self._run(Path(d))
            self.assertEqual(result.returncode, 3)

    def test_unparsed_xml_exits_no_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            (Path(d) / "junit.broken.xml").write_text("not xml <<<")
            result = self._run(Path(d))
            self.assertEqual(result.returncode, 3)


if __name__ == "__main__":
    unittest.main()
