"""Unit tests for send_failed_tests_to_posthog (run by test-github-scripts.yml)."""

from __future__ import annotations

import json
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Iterator
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import send_failed_tests_to_posthog as sender  # noqa: E402

PYTEST_FAILURE_WITH_DOMAINS_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="1" skipped="0" tests="1" time="0.2">
    <testcase classname="tests.foo.test_bar" name="test_it" time="0.1">
      <properties>
        <property name="domains" value='["catalog", "ingestion"]'/>
      </properties>
      <failure message="assert False">assert False</failure>
    </testcase>
  </testsuite>
</testsuites>
"""

PYTEST_FAILURE_WITHOUT_PROPERTIES_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="1" skipped="0" tests="1" time="0.2">
    <testcase classname="tests.foo.test_bar" name="test_it" time="0.1">
      <failure message="assert False">assert False</failure>
    </testcase>
  </testsuite>
</testsuites>
"""

PYTEST_PASS_WITH_DOMAINS_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="0" skipped="0" tests="1" time="0.2">
    <testcase classname="tests.foo.test_bar" name="test_ok" time="0.1">
      <properties>
        <property name="domains" value='["catalog"]'/>
      </properties>
    </testcase>
  </testsuite>
</testsuites>
"""

PYTEST_FAILURE_STRING_PROPERTY_XML = """<?xml version="1.0" encoding="utf-8"?>
<testsuites>
  <testsuite name="pytest" errors="0" failures="1" skipped="0" tests="1" time="0.2">
    <testcase classname="tests.foo.test_bar" name="test_it" time="0.1">
      <properties>
        <property name="owner" value="catalog-team"/>
      </properties>
      <failure message="assert False">assert False</failure>
    </testcase>
  </testsuite>
</testsuites>
"""


@contextmanager
def _tmp_xml(contents: str) -> Iterator[Path]:
    with TemporaryDirectory() as tmp:
        path = Path(tmp) / "junit.smoke.xml"
        path.write_text(contents)
        yield path


def _fake_urlopen(captured: dict):
    def fake_urlopen(request, timeout=None):
        captured["payload"] = json.loads(request.data.decode("utf-8"))
        response = MagicMock()
        response.getcode.return_value = 200
        response.__enter__.return_value = response
        response.__exit__.return_value = False
        return response

    return fake_urlopen


class ParsePytestFailuresTests(unittest.TestCase):
    def test_failure_with_domain_properties_decodes_json_list(self) -> None:
        with _tmp_xml(PYTEST_FAILURE_WITH_DOMAINS_XML) as xml_file:
            failures = sender.parse_pytest_failures(xml_file)

        self.assertEqual(len(failures), 1)
        self.assertEqual(
            failures[0].name, "tests/foo/test_bar.py::test_it"
        )
        self.assertEqual(
            failures[0].custom_properties,
            {"domains": ["catalog", "ingestion"]},
        )

    def test_failure_without_properties_omits_custom_properties(self) -> None:
        with _tmp_xml(PYTEST_FAILURE_WITHOUT_PROPERTIES_XML) as xml_file:
            failures = sender.parse_pytest_failures(xml_file)

        self.assertEqual(len(failures), 1)
        self.assertIsNone(failures[0].custom_properties)

    def test_passed_tests_are_ignored(self) -> None:
        with _tmp_xml(PYTEST_PASS_WITH_DOMAINS_XML) as xml_file:
            failures = sender.parse_pytest_failures(xml_file)

        self.assertEqual(failures, [])

    def test_non_json_property_value_stays_a_string(self) -> None:
        with _tmp_xml(PYTEST_FAILURE_STRING_PROPERTY_XML) as xml_file:
            failures = sender.parse_pytest_failures(xml_file)

        self.assertEqual(len(failures), 1)
        self.assertEqual(
            failures[0].custom_properties, {"owner": "catalog-team"}
        )


class SendPostHogEventTests(unittest.TestCase):
    def test_payload_nests_custom_properties_and_keeps_metadata(self) -> None:
        captured: dict = {}
        test = sender.FailedTest(
            name="tests/foo/test_bar.py::test_it",
            test_type="pytest",
            error_message="assert False",
            custom_properties={"domains": ["catalog", "ingestion"]},
        )
        config = sender.PostHogConfig(api_key="phc_test", host="https://app.posthog.com")
        metadata = {
            "github_repository": "datahub-project/datahub",
            "workflow_name": "Docker Build, Scan, Test",
            "head_branch": "main",
            "run_id": "12345",
        }

        with patch.object(
            sender.urllib.request, "urlopen", side_effect=_fake_urlopen(captured)
        ):
            ok = sender.send_posthog_event(config, test, metadata)

        self.assertTrue(ok)
        properties = captured["payload"]["properties"]
        self.assertEqual(properties["test_name"], test.name)
        self.assertEqual(properties["test_type"], "pytest")
        self.assertEqual(properties["github_repository"], "datahub-project/datahub")
        self.assertEqual(properties["workflow_name"], metadata["workflow_name"])
        self.assertEqual(
            properties["custom_properties"],
            {"domains": ["catalog", "ingestion"]},
        )

    def test_payload_omits_custom_properties_when_absent(self) -> None:
        captured: dict = {}
        test = sender.FailedTest(
            name="tests/foo/test_bar.py::test_it",
            test_type="pytest",
        )
        config = sender.PostHogConfig(api_key="phc_test", host="https://app.posthog.com")

        with patch.object(
            sender.urllib.request, "urlopen", side_effect=_fake_urlopen(captured)
        ):
            ok = sender.send_posthog_event(config, test, {"github_repository": "org/repo"})

        self.assertTrue(ok)
        self.assertNotIn("custom_properties", captured["payload"]["properties"])


if __name__ == "__main__":
    unittest.main()
