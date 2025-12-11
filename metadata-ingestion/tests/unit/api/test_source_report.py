# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import json

from datahub.ingestion.api.source import SourceReport


def test_report_to_string_unsampled():
    source_report: SourceReport = SourceReport()
    source_report.report_warning("key1", "problem 1")
    source_report.report_failure("key2", "reason 2")
    str = source_report.as_string()
    assert "'warnings': [{'message': 'key1', 'context': ['problem 1']}]" in str
    assert "'failures': [{'message': 'key2', 'context': ['reason 2']}]" in str


def test_report_to_string_sampled():
    source_report = SourceReport()
    for i in range(0, 100):
        source_report.report_warning(f"key{i}", "Test message")

    str = source_report.as_string()
    assert "sampled of 100 total elements" in str
    str = source_report.as_json()
    print(str)
    report_as_dict = json.loads(str)
    assert len(report_as_dict["warnings"]) == 11
