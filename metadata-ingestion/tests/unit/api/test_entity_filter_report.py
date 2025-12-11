# SPDX-License-Identifier: Apache-2.0
#
# This file is unmodified from its original version developed by Acryl Data, Inc.,
# and is now included as part of a repository maintained by the National Digital Twin Programme.
# All support, maintenance and further development of this code is now the responsibility
# of the National Digital Twin Programme.

import dataclasses

from datahub.ingestion.api.report import EntityFilterReport, Report, SupportsAsObj


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = EntityFilterReport.field(type="view")


def test_entity_filter_report():
    report = MyReport()
    assert report.views.type == "view"
    assert isinstance(report, SupportsAsObj)

    report2 = MyReport()

    report.views.processed(entity="foo")
    report.views.dropped(entity="bar")

    assert (
        report.as_string() == "{'views': {'filtered': ['bar'], 'processed': ['foo']}}"
    )

    # Verify that the reports don't accidentally share any state.
    assert report2.as_string() == "{'views': {'filtered': [], 'processed': []}}"
