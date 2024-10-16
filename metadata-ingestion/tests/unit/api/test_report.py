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
