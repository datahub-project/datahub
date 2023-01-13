import dataclasses

from datahub.ingestion.api.report import EntityFilterReport, Report


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = EntityFilterReport.field(type="view")


def test_report_types():

    report = MyReport()

    report.views(entity="foo")
    report.views.dropped(entity="bar")

    assert (
        report.as_string() == "{'views': {'filtered': ['bar'], 'processed': ['foo']}}"
    )
    # TODO test attr sorting
