import dataclasses

from datahub.ingestion.api.report import EntityFilterReport, Report
from datahub.ingestion.source.unity.config import UnityCatalogSourceConfig


@dataclasses.dataclass
class MyReport(Report):
    views: EntityFilterReport = EntityFilterReport.field(type="view")


def test_report_types():

    report = MyReport()
    assert report.views.type == "view"

    report2 = MyReport()

    report.views.processed(entity="foo")
    report.views.dropped(entity="bar")

    assert (
        report.as_string() == "{'views': {'filtered': ['bar'], 'processed': ['foo']}}"
    )

    assert report2.as_string() == "{'views': {'filtered': [], 'processed': []}}"


def test_shared_defaults():
    c1 = UnityCatalogSourceConfig(token="s", workspace_url="s")
    c2 = UnityCatalogSourceConfig(token="s", workspace_url="s")

    c1.catalog_pattern.allow += ["foo"]
    assert c2.catalog_pattern.allow == [".*"]
