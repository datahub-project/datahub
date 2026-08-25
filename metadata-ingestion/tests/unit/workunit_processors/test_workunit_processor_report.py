import dataclasses
import json
from dataclasses import dataclass, field

import datahub.ingestion.workunit_processors  # noqa: F401  # registers every processor report
from datahub.ingestion.api.workunit_processor import WorkunitProcessorReport
from datahub.utilities.lossy_collections import LossySet


@dataclass
class _ReportWithNonPrimitiveField(WorkunitProcessorReport):
    # No processor report holds a non-primitive value today, so this stands in for
    # the next one that does.
    sample: LossySet[str] = field(default_factory=LossySet)


def test_as_obj_serializes_non_primitive_field_values() -> None:
    report = _ReportWithNonPrimitiveField()
    report.sample.add("urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)")

    assert json.loads(json.dumps(report.as_obj()))["sample"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
    ]


def test_no_processor_report_declares_a_platform_field() -> None:
    # Report.__post_init__ assigns self.platform, so a subclass field of that name
    # would be silently reset to None at construction.
    for report_class in WorkunitProcessorReport.__subclasses__():
        assert "platform" not in {f.name for f in dataclasses.fields(report_class)}
