import json
from dataclasses import dataclass, field

from datahub.ingestion.api.workunit_processor import WorkunitProcessorReport
from datahub.utilities.lossy_collections import LossySet


@dataclass
class _ReportWithNonPrimitiveField(WorkunitProcessorReport):
    sample: LossySet[str] = field(default_factory=LossySet)


def test_as_obj_serializes_non_primitive_field_values() -> None:
    report = _ReportWithNonPrimitiveField()
    report.sample.add("urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)")

    assert json.loads(json.dumps(report.as_obj()))["sample"] == [
        "urn:li:dataset:(urn:li:dataPlatform:mysql,db.table,PROD)"
    ]


@dataclass
class _ReportWithPrivateField(WorkunitProcessorReport):
    _cache: str = "internal"


def test_as_obj_skips_private_fields() -> None:
    assert _ReportWithPrivateField().as_obj() == {}
