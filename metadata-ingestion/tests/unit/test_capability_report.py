import json
from typing import cast

from datahub.ingestion.api.source import CapabilityReport, TestConnectionReport


def test_basic_capability_report():
    report = TestConnectionReport(
        basic_connectivity=CapabilityReport(
            capable=True, failure_reason=None, mitigation_message=None
        ),
        capability_report={
            "CONTAINERS": CapabilityReport(
                capable=True, failure_reason=None, mitigation_message=None
            ),
            "SCHEMA_METADATA": CapabilityReport(
                capable=True, failure_reason=None, mitigation_message=None
            ),
            "DESCRIPTIONS": CapabilityReport(
                capable=False,
                failure_reason="failed to get descriptions",
                mitigation_message="Enable admin privileges for this account.",
            ),
            "DATA_PROFILING": CapabilityReport(
                capable=True, failure_reason=None, mitigation_message=None
            ),
        },
    )
    foo = cast(dict, report.as_obj())
    assert foo.get("capability_report").get("CONTAINERS").get("capable") == True
    assert foo.get("capability_report").get("SCHEMA_METADATA").get("capable") == True
    assert foo.get("capability_report").get("DESCRIPTIONS").get("capable") == False
    assert (
        foo.get("capability_report").get("DESCRIPTIONS").get("failure_reason")
        == "failed to get descriptions"
    )
    assert (
        foo.get("capability_report").get("DESCRIPTIONS").get("mitigation_message")
        == "Enable admin privileges for this account."
    )

    assert isinstance(json.loads(report.as_json()), dict)
