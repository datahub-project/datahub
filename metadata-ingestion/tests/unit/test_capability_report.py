import json
from typing import cast

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestConnectionReport,
)


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
            SourceCapability.DOMAINS: CapabilityReport(capable=True),
        },
    )
    print(report.as_obj())
    foo = cast(dict, report.as_obj())
    assert isinstance(foo, dict)
    assert foo["capability_report"]["CONTAINERS"]["capable"] is True
    assert foo["capability_report"]["SCHEMA_METADATA"]["capable"] is True
    assert foo["capability_report"]["DESCRIPTIONS"]["capable"] is False
    assert (
        foo["capability_report"]["DESCRIPTIONS"]["failure_reason"]
        == "failed to get descriptions"
    )
    assert (
        foo["capability_report"]["DESCRIPTIONS"]["mitigation_message"]
        == "Enable admin privileges for this account."
    )
    assert foo["capability_report"]["DOMAINS"]["capable"] is True

    assert isinstance(json.loads(report.as_json()), dict)
