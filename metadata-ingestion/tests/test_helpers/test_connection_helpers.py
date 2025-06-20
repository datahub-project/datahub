from typing import Dict, List, Optional, Type, Union

from datahub.ingestion.api.source import (
    CapabilityReport,
    SourceCapability,
    TestableSource,
    TestConnectionReport,
)


def run_test_connection(
    source_cls: Type[TestableSource], config_dict: Dict
) -> TestConnectionReport:
    return source_cls.test_connection(config_dict)


def assert_basic_connectivity_success(report: TestConnectionReport) -> None:
    assert report is not None
    assert report.basic_connectivity
    assert report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason is None


def assert_basic_connectivity_failure(
    report: TestConnectionReport, expected_reason: str
) -> None:
    assert report is not None
    assert report.basic_connectivity
    assert not report.basic_connectivity.capable
    assert report.basic_connectivity.failure_reason
    assert expected_reason in report.basic_connectivity.failure_reason


def assert_capability_report(
    capability_report: Optional[Dict[Union[SourceCapability, str], CapabilityReport]],
    success_capabilities: Optional[List[SourceCapability]] = None,
    failure_capabilities: Optional[Dict[SourceCapability, str]] = None,
) -> None:
    if success_capabilities is None:
        success_capabilities = []
    if failure_capabilities is None:
        failure_capabilities = {}
    assert capability_report
    for capability in success_capabilities:
        assert capability_report[capability]
        assert capability_report[capability].failure_reason is None
    for capability, expected_reason in failure_capabilities.items():
        assert not capability_report[capability].capable
        failure_reason = capability_report[capability].failure_reason
        assert failure_reason
        # Handle different error codes for "Connection refused" across operating systems
        # Linux uses [Errno 111] (usually CI env), macOS uses [Errno 61] (usually local developer env)
        if (
            "Connection refused" in expected_reason
            and "Connection refused" in failure_reason
        ):
            # If both mention connection refused, consider it a match regardless of error code
            assert True
        else:
            # Otherwise do the normal string inclusion check
            assert expected_reason in failure_reason
