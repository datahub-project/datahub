"""
Unit tests for form reporting Pydantic model validation fixes.
These tests verify that GMS data (integer timestamps, etc.) is properly validated and converted.
"""

from datetime import datetime, timezone

import pytest

from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
    DataHubFormReportingData,
    FormReportingRow,
    FormStatus,
    FormType,
    QuestionStatus,
)


def test_timestamp_conversion_handles_mixed_types():
    """Test that validator handles both integer and string timestamps from GMS."""
    # Test with integers (what GMS actually returns)
    data_with_ints = {
        "urn": "urn:li:dataset:test",
        "completedFormsCompletedPromptResponseTimes": [1761226434343, 1761226442398],
    }
    row1 = DataHubFormReportingData.DataHubDatasetSearchRow(**data_with_ints)  # type: ignore[arg-type]
    assert row1.completedFormsCompletedPromptResponseTimes == [
        "1761226434343",
        "1761226442398",
    ]

    # Test with strings (for robustness)
    data_with_strs = {
        "urn": "urn:li:dataset:test",
        "completedFormsCompletedPromptResponseTimes": [
            "1761226434343",
            "1761226442398",
        ],
    }
    row2 = DataHubFormReportingData.DataHubDatasetSearchRow(**data_with_strs)  # type: ignore[arg-type]
    assert row2.completedFormsCompletedPromptResponseTimes == [
        "1761226434343",
        "1761226442398",
    ]


def test_datetime_to_date_conversion_with_fix():
    """Test that datetime is properly converted to date using .date() method."""
    timestamp_ms = 1761226434343

    # With .date() conversion, this should succeed
    row = FormReportingRow(
        form_urn="urn:li:form:test",
        form_type=FormType.DOCUMENTATION,
        form_assigned_date=None,
        form_completed_date=None,
        form_status=FormStatus.COMPLETED,
        question_id="test-question",
        question_status=QuestionStatus.COMPLETED,
        question_completed_date=datetime.fromtimestamp(
            float(timestamp_ms) / 1000, tz=timezone.utc
        ).date(),  # Converted to date
        assignee_urn="urn:li:corpuser:admin",
        asset_urn="urn:li:dataset:test",
        platform_urn="urn:li:dataPlatform:snowflake",
        platform_instance_urn=None,
        domain_urn=None,
        parent_domain_urn=None,
        asset_verified=None,
        snapshot_date=datetime.now().date(),
    )

    # Verify the field is a date object
    assert row.question_completed_date is not None
    assert (
        row.question_completed_date
        == datetime.fromtimestamp(float(timestamp_ms) / 1000, tz=timezone.utc).date()
    )


def test_end_to_end_gms_data_flow():
    """
    Test complete flow: GMS integer timestamps → string conversion → datetime → date conversion.
    Simulates the actual data flow from GMS through the ingestion pipeline.
    """
    # Step 1: GMS returns data with integer timestamps
    gms_data = {
        "urn": "urn:li:dataset:(urn:li:dataPlatform:snowflake,banking.public.account,PROD)",
        "owners": ["urn:li:corpuser:admin"],
        "completedForms": ["urn:li:form:banking-documentation"],
        "incompleteForms": [],
        "verifiedForms": [],
        "completedFormsIncompletePromptIds": [],
        "completedFormsIncompletePromptResponseTimes": [],
        "completedFormsCompletedPromptIds": ["prompt-description", "prompt-owner"],
        "completedFormsCompletedPromptResponseTimes": [
            1761226457588,
            1761226461234,
        ],  # Integers from GMS
        "incompleteFormsIncompletePromptIds": [],
        "incompleteFormsIncompletePromptResponseTimes": [],
        "incompleteFormsCompletedPromptIds": [],
        "incompleteFormsCompletedPromptResponseTimes": [],
        "platform": "snowflake",
        "platformInstance": "PROD",
        "domains": ["urn:li:domain:banking"],
    }

    # Step 2: Create search row - integers should be converted to strings
    search_row = DataHubFormReportingData.DataHubDatasetSearchRow(**gms_data)  # type: ignore[arg-type]
    assert search_row.completedFormsCompletedPromptResponseTimes == [
        "1761226457588",
        "1761226461234",
    ]

    # Step 3: Create reporting rows using the timestamps
    for prompt_id, response_time_str in zip(
        search_row.completedFormsCompletedPromptIds,
        search_row.completedFormsCompletedPromptResponseTimes,
        strict=False,
    ):
        row = FormReportingRow(
            form_urn="urn:li:form:banking-documentation",
            form_type=FormType.DOCUMENTATION,
            form_assigned_date=datetime(2025, 10, 20).date(),
            form_completed_date=datetime(2025, 10, 23).date(),
            form_status=FormStatus.COMPLETED,
            question_id=prompt_id,
            question_status=QuestionStatus.COMPLETED,
            question_completed_date=datetime.fromtimestamp(
                float(response_time_str) / 1000, tz=timezone.utc
            ).date(),  # This is the fix!
            assignee_urn="urn:li:corpuser:admin",
            asset_urn=search_row.urn,
            platform_urn=search_row.platform,
            platform_instance_urn=search_row.platformInstance,
            domain_urn=search_row.domains[0] if search_row.domains else None,
            parent_domain_urn=None,
            asset_verified=None,
            snapshot_date=datetime.now().date(),
        )

        assert isinstance(row.question_completed_date, type(datetime.now().date()))


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
