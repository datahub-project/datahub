"""
Tests for datetime/date field validation in form reporting models.
"""

from datetime import datetime, timezone

import pytest


def test_datetime_field_rejects_datetime_without_conversion():
    """Test that assigning datetime to date field raises validation error."""
    from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
        FormReportingRow,
        FormStatus,
        FormType,
        QuestionStatus,
    )

    # This should fail with the same error we saw:
    # "Datetimes provided to dates should have zero time"
    with pytest.raises(Exception) as exc_info:
        FormReportingRow(
            form_urn="urn:li:form:test",
            form_type=FormType.DOCUMENTATION,
            form_assigned_date=None,
            form_completed_date=None,
            form_status=FormStatus.COMPLETED,
            question_id="test-question",
            question_status=QuestionStatus.COMPLETED,
            question_completed_date=datetime.fromtimestamp(
                1761226434343 / 1000, tz=timezone.utc
            ),  # This is a datetime, but field expects date
            assignee_urn="urn:li:corpuser:admin",
            asset_urn="urn:li:dataset:test",
            platform_urn="urn:li:dataPlatform:postgres",
            platform_instance_urn=None,
            domain_urn=None,
            parent_domain_urn=None,
            asset_verified=None,
            snapshot_date=datetime.now().date(),
        )

    assert "date_from_datetime_inexact" in str(exc_info.value)


def test_correct_datetime_to_date_conversion():
    """Test that converting datetime to date works correctly."""
    from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
        FormReportingRow,
        FormStatus,
        FormType,
        QuestionStatus,
    )

    # This should succeed - convert datetime to date
    row = FormReportingRow(
        form_urn="urn:li:form:test",
        form_type=FormType.DOCUMENTATION,
        form_assigned_date=None,
        form_completed_date=None,
        form_status=FormStatus.COMPLETED,
        question_id="test-question",
        question_status=QuestionStatus.COMPLETED,
        question_completed_date=datetime.fromtimestamp(
            1761226434343 / 1000, tz=timezone.utc
        ).date(),  # Convert to date
        assignee_urn="urn:li:corpuser:admin",
        asset_urn="urn:li:dataset:test",
        platform_urn="urn:li:dataPlatform:postgres",
        platform_instance_urn=None,
        domain_urn=None,
        parent_domain_urn=None,
        asset_verified=None,
        snapshot_date=datetime.now().date(),
    )

    assert row.question_completed_date is not None
