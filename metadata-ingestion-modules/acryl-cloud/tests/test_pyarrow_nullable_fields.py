"""
Test for PyArrow schema generation with nullable fields.
Reproduces: Field pyarrow.Field<form_completed_date: date32[day] not null> was non-nullable but pandas column had 8 null values
"""

from datetime import date
from typing import Optional

import pandas as pd
import pyarrow as pa
import pytest


def test_optional_fields_should_be_nullable():
    """
    Test that Optional[date] fields are marked as nullable in PyArrow schema.
    This reproduces the production error where Optional fields were marked as not null.
    """
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class ModelWithOptionalDate(BaseModelRow):
        id: str
        created_date: date
        completed_date: Optional[date]  # This should be nullable

    schema = ModelWithOptionalDate.arrow_schema()

    # Check the schema fields
    print("\n=== Schema Fields ===")
    for field in schema:
        print(f"  {field.name}: {field.type} - nullable={field.nullable}")

    # Find the completed_date field
    completed_field = [f for f in schema if f.name == "completed_date"][0]

    # This should be nullable since it's Optional
    assert completed_field.nullable, (
        f"Field 'completed_date' should be nullable but got: {completed_field}"
    )

    print("✅ Optional[date] field is correctly marked as nullable")


def test_form_reporting_row_nullable_fields():
    """
    Test the actual FormReportingRow model has correct nullable fields.
    """
    from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
        FormReportingRow,
    )

    schema = FormReportingRow.arrow_schema()

    # These fields should be nullable (they are Optional in the model)
    nullable_fields = [
        "form_assigned_date",
        "form_completed_date",
        "question_completed_date",
        "assignee_urn",
        "platform_instance_urn",
        "domain_urn",
        "parent_domain_urn",
        "asset_verified",
    ]

    print("\n=== FormReportingRow Nullable Fields ===")
    field_dict = {f.name: f for f in schema}

    for field_name in nullable_fields:
        field = field_dict.get(field_name)
        if field:
            print(f"  {field_name}: nullable={field.nullable}")
            assert field.nullable, (
                f"Field '{field_name}' should be nullable but is not: {field}"
            )
        else:
            print(f"  {field_name}: NOT FOUND in schema")

    print("✅ All Optional fields are correctly marked as nullable")


def test_pandas_to_pyarrow_with_null_values():
    """
    Test that we can convert pandas DataFrame with null values to PyArrow when schema is correct.
    This simulates what happens in production.
    """
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class TestModel(BaseModelRow):
        id: str
        count: int
        completed_date: Optional[date]

    # Create test data with null values
    data = [
        {"id": "1", "count": 10, "completed_date": date(2025, 1, 1)},
        {"id": "2", "count": 20, "completed_date": None},  # NULL value
        {"id": "3", "count": 30, "completed_date": date(2025, 1, 3)},
        {"id": "4", "count": 40, "completed_date": None},  # NULL value
    ]

    # Convert to pandas DataFrame
    df = pd.DataFrame(data)

    # Get the schema
    schema = TestModel.arrow_schema()

    print("\n=== Test Data ===")
    print(f"DataFrame shape: {df.shape}")
    print(f"Null counts: {df.isnull().sum().to_dict()}")

    # This should work without errors if schema is correct
    try:
        record_batch = pa.RecordBatch.from_pandas(df, schema=schema)
        print("✅ Successfully converted DataFrame with null values to PyArrow")
        print(f"   RecordBatch: {record_batch.num_rows} rows")
    except ValueError as e:
        if "was non-nullable but pandas column had" in str(e):
            pytest.fail(f"Got the nullable error we're trying to fix: {e}")
        raise


def test_form_reporting_actual_use_case():
    """
    Test with realistic FormReportingRow data that includes null completed_date values.
    This is what happens in production.
    """
    from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
        FormReportingRow,
        FormStatus,
        FormType,
        QuestionStatus,
    )

    # Create rows with null completed_date (for incomplete forms)
    rows = [
        FormReportingRow(
            form_urn="urn:li:form:test1",
            form_type=FormType.DOCUMENTATION,
            form_assigned_date=date(2025, 1, 1),
            form_completed_date=date(2025, 1, 10),  # Completed
            form_status=FormStatus.COMPLETED,
            question_id="q1",
            question_status=QuestionStatus.COMPLETED,
            question_completed_date=date(2025, 1, 10),
            assignee_urn="urn:li:corpuser:admin",
            asset_urn="urn:li:dataset:test",
            platform_urn="urn:li:dataPlatform:postgres",
            platform_instance_urn=None,
            domain_urn=None,
            parent_domain_urn=None,
            asset_verified=None,
            snapshot_date=date(2025, 1, 15),
        ),
        FormReportingRow(
            form_urn="urn:li:form:test2",
            form_type=FormType.DOCUMENTATION,
            form_assigned_date=date(2025, 1, 1),
            form_completed_date=None,  # NOT completed - NULL value
            form_status=FormStatus.IN_PROGRESS,
            question_id="q2",
            question_status=QuestionStatus.NOT_STARTED,
            question_completed_date=None,  # NOT completed - NULL value
            assignee_urn="urn:li:corpuser:user1",
            asset_urn="urn:li:dataset:test2",
            platform_urn="urn:li:dataPlatform:postgres",
            platform_instance_urn=None,
            domain_urn=None,
            parent_domain_urn=None,
            asset_verified=None,
            snapshot_date=date(2025, 1, 15),
        ),
    ]

    # Convert to DataFrame (simulating what happens in production)
    df = pd.DataFrame([row.dict() for row in rows])

    print("\n=== Form Reporting Data ===")
    print(f"Rows: {len(rows)}")
    print(
        f"Null values in form_completed_date: {df['form_completed_date'].isnull().sum()}"
    )
    print(
        f"Null values in question_completed_date: {df['question_completed_date'].isnull().sum()}"
    )

    # Get schema
    schema = FormReportingRow.arrow_schema()

    # This should work without errors
    try:
        record_batch = pa.RecordBatch.from_pandas(df, schema=schema)
        print("✅ Successfully converted FormReportingRow data with nulls to PyArrow")
        print(f"   RecordBatch: {record_batch.num_rows} rows")
    except ValueError as e:
        if "was non-nullable but pandas column had" in str(e):
            pytest.fail(f"Got the production error: {e}")
        raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
