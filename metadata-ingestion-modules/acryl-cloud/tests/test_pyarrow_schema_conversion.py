"""
Test for PyArrow schema conversion from Pydantic models.
Tests the fix for TypeError: issubclass() arg 1 must be a class
"""

from datetime import date, datetime
from typing import List, Optional

import pytest


def test_simple_types_work():
    """Test that simple types (str, int, bool) work correctly."""
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class SimpleModel(BaseModelRow):
        name: str
        age: int
        active: bool
        created_at: datetime
        birth_date: date

    # This should work without errors
    schema = SimpleModel.arrow_schema()
    assert schema is not None
    print("✅ Simple types converted successfully")
    print(f"   Schema: {schema}")


def test_list_types_fail_before_fix():
    """
    Test that List[str] types cause issubclass() error.
    This reproduces the error: TypeError: issubclass() arg 1 must be a class
    """
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class ModelWithList(BaseModelRow):
        tags: List[str]
        numbers: List[int]

    # This should now work with our fix
    try:
        schema = ModelWithList.arrow_schema()
        print("✅ List types converted successfully")
        print(f"   Schema: {schema}")
        assert schema is not None
    except TypeError as e:
        if "issubclass() arg 1 must be a class" in str(e):
            pytest.fail(f"Got the issubclass() error that we're trying to fix: {e}")
        raise


def test_optional_types_fail_before_fix():
    """
    Test that Optional[date] types cause issubclass() error.
    """
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class ModelWithOptional(BaseModelRow):
        name: str
        completed_date: Optional[date]
        modified_date: Optional[datetime]

    # This should now work with our fix
    try:
        schema = ModelWithOptional.arrow_schema()
        print("✅ Optional types converted successfully")
        print(f"   Schema: {schema}")
        assert schema is not None
    except TypeError as e:
        if "issubclass() arg 1 must be a class" in str(e):
            pytest.fail(f"Got the issubclass() error that we're trying to fix: {e}")
        raise


def test_form_reporting_row_schema():
    """
    Test the actual FormReportingRow model that's failing in production.
    This is the real-world scenario from the error logs.
    """
    from acryl_datahub_cloud.datahub_reporting.datahub_form_reporting import (
        FormReportingRow,
    )

    # This is what fails in production with:
    # TypeError: issubclass() arg 1 must be a class
    try:
        schema = FormReportingRow.arrow_schema()
        print("✅ FormReportingRow schema created successfully")
        print(f"   Number of fields: {len(schema)}")

        # Verify some key fields
        field_names = [field.name for field in schema]
        assert "form_urn" in field_names
        assert "question_completed_date" in field_names
        assert "assignee_urn" in field_names

        print(f"   Fields: {', '.join(field_names[:5])}...")
    except TypeError as e:
        if "issubclass() arg 1 must be a class" in str(e):
            pytest.fail(
                f"FormReportingRow.arrow_schema() fails with issubclass() error: {e}"
            )
        raise


def test_complex_nested_types():
    """Test more complex nested type scenarios."""
    from acryl_datahub_cloud.elasticsearch.graph_service import BaseModelRow

    class ComplexModel(BaseModelRow):
        # Simple types
        id: str
        count: int

        # Lists
        tags: List[str]
        scores: List[int]

        # Optional
        description: Optional[str]
        created: Optional[datetime]
        completed: Optional[date]

        # Lists with optional
        owners: List[str]

    try:
        schema = ComplexModel.arrow_schema()
        print("✅ Complex nested types converted successfully")
        print(f"   Schema fields: {len(schema)}")

        # Verify field types
        field_dict = {field.name: str(field.type) for field in schema}

        # Check list types
        assert "list" in field_dict["tags"].lower()
        assert "list" in field_dict["scores"].lower()

        # Check date/time types
        assert (
            "date" in field_dict["completed"].lower()
            or "int32" in field_dict["completed"].lower()
        )

        print(f"   tag type: {field_dict['tags']}")
        print(f"   completed type: {field_dict['completed']}")

    except TypeError as e:
        if "issubclass() arg 1 must be a class" in str(e):
            pytest.fail(f"Complex types fail with issubclass() error: {e}")
        raise


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
