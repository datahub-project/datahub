"""Unit tests for Google Drive label Pydantic models."""

from typing import Any, Dict

from datahub.ingestion.source.google_sheets.models import (
    DriveLabel,
    DriveLabelInfo,
)


class TestDriveLabel:
    """Tests for DriveLabel.from_dict() factory method."""

    def test_from_dict_simple(self):
        """Test creating DriveLabel from API response (simple)."""
        api_response = {"id": "label-789", "fields": {}}

        label = DriveLabel.from_dict(api_response)

        assert label.id == "label-789"
        assert label.fields == {}

    def test_from_dict_with_fields(self):
        """Test creating DriveLabel from API response with fields."""
        api_response: Dict[str, Any] = {
            "id": "label-101",
            "fields": {
                "project": {"text": ["Alpha", "Beta"]},
                "status": {"selection": ["Active"]},
                "category": {"text": ["Research"], "selection": ["Public"]},
            },
        }

        label = DriveLabel.from_dict(api_response)

        assert label.id == "label-101"
        assert "project" in label.fields
        assert label.fields["project"].text == ["Alpha", "Beta"]
        assert label.fields["status"].selection == ["Active"]
        assert label.fields["category"].text == ["Research"]
        assert label.fields["category"].selection == ["Public"]

    def test_from_dict_missing_id(self):
        """Test handling missing ID in API response (defaults to empty string)."""
        api_response: Dict[str, Any] = {"fields": {"test": {"text": ["Value"]}}}

        label = DriveLabel.from_dict(api_response)

        assert label.id == ""
        assert "test" in label.fields


class TestDriveLabelInfo:
    """Tests for DriveLabelInfo.from_dict() factory method."""

    def test_from_dict_empty(self):
        """Test creating DriveLabelInfo from empty API response."""
        api_response: Dict[str, Any] = {}

        label_info = DriveLabelInfo.from_dict(api_response)

        assert label_info.labels == []

    def test_from_dict_with_labels(self):
        """Test creating DriveLabelInfo from API response with labels."""
        api_response: Dict[str, Any] = {
            "labels": [
                {"id": "label-1", "fields": {"name": {"text": ["Project X"]}}},
                {"id": "label-2", "fields": {"status": {"selection": ["Active"]}}},
            ]
        }

        label_info = DriveLabelInfo.from_dict(api_response)

        assert len(label_info.labels) == 2
        assert label_info.labels[0].id == "label-1"
        assert label_info.labels[1].id == "label-2"

    def test_from_dict_real_api_structure(self):
        """Test parsing actual Google Drive API v3 labelInfo structure."""
        api_response: Dict[str, Any] = {
            "labels": [
                {
                    "id": "labelId12345",
                    "fields": {
                        "category": {
                            "selection": ["Marketing"],
                            "text": [],
                        },
                        "project": {
                            "text": ["Q4 Analytics Dashboard"],
                            "selection": [],
                        },
                        "owner": {
                            "text": ["Jane Doe", "john.smith@example.com"],
                        },
                    },
                }
            ]
        }

        label_info = DriveLabelInfo.from_dict(api_response)

        assert len(label_info.labels) == 1
        label = label_info.labels[0]
        assert label.id == "labelId12345"
        assert "category" in label.fields
        assert label.fields["category"].selection == ["Marketing"]
        assert label.fields["project"].text == ["Q4 Analytics Dashboard"]
        assert "john.smith@example.com" in label.fields["owner"].text
