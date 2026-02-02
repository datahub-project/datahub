"""Unit tests for Google Sheets utils module."""

from unittest.mock import MagicMock

from datahub.ingestion.source.google_sheets.config import GoogleSheetsSourceConfig
from datahub.ingestion.source.google_sheets.models import DriveFile
from datahub.ingestion.source.google_sheets.utils import (
    column_letter_to_index,
    extract_tags_from_drive_metadata,
    index_to_column_letter,
)


class TestColumnConversion:
    """Tests for column letter/index conversion functions."""

    def test_column_letter_to_index(self):
        """Test converting column letters to indices."""
        assert column_letter_to_index("A") == 0
        assert column_letter_to_index("B") == 1
        assert column_letter_to_index("Z") == 25
        assert column_letter_to_index("AA") == 26
        assert column_letter_to_index("AB") == 27
        assert column_letter_to_index("AZ") == 51
        assert column_letter_to_index("BA") == 52

    def test_index_to_column_letter(self):
        """Test converting indices to column letters."""
        assert index_to_column_letter(0) == "A"
        assert index_to_column_letter(1) == "B"
        assert index_to_column_letter(25) == "Z"
        assert index_to_column_letter(26) == "AA"
        assert index_to_column_letter(27) == "AB"
        assert index_to_column_letter(51) == "AZ"
        assert index_to_column_letter(52) == "BA"

    def test_column_conversion_round_trip(self):
        """Test that conversion is symmetric."""
        for i in range(100):
            letter = index_to_column_letter(i)
            assert column_letter_to_index(letter) == i


class TestTagExtraction:
    """Tests for tag extraction from Drive metadata."""

    def test_extract_tags_disabled(self):
        """Test that no tags are extracted when disabled."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = False

        sheet = DriveFile(
            id="test123",
            name="Test",
            mimeType="application/vnd.google-apps.spreadsheet",
        )
        tags = extract_tags_from_drive_metadata(sheet, config)

        assert len(tags) == 0

    def test_extract_tags_no_label_info(self):
        """Test that extraction returns empty list when no labelInfo present."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
        )
        tags = extract_tags_from_drive_metadata(sheet, config)

        assert len(tags) == 0

    def test_extract_tags_empty_label_info(self):
        """Test that extraction handles empty labelInfo."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={},
        )
        tags = extract_tags_from_drive_metadata(sheet, config)

        assert len(tags) == 0

    def test_extract_tags_with_label_id(self):
        """Test extraction of label ID as a tag using real Drive API structure."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        # Real Drive API v3 labelInfo structure
        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={"labels": [{"id": "label-123", "fields": {}}]},
        )

        tags = extract_tags_from_drive_metadata(sheet, config)

        assert len(tags) == 1
        assert "drive-label:label-123" in tags[0].tag

    def test_extract_tags_with_text_fields(self):
        """Test extraction of text field values as tags."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={
                "labels": [
                    {
                        "id": "label-123",
                        "fields": {
                            "field1": {"text": ["Project A", "Department B"]},
                        },
                    }
                ]
            },
        )

        tags = extract_tags_from_drive_metadata(sheet, config)

        # Should have 1 label ID tag + 2 text field tags
        assert len(tags) == 3
        tag_values = [tag.tag for tag in tags]
        assert any("drive-label:label-123" in t for t in tag_values)
        assert any("drive-label-field:Project A" in t for t in tag_values)
        assert any("drive-label-field:Department B" in t for t in tag_values)

    def test_extract_tags_with_selection_fields(self):
        """Test extraction of selection field values as tags."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={
                "labels": [
                    {
                        "id": "label-456",
                        "fields": {
                            "status": {"selection": ["active", "reviewed"]},
                        },
                    }
                ]
            },
        )

        tags = extract_tags_from_drive_metadata(sheet, config)

        # Should have 1 label ID tag + 2 selection tags
        assert len(tags) == 3
        tag_values = [tag.tag for tag in tags]
        assert any("drive-label:label-456" in t for t in tag_values)
        assert any("drive-label-selection:active" in t for t in tag_values)
        assert any("drive-label-selection:reviewed" in t for t in tag_values)

    def test_extract_tags_multiple_labels(self):
        """Test extraction from multiple labels."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={
                "labels": [
                    {"id": "label-123", "fields": {"dept": {"text": ["Finance"]}}},
                    {
                        "id": "label-456",
                        "fields": {"status": {"selection": ["active"]}},
                    },
                ]
            },
        )

        tags = extract_tags_from_drive_metadata(sheet, config)

        # Should have 2 label IDs + 1 text field + 1 selection = 4 tags
        assert len(tags) == 4
        tag_values = [tag.tag for tag in tags]
        assert any("drive-label:label-123" in t for t in tag_values)
        assert any("drive-label:label-456" in t for t in tag_values)
        assert any("drive-label-field:Finance" in t for t in tag_values)
        assert any("drive-label-selection:active" in t for t in tag_values)

    def test_extract_tags_mixed_field_types(self):
        """Test extraction with mixed field types (text, selection, and others)."""
        config = MagicMock(spec=GoogleSheetsSourceConfig)
        config.extract_tags = True

        sheet = DriveFile(
            id="test123",
            name="Test Sheet",
            mimeType="application/vnd.google-apps.spreadsheet",
            labelInfo={
                "labels": [
                    {
                        "id": "label-789",
                        "fields": {
                            "project": {"text": ["Alpha"]},
                            "status": {"selection": ["completed"]},
                            "date": {
                                "dateString": ["2024-01-01"]
                            },  # Non-text/selection
                            "number": {"integer": [42]},  # Non-text/selection
                        },
                    }
                ]
            },
        )

        tags = extract_tags_from_drive_metadata(sheet, config)

        # Should have 1 label ID + 1 text + 1 selection = 3 tags
        # (date and number fields are ignored)
        assert len(tags) == 3
        tag_values = [tag.tag for tag in tags]
        assert any("drive-label:label-789" in t for t in tag_values)
        assert any("drive-label-field:Alpha" in t for t in tag_values)
        assert any("drive-label-selection:completed" in t for t in tag_values)
