"""Unit tests for Google Sheets lineage module."""

from unittest.mock import MagicMock

from datahub.ingestion.source.google_sheets.lineage import LineageExtractor
from datahub.ingestion.source.google_sheets.report import GoogleSheetsSourceReport


class TestLineageExtractor:
    """Tests for LineageExtractor."""

    def setup_method(self):
        """Set up test fixtures."""
        self.config = MagicMock()
        self.config.extract_lineage_from_formulas = True
        self.config.extract_column_level_lineage = True
        self.config.enable_cross_platform_lineage = True

        self.report = GoogleSheetsSourceReport()

        self.extractor = LineageExtractor(
            sheets_service=MagicMock(),
            platform_instance=None,
            env="PROD",
            config=self.config,
            report=self.report,
        )

    def test_extract_sheet_id_from_url(self):
        """Test extracting sheet ID from URL."""
        url = "https://docs.google.com/spreadsheets/d/ABC123XYZ/edit"
        sheet_id = self.extractor._extract_sheet_id_from_reference(url)
        assert sheet_id == "ABC123XYZ"

    def test_extract_sheet_id_from_id(self):
        """Test extracting sheet ID when already an ID."""
        sheet_id = "ABC123XYZ456-longid12345"
        result = self.extractor._extract_sheet_id_from_reference(sheet_id)
        assert result == sheet_id

    def test_extract_sheet_id_invalid(self):
        """Test extracting sheet ID from invalid reference."""
        invalid = "abc"  # Too short to be valid
        result = self.extractor._extract_sheet_id_from_reference(invalid)
        assert result is None

    def test_parse_range_simple(self):
        """Test parsing simple A1:B10 range."""
        columns = self.extractor._parse_range_columns("A1:C5")
        assert columns == ["A", "B", "C"]

    def test_parse_range_single_cell(self):
        """Test parsing single cell reference."""
        columns = self.extractor._parse_range_columns("A1")
        assert columns == ["A"]

    def test_parse_range_extended(self):
        """Test parsing extended range beyond Z."""
        columns = self.extractor._parse_range_columns("Z1:AB5")
        assert columns == ["Z", "AA", "AB"]

    def test_extract_bigquery_references(self):
        """Test extracting BigQuery references from formulas."""
        formula = "SELECT * FROM project.dataset.table WHERE value > 100"
        refs = self.extractor._extract_database_references(formula)

        assert len(refs) > 0
        platforms = [ref.platform for ref in refs]
        assert "bigquery" in platforms

    def test_extract_snowflake_references(self):
        """Test extracting Snowflake references from formulas."""
        formula = "SELECT * FROM snowflake.database.schema.table"
        refs = self.extractor._extract_database_references(formula)

        # Should find snowflake reference
        snowflake_refs = [ref for ref in refs if ref.platform == "snowflake"]
        assert len(snowflake_refs) > 0

    def test_extract_multiple_platform_references(self):
        """Test extracting multiple platform references."""
        formula = """
        IMPORTDATA("jdbc:bigquery://my-project/dataset.table")
        + QUERY("jdbc:snowflake://account/db?schema=public")
        """
        refs = self.extractor._extract_database_references(formula)

        platforms = {ref.platform for ref in refs}
        # Should find at least BigQuery
        assert len(platforms) > 0
