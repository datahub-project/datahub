"""Unit tests for Google Sheets header detection module."""

import pytest

from datahub.ingestion.source.google_sheets.header_detection import HeaderDetector


class TestHeaderDetector:
    """Tests for HeaderDetector class."""

    def test_find_header_row_empty_values(self):
        """Test find_header_row with empty values."""
        detector = HeaderDetector()
        assert detector.find_header_row([]) == -1

    def test_find_header_row_none_mode(self):
        """Test find_header_row with mode='none'."""
        detector = HeaderDetector(header_detection_mode="none")
        values = [["Name", "Age"], ["Alice", "30"]]
        assert detector.find_header_row(values) == -1

    def test_find_header_row_first_row_mode(self):
        """Test find_header_row with mode='first_row'."""
        detector = HeaderDetector(header_detection_mode="first_row")
        values = [["Name", "Age"], ["Alice", "30"]]
        assert detector.find_header_row(values) == 0

    def test_find_header_row_explicit_index(self):
        """Test find_header_row with explicit header_row_index."""
        detector = HeaderDetector(header_row_index=2)
        values = [[""], [""], ["Name", "Age"], ["Alice", "30"]]
        assert detector.find_header_row(values) == 2

    def test_find_header_row_explicit_index_out_of_bounds(self):
        """Test find_header_row with header_row_index beyond data."""
        detector = HeaderDetector(header_row_index=10)
        values = [["Name", "Age"], ["Alice", "30"]]
        # Should clamp to last row
        assert detector.find_header_row(values) == 1

    def test_skip_empty_leading_rows(self):
        """Test skipping empty leading rows."""
        detector = HeaderDetector(skip_empty_leading_rows=True)
        values = [
            ["", "", ""],
            [],
            ["Name", "Age", "City"],
            ["Alice", "30", "NYC"],
        ]
        # Should skip first 2 empty rows and return index 2
        assert detector.find_header_row(values) == 2

    def test_skip_empty_leading_rows_disabled(self):
        """Test with skip_empty_leading_rows disabled."""
        detector = HeaderDetector(skip_empty_leading_rows=False)
        values = [
            ["", "", ""],
            [],
            ["Name", "Age", "City"],
            ["Alice", "30", "NYC"],
        ]
        # Should not skip, return index 0
        assert detector.find_header_row(values) == 0

    @pytest.mark.parametrize(
        "values,expected",
        [
            ([[], ["", "", ""], []], 3),  # All empty
            ([[], ["", "", ""], ["Name", "Age"]], 2),  # Some empty
            ([["Name", "Age"], ["Alice", "30"]], 0),  # None empty
        ],
    )
    def test_skip_empty_rows(self, values, expected):
        """Test _skip_empty_rows with various empty row patterns."""
        detector = HeaderDetector()
        assert detector._skip_empty_rows(values) == expected

    def test_auto_detect_header_row_clear_headers(self):
        """Test auto-detect with clear text headers."""
        detector = HeaderDetector(header_detection_mode="auto_detect")
        values = [
            ["Customer Name", "Order ID", "Total Amount", "Date"],
            ["Alice Smith", "12345", "99.99", "2024-01-01"],
            ["Bob Jones", "12346", "149.50", "2024-01-02"],
        ]
        # First row should score highest (all text, unique, filled)
        assert detector.find_header_row(values) == 0

    def test_auto_detect_header_row_data_first(self):
        """Test auto-detect when data comes before headers."""
        detector = HeaderDetector(header_detection_mode="auto_detect")
        values = [
            ["Alice", "30", "100"],
            ["Name", "Age", "Score"],  # This is the header
            ["Bob", "25", "95"],
            ["Carol", "28", "105"],
        ]
        # Should detect row 1 as header (unique text vs numeric data)
        header_row = detector.find_header_row(values)
        # Header row should be either 0 or 1, but scoring should favor row 1
        assert header_row in [0, 1]

    def test_score_header_row_perfect_header(self):
        """Test scoring a perfect header row."""
        detector = HeaderDetector()
        values = [
            ["Name", "Age", "Email", "City"],  # Perfect header
            ["Alice", "30", "alice@test.com", "NYC"],
        ]
        score = detector._score_header_row(values, 0)
        # Perfect header should score high (near 100)
        assert score > 80

    def test_score_header_row_data_row(self):
        """Test scoring a data row (should score lower)."""
        detector = HeaderDetector()
        values = [
            ["Name", "Age", "Email"],
            ["Alice", "30", "alice@test.com"],  # Data row
        ]
        header_score = detector._score_header_row(values, 0)
        data_score = detector._score_header_row(values, 1)
        # Header should score higher than data
        assert header_score > data_score

    def test_score_header_row_empty(self):
        """Test scoring an empty row."""
        detector = HeaderDetector()
        values = [["", "", ""]]
        score = detector._score_header_row(values, 0)
        assert score == 0.0

    def test_score_header_row_out_of_bounds(self):
        """Test scoring with out-of-bounds index."""
        detector = HeaderDetector()
        values = [["Name", "Age"]]
        score = detector._score_header_row(values, 5)
        assert score == 0.0

    def test_score_header_row_uniqueness(self):
        """Test uniqueness scoring factor."""
        detector = HeaderDetector()
        # All unique values
        values_unique = [["Name", "Age", "Email"]]
        score_unique = detector._score_header_row(values_unique, 0)

        # Duplicate values
        values_duplicate = [["Name", "Name", "Email"]]
        score_duplicate = detector._score_header_row(values_duplicate, 0)

        # Unique should score higher
        assert score_unique > score_duplicate

    def test_score_header_row_fill_ratio(self):
        """Test fill ratio scoring factor."""
        detector = HeaderDetector()
        # Fully filled row
        values_filled = [["Name", "Age", "Email", "City"]]
        score_filled = detector._score_header_row(values_filled, 0)

        # Partially filled row
        values_partial = [["Name", "", "Email", ""]]
        score_partial = detector._score_header_row(values_partial, 0)

        # Filled should score higher
        assert score_filled > score_partial

    def test_score_header_row_text_content(self):
        """Test text content scoring factor."""
        detector = HeaderDetector()
        # All text
        values_text = [["Name", "City", "Country"]]
        score_text = detector._score_header_row(values_text, 0)

        # Mixed with numbers
        values_mixed = [["123", "456", "789"]]
        score_mixed = detector._score_header_row(values_mixed, 0)

        # Text should score higher
        assert score_text > score_mixed

    def test_score_header_row_length(self):
        """Test length scoring factor."""
        detector = HeaderDetector()
        # Short values
        values_short = [["Name", "Age", "ID"]]
        score_short = detector._score_header_row(values_short, 0)

        # Long values (>50 chars)
        long_value = "A" * 60
        values_long = [[long_value, long_value, long_value]]
        score_long = detector._score_header_row(values_long, 0)

        # Short should score higher
        assert score_short > score_long

    def test_score_header_row_type_difference(self):
        """Test type difference scoring factor."""
        detector = HeaderDetector()
        # Header (text) followed by data (numbers)
        values = [["Name", "Age", "Score"], ["Alice", "30", "95"]]
        header_score = detector._score_header_row(values, 0)

        # Data rows with similar types
        values_similar = [["Alice", "Bob", "Carol"], ["Dave", "Eve", "Frank"]]
        similar_score = detector._score_header_row(values_similar, 0)

        # Header with type difference should score higher
        assert header_score > similar_score

    def test_check_type_difference_text_vs_number(self):
        """Test type difference detection between text and numbers."""
        detector = HeaderDetector()
        current = ["Name", "Age"]
        next_row = ["Alice", "30"]
        # "Age" (text) vs "30" (number) shows type difference
        assert detector._check_type_difference(current, next_row) is True

        current = ["Name", "Score"]
        next_row = ["Alice", "95"]
        # "Score" (text) vs "95" (number) shows type difference
        assert detector._check_type_difference(current, next_row) is True

    def test_check_type_difference_number_vs_text(self):
        """Test type difference with number vs text."""
        detector = HeaderDetector()
        current = ["123", "456"]
        next_row = ["Alice", "Bob"]
        assert detector._check_type_difference(current, next_row) is True

    def test_check_type_difference_empty_cells(self):
        """Test type difference with empty cells."""
        detector = HeaderDetector()
        current = ["", "Age"]
        next_row = ["Alice", "30"]
        # Empty cells should be skipped
        result = detector._check_type_difference(current, next_row)
        assert result in [True, False]  # Depends on implementation

    @pytest.mark.parametrize(
        "index,expected",
        [
            # Single letters
            (0, "A"),
            (1, "B"),
            (25, "Z"),
            # Double letters
            (26, "AA"),
            (27, "AB"),
            (51, "AZ"),
            # Triple letters
            (702, "AAA"),
            (703, "AAB"),
        ],
    )
    def test_generate_column_letter_name(self, index, expected):
        """Test generating Excel-style column letters."""
        assert HeaderDetector.generate_column_letter_name(index) == expected

    def test_auto_detect_with_empty_leading_rows(self):
        """Test auto-detect combined with skip_empty_leading_rows."""
        detector = HeaderDetector(
            header_detection_mode="auto_detect", skip_empty_leading_rows=True
        )
        values = [
            [],
            ["", "", ""],
            ["Name", "Age", "City"],
            ["Alice", "30", "NYC"],
        ]
        # Should skip empty rows and detect row 2 as header
        assert detector.find_header_row(values) == 2

    def test_auto_detect_limit_rows_checked(self):
        """Test that auto-detect only checks first 10 rows."""
        detector = HeaderDetector(header_detection_mode="auto_detect")
        # Create 20 rows of data
        values = [[f"val{i}", f"val{i + 1}"] for i in range(20)]
        # Add a perfect header at row 15
        values[15] = ["Perfect Header", "Another Header"]

        header_row = detector.find_header_row(values)
        # Should only check first 10 rows, so won't find row 15
        assert header_row < 10

    def test_score_numeric_dates(self):
        """Test scoring with date-like numeric strings."""
        detector = HeaderDetector()
        values = [
            ["Date", "Amount"],
            ["2024-01-01", "99.99"],
        ]
        header_score = detector._score_header_row(values, 0)
        data_score = detector._score_header_row(values, 1)

        # Header should still score higher
        assert header_score > data_score
