"""Header row detection logic for Google Sheets."""

import string
from typing import List


class HeaderDetector:
    """Detects header rows in Google Sheets using configurable strategies."""

    def __init__(
        self,
        header_detection_mode: str = "first_row",
        header_row_index: int | None = None,
        skip_empty_leading_rows: bool = True,
    ):
        self.header_detection_mode = header_detection_mode
        self.header_row_index = header_row_index
        self.skip_empty_leading_rows = skip_empty_leading_rows

    def find_header_row(self, values: List[List[str]]) -> int:
        """Find the header row index based on configuration.

        Returns:
            0-based index of the header row, or -1 if no headers should be used
        """
        if not values:
            return -1

        if self.header_row_index is not None:
            return min(self.header_row_index, len(values) - 1)

        start_row = 0
        if self.skip_empty_leading_rows:
            start_row = self._skip_empty_rows(values)
            if start_row >= len(values):
                return -1

        if self.header_detection_mode == "none":
            return -1
        elif self.header_detection_mode == "first_row":
            return start_row
        elif self.header_detection_mode == "auto_detect":
            return self._auto_detect_header_row(values, start_row)

        return start_row

    def _skip_empty_rows(self, values: List[List[str]]) -> int:
        """Skip empty rows at the beginning of the sheet."""
        for i, row in enumerate(values):
            if any(cell for cell in row):
                return i
        return len(values)

    def _auto_detect_header_row(
        self, values: List[List[str]], start_row: int = 0
    ) -> int:
        """Auto-detect header row using scoring heuristics."""
        if start_row >= len(values):
            return start_row

        max_rows_to_check = min(10, len(values) - start_row)
        best_score = -1
        best_row = start_row

        for i in range(start_row, start_row + max_rows_to_check):
            score = self._score_header_row(values, i)
            if score > best_score:
                best_score = score
                best_row = i

        return best_row

    def _score_header_row(self, values: List[List[str]], row_index: int) -> float:
        """Score a potential header row based on various heuristics.

        Higher score = more likely to be a header row.

        Scoring factors (max 100 points):
        - Fill ratio: 20 points for fully filled row
        - Uniqueness: 30 points if all values are unique
        - Text content: 25 points if all values are text
        - Length: 15 points if all values are short (<50 chars)
        - Type difference: 10 points if types differ from next row
        """
        if row_index >= len(values):
            return 0.0

        row = values[row_index]
        if not row or not any(cell for cell in row):
            return 0.0

        score = 0.0
        non_empty_cells = [cell for cell in row if cell]

        if not non_empty_cells:
            return 0.0

        total_cells = len(row)
        filled_ratio = len(non_empty_cells) / max(total_cells, 1)
        score += filled_ratio * 20

        if len(non_empty_cells) == len(set(non_empty_cells)):
            score += 30

        text_cells = sum(
            1
            for cell in non_empty_cells
            if isinstance(cell, str)
            and not cell.replace(".", "").replace("-", "").isdigit()
        )
        if text_cells == len(non_empty_cells):
            score += 25

        short_cells = sum(1 for cell in non_empty_cells if len(str(cell)) < 50)
        if short_cells == len(non_empty_cells):
            score += 15

        if row_index + 1 < len(values):
            next_row = values[row_index + 1]
            if len(next_row) > 0:
                next_row_types_differ = self._check_type_difference(
                    row[: len(next_row)], next_row
                )
                if next_row_types_differ:
                    score += 10

        return score

    def _check_type_difference(
        self, current_row: List[str], next_row: List[str]
    ) -> bool:
        """Check if current row and next row have different data types."""
        for current_cell, next_cell in zip(current_row, next_row, strict=False):
            if current_cell and next_cell:
                current_is_text = (
                    not str(current_cell).replace(".", "").replace("-", "").isdigit()
                )
                next_is_text = (
                    not str(next_cell).replace(".", "").replace("-", "").isdigit()
                )
                if current_is_text != next_is_text:
                    return True
        return False

    @staticmethod
    def generate_column_letter_name(index: int) -> str:
        """Generate Excel-style column letter (A, B, ..., Z, AA, AB, ...)."""
        result = ""
        while index >= 0:
            result = string.ascii_uppercase[index % 26] + result
            index = index // 26 - 1
        return result
