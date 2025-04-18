import io
import logging
import re
from collections import Counter
from dataclasses import dataclass
from io import TextIOBase
from typing import Any, BinaryIO, Dict, List, Union

import openpyxl
import pandas as pd
from openpyxl.workbook import Workbook

from datahub.ingestion.source.excel.report import ExcelSourceReport

logger = logging.getLogger(__name__)


@dataclass
class ExcelTable:
    df: pd.DataFrame
    header_row: int
    footer_row: int
    row_count: int
    column_count: int
    metadata: Dict[str, Any]
    sheet_name: str


class ExcelFile:
    wb: Workbook
    filename: str
    data: Union[bytes, BinaryIO, str, io.TextIOBase]
    sheet_list: List[str]
    active_sheet: str
    properties: Dict[str, Any]
    report: ExcelSourceReport

    def __init__(
        self,
        filename: str,
        data: Union[bytes, BinaryIO, str, io.TextIOBase],
        report: ExcelSourceReport,
    ) -> None:
        self.filename = filename
        self.data = data
        self.report = report
        self.sheet_list = []
        self.active_sheet = ""
        self.properties = {}

    def load_workbook(self) -> bool:
        file: Union[BinaryIO, None] = None
        try:
            if isinstance(self.data, bytes):
                file = io.BytesIO(self.data)
            elif isinstance(self.data, str):
                file = io.BytesIO(io.StringIO(self.data).getvalue().encode("utf-8"))
            elif hasattr(self.data, "read") and callable(self.data.read):
                if hasattr(self.data, "seekable") and self.data.seekable():
                    if isinstance(self.data, TextIOBase):
                        data = self.data.read()
                        file = io.BytesIO(data.encode("utf-8"))
                    else:
                        file = self.data
                else:
                    content = self.data.read()
                    if isinstance(content, str):
                        content = content.encode("utf-8")
                        file = io.BytesIO(content)
                    elif isinstance(content, bytes):
                        file = io.BytesIO(content)

            if not file:
                raise TypeError(
                    f"File {self.filename}: Expected bytes, string, or file-like object, got {type(self.data)}"
                )

            self.wb = openpyxl.load_workbook(file, data_only=True)
            self.properties = self.read_excel_properties(self.wb)
            self.sheet_list = self.wb.sheetnames
            self.active_sheet = self.wb.active.title
            return True
        except Exception as e:
            self.report.report_file_dropped(self.filename)
            self.report.warning(
                message="Error reading Excel file",
                context=f"Filename={self.filename}",
                exc=e,
            )
            return False

    @property
    def sheet_names(self) -> List[str]:
        return self.sheet_list

    @property
    def active_sheet_name(self) -> str:
        return self.active_sheet

    @property
    def workbook_properties(self) -> Dict[str, Any]:
        return self.properties

    def get_tables(self) -> List[ExcelTable]:
        results: List[ExcelTable] = []
        for sheet in self.sheet_list:
            results.append(self.get_table(sheet))
        return results

    def get_table(self, sheet_name: str) -> ExcelTable:
        sheet = self.wb[sheet_name]

        # Extract all rows from the sheet
        rows = [[cell.value for cell in row] for row in sheet.rows]

        # Find a potential header row
        header_row_idx = self.find_header_row(rows)

        # Find where the footer starts
        footer_start_idx = self.find_footer_start(rows, header_row_idx)

        # Extract metadata before the header
        header_metadata = self.extract_metadata(rows[:header_row_idx])

        # Extract footer metadata
        footer_metadata = {}
        if footer_start_idx < len(rows):
            footer_metadata = self.extract_metadata(rows[footer_start_idx:])

        # Combine metadata
        metadata = {}
        metadata.update(self.properties)

        # Add header metadata
        for key, value in header_metadata.items():
            if key not in metadata:
                metadata[key] = value
            else:
                metadata[f"{key}_1"] = value

        # Add footer metadata
        for key, value in footer_metadata.items():
            if key not in metadata:
                metadata[key] = value
            else:
                metadata[f"{key}_1"] = value

        # Get the header row
        header_row = rows[header_row_idx]

        # Find the last non-empty column in the header row
        last_non_empty_idx = -1
        for i in range(len(header_row) - 1, -1, -1):
            if header_row[i] is not None and str(header_row[i]).strip() != "":
                last_non_empty_idx = i
                break

        # Truncate the header row to remove empty trailing columns
        if last_non_empty_idx >= 0:
            header_row = header_row[: last_non_empty_idx + 1]

        # Create the column names for the DataFrame
        column_names: List[str] = []
        seen_columns: Dict[str, int] = {}
        for i, col in enumerate(header_row):
            if col is None or str(col).strip() == "":
                col_name = f"Unnamed_{i}"
            else:
                col_name = str(col).strip()

            if col_name in seen_columns:
                seen_columns[col_name] += 1
                col_name = f"{col_name}_{seen_columns[col_name]}"
            else:
                seen_columns[col_name] = 0

            column_names.append(col_name)

        # Create the DataFrame with the table data
        data_rows = rows[header_row_idx + 1 : footer_start_idx]

        # Truncate data rows to match the header length
        truncated_data_rows = [
            row[: len(column_names)] if len(row) > len(column_names) else row
            for row in data_rows
        ]

        # Create the final DataFrame
        df = pd.DataFrame(truncated_data_rows, columns=column_names)

        row_count = df.shape[0]
        column_count = df.shape[1]

        return ExcelTable(
            df,
            header_row_idx + 1,
            footer_start_idx,
            row_count,
            column_count,
            metadata,
            sheet.title,
        )

    @staticmethod
    def find_header_row(rows: List[List[Any]]) -> int:
        max_score = -1
        header_idx = 0

        # Skip empty rows at the beginning
        start_idx = 0
        for i, row in enumerate(rows):
            if any(cell is not None and str(cell).strip() != "" for cell in row):
                start_idx = i
                break

        # Evaluate each potential header row with a lookahead
        for i in range(start_idx, len(rows) - 3):
            current_row = rows[i]
            next_rows = rows[i + 1 : i + 4]

            # Skip empty rows
            if not any(
                cell is not None and str(cell).strip() != "" for cell in current_row
            ):
                continue

            score = 0

            # Check if this row has more non-empty cells than previous rows
            non_empty_current = sum(
                1
                for cell in current_row
                if cell is not None and str(cell).strip() != ""
            )
            if i > 0:
                non_empty_prev = sum(
                    1
                    for cell in rows[i - 1]
                    if cell is not None and str(cell).strip() != ""
                )
                if non_empty_current > non_empty_prev:
                    score += 2

            # Check for header-like text (capitalized words, no numbers)
            header_like_cells = sum(
                1
                for cell in current_row
                if cell is not None
                and isinstance(cell, str)
                and re.match(r"^[A-Z][a-zA-Z\s]*$", str(cell).strip())
            )
            score += header_like_cells

            # Check for the pattern: header row (text) followed by data rows (mixed/numeric)
            header_text_count = sum(
                1 for cell in current_row if cell is not None and isinstance(cell, str)
            )
            next_rows_numeric_count = [
                sum(
                    1
                    for cell in row
                    if cell is not None
                    and (
                        isinstance(cell, (int, float))
                        or (
                            isinstance(cell, str)
                            and re.match(r"^-?\d+(\.\d+)?$", str(cell).strip())
                        )
                    )
                )
                for row in next_rows
            ]

            if header_text_count > 0 and all(
                count > 0 for count in next_rows_numeric_count
            ):
                score += 5

            # Check column type consistency in the rows after the potential header
            if len(next_rows) >= 3:
                col_types = []
                for col_idx in range(len(current_row)):
                    if col_idx < len(current_row) and current_row[col_idx] is not None:
                        col_type_counter: Counter = Counter()
                        for row in next_rows:
                            if col_idx < len(row) and row[col_idx] is not None:
                                cell_type = type(row[col_idx]).__name__
                                col_type_counter[cell_type] += 1

                        # Column has consistent types in the following rows
                        if (
                            col_type_counter
                            and col_type_counter.most_common(1)[0][1] >= 2
                        ):
                            col_types.append(col_type_counter.most_common(1)[0][0])

                if (
                    len(col_types) >= 2 and len(set(col_types)) >= 2
                ):  # At least 2 different column types
                    score += 3

            if score > max_score:
                max_score = score
                header_idx = i

        return header_idx

    @staticmethod
    def find_footer_start(rows: List[List[Any]], header_row_idx: int) -> int:
        if header_row_idx + 1 >= len(rows):
            return len(rows)

        # Start with the assumption that all rows after the header are data (no footer)
        footer_start_idx = len(rows)

        # Get the number of columns in the header row to determine table width
        header_row = rows[header_row_idx]
        table_width = sum(
            1 for cell in header_row if cell is not None and str(cell).strip() != ""
        )

        # Get a sample of data rows to establish patterns
        data_sample_idx = min(header_row_idx + 5, len(rows) - 1)
        data_rows = rows[header_row_idx + 1 : data_sample_idx + 1]

        # Check for rows with significantly fewer populated cells than the data rows
        avg_populated_cells = sum(
            sum(1 for cell in row if cell is not None and str(cell).strip() != "")
            for row in data_rows
        ) / len(data_rows)

        # Look for pattern breaks, empty rows, or format changes
        for i in range(header_row_idx + 1, len(rows)):
            current_row = rows[i]

            # Skip completely empty rows unless followed by non-data-like rows
            if not any(
                cell is not None and str(cell).strip() != "" for cell in current_row
            ):
                # Look ahead to see if this empty row marks the start of footer
                if i + 1 < len(rows):
                    next_row = rows[i + 1]
                    next_row_populated = sum(
                        1
                        for cell in next_row
                        if cell is not None and str(cell).strip() != ""
                    )

                    # If the next row has significantly fewer populated cells or is text-heavy,
                    # consider this the start of footer
                    if (
                        next_row_populated < avg_populated_cells * 0.5
                        or sum(
                            1
                            for cell in next_row
                            if cell is not None
                            and isinstance(cell, str)
                            and len(str(cell)) > 20
                        )
                        > 0
                    ):
                        footer_start_idx = i
                        break
                continue

            # Count populated cells
            populated_cells = sum(
                1
                for cell in current_row
                if cell is not None and str(cell).strip() != ""
            )

            # Check for footer indicators
            footer_indicators = [
                "total",
                "sum",
                "average",
                "mean",
                "source",
                "note",
                "footnote",
            ]
            has_footer_text = any(
                cell is not None
                and isinstance(cell, str)
                and any(
                    indicator in str(cell).lower() for indicator in footer_indicators
                )
                for cell in current_row
            )

            # Check for the summary row
            looks_like_summary = has_footer_text and populated_cells <= table_width

            # Check for notes or sources (often longer text spanning multiple columns)
            long_text_cells = sum(
                1
                for cell in current_row
                if cell is not None and isinstance(cell, str) and len(str(cell)) > 50
            )

            # If this looks like the start of the footer, mark it
            if (
                (populated_cells < avg_populated_cells * 0.7 and i > header_row_idx + 3)
                or looks_like_summary
                or long_text_cells > 0
            ):
                footer_start_idx = i
                break

            # Check for inconsistent data types compared to data rows
            if i > header_row_idx + 3:
                data_type_mismatch = 0
                for j, cell in enumerate(current_row):
                    if j < len(header_row) and header_row[j] is not None:
                        # Get the most common data type for this column in previous rows
                        col_types = [
                            type(rows[idx][j]).__name__
                            for idx in range(header_row_idx + 1, i)
                            if idx < len(rows)
                            and j < len(rows[idx])
                            and rows[idx][j] is not None
                        ]
                        if col_types and cell is not None:
                            most_common_type = Counter(col_types).most_common(1)[0][0]
                            if type(cell).__name__ != most_common_type:
                                data_type_mismatch += 1

                # If many columns have type mismatches, this might be a footer row
                if data_type_mismatch > table_width * 0.5:
                    footer_start_idx = i
                    break

        return footer_start_idx

    @staticmethod
    def extract_metadata(rows: List[List[Any]]) -> Dict[str, Any]:
        metadata = {}

        for row in rows:
            if len(row) >= 2 and all(item is None for item in row[2:]):
                key, value = row[:2]
                if key is not None and value is not None:
                    metadata[str(key).strip().rstrip(":=").rstrip()] = str(
                        value
                    ).strip()

        return metadata

    @staticmethod
    def read_excel_properties(wb: Workbook) -> Dict[str, Any]:
        # Core properties from DocumentProperties
        core_props = wb.properties
        properties = {
            "title": core_props.title,
            "author": core_props.creator,
            "subject": core_props.subject,
            "description": core_props.description,
            "keywords": core_props.keywords,
            "category": core_props.category,
            "last_modified_by": core_props.lastModifiedBy,
            "created": core_props.created,
            "modified": core_props.modified,
            "status": core_props.contentStatus,
            "revision": core_props.revision,
            "version": core_props.version,
            "language": core_props.language,
            "identifier": core_props.identifier,
        }

        # Remove None values
        properties = {k: v for k, v in properties.items() if v is not None}

        # Assign custom properties if they exist
        if hasattr(wb, "custom_doc_props"):
            for prop in wb.custom_doc_props.props:
                if prop.value:
                    if prop.name in properties:
                        prop_name = f"custom.{prop.name}"
                    else:
                        prop_name = prop.name
                    properties[prop_name] = prop.value

        return properties
