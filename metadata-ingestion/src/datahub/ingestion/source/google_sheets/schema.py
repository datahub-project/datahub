import string
from typing import Any, List, Optional

from datahub.emitter.mce_builder import make_data_platform_urn
from datahub.ingestion.source.google_sheets.constants import (
    PLATFORM_NAME,
    TYPE_INFERENCE_SAMPLE_SIZE,
)
from datahub.ingestion.source.google_sheets.models import (
    SheetData,
    parse_values_response,
)
from datahub.ingestion.source.google_sheets.utils import infer_column_type
from datahub.metadata.schema_classes import (
    OtherSchemaClass,
    SchemaFieldClass,
    SchemaFieldDataTypeClass,
    SchemaMetadataClass,
)


class SchemaExtractor:
    def __init__(
        self,
        sheets_service: Any,
        sheets_as_datasets: bool,
        header_detection_mode: str = "first_row",
        header_row_index: Optional[int] = None,
        skip_empty_leading_rows: bool = True,
    ):
        self.sheets_service = sheets_service
        self.sheets_as_datasets = sheets_as_datasets
        self.header_detection_mode = header_detection_mode
        self.header_row_index = header_row_index
        self.skip_empty_leading_rows = skip_empty_leading_rows

    def get_schema(
        self,
        sheet_id: str,
        sheet_data: SheetData,
        sheet_name: Optional[str] = None,
    ) -> SchemaMetadataClass:
        """Extract schema from Google Sheet.

        Args:
            sheet_id: The spreadsheet ID
            sheet_data: The spreadsheet metadata (Pydantic model)
            sheet_name: If provided, only extract schema for this specific sheet (for sheets_as_datasets mode)
        """
        fields = []

        sheets = sheet_data.spreadsheet.sheets
        if sheet_name:
            sheets = [s for s in sheets if s.properties.title == sheet_name]

        for sheet in sheets:
            current_sheet_name = sheet.properties.title

            range_name = f"{current_sheet_name}!1:{TYPE_INFERENCE_SAMPLE_SIZE}"
            try:
                result = (
                    self.sheets_service.spreadsheets()
                    .values()
                    .get(spreadsheetId=sheet_id, range=range_name)
                    .execute()
                )
                values_response = parse_values_response(result)

                if not values_response.values:
                    continue

                header_row_idx = self._find_header_row(values_response.values)

                if header_row_idx == -1:
                    headers = [
                        self._generate_column_letter_name(i)
                        for i in range(
                            len(values_response.values[0])
                            if values_response.values
                            else 0
                        )
                    ]
                    data_rows = values_response.values
                else:
                    headers = values_response.values[header_row_idx]
                    data_rows = (
                        values_response.values[header_row_idx + 1 :]
                        if header_row_idx + 1 < len(values_response.values)
                        else []
                    )

                for i, header in enumerate(headers):
                    if not header:
                        header = self._generate_column_letter_name(i)

                    if self.sheets_as_datasets:
                        field_name = header
                    else:
                        field_name = f"{current_sheet_name}.{header}"

                    inferred = infer_column_type(data_rows, i)

                    fields.append(
                        SchemaFieldClass(
                            fieldPath=field_name,
                            type=SchemaFieldDataTypeClass(type=inferred.data_type),
                            nativeDataType=inferred.native_type,
                        )
                    )
            except Exception:
                # Schema extraction failures are logged by caller
                continue

        schema_name = (
            sheet_name if sheet_name else sheet_data.spreadsheet.properties.title
        )
        schema_metadata = SchemaMetadataClass(
            schemaName=schema_name,
            platform=make_data_platform_urn(PLATFORM_NAME),
            version=0,
            fields=fields,
            hash="",
            platformSchema=OtherSchemaClass(rawSchema=""),
        )

        return schema_metadata

    def _find_header_row(self, values: List[List[str]]) -> int:
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
        best_score = -1.0
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
                next_row_types_differ = False
                for _j, (current_cell, next_cell) in enumerate(
                    zip(row[: len(next_row)], next_row, strict=False)
                ):
                    if current_cell and next_cell:
                        current_is_text = (
                            not str(current_cell)
                            .replace(".", "")
                            .replace("-", "")
                            .isdigit()
                        )
                        next_is_text = (
                            not str(next_cell)
                            .replace(".", "")
                            .replace("-", "")
                            .isdigit()
                        )
                        if current_is_text != next_is_text:
                            next_row_types_differ = True
                            break

                if next_row_types_differ:
                    score += 10

        return score

    def _generate_column_letter_name(self, index: int) -> str:
        """Generate Excel-style column letter (A, B, ..., Z, AA, AB, ...)."""
        result = ""
        while index >= 0:
            result = string.ascii_uppercase[index % 26] + result
            index = index // 26 - 1
        return result
